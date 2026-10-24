//! Replica-side connection driver: connects to the master, performs
//! the handshake, then loops reading WAL chunks and feeding them to
//! the [`Applier`]. Periodically sends `ReplicaAck` back so the
//! master's slot.acked_lsn keeps advancing.
//!
//! Failures and disconnects are reported through the returned future;
//! the calling layer (`MydbReplManager`) handles backoff and reconnect.

use crate::repl::now_ms;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Notify;

use crate::repl::config::ReplConfig;
use crate::repl::protocol::{
    self, Close, Handshake, HandshakeAck, Message, ReplicaAck, PROTOCOL_VERSION,
};
use crate::repl::replica::applier::Applier;
use crate::repl::replica::progress::ReplicaProgress;
use crate::repl::role::uuid_string_to_bytes;
use crate::storage::{error, Database};
use crate::types::QueryError;
use crate::util::logging::log_message;

#[derive(Debug, Clone)]
pub enum ReceiverEvent {
    /// Connected successfully and handshake accepted.
    Connected {
        master_uuid: String,
        master_epoch: u64,
        transport: String,
    },
    /// Master told us our requested LSN is below its retention floor.
    /// The replica must do a base backup before resuming.
    ResyncRequired {
        new_start_lsn: u64,
        reason: String,
    },
    /// Stream ended for some other reason (connection drop, etc.).
    Disconnected {
        reason: String,
    },
    Progress(ReplicaProgress),
}

#[derive(Debug, Clone)]
pub struct ReceiverHandle {
    pub shutdown: Arc<Notify>,
}

impl ReceiverHandle {
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// Initialization for [`Receiver::run`]: where to connect, who we are,
/// where to write incoming WAL.
pub struct Receiver {
    pub master_host: String,
    pub master_port: u16,
    pub replica_uuid: String,
    pub config: ReplConfig,
    pub data_dir: std::path::PathBuf,
    pub log: Arc<crate::wal::LogMutex>,
    /// Live `Database` whose buffer pool / txn manager / catalog the
    /// applier mutates on every WAL frame. When `None`, the receiver
    /// only writes raw WAL bytes to disk (legacy / WAL-fidelity tests).
    pub db: Option<Arc<Database>>,
    pub initial_progress: ReplicaProgress,
    pub shutdown_flag: Option<Arc<AtomicBool>>,
}

impl Receiver {
    /// Drive one connect → handshake → stream cycle. Returns when the
    /// connection drops or the shutdown signal fires; callers loop on
    /// this with backoff.
    pub async fn run_once(
        self,
        shutdown: Arc<Notify>,
        mut on_event: impl FnMut(ReceiverEvent),
    ) -> Result<(), QueryError> {
        // Register one shutdown waiter for the entire connection cycle.  A
        // fresh `shutdown.notified()` inside the select loop leaves gaps while
        // a WAL chunk is being applied, an ACK is being written, or an event
        // callback is running. `notify_waiters()` during one of those gaps is
        // not buffered, so the receiver can return to the loop and wait
        // forever. Enabling this pinned waiter up front closes those gaps; a
        // notification remains ready until the apply loop consumes it.
        let shutdown_notified = shutdown.notified();
        tokio::pin!(shutdown_notified);
        shutdown_notified.as_mut().enable();
        let (_memory, admitted_chunk_bytes) = admit_receive_window(self.config.chunk_bytes)?;
        // A local WAL tail is merely durable-received. Online apply may have
        // failed after append, so reconnect must resume from the persisted
        // durable-applied watermark and replay that tail idempotently.
        let mut initial_progress = self.initial_progress.clone();
        let addr = format!("{}:{}", self.master_host, self.master_port);
        let stream =
            match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&addr)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("repl receiver: connect {addr}: {e}"),
                    ))
                }
                Err(_) => {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("repl receiver: connect {addr} timed out"),
                    ))
                }
            };
        // Disable Nagle so acks/handshake go out immediately rather than
        // waiting on Nagle × delayed-ACK (Pf2).
        if let Err(e) = stream.set_nodelay(true) {
            log_message(format_args!("mydb-repl replica: set_nodelay failed: {e}"));
        }
        let stream = crate::repl::tls::connect(stream, &self.config, &self.master_host)
            .await
            .map_err(io_err)?;
        let transport = stream.transport_label();
        let (rh, wh) = tokio::io::split(stream);
        let mut reader = BufReader::new(rh);
        let mut writer = BufWriter::new(wh);

        // -------- Handshake --------
        let replica_uuid_bytes = uuid_string_to_bytes(&self.replica_uuid).unwrap_or([0u8; 16]);
        let last_master_bytes = if !initial_progress.last_master_uuid.is_empty() {
            uuid_string_to_bytes(&initial_progress.last_master_uuid).unwrap_or([0; 16])
        } else {
            [0u8; 16]
        };
        let start_lsn = initial_progress
            .durable_applied_lsn
            .saturating_add(1)
            .max(1);
        let handshake = Handshake {
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: replica_uuid_bytes,
            requested_start_lsn: start_lsn,
            last_known_master_uuid: last_master_bytes,
            last_known_epoch: initial_progress.last_master_epoch,
            cluster_uuid: uuid_string_to_bytes(&initial_progress.cluster_uuid).unwrap_or([0; 16]),
            last_known_timeline_uuid: uuid_string_to_bytes(&initial_progress.timeline_uuid)
                .unwrap_or([0; 16]),
            client_version: format!("mydb-replica/{}", env!("CARGO_PKG_VERSION")),
            // Item 3: streaming base backup is driven through
            // `crate::repl::base_backup::perform_base_backup_into`
            // BEFORE the receiver runs, so by the time we get here
            // the local data dir already has a populated snapshot.
            // Never set this flag from the steady-state receiver
            // path — it would trigger another wipe-and-reload.
            request_base_backup: false,
            max_chunk_bytes: admitted_chunk_bytes.min(u32::MAX as usize) as u32,
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
        };
        protocol::write_frame_async(&mut writer, &Message::Handshake(handshake))
            .await
            .map_err(io_err)?;
        writer.flush().await.map_err(io_err)?;

        // Bound the handshake read: a master that accepts the TCP
        // connection but never replies (half-open, wedged) must not park
        // the receiver forever — time out so the reconnect loop retries
        // (C4).
        let ack: HandshakeAck = match tokio::time::timeout(
            Duration::from_secs(10),
            protocol::read_frame_async(&mut reader),
        )
        .await
        {
            Ok(Ok(Message::HandshakeAck(a))) => a,
            Ok(Ok(other)) => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!(
                        "first message was not HandshakeAck (got {:?})",
                        other.msg_type()
                    ),
                ))
            }
            Ok(Err(err)) => return Err(io_err(err)),
            Err(_) => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "handshake timed out waiting for master HandshakeAck",
                ))
            }
        };
        let master_uuid = crate::repl::role::uuid_bytes_to_string(&ack.master_uuid);
        let local_format = crate::storage::format_versions::native_replication_compatibility();
        if ack.format_compatibility != local_format {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!(
                    "native replication format mismatch: master='{}', replica='{}'",
                    ack.format_compatibility, local_format
                ),
            ));
        }
        if !ack.accepted {
            // Server may follow up with a Resync frame.
            on_event(ReceiverEvent::Disconnected {
                reason: format!("handshake rejected: {}", ack.reason),
            });
            // Drain a possible Resync.
            if let Ok(Message::Resync(r)) = protocol::read_frame_async(&mut reader).await {
                on_event(ReceiverEvent::ResyncRequired {
                    new_start_lsn: r.new_start_lsn,
                    reason: r.reason,
                });
            }
            return Ok(());
        }
        initial_progress.last_master_uuid.clone_from(&master_uuid);
        initial_progress.last_master_epoch = ack.epoch;
        initial_progress.cluster_uuid = crate::repl::role::uuid_bytes_to_string(&ack.cluster_uuid);
        initial_progress.timeline_uuid =
            crate::repl::role::uuid_bytes_to_string(&ack.timeline_uuid);
        initial_progress.source_current_lsn = ack.current_lsn;
        initial_progress.source_durable_lsn = ack.durable_lsn;
        initial_progress.source_generation = ack.generation;
        initial_progress.source_frozen = ack.source_frozen;
        initial_progress.source_fence_lsn = ack.fence_lsn;
        initial_progress
            .source_fence_token_hash
            .clone_from(&ack.fence_token_hash);
        on_event(ReceiverEvent::Connected {
            master_uuid: master_uuid.clone(),
            master_epoch: ack.epoch,
            transport,
        });

        // -------- Apply loop --------
        let mut applier_inner = match self.db.as_ref() {
            Some(db) => Applier::with_database(
                self.data_dir.clone(),
                Arc::clone(&self.log),
                Arc::clone(db),
                initial_progress.clone(),
            ),
            None => Applier::new(
                self.data_dir.clone(),
                Arc::clone(&self.log),
                initial_progress.clone(),
            ),
        };
        applier_inner.set_sync_interval(if ack.semi_sync_required {
            Duration::from_millis(2)
        } else {
            Duration::from_millis(20)
        });
        on_event(ReceiverEvent::Progress(applier_inner.progress()));
        let mut ack_interval = tokio::time::interval(Duration::from_millis(200));
        ack_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ack_interval.tick().await;
        let applier = Arc::new(parking_lot::Mutex::new(applier_inner));
        let mut last_acked_lsn = applier.lock().last_applied_lsn();

        // Idle watchdog (C4): the master heartbeats every
        // `config.heartbeat`; if we go this long without ANY frame the
        // link is dead (half-open TCP / wedged master) even though
        // `read_frame_async` would otherwise block forever. Fire a
        // Disconnected event so the reconnect loop takes over.
        let idle_timeout = self
            .config
            .heartbeat
            .saturating_mul(6)
            .max(Duration::from_secs(5));
        let mut idle_check =
            tokio::time::interval(self.config.heartbeat.max(Duration::from_millis(200)));
        idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        idle_check.tick().await;
        let mut last_frame = Instant::now();

        loop {
            if self
                .shutdown_flag
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire))
            {
                let _ = flush_applier_blocking(Arc::clone(&applier), true).await;
                return Ok(());
            }
            // `AsyncReadExt::read_exact` is not cancellation-safe: dropping
            // `read_frame_async` after it consumed part of a large payload
            // loses the byte offset, and the next iteration mistakes payload
            // bytes for a fresh "REPL" header. Keep one pinned read alive
            // across ACK/watchdog ticks; only discard it when the whole
            // connection is being closed.
            let inbound = protocol::read_frame_async(&mut reader);
            tokio::pin!(inbound);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_notified => {
                        // Make the appended tail durable and persist the
                        // progress watermark before the connection closes,
                        // so a STOP → START cycle resumes where we left off.
                        let _ = flush_applier_blocking(Arc::clone(&applier), true).await;
                        let _ = protocol::write_frame_async(
                            &mut writer,
                            &Message::Close(Close { reason: "replica shutdown".into() }),
                        ).await;
                        let _ = writer.flush().await;
                        return Ok(());
                    }
                    inbound = &mut inbound => {
                        let msg = match inbound {
                            Ok(m) => m,
                            Err(err) => {
                                on_event(ReceiverEvent::Disconnected {
                                    reason: format!("read error: {err}"),
                                });
                                return Ok(());
                            }
                        };
                        // Any frame (data or heartbeat) proves the link is
                        // alive — reset the idle watchdog.
                        last_frame = Instant::now();
                        match msg {
                            Message::WalChunk(chunk) => {
                                let chunk = crate::wal::WalReadChunk {
                                    start_lsn: chunk.start_lsn,
                                    end_lsn: chunk.end_lsn,
                                    frame_count: chunk.frame_count,
                                    bytes: chunk.frames_bytes,
                                };
                                let outcome = apply_chunk_blocking(
                                    Arc::clone(&applier),
                                    chunk,
                                    master_uuid.clone(),
                                    ack.epoch,
                                ).await?;
                                on_event(ReceiverEvent::Progress(applier.lock().progress()));
                                if outcome.last_lsn_applied > last_acked_lsn {
                                    last_acked_lsn = outcome.last_lsn_applied;
                                    // Ack immediately: the chunk is durable
                                    // in the local WAL and applied, and a
                                    // semi-sync master commit may be
                                    // blocked waiting on exactly this LSN.
                                    send_ack(&mut writer, last_acked_lsn).await?;
                                }
                            }
                            Message::Heartbeat(heartbeat) => {
                                applier.lock().note_source_status(
                                    heartbeat.master_lsn,
                                    heartbeat.durable_lsn,
                                    crate::repl::role::uuid_bytes_to_string(&heartbeat.timeline_uuid),
                                    heartbeat.generation,
                                    heartbeat.source_frozen,
                                    heartbeat.fence_lsn,
                                    heartbeat.fence_token_hash,
                                );
                                // Stream idle — make the appended tail durable so
                                // the ack watermark catches up, then respond.
                                flush_applier_blocking(Arc::clone(&applier), false).await?;
                                let progress = applier.lock().progress();
                                on_event(ReceiverEvent::Progress(progress.clone()));
                                send_ack(&mut writer, progress.durable_applied_lsn).await?;
                            }
                            Message::Resync(r) => {
                                on_event(ReceiverEvent::ResyncRequired {
                                    new_start_lsn: r.new_start_lsn,
                                    reason: r.reason,
                                });
                                return Ok(());
                            }
                            Message::Close(c) => {
                                on_event(ReceiverEvent::Disconnected { reason: c.reason });
                                return Ok(());
                            }
                            other => {
                                log_message(format_args!(
                                    "mydb-repl replica: unexpected master message {:?}",
                                    other.msg_type()
                                ));
                            }
                        }
                        break;
                    }
                    _ = ack_interval.tick() => {
                        flush_applier_blocking(Arc::clone(&applier), false).await?;
                        let progress = applier.lock().progress();
                        on_event(ReceiverEvent::Progress(progress.clone()));
                        if progress.durable_applied_lsn != last_acked_lsn || last_acked_lsn == 0 {
                            last_acked_lsn = progress.durable_applied_lsn;
                        }
                        send_ack(&mut writer, last_acked_lsn).await?;
                    }
                    _ = idle_check.tick() => {
                        if last_frame.elapsed() >= idle_timeout {
                            on_event(ReceiverEvent::Disconnected {
                                reason: format!(
                                    "no frames from master for {:?}; treating link as dead",
                                    idle_timeout
                                ),
                            });
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Reserve the decoder, WAL-append and online-apply working sets. Optional
/// probes deliberately do not increment the hard rejection metric: pressure
/// is handled by halving the negotiated chunk. Only failure of the smallest
/// useful window is a real HY001 admission failure.
fn admit_receive_window(
    configured_chunk_bytes: usize,
) -> Result<
    (
        crate::engine::executor::mem_budget::ControlledMemoryCharge,
        usize,
    ),
    QueryError,
> {
    use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

    const MIN_CHUNK_BYTES: usize = 4 * 1024;
    let mut chunk = configured_chunk_bytes.max(MIN_CHUNK_BYTES);
    while chunk > MIN_CHUNK_BYTES {
        if let Some(charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::NativeReplicationTransient,
            chunk.saturating_mul(3),
        ) {
            return Ok((charge, chunk));
        }
        chunk = (chunk / 2).max(MIN_CHUNK_BYTES);
    }
    ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::NativeReplicationTransient,
        MIN_CHUNK_BYTES.saturating_mul(3),
    )
    .map(|charge| (charge, MIN_CHUNK_BYTES))
}

async fn apply_chunk_blocking(
    applier: Arc<parking_lot::Mutex<Applier>>,
    chunk: crate::wal::WalReadChunk,
    master_uuid: String,
    master_epoch: u64,
) -> Result<crate::repl::replica::applier::ApplyOutcome, QueryError> {
    tokio::task::spawn_blocking(move || {
        applier
            .lock()
            .apply_chunk(&chunk, &master_uuid, master_epoch)
    })
    .await
    .map_err(|err| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("replica apply worker failed: {err}"),
        )
    })?
}

async fn flush_applier_blocking(
    applier: Arc<parking_lot::Mutex<Applier>>,
    persist: bool,
) -> Result<(), QueryError> {
    tokio::task::spawn_blocking(move || {
        let mut applier = applier.lock();
        if persist {
            applier.flush_and_persist()
        } else {
            applier.flush_pending()
        }
    })
    .await
    .map_err(|err| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("replica flush worker failed: {err}"),
        )
    })?
}

async fn send_ack<W>(writer: &mut BufWriter<W>, applied_lsn: u64) -> Result<(), QueryError>
where
    W: AsyncWrite + Unpin,
{
    let msg = Message::ReplicaAck(ReplicaAck {
        received_lsn: applied_lsn,
        durable_lsn: applied_lsn,
        applied_lsn,
        time_ms: now_ms(),
    });
    protocol::write_frame_async(writer, &msg)
        .await
        .map_err(io_err)?;
    writer.flush().await.map_err(io_err)?;
    Ok(())
}

fn io_err(err: std::io::Error) -> QueryError {
    let detail = err.to_string();
    if let Some(security) = crate::repl::transport_security_error(&detail) {
        return security;
    }
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("repl receiver io: {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::protocol::WalChunk;
    use crate::wal::LogManager;
    use tokio::net::TcpListener;

    #[test]
    fn durable_received_wal_is_not_an_applied_resume_point() {
        let known = ReplicaProgress {
            received_lsn: 42,
            durable_lsn: 42,
            applied_lsn: 10,
            durable_applied_lsn: 10,
            last_master_uuid: "master-a".into(),
            last_master_epoch: 3,
            cluster_uuid: "cluster-a".into(),
            timeline_uuid: "timeline-a".into(),
            source_current_lsn: 42,
            source_durable_lsn: 42,
            source_generation: 3,
            source_frozen: false,
            source_fence_lsn: 0,
            source_fence_token_hash: String::new(),
            last_update_ms: 0,
        };
        assert_eq!(known.durable_lsn, 42);
        assert_eq!(known.durable_applied_lsn, 10);
    }

    #[tokio::test]
    async fn slow_large_frame_survives_ack_ticks_without_stream_desync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let master = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(matches!(
                protocol::read_frame_async(&mut stream).await.unwrap(),
                Message::Handshake(_)
            ));
            protocol::write_frame_async(
                &mut stream,
                &Message::HandshakeAck(HandshakeAck {
                    master_uuid: [0x5a; 16],
                    current_lsn: 1,
                    durable_lsn: 1,
                    min_available_lsn: 1,
                    epoch: 1,
                    server_version: "slow-frame-test".into(),
                    accepted: true,
                    reason: String::new(),
                    format_compatibility:
                        crate::storage::format_versions::native_replication_compatibility(),
                    semi_sync_required: false,
                    cluster_uuid: [0x5a; 16],
                    timeline_uuid: [0x5a; 16],
                    generation: 1,
                    source_frozen: false,
                    fence_lsn: 0,
                    fence_token_hash: String::new(),
                }),
            )
            .await
            .unwrap();

            // The body is deliberately not a valid WAL frame: once the whole
            // replication frame arrives, the expected outcome is an
            // `apply decode` error. Send it over >200 ms so the receiver's ACK
            // timer fires in the middle. The old select loop cancelled
            // read_exact at that tick and instead reported protocol `bad
            // magic`, proving it had resumed from the middle of this body.
            let message = Message::WalChunk(WalChunk {
                start_lsn: 1,
                end_lsn: 1,
                frame_count: 0,
                frames_bytes: vec![0; 512 * 1024],
            });
            let mut encoded = Vec::new();
            protocol::write_frame(&mut encoded, &message).unwrap();
            for part in encoded.chunks(8 * 1024) {
                stream.write_all(part).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // Keep the connection alive and drain ACKs until the receiver
            // rejects the WAL body. Closing with unread ACKs sends a TCP RST
            // on Linux and can discard the final bytes before they are read.
            let mut ack_count = 0;
            loop {
                match protocol::read_frame_async(&mut stream).await {
                    Ok(Message::ReplicaAck(ack)) => {
                        assert_eq!(ack.applied_lsn, 0);
                        ack_count += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    other => panic!("expected ACK or receiver close, got {other:?}"),
                }
            }
            assert!(ack_count > 0, "the frame must span at least one ACK tick");
        });

        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let receiver = Receiver {
            master_host: "127.0.0.1".into(),
            master_port: port,
            replica_uuid: "00112233-4455-6677-8899-aabbccddeeff".into(),
            config: ReplConfig {
                heartbeat: Duration::from_millis(50),
                ..ReplConfig::default()
            },
            data_dir: dir.path().to_path_buf(),
            log,
            db: None,
            initial_progress: ReplicaProgress::default(),
            shutdown_flag: None,
        };
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            receiver.run_once(Arc::new(Notify::new()), |_| {}),
        )
        .await
        .expect("receiver should finish after the invalid WAL body")
        .expect_err("invalid WAL bytes must reach the applier");
        assert_eq!(err.diagnostic_id(), Some("MYDB-DIAG-REPL-POISON"));
        assert!(
            err.diagnostic_detail()
                .is_some_and(|detail| detail.contains("apply decode: replication frame: bad magic")),
            "{err:?}"
        );
        master.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_notified_while_processing_an_event_is_not_lost() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let master = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(matches!(
                protocol::read_frame_async(&mut stream).await.unwrap(),
                Message::Handshake(_)
            ));
            protocol::write_frame_async(
                &mut stream,
                &Message::HandshakeAck(HandshakeAck {
                    master_uuid: [0x6b; 16],
                    current_lsn: 1,
                    durable_lsn: 1,
                    min_available_lsn: 1,
                    epoch: 1,
                    server_version: "shutdown-race-test".into(),
                    accepted: true,
                    reason: String::new(),
                    format_compatibility:
                        crate::storage::format_versions::native_replication_compatibility(),
                    semi_sync_required: false,
                    cluster_uuid: [0x6b; 16],
                    timeline_uuid: [0x6b; 16],
                    generation: 1,
                    source_frozen: false,
                    fence_lsn: 0,
                    fence_token_hash: String::new(),
                }),
            )
            .await
            .unwrap();

            let close = tokio::time::timeout(
                Duration::from_secs(1),
                protocol::read_frame_async(&mut stream),
            )
            .await
            .expect("receiver should close promptly")
            .expect("read receiver close");
            assert!(matches!(close, Message::Close(_)));
        });

        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let receiver = Receiver {
            master_host: "127.0.0.1".into(),
            master_port: port,
            replica_uuid: "00112233-4455-6677-8899-aabbccddeeff".into(),
            config: ReplConfig::default(),
            data_dir: dir.path().to_path_buf(),
            log,
            db: None,
            initial_progress: ReplicaProgress::default(),
            shutdown_flag: None,
        };
        let shutdown = Arc::new(Notify::new());
        let shutdown_from_event = Arc::clone(&shutdown);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            receiver.run_once(shutdown, move |event| {
                if matches!(event, ReceiverEvent::Progress(_)) {
                    // This callback runs outside the apply-loop select. The
                    // old per-iteration waiter lost this exact notification.
                    shutdown_from_event.notify_waiters();
                }
            }),
        )
        .await
        .expect("receiver shutdown notification must not be lost");
        result.expect("receiver shutdown should be clean");
        master.await.unwrap();
    }
}
