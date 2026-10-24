//! Per-replica WAL sender task. One instance per connected replica.
//!
//! Lifecycle:
//! 1. Handshake — validate replica's `requested_start_lsn` against the
//!    master's available WAL range.
//! 2. Stream loop — read raw WAL frame bytes via [`WalReader`] and
//!    forward them as [`Message::WalChunk`]. Heartbeat when there's
//!    nothing to send.
//! 3. Inbound — drain `ReplicaAck` and update the slot's `acked_lsn`.

use crate::repl::now_ms;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex as PlMutex;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

use crate::repl::config::ReplConfig;
use crate::repl::master::slot::SlotHandle;
use crate::repl::protocol::{
    self, Close, Handshake, HandshakeAck, Heartbeat, Message, Resync, WalChunk, PROTOCOL_VERSION,
};
use crate::storage::{error, Database};
use crate::types::QueryError;
use crate::wal::{WalReader, WalReaderError};

/// Parameters baked at construction; everything the sender needs that's
/// not in the per-replica handshake or in shared state.
#[derive(Clone)]
pub struct SenderEnv {
    pub log: Arc<crate::wal::LogMutex>,
    pub config: ReplConfig,
    pub master_uuid: [u8; 16],
    pub epoch: u64,
    pub server_version: String,
    /// Slot registry, so the sender can durably persist `acked_lsn`
    /// as the replica advances (C1) rather than leaving it only in
    /// memory (which vanished on master crash and let a stale/zero
    /// on-disk `acked_lsn` corrupt WAL retention after restart).
    pub slots: Arc<PlMutex<crate::repl::master::slot::SlotRegistry>>,
    /// Optional live `Database` + data dir, used by the Item 3
    /// streaming base backup path. `None` means "no base backup
    /// support" — the master refuses any handshake whose
    /// `request_base_backup` is true or whose start LSN falls
    /// below retention. Tests that only exercise WAL streaming
    /// leave this `None`; the production service in
    /// `MydbReplManager` populates it.
    pub db: Option<Arc<Database>>,
    pub data_dir: Option<std::path::PathBuf>,
    /// Service-wide cancellation. DEMOTE and process shutdown signal this and
    /// await all sender tasks before touching the storage engine.
    pub shutdown: Arc<tokio::sync::Notify>,
    pub shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    pub role_state: Option<Arc<std::sync::Mutex<crate::repl::role::RoleState>>>,
    pub base_backup_slots: Arc<tokio::sync::Semaphore>,
}

/// Tracks running senders for graceful shutdown. Each running sender
/// holds a Drop guard that decrements the count; the service can poll
/// the count to know when all senders have stopped.
#[derive(Debug, Default)]
pub struct SenderTracker {
    inner: PlMutex<TrackerInner>,
}

#[derive(Debug, Default)]
struct TrackerInner {
    count: u64,
    by_replica: std::collections::HashMap<String, u64>,
}

impl SenderTracker {
    pub fn snapshot(&self) -> SenderTrackerSnapshot {
        let g = self.inner.lock();
        SenderTrackerSnapshot {
            count: g.count,
            replicas: g.by_replica.keys().cloned().collect(),
        }
    }

    fn try_enter(&self, replica_uuid: &str) -> bool {
        let mut g = self.inner.lock();
        if g.by_replica.contains_key(replica_uuid) {
            return false;
        }
        g.count += 1;
        *g.by_replica.entry(replica_uuid.to_string()).or_default() += 1;
        true
    }

    fn leave(&self, replica_uuid: &str) {
        let mut g = self.inner.lock();
        if g.count > 0 {
            g.count -= 1;
        }
        if let Some(v) = g.by_replica.get_mut(replica_uuid) {
            if *v <= 1 {
                g.by_replica.remove(replica_uuid);
            } else {
                *v -= 1;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SenderTrackerSnapshot {
    pub count: u64,
    pub replicas: Vec<String>,
}

/// Top-level sender. Splits the TCP stream, performs the handshake,
/// then drives the stream loop. Returns on EOF, error, or shutdown.
pub async fn run_sender(
    stream: crate::repl::tls::ReplStream,
    env: SenderEnv,
    slot_for: impl Fn(&str) -> SlotHandle,
    tracker: Arc<SenderTracker>,
) -> Result<(), QueryError> {
    if env.shutdown_flag.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(());
    }
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    // -------- Handshake --------
    let inbound = tokio::select! {
        _ = env.shutdown.notified() => return Ok(()),
        result = tokio::time::timeout(Duration::from_secs(10), protocol::read_frame_async(&mut reader)) => {
            result.map_err(|_| error(
                crate::error::catalog::ER_NET_READ_ERROR,
                "replication handshake timed out",
            ))?.map_err(io_err)?
        }
    };
    let req: Handshake = match inbound {
        Message::Handshake(h) => h,
        other => {
            let _ = protocol::write_frame_async(
                &mut writer,
                &Message::Close(Close {
                    reason: format!("expected Handshake, got {:?}", other.msg_type()),
                }),
            )
            .await;
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "first message was not Handshake",
            ));
        }
    };

    if req.protocol_version != PROTOCOL_VERSION {
        let source = source_wire_state(&env);
        let ack = HandshakeAck {
            master_uuid: env.master_uuid,
            current_lsn: 0,
            durable_lsn: 0,
            min_available_lsn: 0,
            epoch: env.epoch,
            server_version: env.server_version.clone(),
            accepted: false,
            reason: format!(
                "protocol version mismatch: replica={}, master={}",
                req.protocol_version, PROTOCOL_VERSION
            ),
            format_compatibility: crate::storage::format_versions::native_replication_compatibility(
            ),
            semi_sync_required: env.config.mode == crate::repl::config::ReplMode::SemiSync,
            cluster_uuid: source.cluster_uuid,
            timeline_uuid: source.timeline_uuid,
            generation: source.generation,
            source_frozen: source.frozen,
            fence_lsn: source.fence_lsn,
            fence_token_hash: source.fence_token_hash,
        };
        let _ = protocol::write_frame_async(&mut writer, &Message::HandshakeAck(ack)).await;
        let _ = writer.flush().await;
        return Ok(());
    }

    let (current_lsn, min_available_lsn) = {
        let log = env.log.lock();
        (log.next_lsn(), log.min_available_lsn())
    };
    let in_range =
        req.requested_start_lsn >= min_available_lsn && req.requested_start_lsn <= current_lsn;
    // Item 3: stream a base backup ONLY when the replica explicitly
    // asked for one. The old implicit trigger (requested LSN below the
    // retention floor) pushed BaseBackup* messages at a steady-state
    // receiver that doesn't handle them; that case now answers with a
    // Resync so the replica re-seeds through its bootstrap path.
    let backup_permit = if req.request_base_backup {
        Arc::clone(&env.base_backup_slots).try_acquire_owned().ok()
    } else {
        None
    };
    let needs_base_backup = req.request_base_backup
        && env.db.is_some()
        && env.data_dir.is_some()
        && backup_permit.is_some();

    let source = source_wire_state(&env);
    // Protocol v2 uses cluster/timeline ancestry rather than node UUID. A
    // planned promotion necessarily changes node UUID; rejecting that node
    // would force every healthy standby to re-seed after each switchover.
    let mut fork_reason: Option<String> = None;
    if req.cluster_uuid != [0u8; 16] && req.cluster_uuid != source.cluster_uuid {
        fork_reason = Some("cluster UUID mismatch".to_string());
    } else if req.last_known_timeline_uuid != [0u8; 16]
        && req.last_known_timeline_uuid != source.timeline_uuid
    {
        let follows_parent = source.parent_timeline_uuid == Some(req.last_known_timeline_uuid)
            && req.requested_start_lsn <= source.fork_lsn.saturating_add(1);
        if !follows_parent {
            fork_reason = Some(format!(
                "timeline diverged: replica={} master={} fork_lsn={}",
                crate::repl::role::uuid_bytes_to_string(&req.last_known_timeline_uuid),
                crate::repl::role::uuid_bytes_to_string(&source.timeline_uuid),
                source.fork_lsn
            ));
        }
    } else if req.last_known_master_uuid == env.master_uuid && req.last_known_epoch > env.epoch {
        fork_reason = Some(format!(
            "replica generation {} is ahead of source {}",
            req.last_known_epoch, env.epoch
        ));
    }

    let local_format = crate::storage::format_versions::native_replication_compatibility();
    if req.format_compatibility != local_format {
        fork_reason = Some(format!(
            "physical format mismatch: replica='{}', master='{}'",
            req.format_compatibility, local_format
        ));
    }
    let accepted = (if req.request_base_backup {
        needs_base_backup
    } else {
        in_range
    }) && fork_reason.is_none();
    let reason = if req.request_base_backup && backup_permit.is_none() {
        "base backup concurrency limit reached".to_string()
    } else if req.request_base_backup && !needs_base_backup {
        "base backup is unavailable on this source".to_string()
    } else if let Some(r) = fork_reason.clone() {
        r
    } else if accepted {
        String::new()
    } else {
        format!(
            "requested_start_lsn={} outside available range [{}, {}]",
            req.requested_start_lsn, min_available_lsn, current_lsn
        )
    };
    let ack = HandshakeAck {
        master_uuid: env.master_uuid,
        current_lsn,
        durable_lsn: env.log.lock().durable_lsn(),
        min_available_lsn,
        epoch: env.epoch,
        server_version: env.server_version.clone(),
        accepted,
        reason: reason.clone(),
        format_compatibility: local_format,
        semi_sync_required: env.config.mode == crate::repl::config::ReplMode::SemiSync,
        cluster_uuid: source.cluster_uuid,
        timeline_uuid: source.timeline_uuid,
        generation: source.generation,
        source_frozen: source.frozen,
        fence_lsn: source.fence_lsn,
        fence_token_hash: source.fence_token_hash,
    };
    protocol::write_frame_async(&mut writer, &Message::HandshakeAck(ack))
        .await
        .map_err(io_err)?;
    writer.flush().await.map_err(io_err)?;
    if !accepted {
        // Hint that a Resync (base backup) is needed when streaming can
        // never satisfy this replica: its LSN fell below the retention
        // floor, or it is AHEAD of us (it applied WAL a crashed master
        // lost — a divergence only a re-seed repairs).
        let resync_reason = if let Some(fr) = &fork_reason {
            // A fork can only be repaired by re-seeding from this
            // master's timeline; tell the replica explicitly instead of
            // making it parse `ack.reason` text (C13).
            Some(format!("{fr}; re-seed via base backup"))
        } else if req.requested_start_lsn < min_available_lsn {
            Some(format!(
                "requested LSN {} is below master's min_available_lsn {}",
                req.requested_start_lsn, min_available_lsn
            ))
        } else if req.requested_start_lsn > current_lsn {
            Some(format!(
                "requested LSN {} is ahead of master's next LSN {} (diverged timeline)",
                req.requested_start_lsn, current_lsn
            ))
        } else {
            None
        };
        if let Some(reason) = resync_reason {
            let _ = protocol::write_frame_async(
                &mut writer,
                &Message::Resync(Resync {
                    new_start_lsn: min_available_lsn,
                    reason,
                }),
            )
            .await;
            let _ = writer.flush().await;
        }
        return Ok(());
    }

    let replica_uuid = crate::repl::role::uuid_bytes_to_string(&req.replica_uuid);
    if !tracker.try_enter(&replica_uuid) {
        let _ = protocol::write_frame_async(
            &mut writer,
            &Message::Close(Close {
                reason: format!("replica UUID {replica_uuid} is already connected"),
            }),
        )
        .await;
        let _ = writer.flush().await;
        return Ok(());
    }
    let slot = slot_for(&replica_uuid);
    if needs_base_backup {
        // The backup checkpoint is taken at or after the handshake's current
        // LSN, so this new slot never needs older WAL. Seed before checkpoint
        // to prevent an ack=0 slot from being invalidated by the retention cap
        // while its own backup is being produced.
        slot.activate_at(current_lsn.saturating_sub(1));
        env.slots.lock().persist(&replica_uuid)?;
    }
    // Item 3: if the replica needs a base backup, stream it inline
    // *before* taking over to the WAL streaming loop. The
    // `BaseBackupEnd.backup_lsn` becomes the new start for WAL
    // streaming so the replica sees a self-consistent
    // (page-snapshot + replayable-WAL-tail) view.
    let mut effective_start_lsn = req.requested_start_lsn;
    if needs_base_backup {
        let db = env.db.as_ref().expect("needs_base_backup gated by db Some");
        let data_dir = env
            .data_dir
            .as_ref()
            .expect("needs_base_backup gated by data_dir Some");
        match crate::repl::base_backup::stream_base_backup(&mut writer, db, data_dir).await {
            Ok(snapshot) => {
                effective_start_lsn = snapshot.backup_lsn.saturating_add(1);
            }
            Err(err) => {
                tracker.leave(&replica_uuid);
                disconnect_slot(&slot);
                return Err(err);
            }
        }
    }
    drop(backup_permit);
    // Seed the slot's retention floor to just below where this replica
    // starts streaming, so a not-yet-acked slot (acked_lsn == 0) doesn't
    // pin the master's WAL retention to LSN 0. A validated resume may move
    // retention backwards if the replica recovered an older durable point;
    // a backup keeps the protection established before its checkpoint.
    if !needs_base_backup {
        slot.activate_at(effective_start_lsn.saturating_sub(1));
    } else {
        slot.ensure_floor(effective_start_lsn.saturating_sub(1));
    }
    // Persist the seeded floor so a master crash right after connect
    // recovers a sane acked_lsn rather than 0.
    if let Err(e) = env.slots.lock().persist(&replica_uuid) {
        crate::util::logging::log_message(format_args!(
            "mydb-repl master: slot persist after seed failed for {replica_uuid}: {}",
            e.message()
        ));
    }
    let effective_req = Handshake {
        requested_start_lsn: effective_start_lsn,
        ..req
    };
    if env.shutdown_flag.load(std::sync::atomic::Ordering::Acquire) {
        tracker.leave(&replica_uuid);
        disconnect_slot(&slot);
        return Ok(());
    }
    let result = run_stream_loop(effective_req, slot.clone(), env, reader, writer).await;
    tracker.leave(&replica_uuid);
    // Connection membership and WAL retention are different concepts. A
    // disconnected replica keeps its slot/history but leaves semi-sync quorum
    // immediately, avoiding a timeout on every commit for slot_timeout.
    // `run_stream_loop` owns `env`, so use the slot's shared state directly.
    disconnect_slot(&slot);
    result
}

fn disconnect_slot(slot: &SlotHandle) {
    slot.state.lock().connected = false;
    let (signal, cvar) = &*slot.ack_signal;
    if let Ok(mut generation) = signal.lock() {
        *generation = generation.saturating_add(1);
        cvar.notify_all();
    }
}

async fn run_stream_loop<R, W>(
    req: Handshake,
    slot: SlotHandle,
    env: SenderEnv,
    mut reader: BufReader<R>,
    mut writer: BufWriter<W>,
) -> Result<(), QueryError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let requested_chunk_bytes = usize::try_from(req.max_chunk_bytes)
        .unwrap_or(usize::MAX)
        .clamp(4 * 1024, env.config.chunk_bytes);
    let _memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,
        requested_chunk_bytes.saturating_mul(3),
    )?;
    let mut wal_reader = match WalReader::open(&env.log, req.requested_start_lsn) {
        Ok(r) => r,
        Err(WalReaderError::Io(err)) => return Err(err),
        Err(err) => {
            let resync = Resync {
                new_start_lsn: match &err {
                    WalReaderError::Truncated { min_available, .. } => *min_available,
                    _ => 0,
                },
                reason: format!("WalReader open failed: {err:?}"),
            };
            let _ = protocol::write_frame_async(&mut writer, &Message::Resync(resync)).await;
            let _ = writer.flush().await;
            return Ok(());
        }
    };
    // Durable cap: never ship a frame the master hasn't fsynced. A
    // crash that loses an unsynced WAL tail must not leave a replica
    // holding records the recovered master never had (silent fork).
    // Only enforced when the bound Database runs strict durability —
    // relaxed/memory modes fsync too rarely for the cap to be useful,
    // and raw-LogManager tests have no Database at all.
    let cap_to_durable = env
        .db
        .as_ref()
        .map(|db| db.durability_mode() == crate::storage::DurabilityMode::Strict)
        .unwrap_or(false);
    let (mut new_lsn_rx, mut durable_rx) = {
        let g = env.log.lock();
        (g.subscribe_new_lsn(), g.subscribe_durable_lsn())
    };
    let mut last_known_lsn = wal_reader
        .latest_known_lsn()
        .max(env.log.lock().next_lsn().saturating_sub(1));
    // A service can be started during promotion after the LogManager durable
    // atomic has advanced but before a newly-subscribed watch receiver has
    // observed/broadcast that value. Seed from the authoritative LogManager;
    // otherwise a caught-up replica can remain heartbeat-only until some
    // unrelated future fsync changes the watch channel.
    let mut durable_lsn = env
        .log
        .lock()
        .durable_lsn()
        .max(*durable_rx.borrow_and_update());

    // End-to-end backpressure: bytes shipped but not yet acked by the
    // replica. Past the window we stop reading WAL and let acks drain
    // it, so a slow replica can't make the sender buffer the world.
    let max_inflight_bytes = env
        .config
        .max_inflight_bytes
        .max(requested_chunk_bytes as u64);
    let mut inflight: std::collections::VecDeque<(u64, u64)> = std::collections::VecDeque::new();
    let mut inflight_bytes: u64 = 0;
    let mut highest_sent_lsn = req.requested_start_lsn.saturating_sub(1);
    // Throttled slot persistence (C1): flush the advancing acked_lsn to
    // disk at most once/sec so a master crash recovers a real retention
    // floor instead of a stale/zero one. Time-throttling keeps this off
    // the per-ack hot path; a ≤1s-stale floor is safe (it only retains
    // marginally more WAL, never less).
    let mut last_persist: Option<Instant> = None;
    const SLOT_PERSIST_INTERVAL: Duration = Duration::from_secs(1);

    let mut heartbeat = tokio::time::interval(env.config.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick

    let mut last_inbound = Instant::now();
    let peer_timeout = (env.config.heartbeat * 6).max(Duration::from_secs(30));
    let mut inbound = Box::pin(protocol::read_frame_async(&mut reader));
    loop {
        if env.shutdown_flag.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        // Drain any frames that are ≤ the safe watermark, respecting
        // the inflight window.
        let safe_high_lsn = if cap_to_durable {
            last_known_lsn.min(durable_lsn)
        } else {
            last_known_lsn
        };
        while inflight_bytes < max_inflight_bytes {
            let chunk = if cap_to_durable {
                wal_reader.next_durable_chunk(safe_high_lsn, requested_chunk_bytes)
            } else {
                wal_reader.next_chunk(safe_high_lsn, requested_chunk_bytes)
            };
            match chunk {
                Ok(Some(chunk)) => {
                    let chunk_len = chunk.bytes.len() as u64;
                    let msg = Message::WalChunk(WalChunk {
                        start_lsn: chunk.start_lsn,
                        end_lsn: chunk.end_lsn,
                        frame_count: chunk.frame_count,
                        frames_bytes: chunk.bytes,
                    });
                    if let Err(err) = protocol::write_frame_async(&mut writer, &msg).await {
                        return Err(io_err(err));
                    }
                    inflight.push_back((chunk.end_lsn, chunk_len));
                    inflight_bytes = inflight_bytes.saturating_add(chunk_len);
                    highest_sent_lsn = highest_sent_lsn.max(chunk.end_lsn);
                }
                Ok(None) => break,
                Err(WalReaderError::Truncated {
                    requested,
                    min_available,
                }) => {
                    let _ = protocol::write_frame_async(
                        &mut writer,
                        &Message::Resync(Resync {
                            new_start_lsn: min_available,
                            reason: format!(
                                "WAL truncated past LSN {} (min_available={})",
                                requested, min_available
                            ),
                        }),
                    )
                    .await;
                    let _ = writer.flush().await;
                    return Ok(());
                }
                Err(WalReaderError::FutureLsn { .. }) => {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "WAL reader saw future LSN",
                    ));
                }
                Err(WalReaderError::Io(e)) => {
                    // If the cursor fell below the master's retention
                    // floor (its segment was pruned out from under the
                    // reader while we were streaming), surface a Resync
                    // so the replica re-seeds, instead of a hard IO
                    // disconnect that forces a reconnect round-trip to
                    // discover the same thing (C6).
                    let min_available = env.log.lock().min_available_lsn();
                    if wal_reader.cursor_lsn() < min_available {
                        let _ = protocol::write_frame_async(
                            &mut writer,
                            &Message::Resync(Resync {
                                new_start_lsn: min_available,
                                reason: format!(
                                    "WAL segment recycled past cursor {} (min_available={})",
                                    wal_reader.cursor_lsn(),
                                    min_available
                                ),
                            }),
                        )
                        .await;
                        let _ = writer.flush().await;
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
        writer.flush().await.map_err(io_err)?;

        // Wait for the next event: new WAL, durable advance, heartbeat
        // tick, or inbound ack.
        tokio::select! {
            biased;
            _ = env.shutdown.notified() => {
                let _ = protocol::write_frame_async(
                    &mut writer,
                    &Message::Close(Close { reason: "master service stopping".into() }),
                ).await;
                return Ok(());
            }
            new = new_lsn_rx.changed() => {
                if new.is_err() {
                    // master gone — graceful close
                    return Ok(());
                }
                last_known_lsn = *new_lsn_rx.borrow_and_update();
            }
            durable = durable_rx.changed() => {
                if durable.is_err() {
                    return Ok(());
                }
                durable_lsn = *durable_rx.borrow_and_update();
            }
            _ = heartbeat.tick() => {
                if last_inbound.elapsed() >= peer_timeout {
                    return Err(io_err(std::io::Error::new(std::io::ErrorKind::TimedOut,
                        "replica sent no complete frame before peer timeout")));
                }
                let source = source_wire_state(&env);
                let msg = Message::Heartbeat(Heartbeat {
                    master_lsn: last_known_lsn,
                    durable_lsn,
                    master_time_ms: now_ms(),
                    timeline_uuid: source.timeline_uuid,
                    generation: source.generation,
                    source_frozen: source.frozen,
                    fence_lsn: source.fence_lsn,
                    fence_token_hash: source.fence_token_hash,
                });
                if let Err(err) = protocol::write_frame_async(&mut writer, &msg).await {
                    return Err(io_err(err));
                }
                writer.flush().await.map_err(io_err)?;
            }
            message = &mut inbound => {
                // Re-arm only after a complete frame. Other select branches
                // must never discard bytes already consumed by read_exact.
                drop(inbound);
                inbound = Box::pin(protocol::read_frame_async(&mut reader));
                last_inbound = Instant::now();
                match message {
                    Ok(Message::ReplicaAck(ack)) => {
                        if ack.applied_lsn > ack.durable_lsn
                            || ack.durable_lsn > ack.received_lsn
                            || ack.received_lsn > highest_sent_lsn
                            || ack.applied_lsn > durable_lsn.max(highest_sent_lsn)
                        {
                            return Err(error(
                                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                                format!(
                                    "invalid replica ACK: applied={} durable={} received={} highest_sent={} master_durable={}",
                                    ack.applied_lsn, ack.durable_lsn, ack.received_lsn, highest_sent_lsn, durable_lsn
                                ),
                            ));
                        }
                        // v1 carries received/applied; the receiver emits both
                        // only after its local WAL durability barrier. Never use
                        // received to satisfy semi-sync.
                        let acked = ack.applied_lsn;
                        slot.record_ack(acked, now_ms());
                        while let Some(&(end_lsn, len)) = inflight.front() {
                            if end_lsn > acked {
                                break;
                            }
                            inflight_bytes = inflight_bytes.saturating_sub(len);
                            inflight.pop_front();
                        }
                        // Persist the advanced acked_lsn, time-throttled.
                        let due = last_persist
                            .map(|t| t.elapsed() >= SLOT_PERSIST_INTERVAL)
                            .unwrap_or(true);
                        if due {
                            if let Err(e) = env.slots.lock().persist(&slot.uuid) {
                                crate::util::logging::log_message(format_args!(
                                    "mydb-repl master: slot persist on ack failed for {}: {}",
                                    slot.uuid, e.message()
                                ));
                            }
                            last_persist = Some(Instant::now());
                        }
                    }
                    Ok(Message::Close(c)) => {
                        let _ = c;
                        return Ok(());
                    }
                    Ok(other) => {
                        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            format!("unexpected inbound message {:?}", other.msg_type()),
                        ));
                    }
                    Err(err) => return Err(io_err(err)),
                }
            }
        }
    }
}

fn io_err(err: std::io::Error) -> QueryError {
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("repl sender io: {err}"),
    )
}

struct SourceWireState {
    cluster_uuid: [u8; 16],
    timeline_uuid: [u8; 16],
    parent_timeline_uuid: Option<[u8; 16]>,
    fork_lsn: u64,
    generation: u64,
    frozen: bool,
    fence_lsn: u64,
    fence_token_hash: String,
}

fn source_wire_state(env: &SenderEnv) -> SourceWireState {
    if let Some(role) = &env.role_state {
        if let Ok(state) = role.lock() {
            let fence = state.write_fence.as_ref();
            return SourceWireState {
                cluster_uuid: crate::repl::role::uuid_string_to_bytes(&state.cluster_uuid)
                    .unwrap_or(env.master_uuid),
                timeline_uuid: crate::repl::role::uuid_string_to_bytes(&state.timeline_uuid)
                    .unwrap_or(env.master_uuid),
                parent_timeline_uuid: state
                    .parent_timeline_uuid
                    .as_deref()
                    .and_then(crate::repl::role::uuid_string_to_bytes),
                fork_lsn: state.fork_lsn,
                generation: state.generation,
                frozen: fence.is_some(),
                fence_lsn: fence.map(|fence| fence.fence_lsn).unwrap_or(0),
                fence_token_hash: fence
                    .map(|fence| crate::repl::role::fence_token_hash(&fence.token))
                    .unwrap_or_default(),
            };
        }
    }
    SourceWireState {
        cluster_uuid: env.master_uuid,
        timeline_uuid: env.master_uuid,
        parent_timeline_uuid: None,
        fork_lsn: 0,
        generation: env.epoch,
        frozen: false,
        fence_lsn: 0,
        fence_token_hash: String::new(),
    }
}
