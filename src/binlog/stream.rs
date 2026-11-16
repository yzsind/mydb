//! File-backed, quota-controlled transaction bodies for bulk DDL publication.
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::engine::executor::spill_quota::ProtocolSpillFile;

pub(super) struct EncodedSpool {
    file: parking_lot::Mutex<ProtocolSpillFile>,
    body_bytes: AtomicU64,
    file_bytes: AtomicU64,
    max_payload: AtomicU64,
    checksum: parking_lot::Mutex<Hasher>,
}

impl std::fmt::Debug for EncodedSpool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedSpool")
            .field("bytes", &self.body_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl EncodedSpool {
    pub(super) fn append(&self, config: &BinlogConfig, op: &BinlogTxnOp) -> Result<(), QueryError> {
        let _input = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BinlogTransient,
            binlog_op_nested_bytes(op),
        )?;
        let events = encode_pending_ops(std::slice::from_ref(op))?;
        let _encoded = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BinlogTransient,
            events.iter().map(|(_, bytes, _)| bytes.capacity()).sum(),
        )?;
        let mut file = self.file.lock();
        let mut checksum = self.checksum.lock();
        for (kind, payload, force) in events {
            crate::engine::cancel::check_cancelled()?;
            let length = u32::try_from(payload.len()).map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "binlog spool event exceeds u32",
                )
            })?;
            let mut header = [0u8; 6];
            header[0] = kind;
            header[1] = force as u8;
            header[2..].copy_from_slice(&length.to_le_bytes());
            file.write_all(&header)
                .and_then(|_| file.write_all(&payload))
                .map_err(spill_error)?;
            checksum.update(&header);
            checksum.update(&payload);
            self.body_bytes.fetch_add(
                encoded_event_size(config, payload.len(), force)?,
                Ordering::Relaxed,
            );
            self.file_bytes
                .fetch_add(payload.len() as u64 + 6, Ordering::Relaxed);
            self.max_payload
                .fetch_max(payload.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn write_body(
        &self,
        config: &BinlogConfig,
        writer: &mut File,
        mut position: u64,
    ) -> Result<(u64, u64), QueryError> {
        let max = usize::try_from(self.max_payload.load(Ordering::Relaxed))
            .map_err(|_| stream_error("binlog spool payload overflow"))?;
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BinlogTransient,
            max.saturating_mul(2).saturating_add(64),
        )?;
        let mut file = self.file.lock();
        file.rewind()?;
        let mut remaining = self.file_bytes.load(Ordering::Relaxed);
        let mut checksum = Hasher::new();
        let mut calls = 0;
        let start = position;
        while remaining != 0 {
            let mut header = [0u8; 6];
            file.read_exact(&mut header).map_err(spill_error)?;
            let length = u32::from_le_bytes(header[2..].try_into().unwrap()) as usize;
            if remaining < 6 || length > max || length as u64 > remaining - 6 || header[1] > 1 {
                return Err(stream_error("corrupt binlog spool frame"));
            }
            let mut payload = vec![0; length];
            file.read_exact(&mut payload).map_err(spill_error)?;
            checksum.update(&header);
            checksum.update(&payload);
            let encoded = encode_event(config, header[0], position, &payload, header[1] != 0);
            writer.write_all(&encoded).map_err(io_error)?;
            position += encoded.len() as u64;
            remaining -= length as u64 + 6;
            calls += 1;
        }
        if checksum.finalize() != self.checksum.lock().clone().finalize()
            || position - start != self.body_bytes.load(Ordering::Relaxed)
        {
            return Err(stream_error("binlog spool checksum or length mismatch"));
        }
        Ok((calls, position - start))
    }
}

fn stream_error(message: impl Into<String>) -> QueryError {
    error(crate::error::catalog::ER_GET_ERRNO, message)
}

fn spill_error(err: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&err).unwrap_or_else(|| io_error(err))
}

#[derive(Debug)]
pub(super) struct StagedSpool {
    pub(super) body: Arc<EncodedSpool>,
    pub(super) body_position: u64,
    pub(super) suffix: Vec<BinlogEventChunk>,
}

#[derive(Debug)]
pub(super) struct StagedWrite {
    pub(super) chunks: Vec<BinlogEventChunk>,
    pub(super) spool: Option<StagedSpool>,
}

/// Switch only this transaction to bounded, file-backed event collection.
pub(crate) fn begin_bulk_transaction_spool(
    txn_id: u64,
    data_dir: &Path,
    connection_id: u32,
) -> Result<(), QueryError> {
    let manager = require_manager()?;
    let pending = manager
        .state
        .lock()
        .pending
        .remove(&txn_id)
        .ok_or_else(|| stream_error("bulk binlog requires a preceding CREATE event"))?;
    let mut lease = PendingBinlogLease {
        manager: &manager,
        txn_id,
        pending: Some(pending),
    };
    let spool = Arc::new(EncodedSpool {
        file: parking_lot::Mutex::new(ProtocolSpillFile::create(data_dir, connection_id)?),
        body_bytes: AtomicU64::new(0),
        file_bytes: AtomicU64::new(0),
        max_payload: AtomicU64::new(0),
        checksum: parking_lot::Mutex::new(Hasher::new()),
    });
    for op in &lease.pending.as_ref().unwrap().ops {
        spool.append(&manager.config, op)?;
    }
    let memory_charge = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::BinlogTransient,
        std::mem::size_of::<BinlogTxnOp>() + std::mem::size_of::<EncodedSpool>(),
    )?;
    manager.state.lock().pending.insert(
        txn_id,
        PendingBinlogTxn {
            ops: vec![BinlogTxnOp::Spool { spool }],
            memory_charge,
        },
    );
    lease.pending.take();
    Ok(())
}

impl BinlogSourceManager {
    pub(super) fn stage_spooled_txn_commit(
        &self,
        body: Arc<EncodedSpool>,
        commit_scn: u64,
    ) -> Result<BinlogCommit, QueryError> {
        let mut state = self.state.lock();
        let total = body.body_bytes.load(Ordering::Relaxed)
            + encoded_event_size(
                &self.config,
                encode_gtid_payload(&self.config, 1, 1, true).len(),
                false,
            )?
            + encoded_event_size(
                &self.config,
                encode_query_payload(0, "", "BEGIN").len(),
                false,
            )?
            + encoded_event_size(&self.config, 8, false)?;
        let limit = self
            .rotate_bytes
            .load(Ordering::Relaxed)
            .min(self.config.retention_bytes.unwrap_or(u64::MAX))
            .max(4);
        if state.active_pos >= limit
            || state.active_pos.saturating_add(total) > limit
            || state.active_pos.saturating_add(total) > BINLOG_FILE_POSITION_LIMIT
        {
            drop(state);
            self.rotate()?;
            state = self.state.lock();
        }
        if state
            .active_pos
            .checked_add(total)
            .is_none_or(|end| end > BINLOG_FILE_POSITION_LIMIT)
        {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "binary log transaction exceeds the 4 GiB file-position range",
            ));
        }
        let gtid = self.next_gtid_locked(&mut state)?;
        let file = state.active_file.clone();
        let start_pos = state.active_pos;
        let prefix = self.append_events_locked(
            &mut state,
            vec![
                (
                    GTID_EVENT,
                    encode_gtid_payload(&self.config, gtid.gno, gtid.sequence_number, true),
                    false,
                ),
                (QUERY_EVENT, encode_query_payload(0, "", "BEGIN"), false),
            ],
            false,
            false,
            false,
        )?;
        let body_position = state.active_pos;
        state.active_pos += body.body_bytes.load(Ordering::Relaxed);
        let suffix = self.append_events_locked(
            &mut state,
            vec![(XID_EVENT, commit_scn.to_le_bytes().to_vec(), false)],
            false,
            false,
            false,
        )?;
        let reference = crate::wal::WalBinlogRef {
            file,
            start_pos,
            end_pos: state.active_pos,
            gtid: Some(gtid.to_string()),
        };
        let order = self.commit_order.allocate(true);
        let mut group = self.group_sync.state.lock();
        if let Some(message) = &group.fatal_error {
            self.commit_order.abort(order);
            return Err(io_error(std::io::Error::other(message.clone())));
        }
        group.requested_order = group.requested_order.max(order);
        group.writes.insert(
            order,
            StagedWrite {
                chunks: prefix,
                spool: Some(StagedSpool {
                    body,
                    body_position,
                    suffix,
                }),
            },
        );
        self.group_sync.cv.notify_all();
        Ok(BinlogCommit {
            reference,
            chunks: Vec::new(),
            streamed: true,
            order,
            order_state: Arc::clone(&self.commit_order),
            group_sync: Arc::clone(&self.group_sync),
            durable: AtomicBool::new(false),
            published: false,
        })
    }
}
