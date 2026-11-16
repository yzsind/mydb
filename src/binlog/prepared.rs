//! Durable XA binlog intent. Bounded files are protected by the branch's WAL;
//! PREPARE references their exact length and checksum, including table maps.
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::wal::PreparedBinlogRef;

const CHUNK_BYTES: usize = 1024 * 1024;
const DIRECTORY: &str = "system/xa-binlog";

pub(crate) fn chunk_path(txn_id: u64, chunk: u32) -> String {
    format!("{DIRECTORY}/{txn_id}/{chunk:08}.bin")
}

pub(crate) fn prepare(
    txn_id: u64,
    mut write: impl FnMut(String, Vec<u8>) -> Result<(), QueryError>,
) -> Result<Option<PreparedBinlogRef>, QueryError> {
    let Some(manager) = manager() else {
        return Ok(None);
    };
    let pending = manager.state.lock().pending.remove(&txn_id);
    let Some(pending) = pending else {
        return Ok(None);
    };
    let lease = PendingBinlogLease {
        manager: &manager,
        txn_id,
        pending: Some(pending),
    };
    let mut descriptor = PreparedBinlogRef {
        chunks: 0,
        bytes: 0,
        crc32: 0,
    };
    let mut checksum = Hasher::new();
    let _scratch =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::BinlogTransient, CHUNK_BYTES)?;
    let mut chunk = Vec::with_capacity(CHUNK_BYTES);
    for op in &lease.pending.as_ref().unwrap().ops {
        let events = encode_pending_ops(std::slice::from_ref(op))?;
        let _encoded = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BinlogTransient,
            events.iter().map(|(_, bytes, _)| bytes.capacity()).sum(),
        )?;
        for (kind, bytes, _) in events {
            let len = u32::try_from(bytes.len()).map_err(|_| {
                error(
                    crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                    "XA binlog event exceeds u32 length",
                )
            })?;
            let mut header = [0u8; 5];
            header[0] = kind;
            header[1..].copy_from_slice(&len.to_le_bytes());
            for mut input in [header.as_slice(), bytes.as_slice()] {
                checksum.update(input);
                descriptor.bytes = descriptor
                    .bytes
                    .checked_add(input.len() as u64)
                    .ok_or_else(|| corrupt("intent length overflow"))?;
                while !input.is_empty() {
                    let n = input.len().min(CHUNK_BYTES - chunk.len());
                    chunk.extend_from_slice(&input[..n]);
                    input = &input[n..];
                    if chunk.len() == CHUNK_BYTES {
                        write(
                            chunk_path(txn_id, descriptor.chunks),
                            std::mem::replace(&mut chunk, Vec::with_capacity(CHUNK_BYTES)),
                        )?;
                        descriptor.chunks = descriptor
                            .chunks
                            .checked_add(1)
                            .ok_or_else(|| corrupt("too many chunks"))?;
                    }
                }
            }
        }
    }
    if !chunk.is_empty() {
        write(chunk_path(txn_id, descriptor.chunks), chunk)?;
        descriptor.chunks = descriptor
            .chunks
            .checked_add(1)
            .ok_or_else(|| corrupt("too many chunks"))?;
    }
    descriptor.crc32 = checksum.finalize();
    Ok((descriptor.bytes != 0).then_some(descriptor))
}

fn corrupt(detail: &str) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("invalid prepared XA binlog intent: {detail}"),
    )
}

pub(crate) fn restore(
    data_dir: &Path,
    txn_id: u64,
    intent: &PreparedBinlogRef,
) -> Result<(), QueryError> {
    // Disabling the source after restart cannot silently discard a prepared
    // branch's already accepted logging obligation.
    let manager = require_manager()?;
    if manager.txn_len(txn_id) != 0 {
        return Ok(());
    }
    let mut reader = IntentReader::new(data_dir, txn_id, intent)?;
    let mut events = Vec::new();
    let mut charge =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::BinlogTransient, CHUNK_BYTES)?;
    while reader.remaining != 0 {
        let mut header = [0u8; 5];
        reader.read_exact(&mut header).map_err(io_error)?;
        if !matches!(
            header[0],
            QUERY_EVENT
                | TABLE_MAP_EVENT
                | WRITE_ROWS_EVENT_V2
                | UPDATE_ROWS_EVENT_V2
                | DELETE_ROWS_EVENT_V2
        ) {
            return Err(corrupt("unexpected event type"));
        }
        let len = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
        if len as u64 > reader.remaining {
            return Err(corrupt("truncated event"));
        }
        charge.try_grow(len.saturating_add(std::mem::size_of::<(u8, Vec<u8>, bool)>() * 2))?;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes).map_err(io_error)?;
        events.push((header[0], bytes, false));
    }
    if reader.checksum.finalize() != intent.crc32 {
        return Err(corrupt("checksum mismatch"));
    }
    // Transfer the reservation with the encoded payload; never briefly charge
    // the same restored transaction twice under a hard memory budget.
    manager.state.lock().pending.insert(
        txn_id,
        PendingBinlogTxn {
            ops: vec![BinlogTxnOp::Encoded { events }],
            memory_charge: charge,
        },
    );
    Ok(())
}

struct IntentReader<'a> {
    data_dir: &'a Path,
    txn_id: u64,
    next_chunk: u32,
    remaining: u64,
    chunk: std::io::Cursor<Vec<u8>>,
    checksum: Hasher,
}

impl<'a> IntentReader<'a> {
    fn new(
        data_dir: &'a Path,
        txn_id: u64,
        intent: &PreparedBinlogRef,
    ) -> Result<Self, QueryError> {
        if intent.bytes == 0
            || intent.bytes.div_ceil(CHUNK_BYTES as u64) != u64::from(intent.chunks)
        {
            return Err(corrupt("invalid chunk count"));
        }
        Ok(Self {
            data_dir,
            txn_id,
            next_chunk: 0,
            remaining: intent.bytes,
            chunk: std::io::Cursor::new(Vec::new()),
            checksum: Hasher::new(),
        })
    }
}

impl Read for IntentReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 || out.is_empty() {
            return Ok(0);
        }
        if self.chunk.position() as usize == self.chunk.get_ref().len() {
            let path = self.data_dir.join(chunk_path(self.txn_id, self.next_chunk));
            let expected = self.remaining.min(CHUNK_BYTES as u64);
            let mut file = File::open(path)?;
            if file.metadata()?.len() != expected {
                return Err(std::io::Error::other(
                    "invalid prepared XA binlog chunk length",
                ));
            }
            self.chunk = std::io::Cursor::new(Vec::new());
            let mut bytes = vec![0; expected as usize];
            file.read_exact(&mut bytes)?;
            self.chunk = std::io::Cursor::new(bytes);
            self.next_chunk += 1;
        }
        let n = self.chunk.read(out)?;
        self.remaining -= n as u64;
        self.checksum.update(&out[..n]);
        Ok(n)
    }
}

pub(crate) fn cleanup(data_dir: &Path, txn_id: u64) {
    let path = data_dir.join(DIRECTORY).join(txn_id.to_string());
    if let Err(err) = fs::remove_dir_all(path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            crate::util::logging::log_message(format_args!(
                "XA binlog intent cleanup {txn_id}: {err}"
            ));
        }
    }
}

pub(crate) fn cleanup_finished(
    data_dir: &Path,
    live: &HashSet<u64>,
) -> Result<HashSet<u64>, QueryError> {
    let entries = match fs::read_dir(data_dir.join(DIRECTORY)) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(err) => return Err(io_error(err)),
    };
    let mut retained = HashSet::new();
    for entry in entries {
        let entry = entry.map_err(io_error)?;
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
        {
            if live.contains(&id) {
                retained.insert(id);
            } else {
                cleanup(data_dir, id);
            }
        }
    }
    Ok(retained)
}
