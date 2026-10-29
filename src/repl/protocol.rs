//! Binary wire protocol for mydb-native physical WAL replication.
//!
//! Frame on the wire (little-endian throughout):
//!
//! ```text
//! [4B magic = "REPL"] [1B msg_type] [4B payload_len] [payload...] [4B crc32]
//! ```
//!
//! The CRC32 covers `msg_type || payload_len || payload`. Maximum payload
//! size is 64 MiB to bound memory at decode time.
//!
//! Independent from the MySQL binlog protocol: this carries raw mydb WAL
//! frame bytes between mydb instances, with handshake / ack / heartbeat
//! envelope messages around it.

#[cfg(any(test, feature = "fuzzing"))]
use std::io::Read;
#[cfg(test)]
use std::io::Write;
use std::io::{Error, ErrorKind, Result as IoResult};

use crc32fast::Hasher as Crc32;

pub const MAGIC: [u8; 4] = *b"REPL";
pub const PROTOCOL_VERSION: u32 = 3;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const FRAME_HEADER_LEN: usize = 4 + 1 + 4;
pub const FRAME_TRAILER_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    Handshake = 1,
    HandshakeAck = 2,
    WalChunk = 3,
    Heartbeat = 4,
    ReplicaAck = 5,
    Resync = 6,
    Close = 7,
    /// Phase / Item 3: streaming base backup announce. Carried from
    /// master to replica as the first message of a base backup
    /// sequence. Lists every file the replica must replace, with
    /// total sizes, then `BaseBackupChunk` messages carry the bytes
    /// and `BaseBackupEnd` marks the end of the sequence.
    BaseBackupStart = 8,
    BaseBackupChunk = 9,
    BaseBackupEnd = 10,
}

impl MsgType {
    fn from_u8(b: u8) -> IoResult<Self> {
        Ok(match b {
            1 => MsgType::Handshake,
            2 => MsgType::HandshakeAck,
            3 => MsgType::WalChunk,
            4 => MsgType::Heartbeat,
            5 => MsgType::ReplicaAck,
            6 => MsgType::Resync,
            7 => MsgType::Close,
            8 => MsgType::BaseBackupStart,
            9 => MsgType::BaseBackupChunk,
            10 => MsgType::BaseBackupEnd,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unknown msg_type {b}"),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Handshake(Handshake),
    HandshakeAck(HandshakeAck),
    WalChunk(WalChunk),
    Heartbeat(Heartbeat),
    ReplicaAck(ReplicaAck),
    Resync(Resync),
    Close(Close),
    BaseBackupStart(BaseBackupStart),
    BaseBackupChunk(BaseBackupChunk),
    BaseBackupEnd(BaseBackupEnd),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub auth_token: crate::repl::config::ReplAuthToken,
    pub protocol_version: u32,
    pub replica_uuid: [u8; 16],
    pub requested_start_lsn: u64,
    pub last_known_master_uuid: [u8; 16],
    pub last_known_epoch: u64,
    pub cluster_uuid: [u8; 16],
    pub last_known_timeline_uuid: [u8; 16],
    pub client_version: String,
    /// When `true`, the replica is explicitly asking for a full
    /// base backup (Item 3 streaming base backup). This is the
    /// "fresh / wiped data dir" case: the replica has no useful
    /// state, requested_start_lsn would be 0 or below the master's
    /// retention floor, and re-applying from the master is
    /// pointless without a base of pages first.
    pub request_base_backup: bool,
    /// Largest WAL payload the replica has admitted through its process
    /// memory governor. The sender must not exceed this value. Keeping this
    /// in the v2 handshake lets a memory-constrained replica reduce its
    /// receive/apply scratch instead of getting stuck in a 1037 reconnect
    /// loop while the master's static chunk size remains larger.
    pub max_chunk_bytes: u32,
    pub format_compatibility: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeAck {
    pub heartbeat_ms: u64,
    pub master_uuid: [u8; 16],
    pub current_lsn: u64,
    pub durable_lsn: u64,
    pub min_available_lsn: u64,
    pub epoch: u64,
    pub server_version: String,
    pub accepted: bool,
    pub reason: String,
    pub format_compatibility: String,
    pub semi_sync_required: bool,
    pub cluster_uuid: [u8; 16],
    pub timeline_uuid: [u8; 16],
    pub generation: u64,
    pub source_frozen: bool,
    pub fence_lsn: u64,
    pub fence_token_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalChunk {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub frame_count: u32,
    pub frames_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub master_lsn: u64,
    pub durable_lsn: u64,
    pub master_time_ms: u64,
    pub timeline_uuid: [u8; 16],
    pub generation: u64,
    pub source_frozen: bool,
    pub fence_lsn: u64,
    pub fence_token_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAck {
    pub received_lsn: u64,
    pub durable_lsn: u64,
    pub applied_lsn: u64,
    pub time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resync {
    pub new_start_lsn: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub reason: String,
}

/// File entry inside a `BaseBackupStart`. `path` is the data-dir
/// relative path (e.g. `tables/2.heap`) and `len` is the total byte
/// length the replica should expect to receive across all
/// `BaseBackupChunk` messages for this path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseBackupFile {
    pub path: String,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseBackupStart {
    /// LSN at which the master took the on-disk snapshot. The
    /// replica's `progress.last_applied_lsn` is set to this value
    /// after a successful base backup, and subsequent WAL streaming
    /// resumes from `backup_lsn + 1`.
    pub backup_lsn: u64,
    pub files: Vec<BaseBackupFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseBackupChunk {
    pub path: String,
    pub offset: u64,
    pub bytes: Vec<u8>,
    /// True if this is the final chunk for `path`.
    pub eof: bool,
    /// SHA-256 of the complete declared file on the EOF chunk; zero otherwise.
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseBackupEnd {
    pub backup_lsn: u64,
}

impl Message {
    pub fn msg_type(&self) -> MsgType {
        match self {
            Message::Handshake(_) => MsgType::Handshake,
            Message::HandshakeAck(_) => MsgType::HandshakeAck,
            Message::WalChunk(_) => MsgType::WalChunk,
            Message::Heartbeat(_) => MsgType::Heartbeat,
            Message::ReplicaAck(_) => MsgType::ReplicaAck,
            Message::Resync(_) => MsgType::Resync,
            Message::Close(_) => MsgType::Close,
            Message::BaseBackupStart(_) => MsgType::BaseBackupStart,
            Message::BaseBackupChunk(_) => MsgType::BaseBackupChunk,
            Message::BaseBackupEnd(_) => MsgType::BaseBackupEnd,
        }
    }

    pub fn encode_payload(&self, out: &mut Vec<u8>) {
        match self {
            Message::Handshake(m) => encode_handshake(m, out),
            Message::HandshakeAck(m) => encode_handshake_ack(m, out),
            Message::WalChunk(m) => encode_wal_chunk(m, out),
            Message::Heartbeat(m) => encode_heartbeat(m, out),
            Message::ReplicaAck(m) => encode_replica_ack(m, out),
            Message::Resync(m) => encode_resync(m, out),
            Message::Close(m) => encode_close(m, out),
            Message::BaseBackupStart(m) => encode_base_backup_start(m, out),
            Message::BaseBackupChunk(m) => encode_base_backup_chunk(m, out),
            Message::BaseBackupEnd(m) => encode_base_backup_end(m, out),
        }
    }

    pub fn decode_payload(msg_type: MsgType, payload: &[u8]) -> IoResult<Self> {
        let mut r = Reader::new(payload);
        let msg = match msg_type {
            MsgType::Handshake => Message::Handshake(decode_handshake(&mut r)?),
            MsgType::HandshakeAck => Message::HandshakeAck(decode_handshake_ack(&mut r)?),
            MsgType::WalChunk => Message::WalChunk(decode_wal_chunk(&mut r)?),
            MsgType::Heartbeat => Message::Heartbeat(decode_heartbeat(&mut r)?),
            MsgType::ReplicaAck => Message::ReplicaAck(decode_replica_ack(&mut r)?),
            MsgType::Resync => Message::Resync(decode_resync(&mut r)?),
            MsgType::Close => Message::Close(decode_close(&mut r)?),
            MsgType::BaseBackupStart => Message::BaseBackupStart(decode_base_backup_start(&mut r)?),
            MsgType::BaseBackupChunk => Message::BaseBackupChunk(decode_base_backup_chunk(&mut r)?),
            MsgType::BaseBackupEnd => Message::BaseBackupEnd(decode_base_backup_end(&mut r)?),
        };
        if !r.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "trailing bytes in payload",
            ));
        }
        Ok(msg)
    }
}

// -------------------------- Shared framing --------------------------

fn encode_header(msg_type: u8, payload_len: usize) -> IoResult<[u8; FRAME_HEADER_LEN]> {
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(Error::new(ErrorKind::InvalidInput, "payload exceeds 64MiB"));
    }
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[..4].copy_from_slice(&MAGIC);
    header[4] = msg_type;
    header[5..].copy_from_slice(&(payload_len as u32).to_le_bytes());
    Ok(header)
}

/// Encode directly after the reserved header, avoiding a temporary payload
/// allocation and copy. Large async data messages use segmented writes below.
fn encode_frame(msg: &Message) -> IoResult<Vec<u8>> {
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + 64 + FRAME_TRAILER_LEN);
    frame.resize(FRAME_HEADER_LEN, 0);
    msg.encode_payload(&mut frame);
    let header = encode_header(msg.msg_type() as u8, frame.len() - FRAME_HEADER_LEN)?;
    frame[..FRAME_HEADER_LEN].copy_from_slice(&header);
    let crc = crc32fast::hash(&frame[4..]);
    // A large string can leave capacity exactly at the payload end. Only
    // four bytes remain; do not double the complete frame for its trailer.
    frame.reserve_exact(FRAME_TRAILER_LEN);
    frame.extend_from_slice(&crc.to_le_bytes());
    Ok(frame)
}

fn decode_header(header: &[u8; FRAME_HEADER_LEN]) -> IoResult<(MsgType, usize)> {
    if header[..4] != MAGIC {
        return Err(Error::new(ErrorKind::InvalidData, "bad magic"));
    }
    let msg_type = MsgType::from_u8(header[4])?;
    let payload_len = u32::from_le_bytes([header[5], header[6], header[7], header[8]]) as usize;
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(Error::new(ErrorKind::InvalidData, "payload too large"));
    }
    Ok((msg_type, payload_len))
}

fn decode_frame_payload(
    header: &[u8; FRAME_HEADER_LEN],
    msg_type: MsgType,
    payload: Vec<u8>,
    trailer: [u8; FRAME_TRAILER_LEN],
) -> IoResult<Message> {
    let mut crc = Crc32::new();
    crc.update(&header[4..]);
    crc.update(&payload);
    if crc.finalize() != u32::from_le_bytes(trailer) {
        return Err(Error::new(ErrorKind::InvalidData, "frame CRC mismatch"));
    }
    decode_payload_owned(msg_type, payload)
}

// -------------------------- Sync I/O --------------------------

#[cfg(test)]
pub fn write_frame<W: Write>(w: &mut W, msg: &Message) -> IoResult<()> {
    w.write_all(&encode_frame(msg)?)
}

#[cfg(any(test, feature = "fuzzing"))]
pub fn read_frame<R: Read>(r: &mut R) -> IoResult<Message> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut header)?;
    let (msg_type, payload_len) = decode_header(&header)?;
    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload)?;
    let mut trailer = [0u8; FRAME_TRAILER_LEN];
    r.read_exact(&mut trailer)?;
    decode_frame_payload(&header, msg_type, payload, trailer)
}

// -------------------------- Async (tokio) I/O --------------------------

pub async fn write_frame_async<W>(w: &mut W, msg: &Message) -> IoResult<()>
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    // Hot data messages already own a large byte buffer. Encode their small
    // metadata prefix separately and write header/prefix/body/CRC through the
    // caller's BufWriter, avoiding a second full-size frame allocation/copy.
    match msg {
        Message::WalChunk(chunk) => {
            let body_len = u32::try_from(chunk.frames_bytes.len())
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "WAL chunk body exceeds u32"))?;
            let mut prefix = [0u8; 24];
            prefix[..8].copy_from_slice(&chunk.start_lsn.to_le_bytes());
            prefix[8..16].copy_from_slice(&chunk.end_lsn.to_le_bytes());
            prefix[16..20].copy_from_slice(&chunk.frame_count.to_le_bytes());
            prefix[20..].copy_from_slice(&body_len.to_le_bytes());
            return write_frame_parts_async(
                w,
                MsgType::WalChunk as u8,
                &prefix,
                &chunk.frames_bytes,
            )
            .await;
        }
        Message::BaseBackupChunk(chunk) => {
            let mut prefix = Vec::with_capacity(chunk.path.len() + 4 + 8 + 1 + 32 + 4);
            put_str(&mut prefix, &chunk.path);
            put_u64(&mut prefix, chunk.offset);
            prefix.push(u8::from(chunk.eof));
            prefix.extend_from_slice(&chunk.sha256);
            put_u32(
                &mut prefix,
                u32::try_from(chunk.bytes.len()).map_err(|_| {
                    Error::new(ErrorKind::InvalidInput, "backup chunk body exceeds u32")
                })?,
            );
            return write_frame_parts_async(
                w,
                MsgType::BaseBackupChunk as u8,
                &prefix,
                &chunk.bytes,
            )
            .await;
        }
        _ => {}
    }
    let frame = encode_frame(msg)?;
    // Framing does not imply a transport flush. Callers batch WAL/base-backup
    // frames and flush at latency boundaries (handshake, ACK, heartbeat or
    // inflight-window drain); flushing here doubled syscalls and defeated
    // BufWriter coalescing.
    w.write_all(&frame).await
}

async fn write_frame_parts_async<W>(
    w: &mut W,
    msg_type: u8,
    prefix: &[u8],
    body: &[u8],
) -> IoResult<()>
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let payload_len = prefix
        .len()
        .checked_add(body.len())
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "payload length overflow"))?;
    let header = encode_header(msg_type, payload_len)?;
    let mut crc = Crc32::new();
    crc.update(&header[4..]);
    crc.update(prefix);
    crc.update(body);
    w.write_all(&header).await?;
    w.write_all(prefix).await?;
    w.write_all(body).await?;
    w.write_all(&crc.finalize().to_le_bytes()).await
}

pub async fn read_frame_async<R>(r: &mut R) -> IoResult<Message>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    read_frame_async_inner(r, MAX_PAYLOAD_BYTES, false, false)
        .await
        .map(|(message, _memory)| message)
}

#[cfg(test)]
pub(crate) async fn read_frame_async_limited<R>(
    r: &mut R,
    max_wal_bytes: usize,
) -> IoResult<Message>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    read_frame_async_inner(r, max_wal_bytes, true, true)
        .await
        .map(|(message, _memory)| message)
}

pub(crate) async fn read_frame_async_window<R>(
    r: &mut R,
    window: usize,
) -> IoResult<(
    Message,
    Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
)>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    read_frame_async_inner(r, window, true, false).await
}

async fn read_frame_async_inner<R>(
    r: &mut R,
    max_wal_bytes: usize,
    precharged: bool,
    hard_limit: bool,
) -> IoResult<(
    Message,
    Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
)>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    let mut header = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut header).await?;
    let (msg_type, payload_len) = decode_header(&header)?;
    if precharged
        && matches!(
            msg_type,
            MsgType::BaseBackupStart | MsgType::BaseBackupChunk | MsgType::BaseBackupEnd
        )
    {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unexpected base backup frame on WAL stream",
        ));
    }
    let limit = match msg_type {
        MsgType::WalChunk if hard_limit => max_wal_bytes.saturating_add(24),
        MsgType::WalChunk => MAX_PAYLOAD_BYTES,
        MsgType::BaseBackupStart => MAX_PAYLOAD_BYTES,
        MsgType::BaseBackupChunk => 1024 * 1024 + 8192,
        _ => 8192,
    };
    if payload_len > limit {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "replication frame exceeds admitted receive limit",
        ));
    }
    // A physical WAL frame cannot be split. Reserve additional decode/apply
    // scratch for a single wide frame that exceeds the normal chunk window,
    // and retain that charge until the receiver has finished applying it.
    let extra = if precharged {
        payload_len.saturating_sub(max_wal_bytes).saturating_mul(3)
    } else {
        payload_len
    };
    let memory = if extra == 0 {
        None
    } else {
        Some(
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,
            extra,
        ).map_err(|err| Error::new(ErrorKind::OutOfMemory, err.message().to_string()))?
    )
    };
    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload).await?;
    let mut trailer = [0u8; FRAME_TRAILER_LEN];
    r.read_exact(&mut trailer).await?;
    decode_frame_payload(&header, msg_type, payload, trailer).map(|message| (message, memory))
}

fn decode_payload_owned(msg_type: MsgType, mut payload: Vec<u8>) -> IoResult<Message> {
    match msg_type {
        MsgType::WalChunk => {
            let (start_lsn, end_lsn, frame_count, body_start, body_len) = {
                let mut reader = Reader::new(&payload);
                let start_lsn = reader.get_u64()?;
                let end_lsn = reader.get_u64()?;
                let frame_count = reader.get_u32()?;
                let body_len = reader.get_u32()? as usize;
                reader.require(body_len)?;
                let body_start = reader.pos;
                if body_start + body_len != payload.len() {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "trailing WAL chunk bytes",
                    ));
                }
                (start_lsn, end_lsn, frame_count, body_start, body_len)
            };
            payload.copy_within(body_start..body_start + body_len, 0);
            payload.truncate(body_len);
            Ok(Message::WalChunk(WalChunk {
                start_lsn,
                end_lsn,
                frame_count,
                frames_bytes: payload,
            }))
        }
        MsgType::BaseBackupChunk => {
            let (path, offset, eof, sha256, body_start, body_len) = {
                let mut reader = Reader::new(&payload);
                let path = reader.get_str()?;
                let offset = reader.get_u64()?;
                let eof = reader.get_u8()? != 0;
                let sha256 = reader.get_array_32()?;
                let body_len = reader.get_u32()? as usize;
                reader.require(body_len)?;
                let body_start = reader.pos;
                if body_start + body_len != payload.len() {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "trailing backup chunk bytes",
                    ));
                }
                (path, offset, eof, sha256, body_start, body_len)
            };
            payload.copy_within(body_start..body_start + body_len, 0);
            payload.truncate(body_len);
            Ok(Message::BaseBackupChunk(BaseBackupChunk {
                path,
                offset,
                bytes: payload,
                eof,
                sha256,
            }))
        }
        _ => Message::decode_payload(msg_type, &payload),
    }
}

// -------------------------- Per-message codecs --------------------------

fn encode_handshake(m: &Handshake, out: &mut Vec<u8>) {
    put_u32(out, m.protocol_version);
    out.extend_from_slice(&m.replica_uuid);
    put_u64(out, m.requested_start_lsn);
    out.extend_from_slice(&m.last_known_master_uuid);
    put_u64(out, m.last_known_epoch);
    out.extend_from_slice(&m.cluster_uuid);
    out.extend_from_slice(&m.last_known_timeline_uuid);
    put_str(out, &m.client_version);
    out.push(if m.request_base_backup { 1 } else { 0 });
    put_u32(out, m.max_chunk_bytes);
    put_str(out, &m.format_compatibility);
    put_str(out, m.auth_token.as_str());
}

fn decode_handshake(r: &mut Reader<'_>) -> IoResult<Handshake> {
    Ok(Handshake {
        protocol_version: r.get_u32()?,
        replica_uuid: r.get_uuid()?,
        requested_start_lsn: r.get_u64()?,
        last_known_master_uuid: r.get_uuid()?,
        last_known_epoch: r.get_u64()?,
        cluster_uuid: r.get_uuid()?,
        last_known_timeline_uuid: r.get_uuid()?,
        client_version: r.get_str()?,
        request_base_backup: r.get_u8()? != 0,
        max_chunk_bytes: r.get_u32()?,
        format_compatibility: r.get_str()?,
        auth_token: r.get_str()?.into(),
    })
}

fn encode_handshake_ack(m: &HandshakeAck, out: &mut Vec<u8>) {
    out.extend_from_slice(&m.master_uuid);
    put_u64(out, m.current_lsn);
    put_u64(out, m.durable_lsn);
    put_u64(out, m.min_available_lsn);
    put_u64(out, m.epoch);
    put_str(out, &m.server_version);
    out.push(if m.accepted { 1 } else { 0 });
    put_str(out, &m.reason);
    put_str(out, &m.format_compatibility);
    out.push(u8::from(m.semi_sync_required));
    out.extend_from_slice(&m.cluster_uuid);
    out.extend_from_slice(&m.timeline_uuid);
    put_u64(out, m.generation);
    out.push(u8::from(m.source_frozen));
    put_u64(out, m.fence_lsn);
    put_str(out, &m.fence_token_hash);
    put_u64(out, m.heartbeat_ms);
}

fn decode_handshake_ack(r: &mut Reader<'_>) -> IoResult<HandshakeAck> {
    Ok(HandshakeAck {
        master_uuid: r.get_uuid()?,
        current_lsn: r.get_u64()?,
        durable_lsn: r.get_u64()?,
        min_available_lsn: r.get_u64()?,
        epoch: r.get_u64()?,
        server_version: r.get_str()?,
        accepted: r.get_u8()? != 0,
        reason: r.get_str()?,
        format_compatibility: r.get_str()?,
        semi_sync_required: r.get_u8()? != 0,
        cluster_uuid: r.get_uuid()?,
        timeline_uuid: r.get_uuid()?,
        generation: r.get_u64()?,
        source_frozen: r.get_u8()? != 0,
        fence_lsn: r.get_u64()?,
        fence_token_hash: r.get_str()?,
        heartbeat_ms: r.get_u64()?,
    })
}

fn encode_wal_chunk(m: &WalChunk, out: &mut Vec<u8>) {
    put_u64(out, m.start_lsn);
    put_u64(out, m.end_lsn);
    put_u32(out, m.frame_count);
    put_bytes(out, &m.frames_bytes);
}

fn decode_wal_chunk(r: &mut Reader<'_>) -> IoResult<WalChunk> {
    Ok(WalChunk {
        start_lsn: r.get_u64()?,
        end_lsn: r.get_u64()?,
        frame_count: r.get_u32()?,
        frames_bytes: r.get_bytes()?,
    })
}

fn encode_heartbeat(m: &Heartbeat, out: &mut Vec<u8>) {
    put_u64(out, m.master_lsn);
    put_u64(out, m.durable_lsn);
    put_u64(out, m.master_time_ms);
    out.extend_from_slice(&m.timeline_uuid);
    put_u64(out, m.generation);
    out.push(u8::from(m.source_frozen));
    put_u64(out, m.fence_lsn);
    put_str(out, &m.fence_token_hash);
}

fn decode_heartbeat(r: &mut Reader<'_>) -> IoResult<Heartbeat> {
    Ok(Heartbeat {
        master_lsn: r.get_u64()?,
        durable_lsn: r.get_u64()?,
        master_time_ms: r.get_u64()?,
        timeline_uuid: r.get_uuid()?,
        generation: r.get_u64()?,
        source_frozen: r.get_u8()? != 0,
        fence_lsn: r.get_u64()?,
        fence_token_hash: r.get_str()?,
    })
}

fn encode_replica_ack(m: &ReplicaAck, out: &mut Vec<u8>) {
    put_u64(out, m.received_lsn);
    put_u64(out, m.durable_lsn);
    put_u64(out, m.applied_lsn);
    put_u64(out, m.time_ms);
}

fn decode_replica_ack(r: &mut Reader<'_>) -> IoResult<ReplicaAck> {
    Ok(ReplicaAck {
        received_lsn: r.get_u64()?,
        durable_lsn: r.get_u64()?,
        applied_lsn: r.get_u64()?,
        time_ms: r.get_u64()?,
    })
}

fn encode_resync(m: &Resync, out: &mut Vec<u8>) {
    put_u64(out, m.new_start_lsn);
    put_str(out, &m.reason);
}

fn decode_resync(r: &mut Reader<'_>) -> IoResult<Resync> {
    Ok(Resync {
        new_start_lsn: r.get_u64()?,
        reason: r.get_str()?,
    })
}

fn encode_close(m: &Close, out: &mut Vec<u8>) {
    put_str(out, &m.reason);
}

fn decode_close(r: &mut Reader<'_>) -> IoResult<Close> {
    Ok(Close {
        reason: r.get_str()?,
    })
}

fn encode_base_backup_start(m: &BaseBackupStart, out: &mut Vec<u8>) {
    put_u64(out, m.backup_lsn);
    put_u32(out, m.files.len() as u32);
    for entry in &m.files {
        put_str(out, &entry.path);
        put_u64(out, entry.len);
    }
}

fn decode_base_backup_start(r: &mut Reader<'_>) -> IoResult<BaseBackupStart> {
    let backup_lsn = r.get_u64()?;
    let n = r.get_u32()? as usize;
    // Each entry needs a u32 path length and a u64 file length, even
    // for an empty path. Validate before allocating attacker-controlled n.
    if n > (r.buf.len() - r.pos) / 12 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "base backup manifest count exceeds payload",
        ));
    }
    let mut files = Vec::new();
    files.try_reserve_exact(n).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "base backup manifest allocation failed",
        )
    })?;
    for _ in 0..n {
        let path = r.get_str()?;
        let len = r.get_u64()?;
        files.push(BaseBackupFile { path, len });
    }
    Ok(BaseBackupStart { backup_lsn, files })
}

fn encode_base_backup_chunk(m: &BaseBackupChunk, out: &mut Vec<u8>) {
    put_str(out, &m.path);
    put_u64(out, m.offset);
    out.push(if m.eof { 1 } else { 0 });
    out.extend_from_slice(&m.sha256);
    put_bytes(out, &m.bytes);
}

fn decode_base_backup_chunk(r: &mut Reader<'_>) -> IoResult<BaseBackupChunk> {
    Ok(BaseBackupChunk {
        path: r.get_str()?,
        offset: r.get_u64()?,
        eof: r.get_u8()? != 0,
        sha256: r.get_array_32()?,
        bytes: r.get_bytes()?,
    })
}

fn encode_base_backup_end(m: &BaseBackupEnd, out: &mut Vec<u8>) {
    put_u64(out, m.backup_lsn);
}

fn decode_base_backup_end(r: &mut Reader<'_>) -> IoResult<BaseBackupEnd> {
    Ok(BaseBackupEnd {
        backup_lsn: r.get_u64()?,
    })
}

// -------------------------- Primitive codecs --------------------------

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn require(&self, n: usize) -> IoResult<()> {
        if n > self.buf.len().saturating_sub(self.pos) {
            return Err(Error::new(ErrorKind::UnexpectedEof, "truncated payload"));
        }
        Ok(())
    }

    #[inline]
    fn get_word<const N: usize>(&mut self) -> IoResult<[u8; N]> {
        crate::util::bytes::read_word(self.buf, &mut self.pos)
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "truncated payload"))
    }

    fn get_u8(&mut self) -> IoResult<u8> {
        self.get_word::<1>().map(|[byte]| byte)
    }

    fn get_u32(&mut self) -> IoResult<u32> {
        self.get_word().map(u32::from_le_bytes)
    }

    fn get_u64(&mut self) -> IoResult<u64> {
        self.get_word().map(u64::from_le_bytes)
    }

    fn get_uuid(&mut self) -> IoResult<[u8; 16]> {
        self.get_word()
    }

    fn get_array_32(&mut self) -> IoResult<[u8; 32]> {
        self.get_word()
    }

    fn get_bytes(&mut self) -> IoResult<Vec<u8>> {
        let len = self.get_u32()? as usize;
        if len > MAX_PAYLOAD_BYTES {
            return Err(Error::new(ErrorKind::InvalidData, "bytes length too large"));
        }
        self.require(len)?;
        let v = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    fn get_str(&mut self) -> IoResult<String> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip(msg: &Message) -> Message {
        let mut buf = Vec::new();
        write_frame(&mut buf, msg).unwrap();
        let mut cur = Cursor::new(&buf);
        read_frame(&mut cur).unwrap()
    }

    #[test]
    fn primitive_readers_preserve_values_and_cursor_on_truncation() {
        let bytes: Vec<u8> = (0..65).collect();
        for len in 0..=bytes.len() {
            for start in 0..=len {
                for width in [1, 4, 8, 16, 32] {
                    let mut reader = Reader {
                        buf: &bytes[..len],
                        pos: start,
                    };
                    let result = match width {
                        1 => reader.get_u8().map(|v| vec![v]),
                        4 => reader.get_u32().map(|v| v.to_le_bytes().to_vec()),
                        8 => reader.get_u64().map(|v| v.to_le_bytes().to_vec()),
                        16 => reader.get_uuid().map(|v| v.to_vec()),
                        32 => reader.get_array_32().map(|v| v.to_vec()),
                        _ => unreachable!(),
                    };
                    if start + width <= len {
                        assert_eq!(result.unwrap(), bytes[start..start + width]);
                        assert_eq!(reader.pos, start + width);
                    } else {
                        let error = result.unwrap_err();
                        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
                        assert_eq!(error.to_string(), "truncated payload");
                        assert_eq!(reader.pos, start);
                    }
                }
            }
        }
        let prefix = 5u32.to_le_bytes();
        let mut reader = Reader::new(&prefix);
        assert_eq!(
            reader.get_bytes().unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        assert_eq!(reader.pos, 4);
    }

    #[test]
    fn handshake_round_trip() {
        let msg = Message::Handshake(Handshake {
            auth_token: crate::repl::config::ReplConfig::test_defaults().auth_token,
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: [1; 16],
            requested_start_lsn: 12345,
            last_known_master_uuid: [2; 16],
            last_known_epoch: 7,
            cluster_uuid: [3; 16],
            last_known_timeline_uuid: [4; 16],
            client_version: "mydb-0.1.0".to_string(),
            request_base_backup: true,
            max_chunk_bytes: 256 * 1024,
            format_compatibility: "formats-v2".into(),
        });
        assert_eq!(round_trip(&msg), msg);
    }

    #[test]
    fn protocol_v2_rejects_truncated_v1_handshake() {
        let mut payload = Vec::new();
        super::encode_handshake(
            &Handshake {
                auth_token: crate::repl::config::ReplConfig::test_defaults().auth_token,
                protocol_version: PROTOCOL_VERSION,
                replica_uuid: [3; 16],
                requested_start_lsn: 1,
                last_known_master_uuid: [0; 16],
                last_known_epoch: 0,
                cluster_uuid: [0; 16],
                last_known_timeline_uuid: [0; 16],
                client_version: "old-replica".into(),
                request_base_backup: false,
                max_chunk_bytes: 1024 * 1024,
                format_compatibility: "formats-v2".into(),
            },
            &mut payload,
        );
        payload.truncate(payload.len().saturating_sub("formats-v2".len() + 4));
        assert!(Message::decode_payload(MsgType::Handshake, &payload).is_err());
    }

    #[test]
    fn base_backup_messages_round_trip() {
        let start = Message::BaseBackupStart(BaseBackupStart {
            backup_lsn: 1000,
            files: vec![
                BaseBackupFile {
                    path: "schemas/test/tables/t__2__data.heap".into(),
                    len: 16384,
                },
                BaseBackupFile {
                    path: "schemas/test/indexes/t__2__pk.idx".into(),
                    len: 32768,
                },
            ],
        });
        let chunk = Message::BaseBackupChunk(BaseBackupChunk {
            path: "schemas/test/tables/t__2__data.heap".into(),
            offset: 0,
            bytes: vec![1, 2, 3, 4, 5],
            eof: false,
            sha256: [0; 32],
        });
        let end = Message::BaseBackupEnd(BaseBackupEnd { backup_lsn: 1000 });
        for m in [start, chunk, end] {
            assert_eq!(round_trip(&m), m);
        }
    }

    #[test]
    fn handshake_ack_round_trip() {
        let msg = Message::HandshakeAck(HandshakeAck {
            heartbeat_ms: 1000,
            master_uuid: [9; 16],
            current_lsn: 99_999,
            durable_lsn: 99_990,
            min_available_lsn: 12,
            epoch: 3,
            server_version: "mydb-server-0.1".to_string(),
            accepted: false,
            reason: "WAL truncated past requested LSN".to_string(),
            format_compatibility: "formats-v2".into(),
            semi_sync_required: true,
            cluster_uuid: [8; 16],
            timeline_uuid: [7; 16],
            generation: 4,
            source_frozen: true,
            fence_lsn: 99_000,
            fence_token_hash: "fence".into(),
        });
        assert_eq!(round_trip(&msg), msg);
    }

    #[test]
    fn wal_chunk_round_trip_with_binary_payload() {
        let mut frames = Vec::with_capacity(2048);
        for i in 0..2048u16 {
            frames.push((i & 0xff) as u8);
        }
        let msg = Message::WalChunk(WalChunk {
            start_lsn: 100,
            end_lsn: 200,
            frame_count: 4,
            frames_bytes: frames,
        });
        assert_eq!(round_trip(&msg), msg);
    }

    #[test]
    fn heartbeat_replica_ack_resync_close_round_trip() {
        let msgs = vec![
            Message::Heartbeat(Heartbeat {
                master_lsn: 555,
                durable_lsn: 550,
                master_time_ms: 1_700_000_000_000,
                timeline_uuid: [7; 16],
                generation: 3,
                source_frozen: true,
                fence_lsn: 540,
                fence_token_hash: "hash".into(),
            }),
            Message::ReplicaAck(ReplicaAck {
                received_lsn: 555,
                durable_lsn: 550,
                applied_lsn: 540,
                time_ms: 1_700_000_001_000,
            }),
            Message::Resync(Resync {
                new_start_lsn: 4096,
                reason: "WAL truncated".to_string(),
            }),
            Message::Close(Close {
                reason: "shutdown".to_string(),
            }),
        ];
        for m in msgs {
            assert_eq!(round_trip(&m), m);
        }
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut buf = Vec::new();
        let msg = Message::Heartbeat(Heartbeat {
            master_lsn: 1,
            durable_lsn: 1,
            master_time_ms: 2,
            timeline_uuid: [0; 16],
            generation: 0,
            source_frozen: false,
            fence_lsn: 0,
            fence_token_hash: String::new(),
        });
        write_frame(&mut buf, &msg).unwrap();
        buf[0] = 0x00;
        let err = read_frame(&mut Cursor::new(&buf)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn crc_corruption_is_rejected() {
        let mut buf = Vec::new();
        let msg = Message::Heartbeat(Heartbeat {
            master_lsn: 1,
            durable_lsn: 1,
            master_time_ms: 2,
            timeline_uuid: [0; 16],
            generation: 0,
            source_frozen: false,
            fence_lsn: 0,
            fence_token_hash: String::new(),
        });
        write_frame(&mut buf, &msg).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        let err = read_frame(&mut Cursor::new(&buf)).unwrap_err();
        assert!(err.to_string().contains("CRC"));
    }

    #[test]
    fn payload_corruption_is_rejected_by_crc() {
        let mut buf = Vec::new();
        let msg = Message::ReplicaAck(ReplicaAck {
            received_lsn: 100,
            durable_lsn: 95,
            applied_lsn: 90,
            time_ms: 1,
        });
        write_frame(&mut buf, &msg).unwrap();
        // Flip a bit inside the payload (offset 9 = first payload byte).
        buf[9] ^= 0x01;
        let err = read_frame(&mut Cursor::new(&buf)).unwrap_err();
        assert!(err.to_string().contains("CRC"));
    }

    #[test]
    fn unknown_msg_type_rejected() {
        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &Message::Heartbeat(Heartbeat {
                master_lsn: 0,
                durable_lsn: 0,
                master_time_ms: 0,
                timeline_uuid: [0; 16],
                generation: 0,
                source_frozen: false,
                fence_lsn: 0,
                fence_token_hash: String::new(),
            }),
        )
        .unwrap();
        buf[4] = 99; // unknown type
                     // CRC will also be wrong; either error is acceptable as long as
                     // we don't decode a Heartbeat as something else.
        let err = read_frame(&mut Cursor::new(&buf)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &Message::Heartbeat(Heartbeat {
                master_lsn: 0,
                durable_lsn: 0,
                master_time_ms: 0,
                timeline_uuid: [0; 16],
                generation: 0,
                source_frozen: false,
                fence_lsn: 0,
                fence_token_hash: String::new(),
            }),
        )
        .unwrap();
        buf.truncate(buf.len() - 2);
        let err = read_frame(&mut Cursor::new(&buf)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn async_round_trip_handshake_and_chunk() {
        use tokio::io::duplex;

        let (mut client, mut server) = duplex(64 * 1024);
        let handshake = Message::Handshake(Handshake {
            auth_token: crate::repl::config::ReplConfig::test_defaults().auth_token,
            protocol_version: PROTOCOL_VERSION,
            replica_uuid: [5; 16],
            requested_start_lsn: 42,
            last_known_master_uuid: [0; 16],
            last_known_epoch: 0,
            cluster_uuid: [0; 16],
            last_known_timeline_uuid: [0; 16],
            client_version: "async-test".to_string(),
            request_base_backup: false,
            max_chunk_bytes: 64 * 1024,
            format_compatibility: "formats-v2".into(),
        });
        let chunk = Message::WalChunk(WalChunk {
            start_lsn: 42,
            end_lsn: 100,
            frame_count: 1,
            frames_bytes: vec![1, 2, 3, 4, 5],
        });

        let chunk_clone = chunk.clone();
        let handshake_clone = handshake.clone();
        let writer = tokio::spawn(async move {
            write_frame_async(&mut client, &handshake_clone)
                .await
                .unwrap();
            write_frame_async(&mut client, &chunk_clone).await.unwrap();
        });

        let h = read_frame_async(&mut server).await.unwrap();
        let c = read_frame_async(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(h, handshake);
        assert_eq!(c, chunk);
    }
}

#[cfg(test)]
#[test]
fn audit_manifest_count_is_bounded_before_allocation() {
    for count in [1, 1_000_000, u32::MAX] {
        let mut payload = Vec::new();
        put_u64(&mut payload, 42);
        put_u32(&mut payload, count);
        let err = decode_base_backup_start(&mut Reader::new(&payload)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }
    let mut payload = Vec::new();
    encode_base_backup_start(
        &BaseBackupStart {
            backup_lsn: 42,
            files: vec![BaseBackupFile {
                path: "schemas/test/x".into(),
                len: 17,
            }],
        },
        &mut payload,
    );
    assert_eq!(
        decode_base_backup_start(&mut Reader::new(&payload))
            .unwrap()
            .files
            .len(),
        1
    );
}

#[cfg(test)]
mod framing_tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    #[derive(Default)]
    struct FragmentedWriter {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl AsyncWrite for FragmentedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<IoResult<usize>> {
            let count = bytes.len().min(7);
            self.bytes.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }
        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IoResult<()>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IoResult<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // Deliberately independent of the frame helpers: this is the existing
    // wire layout, built from the unchanged per-message payload codec.
    fn reference_frame(message: &Message) -> Vec<u8> {
        let mut payload = Vec::new();
        message.encode_payload(&mut payload);
        let mut frame = b"REPL".to_vec();
        frame.push(message.msg_type() as u8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&crc32fast::hash(&frame[4..]).to_le_bytes());
        frame
    }

    fn ack() -> Message {
        Message::ReplicaAck(ReplicaAck {
            received_lsn: 1,
            durable_lsn: 2,
            applied_lsn: 3,
            time_ms: 4,
        })
    }

    #[tokio::test]
    async fn frame_writers_preserve_wire_bytes_partial_writes_and_flush_boundaries() {
        let messages = [
            ack(),
            Message::Close(Close {
                reason: String::new(),
            }),
            Message::Resync(Resync {
                new_start_lsn: u64::MAX,
                reason: "同步恢复🙂".repeat(40),
            }),
            Message::WalChunk(WalChunk {
                start_lsn: u64::MAX - 1,
                end_lsn: u64::MAX,
                frame_count: u32::MAX,
                frames_bytes: (0..65536).map(|value| value as u8).collect(),
            }),
            Message::BaseBackupChunk(BaseBackupChunk {
                path: "schemas/测试/t.heap".into(),
                offset: u64::MAX,
                bytes: (0..4096).map(|value| value as u8).collect(),
                eof: true,
                sha256: [0xA5; 32],
            }),
        ];
        for message in messages {
            let expected = reference_frame(&message);
            let mut writer = FragmentedWriter::default();
            write_frame_async(&mut writer, &message).await.unwrap();
            assert!(writer.bytes == expected, "async {:?}", message.msg_type());
            assert_eq!(writer.flushes, 0);
            let mut sync_bytes = Vec::new();
            write_frame(&mut sync_bytes, &message).unwrap();
            assert!(sync_bytes == expected, "sync {:?}", message.msg_type());
            assert!(read_frame_async(&mut expected.as_slice()).await.unwrap() == message);
        }
    }

    #[tokio::test]
    async fn frame_readers_agree_on_corrupt_and_truncated_inputs() {
        let valid = reference_frame(&ack());
        let mut malformed = Vec::new();
        for cut in 0..valid.len() {
            malformed.push(valid[..cut].to_vec());
        }
        for index in [0, 4, 9, valid.len() - 1] {
            let mut bytes = valid.clone();
            bytes[index] ^= 0xff;
            malformed.push(bytes);
        }
        let mut oversized = valid[..FRAME_HEADER_LEN].to_vec();
        oversized[5..9].copy_from_slice(&((MAX_PAYLOAD_BYTES + 1) as u32).to_le_bytes());
        malformed.push(oversized);
        let mut trailing = valid.clone();
        trailing.insert(trailing.len() - FRAME_TRAILER_LEN, 42);
        let payload_len = trailing.len() - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
        trailing[5..9].copy_from_slice(&(payload_len as u32).to_le_bytes());
        let crc_pos = trailing.len() - FRAME_TRAILER_LEN;
        let crc = crc32fast::hash(&trailing[4..crc_pos]);
        trailing[crc_pos..].copy_from_slice(&crc.to_le_bytes());
        malformed.push(trailing);
        for bytes in malformed {
            let sync_error = read_frame(&mut bytes.as_slice()).unwrap_err();
            let async_error = read_frame_async(&mut bytes.as_slice()).await.unwrap_err();
            assert_eq!(sync_error.kind(), async_error.kind());
            if sync_error.kind() != ErrorKind::UnexpectedEof {
                assert_eq!(sync_error.to_string(), async_error.to_string());
            }
        }
        let header = encode_header(MsgType::Close as u8, MAX_PAYLOAD_BYTES).unwrap();
        assert_eq!(
            decode_header(&header).unwrap(),
            (MsgType::Close, MAX_PAYLOAD_BYTES)
        );
        assert_eq!(
            encode_header(MsgType::Close as u8, MAX_PAYLOAD_BYTES + 1)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[cfg(test)]
mod release_audit_tests {
    use super::*;
    #[tokio::test]
    async fn oversized_wal_header_is_rejected_before_body_read() {
        let header = encode_header(MsgType::WalChunk as u8, 1024 * 1024).unwrap();
        let mut input = &header[..];
        let err = read_frame_async_limited(&mut input, 4096)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("receive limit"));
    }
    #[tokio::test]
    async fn wide_wal_frame_keeps_extra_memory_reserved_through_apply() {
        let mut encoded = Vec::new();
        write_frame(
            &mut encoded,
            &Message::WalChunk(WalChunk {
                start_lsn: 1,
                end_lsn: 1,
                frame_count: 1,
                frames_bytes: vec![7; 8192],
            }),
        )
        .unwrap();
        let (message, memory) = read_frame_async_window(&mut &encoded[..], 4096)
            .await
            .unwrap();
        assert!(matches!(message, Message::WalChunk(chunk) if chunk.frames_bytes.len() == 8192));
        assert!(
            memory.is_some(),
            "a wide physical frame must retain its extra reservation"
        );
        let header = encode_header(MsgType::BaseBackupStart as u8, MAX_PAYLOAD_BYTES).unwrap();
        let err = read_frame_async_window(&mut &header[..], 4096)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn reader_length_overflow_is_rejected() {
        let mut reader = Reader::new(&[0, 1]);
        reader.pos = 1;
        assert!(reader.require(usize::MAX).is_err());
    }
}
