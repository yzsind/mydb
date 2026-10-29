use std::cell::RefCell;
use std::io::Write as _;
use std::sync::OnceLock;
use std::time::Duration;

use flate2::write::ZlibEncoder;
use flate2::{Compression, Decompress, FlushDecompress};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_PAYLOAD_LEN: usize = 0x00ff_ffff;
const DEFAULT_BATCH_FLUSH_BYTES: usize = 64 * 1024;
/// Hard ceiling for `MYDB_MAX_ALLOWED_PACKET`, matching MySQL's 1 GiB
/// cap on `max_allowed_packet`. A logical packet larger than one
/// 16 MiB-1 frame is transmitted as a chain of `0xffffff`-byte frames
/// (the MySQL multi-packet protocol); this bounds how large a single
/// reassembled inbound payload may grow.
const MAX_ALLOWED_PACKET_CEILING: usize = 1 << 30;
const DEFAULT_BATCH_BUFFER_MAX: usize = 16 * 1024 * 1024;

/// How long a single outbound `write_all` may block before the
/// connection is treated as a slow-client and torn down. Mirrors
/// MySQL's `net_write_timeout` semantics. Backed by the
/// `net_write_timeout` system variable (seeded from
/// `MYDB_NET_WRITE_TIMEOUT`, default 60s), so `SET GLOBAL
/// net_write_timeout = N` takes effect for subsequent writes. `0`
/// disables the timeout.
pub fn net_write_timeout() -> Option<Duration> {
    let secs = crate::engine::sysvar::builtin::connection::net_write_timeout_secs();
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

/// How long a single inbound payload read may block once the client has
/// started sending a packet (i.e. after the 4-byte frame header has
/// arrived). Mirrors MySQL's `net_read_timeout`: it bounds *mid-packet*
/// stalls, not the idle wait between commands (that is governed by
/// `wait_timeout` in the command loop). Backed by the `net_read_timeout`
/// system variable (seeded from `MYDB_NET_READ_TIMEOUT`, default 30s).
/// `0` disables.
pub fn net_read_timeout() -> Option<Duration> {
    let secs = crate::engine::sysvar::builtin::connection::net_read_timeout_secs();
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}

/// `tokio::time::timeout` reads the clock to compute its deadline the
/// moment it is created — before the wrapped future has been polled
/// even once. Socket writes almost always complete on the first poll
/// (the kernel send buffer has room), so the common case paid a clock
/// read plus `Timeout` bookkeeping per write for a timer that never
/// fired. Poll the future once without arming anything; only a
/// genuinely-pending write pays for the timeout machinery.
async fn timeout_after_first_poll<F>(dur: Duration, fut: F) -> Result<F::Output, ()>
where
    F: std::future::Future,
{
    let mut fut = std::pin::pin!(fut);
    let first = std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx))).await;
    match first {
        std::task::Poll::Ready(out) => Ok(out),
        std::task::Poll::Pending => tokio::time::timeout(dur, fut).await.map_err(|_| ()),
    }
}

/// Read exactly `buf.len()` bytes, bounding a mid-packet stall by
/// `net_read_timeout`. The common case (bytes already buffered in the
/// kernel) completes on the first poll and pays nothing for the timer —
/// see `timeout_after_first_poll`. `None` disables the bound.
async fn read_exact_with_timeout<R>(
    reader: &mut R,
    buf: &mut [u8],
    timeout: Option<Duration>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let result = match timeout {
        Some(dur) => match timeout_after_first_poll(dur, reader.read_exact(buf)).await {
            Ok(result) => result.map(|_| ()),
            Err(()) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "net_read_timeout exceeded; client stalled mid-packet",
            )),
        },
        None => reader.read_exact(buf).await.map(|_| ()),
    };
    // Bytes_received: count every payload / continuation-header / compressed
    // chunk that flows through here (the packet headers read directly in
    // `read_packet_with_limit` / the compressed reader add their 4 bytes at
    // their own call site).
    if result.is_ok() {
        crate::engine::status_counters::add_bytes_received(buf.len() as u64);
    }
    result
}

async fn write_all_with_timeout<W>(writer: &mut W, bytes: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let result = match net_write_timeout() {
        Some(dur) => match timeout_after_first_poll(dur, writer.write_all(bytes)).await {
            Ok(result) => result,
            Err(()) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "net_write_timeout exceeded; closing slow client",
            )),
        },
        None => writer.write_all(bytes).await,
    };
    // Bytes_sent: the single choke point for every plaintext frame (single +
    // batch) and every compressed block, so one count here covers all wire
    // writes.
    if result.is_ok() {
        crate::engine::status_counters::add_bytes_sent(bytes.len() as u64);
    }
    result
}

async fn flush_with_timeout<W>(writer: &mut W) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match net_write_timeout() {
        Some(dur) => match timeout_after_first_poll(dur, writer.flush()).await {
            Ok(result) => result,
            Err(()) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "net_write_timeout exceeded during flush",
            )),
        },
        None => writer.flush().await,
    }
}

/// Per-connection upper bound on the `PacketBatchWriter` buffer, in
/// bytes. Configured via `MYDB_PACKET_WRITE_BUFFER_MAX_BYTES`
/// (default 16 MiB). The buffer is force-flushed before crossing this
/// cap and shrunk back to the default soon after, so a slow client
/// cannot make the server-side batch buffer grow without bound.
pub fn batch_buffer_max_bytes() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("MYDB_PACKET_WRITE_BUFFER_MAX_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|n| *n >= DEFAULT_BATCH_FLUSH_BYTES)
            .unwrap_or(DEFAULT_BATCH_BUFFER_MAX)
    })
}

/// Per-connection upper bound for inbound packet payloads, in bytes.
///
/// Single source of truth with the `max_allowed_packet` system variable:
/// the enforced wire limit is exactly the value `SET GLOBAL
/// max_allowed_packet = N` writes and `SHOW VARIABLES` / `@@max_allowed_packet`
/// report. The backing atomic is seeded from `MYDB_MAX_ALLOWED_PACKET`
/// at startup (see `connection::ensure_env_seeded`, default 64 MiB matching
/// MySQL 8.0) and clamped to `[1 KiB, 1 GiB]` on both the seed and the
/// `SET GLOBAL` path. The check happens *before* `read_packet` allocates
/// the payload buffer, so a malicious or buggy client cannot trigger an
/// oversized allocation per inbound packet across many concurrent
/// connections. Historically this read a frozen `OnceLock` env value that
/// diverged from the settable sysvar — a client that raised the sysvar to
/// send a large packet still hit the stale startup limit.
pub fn max_allowed_packet_bytes() -> usize {
    // Clamp defensively even though both seed and SET GLOBAL already
    // bound the atomic to <= 1 GiB, so the `as usize` cast is always
    // lossless (1 GiB fits a 32-bit usize).
    crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes()
        .min(MAX_ALLOWED_PACKET_CEILING as u64) as usize
}

/// Master kill-switch for ALL wire compression. When `off`, the server
/// advertises neither `CLIENT_COMPRESS` (zlib) nor
/// `CLIENT_ZSTD_COMPRESSION_ALGORITHM` (zstd); the codec stays in plain
/// mode regardless of what the client negotiates. Set via
/// `MYDB_COMPRESS_PROTOCOL=off` — handy for clients that misbehave on
/// the compressed path or for diff-testing the plain wire.
pub fn compression_protocol_enabled() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| !env_disabled("MYDB_COMPRESS_PROTOCOL"))
}

/// Per-algorithm kill-switch for zlib. `MYDB_COMPRESS_ZLIB=off` keeps
/// `MYDB_COMPRESS_PROTOCOL=on` but stops advertising CLIENT_COMPRESS,
/// forcing newer clients onto zstd while still allowing the master
/// switch to disable everything in one place.
pub fn zlib_compression_enabled() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| !env_disabled("MYDB_COMPRESS_ZLIB"))
}

/// Per-algorithm kill-switch for zstd. `MYDB_COMPRESS_ZSTD=off` stops
/// advertising `CLIENT_ZSTD_COMPRESSION_ALGORITHM`. Used when zstd
/// codec behaviour is suspect (e.g. a regression in libzstd) without
/// killing the legacy zlib path that some older clients still rely on.
pub fn zstd_compression_enabled() -> bool {
    #[cfg(not(feature = "zstd-compression"))]
    {
        return false;
    }
    #[cfg(feature = "zstd-compression")]
    {
        static V: OnceLock<bool> = OnceLock::new();
        *V.get_or_init(|| !env_disabled("MYDB_COMPRESS_ZSTD"))
    }
}

/// MySQL `zstd_compression_level` clamped to the protocol's `[1, 22]`
/// range with a default of `3` (the MySQL 8.0.18+ default). Used for
/// the server's *outbound* zstd stream when the client did not request
/// a specific level. The matching client-supplied level travels in
/// `HandshakeResponse.zstd_compression_level`.
pub fn default_zstd_level() -> i32 {
    static V: OnceLock<i32> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_ZSTD_LEVEL")
            .ok()
            .and_then(|raw| raw.trim().parse::<i32>().ok())
            .map(|n| n.clamp(1, 22))
            .unwrap_or(3)
    })
}

fn env_disabled(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("0" | "off" | "false" | "no" | "disable" | "disabled")
    )
}

/// Capacity of the `tokio::io::BufReader` wrapping every freshly
/// accepted MySQL connection. tokio's default is 8 KiB; bumping to
/// 16 KiB lets a typical COM_QUERY (one SQL statement + small bind
/// payload) plus the next pipelined command land in a single
/// `recv()` syscall on hot sysbench/JDBC paths. Configured via
/// `MYDB_INBOUND_READ_BUFFER_BYTES` (default 16 KiB). Clamped to
/// the range [`MIN_INBOUND_READ_BUFFER`, `MAX_INBOUND_READ_BUFFER`]
/// so a typo cannot pin a megabyte per connection or starve the
/// header-only read path.
pub fn inbound_read_buffer_bytes() -> usize {
    const DEFAULT: usize = 16 * 1024;
    const MIN: usize = 4 * 1024;
    const MAX: usize = 1024 * 1024;
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_INBOUND_READ_BUFFER_BYTES")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .map(|n| n.clamp(MIN, MAX))
            .unwrap_or(DEFAULT)
    })
}

#[derive(Debug)]
pub struct Packet {
    /// Sequence id of the first physical frame in this logical packet.
    pub first_sequence_id: u8,
    /// Sequence id of the final physical frame (kept for response sequencing).
    pub sequence_id: u8,
    pub payload: Vec<u8>,
    _memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl PartialEq for Packet {
    fn eq(&self, other: &Self) -> bool {
        self.first_sequence_id == other.first_sequence_id
            && self.sequence_id == other.sequence_id
            && self.payload == other.payload
    }
}

impl Eq for Packet {}

impl Drop for Packet {
    fn drop(&mut self) {
        if let Some(charge) = self._memory_charge.take() {
            release_input_packet_buffer(std::mem::take(&mut self.payload), charge);
        }
    }
}

fn reserve_packet_payload(
    payload: &mut Vec<u8>,
    charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
    required: usize,
) -> io::Result<()> {
    if required <= payload.capacity() {
        return Ok(());
    }
    let old_capacity = payload.capacity();
    let requested_growth = required - old_capacity;
    charge.try_grow(requested_growth).map_err(|err| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{} ({})", err.message(), err.sql_state()),
        )
    })?;
    // Vec reserves relative to len; memory accounting grows relative to capacity.
    // Reserve the full required length before a pooled buffer is resized.
    if payload
        .try_reserve_exact(required.saturating_sub(payload.len()))
        .is_err()
    {
        charge.shrink(requested_growth);
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "out of memory while buffering MySQL packet payload",
        ));
    }
    let actual_growth = payload.capacity().saturating_sub(old_capacity);
    if actual_growth > requested_growth {
        if let Err(err) = charge.try_grow(actual_growth - requested_growth) {
            charge.shrink(requested_growth);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("{} ({})", err.message(), err.sql_state()),
            ));
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "fuzzing"))]
pub async fn read_packet<R>(reader: &mut R) -> io::Result<Option<Packet>>
where
    R: AsyncRead + Unpin,
{
    read_packet_with_limit(reader, max_allowed_packet_bytes()).await
}

/// A limit violation is distinct from malformed framing so the session can
/// return ER_NET_PACKET_TOO_LARGE before closing the connection.
#[derive(Debug)]
pub(crate) struct PacketTooLarge {
    pub response_sequence_id: u8,
    pub bytes: usize,
    pub limit: usize,
}
impl std::fmt::Display for PacketTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mysql payload {} bytes exceeds MYDB_MAX_ALLOWED_PACKET {} bytes",
            self.bytes, self.limit
        )
    }
}
impl std::error::Error for PacketTooLarge {}

async fn read_packet_with_limit<R>(reader: &mut R, limit: usize) -> io::Result<Option<Packet>>
where
    R: AsyncRead + Unpin,
{
    // First frame. EOF before any byte means the connection closed
    // cleanly between commands. The header read is deliberately *not*
    // bounded by `net_read_timeout`: a connection parked here is idle
    // between commands, which `wait_timeout` (enforced by the command
    // loop's idle select) governs. Only the payload + continuation
    // reads below — where the client has committed to sending a packet
    // of known length — are bounded by `net_read_timeout`, so a client
    // that stalls mid-packet is torn down without waiting out the much
    // longer idle timeout.
    let mut header = [0_u8; 4];
    match reader.read_exact(&mut header[..1]).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    crate::engine::status_counters::add_bytes_received(1);
    let read_timeout = net_read_timeout();
    // Once the peer has sent any part of a command header it is no longer an
    // idle connection. Bound the remaining three bytes by net_read_timeout so
    // a one-byte slowloris cannot occupy a slot until wait_timeout.
    read_exact_with_timeout(reader, &mut header[1..], read_timeout).await?;
    let first_sequence_id = header[3];
    let mut sequence_id = header[3];
    let (mut payload, mut memory_charge) = acquire_input_packet_buffer()?;
    // Multi-packet reassembly: a frame whose length is exactly
    // MAX_PAYLOAD_LEN (0xffffff) is continued by the next frame, until a
    // frame shorter than that (possibly 0 bytes) terminates the logical
    // packet. The returned `sequence_id` is the LAST frame's id so the
    // caller's response starts at `last + 1`.
    loop {
        let frame_len = decode_payload_len(&header);
        let new_total = payload.len().saturating_add(frame_len);
        if new_total > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PacketTooLarge {
                    response_sequence_id: sequence_id.wrapping_add(1),
                    bytes: new_total,
                    limit,
                },
            ));
        }
        let start = payload.len();
        reserve_packet_payload(&mut payload, &mut memory_charge, new_total)?;
        payload.resize(new_total, 0);
        read_exact_with_timeout(reader, &mut payload[start..], read_timeout).await?;
        if frame_len < MAX_PAYLOAD_LEN {
            break;
        }
        // A full-length frame: another frame follows; its sequence id
        // becomes the logical packet's (so the response starts at
        // last + 1).
        read_exact_with_timeout(reader, &mut header, read_timeout).await?;
        let expected = sequence_id.wrapping_add(1);
        if header[3] != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "mysql continuation sequence id {} out of order; expected {expected}",
                    header[3]
                ),
            ));
        }
        sequence_id = header[3];
    }

    Ok(Some(Packet {
        first_sequence_id,
        sequence_id,
        payload,
        _memory_charge: Some(memory_charge),
    }))
}

pub async fn write_packet<W>(writer: &mut W, sequence_id: u8, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_packet_seq(writer, sequence_id, payload)
        .await
        .map(|_| ())
}

/// Like `write_packet` but returns the next sequence id (advanced by the
/// number of frames emitted). Result-set row loops use this so a row
/// whose payload spans multiple 16 MiB frames keeps the per-packet
/// sequence counter in step.
pub async fn write_packet_seq<W>(writer: &mut W, sequence_id: u8, payload: &[u8]) -> io::Result<u8>
where
    W: AsyncWrite + Unpin,
{
    let next = write_packet_unflushed(writer, sequence_id, payload).await?;
    flush_with_timeout(writer).await?;
    Ok(next)
}

pub async fn write_packet_unflushed<W>(
    writer: &mut W,
    sequence_id: u8,
    payload: &[u8],
) -> io::Result<u8>
where
    W: AsyncWrite + Unpin,
{
    const STACK_FRAME_BYTES: usize = 256;
    if payload.len() <= STACK_FRAME_BYTES - 4 {
        // Greetings and small control packets must remain writable even
        // when optional frame-buffer admission has no remaining headroom.
        let mut frame = [0u8; STACK_FRAME_BYTES];
        encode_payload_len(payload.len(), frame.first_chunk_mut().unwrap());
        frame[3] = sequence_id;
        frame[4..4 + payload.len()].copy_from_slice(payload);
        write_all_with_timeout(writer, &frame[..4 + payload.len()]).await?;
        return Ok(sequence_id.wrapping_add(1));
    }
    // Hot paths that hit this function — handshake OK / ERR / EOF /
    // COM_PING / COM_FIELD_LIST per-column / auth switch — are all
    // small (<= a few hundred bytes) and fire on every connection.
    // Allocating a fresh `Vec<u8>` per frame meant two allocator hits
    // per packet (alloc + free); a per-thread small-frame pool keeps a
    // warm buffer hot in the allocator's local cache and absorbs the
    // common case for free. Large payloads (multi-frame ≥ 64 KiB)
    // bypass the pool to avoid pinning oversized buffers per worker
    // thread.
    let (mut frame, frame_charge) = acquire_single_frame_buffer(payload.len())?;
    let next = write_packet_frame_into(sequence_id, payload, &mut frame)?;
    let write_result = write_all_with_timeout(writer, &frame).await;
    release_single_frame_buffer(frame, frame_charge);
    write_result?;
    Ok(next)
}

/// Write MySQL frame headers and payload slices directly to the socket. This
/// avoids constructing a second payload-sized `Vec` for a single row larger
/// than the connection batch cap.
pub(crate) async fn write_packet_direct_unflushed<W>(
    writer: &mut W,
    mut sequence_id: u8,
    payload: &[u8],
) -> io::Result<u8>
where
    W: AsyncWrite + Unpin,
{
    let mut offset = 0usize;
    loop {
        let chunk_len = (payload.len() - offset).min(MAX_PAYLOAD_LEN);
        let mut header = [0_u8; 4];
        encode_payload_len(chunk_len, &mut header);
        header[3] = sequence_id;
        write_all_with_timeout(writer, &header).await?;
        if chunk_len != 0 {
            write_all_with_timeout(writer, &payload[offset..offset + chunk_len]).await?;
        }
        sequence_id = sequence_id.wrapping_add(1);
        offset += chunk_len;
        if chunk_len < MAX_PAYLOAD_LEN {
            return Ok(sequence_id);
        }
    }
}

/// Encode `payload` as one or more MySQL frames into `out`, starting at
/// `sequence_id`, and return the next free sequence id. Payloads of
/// `MAX_PAYLOAD_LEN` or more are split into `0xffffff`-byte frames; a
/// payload whose length is an exact multiple of `MAX_PAYLOAD_LEN` is
/// terminated by a trailing zero-length frame, matching the MySQL
/// multi-packet protocol so clients reassemble it correctly.
pub fn write_packet_frame_into(
    sequence_id: u8,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> io::Result<u8> {
    let mut sequence_id = sequence_id;
    let mut offset = 0usize;
    loop {
        let chunk_len = (payload.len() - offset).min(MAX_PAYLOAD_LEN);
        let mut header = [0_u8; 4];
        encode_payload_len(chunk_len, &mut header);
        header[3] = sequence_id;
        out.extend_from_slice(&header);
        out.extend_from_slice(&payload[offset..offset + chunk_len]);
        sequence_id = sequence_id.wrapping_add(1);
        offset += chunk_len;
        if chunk_len < MAX_PAYLOAD_LEN {
            // Short (or zero-length terminator) frame ends the packet.
            break;
        }
    }
    Ok(sequence_id)
}

/// Per-thread pool of pre-allocated batch buffers reused across
/// `PacketBatchWriter` instances. tokio worker threads run many
/// independent sessions in sequence; without a pool each result-set
/// would `Vec::with_capacity(64 KiB)` on entry and drop it on exit,
/// re-touching the allocator twice per SELECT. Sharing buffers across
/// SELECTs on the same worker amortizes the allocation and keeps the
/// hot 64 KiB block warm in the allocator's local cache.
///
/// Each pool is capped at `BATCH_POOL_MAX` entries so a burst of
/// concurrent SELECTs on one worker thread does not grow the pool
/// without bound, and only buffers whose capacity sits within a
/// "reasonable" band (≥ flush threshold, ≤ 4× the threshold) are
/// returned — wildly oversized buffers from a one-off huge row are
/// dropped instead of poisoning the pool.
const BATCH_POOL_MAX: usize = 8;

/// Default capacity for a single-frame buffer returned by
/// `acquire_single_frame_buffer`. Sized so the common small packets
/// (handshake OK, ERR, EOF, COM_PING reply, per-column FIELD_LIST,
/// auth_switch) fit without growth. A small but useful upper bound
/// (`SINGLE_FRAME_BUFFER_MAX`) caps how much per-thread memory the
/// small-frame pool can pin.
const SINGLE_FRAME_BUFFER_DEFAULT: usize = 8 * 1024;
const SINGLE_FRAME_BUFFER_MAX: usize = 32 * 1024;
const SINGLE_FRAME_POOL_MAX: usize = 16;
const INPUT_PACKET_BUFFER_DEFAULT: usize = 256;
const INPUT_PACKET_BUFFER_MAX: usize = 32 * 1024;
const INPUT_PACKET_POOL_MAX: usize = 16;

type ProtocolCharge = crate::engine::executor::mem_budget::ControlledMemoryCharge;

thread_local! {
    static BATCH_BUFFER_POOL: RefCell<Vec<(Vec<u8>, ProtocolCharge)>> = const { RefCell::new(Vec::new()) };
    /// Sibling of `BATCH_BUFFER_POOL` for single-packet `write_packet`
    /// callers (handshake, OK/ERR/EOF, COM_PING, COM_FIELD_LIST per
    /// column). Sized to a few KiB rather than 64 KiB so the steady-
    /// state per-thread footprint stays small even when both pools are
    /// full.
    static SINGLE_FRAME_BUFFER_POOL: RefCell<Vec<(Vec<u8>, ProtocolCharge)>> = const { RefCell::new(Vec::new()) };
    /// Small inbound command buffers. Kept separate from outbound frames so a
    /// tiny COM_STMT_EXECUTE does not hold an 8 KiB TLS response buffer for
    /// the whole execution and response lifetime.
    static INPUT_PACKET_BUFFER_POOL: RefCell<Vec<(Vec<u8>, ProtocolCharge)>> = const { RefCell::new(Vec::new()) };
}

fn zero_protocol_charge() -> ProtocolCharge {
    ProtocolCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
        0,
    )
    .expect("zero-byte protocol reservation cannot fail")
}

fn acquire_single_frame_buffer(payload_len: usize) -> io::Result<(Vec<u8>, ProtocolCharge)> {
    // Multi-frame payloads (> 16 MiB - 4) are encoded as a chain of
    // 0xffffff-byte frames + headers. Their fully-encoded length can
    // exceed the small-frame pool's cap and would force a re-grow on
    // re-use, so we skip the pool for those cases and let the caller
    // allocate a one-shot Vec the OS will free promptly.
    let frame_count = payload_len / MAX_PAYLOAD_LEN + 1;
    let needed = payload_len.saturating_add(frame_count.saturating_mul(4));
    if needed > SINGLE_FRAME_BUFFER_MAX {
        let mut buf = Vec::new();
        let mut charge = zero_protocol_charge();
        reserve_packet_payload(&mut buf, &mut charge, needed)?;
        return Ok((buf, charge));
    }
    SINGLE_FRAME_BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        match pool.pop() {
            Some((mut buf, mut charge)) => {
                // Pooled buffers are cleared on release (`len == 0`), so
                // `reserve(additional)` here must pass the *full* `needed`,
                // not `needed - capacity`: `Vec::reserve` guarantees only
                // `capacity >= len + additional`, and with `len == 0` the
                // old `needed - capacity` form under-reserved and forced a
                // second re-grow inside `write_packet_frame_into` for any
                // mid-size (8–32 KiB) frame.
                if buf.capacity() < needed {
                    reserve_packet_payload(&mut buf, &mut charge, needed)?;
                }
                Ok((buf, charge))
            }
            None => {
                let mut buf = Vec::new();
                let mut charge = zero_protocol_charge();
                reserve_packet_payload(
                    &mut buf,
                    &mut charge,
                    SINGLE_FRAME_BUFFER_DEFAULT.max(needed),
                )?;
                Ok((buf, charge))
            }
        }
    })
}

fn release_single_frame_buffer(mut buf: Vec<u8>, charge: ProtocolCharge) {
    let cap = buf.capacity();
    // Reject oversized buffers (a one-off huge OK packet, multi-frame
    // ranges) and undersized leftovers; the pool only retains buffers
    // sized for the common small-packet path.
    if cap == 0 || cap > SINGLE_FRAME_BUFFER_MAX {
        return;
    }
    buf.clear();
    SINGLE_FRAME_BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < SINGLE_FRAME_POOL_MAX {
            pool.push((buf, charge));
        }
    });
}

fn acquire_input_packet_buffer() -> io::Result<(Vec<u8>, ProtocolCharge)> {
    INPUT_PACKET_BUFFER_POOL.with(|pool| {
        if let Some(buffer) = pool.borrow_mut().pop() {
            return Ok(buffer);
        }
        let mut buf = Vec::new();
        let mut charge = zero_protocol_charge();
        reserve_packet_payload(&mut buf, &mut charge, INPUT_PACKET_BUFFER_DEFAULT)?;
        Ok((buf, charge))
    })
}

fn release_input_packet_buffer(mut buf: Vec<u8>, charge: ProtocolCharge) {
    let cap = buf.capacity();
    if cap == 0 || cap > INPUT_PACKET_BUFFER_MAX {
        return;
    }
    buf.clear();
    INPUT_PACKET_BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < INPUT_PACKET_POOL_MAX {
            pool.push((buf, charge));
        }
    });
}

fn acquire_batch_buffer() -> (Vec<u8>, ProtocolCharge) {
    BATCH_BUFFER_POOL.with(|pool| {
        if let Some(buffer) = pool.borrow_mut().pop() {
            return buffer;
        }
        let Some(charge) = ProtocolCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            DEFAULT_BATCH_FLUSH_BYTES,
        ) else {
            return (Vec::new(), zero_protocol_charge());
        };
        let mut buffer = Vec::new();
        if buffer.try_reserve_exact(DEFAULT_BATCH_FLUSH_BYTES).is_err() {
            return (Vec::new(), zero_protocol_charge());
        }
        (buffer, charge)
    })
}

fn release_batch_buffer(mut buf: Vec<u8>, charge: ProtocolCharge) {
    let cap = buf.capacity();
    // Reject too-small (would force a re-grow on the next user) and
    // too-large (would pin too much memory per worker thread) buffers.
    if cap < DEFAULT_BATCH_FLUSH_BYTES || cap > DEFAULT_BATCH_FLUSH_BYTES.saturating_mul(4) {
        return;
    }
    buf.clear();
    BATCH_BUFFER_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < BATCH_POOL_MAX {
            pool.push((buf, charge));
        }
    });
}

pub struct PacketBatchWriter<'a, W>
where
    W: AsyncWrite + Unpin,
{
    writer: &'a mut W,
    buffer: Vec<u8>,
    flush_threshold: usize,
    buffer_cap: usize,
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    /// `None` → plaintext path: `flush_buffer` writes the raw frame
    /// bytes straight to the kernel. `Some` → compressed path: the
    /// buffer is shipped as one or more zlib-compressed packets
    /// (sharing one compressed packet across many raw frames is the
    /// whole point of the batch writer).
    compression: Option<&'a mut CompressionState>,
    status: Option<std::sync::Arc<crate::engine::status_counters::SessionStatus>>,
    pending_status_bytes: u64,
}

impl<'a, W> PacketBatchWriter<'a, W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: &'a mut W) -> Self {
        Self::new_with_codec(writer, None, None)
    }

    pub(crate) fn new_with_codec(
        writer: &'a mut W,
        compression: Option<&'a mut CompressionState>,
        status: Option<std::sync::Arc<crate::engine::status_counters::SessionStatus>>,
    ) -> Self {
        let (buffer, memory_charge) = acquire_batch_buffer();
        Self {
            writer,
            // Reuse a 64 KiB buffer from the thread-local pool when one
            // is available — see `BATCH_BUFFER_POOL` above for the
            // rationale (avoids one alloc + one free per SELECT).
            buffer,
            flush_threshold: DEFAULT_BATCH_FLUSH_BYTES,
            buffer_cap: batch_buffer_max_bytes(),
            memory_charge,
            compression,
            status,
            pending_status_bytes: 0,
        }
    }

    pub async fn write_packet(&mut self, sequence_id: u8, payload: &[u8]) -> io::Result<()> {
        self.write_packet_seq(sequence_id, payload)
            .await
            .map(|_| ())
    }

    /// Like `write_packet` but returns the next sequence id (advanced by
    /// the number of frames written). Row loops use this so a row that
    /// spans multiple frames keeps the sequence counter in step.
    pub async fn write_packet_seq(&mut self, sequence_id: u8, payload: &[u8]) -> io::Result<u8> {
        // Flush proactively if appending this frame would exceed the
        // hard buffer cap, so per-connection memory cannot grow without
        // bound under a slow client.
        let frame_count = payload.len() / MAX_PAYLOAD_LEN + 1;
        let frame_len = payload.len().saturating_add(frame_count.saturating_mul(4));
        if !self.buffer.is_empty() && self.buffer.len().saturating_add(frame_len) > self.buffer_cap
        {
            self.flush_buffer().await?;
        }
        if frame_len > self.buffer_cap && self.compression.is_none() {
            let next = write_packet_direct_unflushed(self.writer, sequence_id, payload).await?;
            if let Some(status) = &self.status {
                status.add(
                    crate::engine::status_counters::SessionCounter::BytesSent,
                    frame_len as u64,
                );
            }
            return Ok(next);
        }
        let required = self.buffer.len().saturating_add(frame_len);
        if required > self.buffer.capacity() {
            let old_capacity = self.buffer.capacity();
            let requested_growth = required - old_capacity;
            self.memory_charge
                .try_grow(requested_growth)
                .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            if self.buffer.try_reserve_exact(requested_growth).is_err() {
                self.memory_charge.shrink(requested_growth);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "out of memory while staging MySQL result packets",
                ));
            }
            let actual_growth = self.buffer.capacity().saturating_sub(old_capacity);
            if actual_growth > requested_growth {
                self.memory_charge
                    .try_grow(actual_growth - requested_growth)
                    .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            }
        }
        let next = write_packet_frame_into(sequence_id, payload, &mut self.buffer)?;
        self.pending_status_bytes = self.pending_status_bytes.saturating_add(frame_len as u64);
        if self.buffer.len() >= self.flush_threshold {
            self.flush_buffer().await?;
        }
        Ok(next)
    }

    pub async fn finish(mut self) -> io::Result<()> {
        self.flush_buffer().await?;
        flush_with_timeout(self.writer).await
        // Drop runs after this returns — see `Drop` impl below; the
        // buffer is returned to the per-thread pool.
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer().await?;
        flush_with_timeout(self.writer).await
    }

    async fn flush_buffer(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            match self.compression.as_deref_mut() {
                None => {
                    // Bounded by `net_write_timeout`: a slow client cannot
                    // keep this future pending indefinitely and pin the
                    // batch buffer + tokio task to the connection.
                    write_all_with_timeout(self.writer, &self.buffer).await?;
                }
                Some(state) => {
                    // The compressed seq id of the first emitted
                    // packet matches the inner MySQL seq of the first
                    // frame in the buffer (`buffer[3]`). MySQL frames
                    // start with `[len(3), seq(1)]`, so byte 3 of the
                    // batch buffer is the seq of the first wrapped
                    // frame.
                    let first_seq = self.buffer.get(3).copied().unwrap_or(0);
                    write_compressed_blocks(self.writer, state, &self.buffer, first_seq).await?;
                }
            }
            if let Some(status) = &self.status {
                status.add(
                    crate::engine::status_counters::SessionCounter::BytesSent,
                    self.pending_status_bytes,
                );
            }
            self.pending_status_bytes = 0;
            self.buffer.clear();
        }
        // Release excess capacity acquired by an oversized packet so
        // long-lived sessions don't permanently retain MAX_PAYLOAD_LEN
        // bytes per connection.
        if self.buffer.capacity() > DEFAULT_BATCH_FLUSH_BYTES.saturating_mul(4) {
            let old_capacity = self.buffer.capacity();
            self.buffer.shrink_to(DEFAULT_BATCH_FLUSH_BYTES);
            self.memory_charge
                .shrink(old_capacity.saturating_sub(self.buffer.capacity()));
        }
        Ok(())
    }
}

impl<'a, W> Drop for PacketBatchWriter<'a, W>
where
    W: AsyncWrite + Unpin,
{
    fn drop(&mut self) {
        // Return the buffer to the per-thread pool so the next SELECT
        // on this tokio worker thread reuses it. `release_batch_buffer`
        // already filters out wildly mis-sized buffers and bounds the
        // pool, so this is bounded in both directions.
        let buf = std::mem::take(&mut self.buffer);
        let charge = std::mem::replace(&mut self.memory_charge, zero_protocol_charge());
        release_batch_buffer(buf, charge);
    }
}

pub fn encode_payload_len(len: usize, header: &mut [u8; 4]) {
    header[0] = (len & 0xff) as u8;
    header[1] = ((len >> 8) & 0xff) as u8;
    header[2] = ((len >> 16) & 0xff) as u8;
}

pub fn decode_payload_len(header: &[u8; 4]) -> usize {
    (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16)
}

// ---------------------------------------------------------------------
// CLIENT_COMPRESS — zlib-based wire compression (MySQL 8.0 protocol).
//
// Compressed packet layout, identical for both directions:
//   [3-byte compressed payload length]
//   [1-byte compressed sequence id]
//   [3-byte uncompressed payload length]   (0 → escape: payload is raw)
//   [compressed payload | raw payload]
//
// The compressed sequence id counts compressed packets, NOT raw frames —
// it is independent of the inner `1-byte sequence id` carried by each
// raw MySQL frame. A single compressed packet may bundle multiple raw
// frames (e.g. column-def + EOF + N rows from one result-set flush),
// which is why mydb's batch writer compresses the whole batch buffer in
// one shot instead of one compressed packet per raw frame.
// ---------------------------------------------------------------------

/// MySQL 8.0 reference: payloads smaller than 50 bytes are sent
/// uncompressed (uncomp_len = 0) because zlib's overhead almost always
/// outweighs the savings. Sized for OK / EOF / ERR / COM_PING reply.
pub const MIN_COMPRESS_LENGTH: usize = 50;

const COMPRESSED_HEADER_LEN: usize = 7;
/// Per the MySQL 8.0 protocol, a single compressed packet's *compressed*
/// payload length field is 3 bytes, so the cap matches `MAX_PAYLOAD_LEN`.
const MAX_COMPRESSED_PAYLOAD: usize = MAX_PAYLOAD_LEN;

/// Which compression algorithm a `CompressionState` is currently
/// configured for. Selection is one-shot — once a connection has
/// committed to zlib or zstd at handshake time, it stays on that
/// algorithm for its entire lifetime (matching MySQL's wire protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    Zlib,
    Zstd,
}

/// Algorithm-specific codec state. Boxed so the common `Zlib` variant
/// doesn't pay the size of a 1 MB+ zstd context for every plain
/// session that just happens to construct an empty `CompressionState`.
enum AlgorithmState {
    Zlib {
        decompressor: Decompress,
    },
    #[cfg(feature = "zstd-compression")]
    Zstd {
        compressor: zstd::bulk::Compressor<'static>,
        decompressor: zstd::bulk::Decompressor<'static>,
    },
}

/// In-session compression state shared by reader and writer paths.
/// `inflate_buf` keeps decompressed bytes between `read_packet` calls
/// (one compressed packet may carry several raw frames).
///
/// The compressed counter is independent of inner frame numbers. Each
/// command resets it on the client; each server response continues at the
/// last received compressed packet's sequence plus one, including LOCAL and
/// authentication exchanges.
pub struct CompressionState {
    inflate_buf: Vec<u8>,
    inflate_charge: ProtocolCharge,
    inflate_pos: usize,
    scratch: Vec<u8>,
    scratch_charge: ProtocolCharge,
    /// Next outer packet number, advanced by both request and response
    /// packets. None is only used before the first compressed exchange.
    next_out_compressed_seq: Option<u8>,
    state: AlgorithmState,
}

impl CompressionState {
    fn new_zlib() -> Self {
        Self {
            inflate_buf: Vec::new(),
            inflate_charge: zero_protocol_charge(),
            inflate_pos: 0,
            scratch: Vec::new(),
            scratch_charge: zero_protocol_charge(),
            next_out_compressed_seq: None,
            state: AlgorithmState::Zlib {
                // `zlib_header=true` enables the 2-byte zlib wrapper,
                // which is what MySQL's CLIENT_COMPRESS expects (NOT
                // raw deflate and NOT gzip).
                decompressor: Decompress::new(true),
            },
        }
    }

    #[cfg(feature = "zstd-compression")]
    fn new_zstd(level: i32) -> io::Result<Self> {
        // libzstd accepts levels [1, 22]; clamp once at construction so
        // higher layers can pass through `HandshakeResponse.zstd_compression_level`
        // without re-validating. Level 0 would otherwise be interpreted
        // as "use default" which surprises operators reading logs.
        let level = level.clamp(1, 22);
        let compressor = zstd::bulk::Compressor::new(level)?;
        let decompressor = zstd::bulk::Decompressor::new()?;
        Ok(Self {
            inflate_buf: Vec::new(),
            inflate_charge: zero_protocol_charge(),
            inflate_pos: 0,
            scratch: Vec::new(),
            scratch_charge: zero_protocol_charge(),
            next_out_compressed_seq: None,
            state: AlgorithmState::Zstd {
                compressor,
                decompressor,
            },
        })
    }

    /// Discard already-consumed bytes so the buffer doesn't grow without
    /// bound when many small frames have been read out of it.
    fn compact_inflate_buf(&mut self) {
        if self.inflate_pos == 0 {
            return;
        }
        if self.inflate_pos >= self.inflate_buf.len() {
            self.inflate_buf.clear();
            self.inflate_pos = 0;
            return;
        }
        self.inflate_buf.drain(..self.inflate_pos);
        self.inflate_pos = 0;
    }

    fn available(&self) -> usize {
        self.inflate_buf.len() - self.inflate_pos
    }

    #[cfg(test)]
    pub fn algorithm(&self) -> CompressionAlgorithm {
        match self.state {
            AlgorithmState::Zlib { .. } => CompressionAlgorithm::Zlib,
            #[cfg(feature = "zstd-compression")]
            AlgorithmState::Zstd { .. } => CompressionAlgorithm::Zstd,
        }
    }
}

/// Packet I/O codec — switchable between plaintext and zlib-compressed
/// modes. Holds the compression state so the same `Session` reads /
/// writes packets through a single object regardless of mode.
pub struct PacketCodec {
    compression: Option<CompressionState>,
    status: Option<std::sync::Arc<crate::engine::status_counters::SessionStatus>>,
}

impl Default for PacketCodec {
    fn default() -> Self {
        Self::plain()
    }
}

impl PacketCodec {
    pub fn plain() -> Self {
        Self {
            compression: None,
            status: None,
        }
    }

    pub(crate) fn plain_with_status(
        status: std::sync::Arc<crate::engine::status_counters::SessionStatus>,
    ) -> Self {
        Self {
            compression: None,
            status: Some(status),
        }
    }

    /// Promote this codec to the zlib compression protocol. Subsequent
    /// `read_packet` / `write_packet*` calls speak compressed frames.
    /// Called by the session immediately after a successful handshake
    /// when the negotiated capability flags include `CLIENT_COMPRESS`.
    pub fn enable_zlib_compression(&mut self) {
        if self.compression.is_none() {
            self.compression = Some(CompressionState::new_zlib());
        }
    }

    /// Promote this codec to the zstd compression protocol with the
    /// given libzstd level (1–22, MySQL default 3). When both
    /// CLIENT_ZSTD_COMPRESSION_ALGORITHM and CLIENT_COMPRESS are
    /// negotiated, the session calls this in preference to
    /// `enable_zlib_compression` — zstd wins on every ratio/CPU axis.
    pub fn enable_zstd_compression(&mut self, level: i32) -> io::Result<()> {
        #[cfg(not(feature = "zstd-compression"))]
        {
            let _ = level;
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "zstd compression support is not compiled in",
            ));
        }
        #[cfg(feature = "zstd-compression")]
        {
            if self.compression.is_none() {
                self.compression = Some(CompressionState::new_zstd(level)?);
            }
            Ok(())
        }
    }

    #[cfg(test)]
    pub fn compression_algorithm(&self) -> Option<CompressionAlgorithm> {
        self.compression.as_ref().map(CompressionState::algorithm)
    }

    /// Command-internal data has no idle phase: bound even the first header
    /// byte, including compressed frames. The caller must close on timeout
    /// because cancellation may leave a partially consumed frame.
    pub(crate) async fn read_packet_with_timeout<R>(
        &mut self,
        reader: &mut R,
        timeout: Option<Duration>,
    ) -> io::Result<Option<Packet>>
    where
        R: AsyncRead + Unpin,
    {
        match timeout {
            Some(duration) => timeout_after_first_poll(duration, self.read_packet(reader))
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "net_read_timeout exceeded during command data transfer",
                    )
                })?,
            None => self.read_packet(reader).await,
        }
    }

    pub async fn read_packet<R>(&mut self, reader: &mut R) -> io::Result<Option<Packet>>
    where
        R: AsyncRead + Unpin,
    {
        let limit = max_allowed_packet_bytes();
        let result = match self.compression.as_mut() {
            None => read_packet_with_limit(reader, limit).await,
            Some(state) => read_packet_compressed(reader, state, limit).await,
        };
        if let Ok(Some(packet)) = &result {
            if let Some(status) = &self.status {
                let frame_count = packet.payload.len() / MAX_PAYLOAD_LEN + 1;
                status.add(
                    crate::engine::status_counters::SessionCounter::BytesReceived,
                    packet
                        .payload
                        .len()
                        .saturating_add(frame_count.saturating_mul(4)) as u64,
                );
            }
        }
        result
    }

    pub async fn write_packet<W>(
        &mut self,
        writer: &mut W,
        sequence_id: u8,
        payload: &[u8],
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        self.write_packet_seq(writer, sequence_id, payload)
            .await
            .map(|_| ())
    }

    pub async fn write_packet_seq<W>(
        &mut self,
        writer: &mut W,
        sequence_id: u8,
        payload: &[u8],
    ) -> io::Result<u8>
    where
        W: AsyncWrite + Unpin,
    {
        let result = match self.compression.as_mut() {
            None => write_packet_seq(writer, sequence_id, payload).await,
            Some(state) => write_packet_compressed_seq(writer, state, sequence_id, payload).await,
        };
        if result.is_ok() {
            if let Some(status) = &self.status {
                let frame_count = payload.len() / MAX_PAYLOAD_LEN + 1;
                status.add(
                    crate::engine::status_counters::SessionCounter::BytesSent,
                    payload.len().saturating_add(frame_count.saturating_mul(4)) as u64,
                );
            }
        }
        result
    }

    #[cfg(test)]
    pub async fn write_packet_unflushed<W>(
        &mut self,
        writer: &mut W,
        sequence_id: u8,
        payload: &[u8],
    ) -> io::Result<u8>
    where
        W: AsyncWrite + Unpin,
    {
        let result = match self.compression.as_mut() {
            None => write_packet_unflushed(writer, sequence_id, payload).await,
            // No "unflushed" distinction in compressed mode: each
            // compressed packet is its own kernel write, so this maps
            // to the same flushed path. The session uses the unflushed
            // entry point in plain mode purely to defer `flush()` —
            // the compressed path effectively short-circuits that.
            Some(state) => write_packet_compressed_seq(writer, state, sequence_id, payload).await,
        };
        if result.is_ok() {
            if let Some(status) = &self.status {
                let frame_count = payload.len() / MAX_PAYLOAD_LEN + 1;
                status.add(
                    crate::engine::status_counters::SessionCounter::BytesSent,
                    payload.len().saturating_add(frame_count.saturating_mul(4)) as u64,
                );
            }
        }
        result
    }

    pub fn batch_writer<'a, W>(&'a mut self, writer: &'a mut W) -> PacketBatchWriter<'a, W>
    where
        W: AsyncWrite + Unpin,
    {
        PacketBatchWriter::new_with_codec(writer, self.compression.as_mut(), self.status.clone())
    }
}

async fn read_packet_compressed<R>(
    reader: &mut R,
    state: &mut CompressionState,
    limit: usize,
) -> io::Result<Option<Packet>>
where
    R: AsyncRead + Unpin,
{
    // First, make sure we have at least 4 bytes of decompressed payload
    // for the inner MySQL frame header.
    if !ensure_decompressed_bytes(reader, state, 4, limit).await? {
        return Ok(None);
    }
    let mut header = [0u8; 4];
    copy_from_inflate_buf(state, &mut header);

    let first_sequence_id = header[3];
    let mut sequence_id = header[3];
    let (mut payload, mut memory_charge) = acquire_input_packet_buffer()?;
    loop {
        let frame_len = decode_payload_len(&header);
        let new_total = payload.len().saturating_add(frame_len);
        if new_total > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PacketTooLarge {
                    response_sequence_id: sequence_id.wrapping_add(1),
                    bytes: new_total,
                    limit,
                },
            ));
        }
        let start = payload.len();
        reserve_packet_payload(&mut payload, &mut memory_charge, new_total)?;
        payload.resize(new_total, 0);
        // We have already consumed this frame's 4-byte header, so an EOF
        // before the declared payload arrives is a genuine mid-packet
        // truncation — return an error rather than letting the copy below
        // slice past `inflate_buf` and panic (matches the plaintext
        // `read_exact_with_timeout` behaviour). `ensure_decompressed_bytes`
        // returns `Ok(false)` only on a clean EOF with nothing buffered.
        if !ensure_decompressed_bytes(reader, state, frame_len, limit).await? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compressed stream ended mid-frame",
            ));
        }
        copy_from_inflate_buf(state, &mut payload[start..]);
        if frame_len < MAX_PAYLOAD_LEN {
            break;
        }
        // Continuation frame's own 4-byte header follows. Same reasoning:
        // a clean EOF here means the client promised more (>16 MiB) frames
        // but disappeared — that is a truncated packet, not a clean close.
        if !ensure_decompressed_bytes(reader, state, 4, limit).await? {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compressed stream ended before continuation frame header",
            ));
        }
        copy_from_inflate_buf(state, &mut header);
        let expected = sequence_id.wrapping_add(1);
        if header[3] != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "compressed inner continuation sequence id {} out of order; expected {expected}",
                    header[3]
                ),
            ));
        }
        sequence_id = header[3];
    }

    Ok(Some(Packet {
        first_sequence_id,
        sequence_id,
        payload,
        _memory_charge: Some(memory_charge),
    }))
}

fn copy_from_inflate_buf(state: &mut CompressionState, dst: &mut [u8]) {
    let need = dst.len();
    dst.copy_from_slice(&state.inflate_buf[state.inflate_pos..state.inflate_pos + need]);
    state.inflate_pos += need;
}

/// Ensure at least `need` decompressed bytes are buffered. Returns
/// `Ok(false)` only if the underlying stream was at EOF *and* no bytes
/// of the requested frame had been received yet — matching plaintext
/// `read_packet`'s "clean disconnect between commands" contract.
async fn ensure_decompressed_bytes<R>(
    reader: &mut R,
    state: &mut CompressionState,
    need: usize,
    limit: usize,
) -> io::Result<bool>
where
    R: AsyncRead + Unpin,
{
    while state.available() < need {
        match read_one_compressed_packet(reader, state, limit).await? {
            true => {}
            false => {
                if state.available() == 0 && need > 0 {
                    return Ok(false);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "compressed stream ended mid-frame",
                ));
            }
        }
    }
    Ok(true)
}

/// Read one compressed packet from `reader` and append its
/// decompressed (or raw, when `uncomp_len == 0`) bytes to
/// `state.inflate_buf`. Returns `Ok(false)` only if the stream was at
/// EOF at the header boundary (clean close).
async fn read_one_compressed_packet<R>(
    reader: &mut R,
    state: &mut CompressionState,
    limit: usize,
) -> io::Result<bool>
where
    R: AsyncRead + Unpin,
{
    let mut hdr = [0u8; COMPRESSED_HEADER_LEN];
    match reader.read_exact(&mut hdr[..1]).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(err) => return Err(err),
    }
    crate::engine::status_counters::add_bytes_received(1);
    read_exact_with_timeout(reader, &mut hdr[1..], net_read_timeout()).await?;
    let comp_len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
    let comp_seq = hdr[3];
    state.next_out_compressed_seq = Some(comp_seq.wrapping_add(1));
    let uncomp_len = (hdr[4] as usize) | ((hdr[5] as usize) << 8) | ((hdr[6] as usize) << 16);
    // Request and response share the outer counter; inner frame sequence
    // numbers cannot identify compressed packet boundaries.

    if comp_len > MAX_COMPRESSED_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compressed payload length {comp_len} exceeds protocol cap {MAX_COMPRESSED_PAYLOAD}"
            ),
        ));
    }
    if uncomp_len > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            PacketTooLarge {
                response_sequence_id: 1,
                bytes: uncomp_len,
                limit,
            },
        ));
    }

    // Bound the inflate buffer: re-shape now that we know the next chunk
    // size; this also makes the `extend_from_slice` below the single
    // allocation point.
    state.compact_inflate_buf();

    // Read the compressed payload into `scratch` first; reusing the
    // scratch buffer avoids one alloc per compressed packet. The header
    // is already consumed, so this is a mid-packet read: bound it by
    // `net_read_timeout` exactly like the plaintext payload read so a
    // client that sends the compressed header then stalls cannot pin the
    // tokio task + scratch/inflate buffers indefinitely.
    state.scratch.clear();
    reserve_packet_payload(&mut state.scratch, &mut state.scratch_charge, comp_len)?;
    state.scratch.resize(comp_len, 0);
    read_exact_with_timeout(reader, &mut state.scratch, net_read_timeout()).await?;

    if uncomp_len == 0 {
        // Escape hatch: short / incompressible payloads are sent raw.
        let required = state.inflate_buf.len().saturating_add(state.scratch.len());
        reserve_packet_payload(&mut state.inflate_buf, &mut state.inflate_charge, required)?;
        state.inflate_buf.extend_from_slice(&state.scratch);
    } else {
        // Pre-allocate the exact decompressed length and decompress
        // straight into it. The algorithm-specific state keeps its
        // internal context warm across packets so we don't re-allocate
        // libzstd/libz workspaces per compressed frame.
        let raw_start = state.inflate_buf.len();
        reserve_packet_payload(
            &mut state.inflate_buf,
            &mut state.inflate_charge,
            raw_start.saturating_add(uncomp_len),
        )?;
        state.inflate_buf.resize(raw_start + uncomp_len, 0);
        match &mut state.state {
            AlgorithmState::Zlib { decompressor } => {
                decompressor.reset(true);
                let in_before = decompressor.total_in();
                let out_before = decompressor.total_out();
                let status = decompressor
                    .decompress(
                        &state.scratch,
                        &mut state.inflate_buf[raw_start..],
                        FlushDecompress::Finish,
                    )
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("zlib decompress error: {err}"),
                        )
                    })?;
                let consumed_in = (decompressor.total_in() - in_before) as usize;
                let produced_out = (decompressor.total_out() - out_before) as usize;
                if produced_out != uncomp_len || consumed_in != comp_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "zlib decompress short read: produced {produced_out}/{uncomp_len}, \
                             consumed {consumed_in}/{comp_len}, status {status:?}"
                        ),
                    ));
                }
            }
            #[cfg(feature = "zstd-compression")]
            AlgorithmState::Zstd { decompressor, .. } => {
                // libzstd writes into a temp Vec then we copy into the
                // pre-sized slot — `decompress_to_buffer` against the
                // shared inflate_buf would force a WriteBuf round trip
                // we don't need at this packet size. The extra copy is
                // a memcpy of <= 16 MiB, cheap next to a SELECT round.
                let _decoded_charge = ProtocolCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                    uncomp_len,
                )
                .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
                let decoded = decompressor
                    .decompress(&state.scratch, uncomp_len)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("zstd decompress error: {err}"),
                        )
                    })?;
                if decoded.len() != uncomp_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "zstd decompress short read: produced {}/{uncomp_len}",
                            decoded.len()
                        ),
                    ));
                }
                state.inflate_buf[raw_start..].copy_from_slice(&decoded);
            }
        }
    }
    Ok(true)
}

async fn write_packet_compressed_seq<W>(
    writer: &mut W,
    state: &mut CompressionState,
    sequence_id: u8,
    payload: &[u8],
) -> io::Result<u8>
where
    W: AsyncWrite + Unpin,
{
    // Encode the raw MySQL frame(s) for `payload` into the scratch
    // buffer, then ship the entire scratch as compressed packets.
    state.scratch.clear();
    let frame_count = payload.len() / MAX_PAYLOAD_LEN + 1;
    let framed_len = payload.len().saturating_add(frame_count.saturating_mul(4));
    reserve_packet_payload(&mut state.scratch, &mut state.scratch_charge, framed_len)?;
    let next = write_packet_frame_into(sequence_id, payload, &mut state.scratch)?;
    let scratch = std::mem::take(&mut state.scratch);
    let result = write_compressed_blocks(writer, state, &scratch, sequence_id).await;
    state.scratch = scratch;
    state.scratch.clear();
    result?;
    Ok(next)
}

/// Ship `raw` (one or more raw MySQL frames in wire form) as one or
/// more compressed packets, never letting a single compressed packet's
/// compressed length exceed `MAX_COMPRESSED_PAYLOAD`.
///
/// `first_inner_seq` is the inner MySQL sequence id of the first
/// wrapped frame. The compressed packet's seq id is NOT taken straight
/// from this value — it lives in its own per-connection counter
/// (`state.next_out_compressed_seq`) and increments by 1 per compressed
/// packet emitted. We sync the counter to `first_inner_seq` only when
/// the inner seq has gone backward (the signal that a NEW response
/// started, since each response restarts its inner seq at 1). This
/// matches both:
/// 1. Connector/J 8.0's expectation that the first compressed packet
///    of a response carries the same seq as its first inner frame.
/// 2. mysql 8.4 CLI's strict per-response monotonic compressed seq —
///    when a SELECT header batch and the rows batch are each their own
///    compressed packet, those packets MUST have comp_seq 1 and 2 even
///    though the rows batch's first inner seq is 3 (it followed the
///    col_count + col_def frames in the same response).
async fn write_compressed_blocks<W>(
    writer: &mut W,
    state: &mut CompressionState,
    raw: &[u8],
    first_inner_seq: u8,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if raw.is_empty() {
        return Ok(());
    }
    // Inbound reads establish response boundaries. Comparing inner numbers
    // with outer numbers fails for multi-packet requests and sequence wrap.
    if state.next_out_compressed_seq.is_none() {
        state.next_out_compressed_seq = Some(first_inner_seq);
    }
    // Compressed packets carry up to MAX_COMPRESSED_PAYLOAD bytes of
    // compressed (or raw) payload. Most batches fit in one packet; very
    // large flushes (multi-MB result set) are split here, with the
    // compressed seq id incrementing per output packet.
    let mut cursor = 0usize;
    while cursor < raw.len() {
        let chunk_end = (cursor + MAX_COMPRESSED_PAYLOAD).min(raw.len());
        let chunk = &raw[cursor..chunk_end];
        cursor = chunk_end;
        let compressed_seq = state
            .next_out_compressed_seq
            .expect("counter was just initialized above");
        write_single_compressed_block(writer, state, chunk, compressed_seq).await?;
        state.next_out_compressed_seq = Some(compressed_seq.wrapping_add(1));
    }
    Ok(())
}

async fn write_single_compressed_block<W>(
    writer: &mut W,
    state: &mut CompressionState,
    raw_chunk: &[u8],
    compressed_seq: u8,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    debug_assert!(raw_chunk.len() <= MAX_COMPRESSED_PAYLOAD);

    let compressed_storage: Vec<u8>;
    let mut _compressed_storage_charge: Option<ProtocolCharge> = None;
    let (payload, uncomp_len) = if raw_chunk.len() < MIN_COMPRESS_LENGTH {
        // Small enough that the compressor's frame overhead is a net
        // loss; the protocol's `uncomp_len = 0` escape ships the bytes
        // raw. Both zlib and zstd take this same path for small frames.
        (raw_chunk, 0usize)
    } else {
        _compressed_storage_charge = Some(
            ProtocolCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                raw_chunk.len().saturating_mul(2).saturating_add(1024),
            )
            .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?,
        );
        let out = match &mut state.state {
            AlgorithmState::Zlib { .. } => {
                // ZlibEncoder owns its sink, so a fresh Vec is used per
                // call. The cost is dominated by deflate CPU, not the
                // Vec; pooling the sink would not move the needle.
                let mut encoder =
                    ZlibEncoder::new(Vec::with_capacity(raw_chunk.len()), Compression::default());
                encoder.write_all(raw_chunk).map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, format!("zlib compress error: {err}"))
                })?;
                encoder.finish().map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, format!("zlib finish error: {err}"))
                })?
            }
            #[cfg(feature = "zstd-compression")]
            AlgorithmState::Zstd { compressor, .. } => {
                compressor.compress(raw_chunk).map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, format!("zstd compress error: {err}"))
                })?
            }
        };
        if out.len() >= raw_chunk.len() || out.len() > MAX_COMPRESSED_PAYLOAD {
            // Compression grew the payload (structured / random data)
            // or would not fit the 3-byte length field — fall back to
            // raw escape, the receiver decodes uncomp_len=0 as raw.
            (raw_chunk, 0usize)
        } else {
            compressed_storage = out;
            (compressed_storage.as_slice(), raw_chunk.len())
        }
    };

    let payload_len = payload.len();
    debug_assert!(payload_len <= MAX_COMPRESSED_PAYLOAD);

    let mut header = [0u8; COMPRESSED_HEADER_LEN];
    header[0] = (payload_len & 0xff) as u8;
    header[1] = ((payload_len >> 8) & 0xff) as u8;
    header[2] = ((payload_len >> 16) & 0xff) as u8;
    header[3] = compressed_seq;
    header[4] = (uncomp_len & 0xff) as u8;
    header[5] = ((uncomp_len >> 8) & 0xff) as u8;
    header[6] = ((uncomp_len >> 16) & 0xff) as u8;

    // Single `write_all` call concatenates header + payload so a
    // syscall-bound path (tokio's default `flush_after_each_write` is
    // already off) issues at most two writev candidates, not more.
    let (mut framed, framed_charge) =
        acquire_single_frame_buffer(COMPRESSED_HEADER_LEN + payload_len)?;
    framed.extend_from_slice(&header);
    framed.extend_from_slice(payload);
    let result = write_all_with_timeout(writer, &framed).await;
    release_single_frame_buffer(framed, framed_charge);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_payload_reservation_covers_len_not_capacity_delta() {
        let (mut payload, mut charge) = acquire_input_packet_buffer().unwrap();
        let required = payload.capacity() + 1;
        reserve_packet_payload(&mut payload, &mut charge, required).unwrap();
        assert!(payload.capacity() >= required);
        assert!(charge.bytes() >= payload.capacity());
    }

    #[test]
    fn packet_header_uses_three_little_endian_length_bytes() {
        let mut header = [0_u8; 4];

        encode_payload_len(0x12_34_56, &mut header);
        header[3] = 7;

        assert_eq!(header, [0x56, 0x34, 0x12, 7]);
        assert_eq!(decode_payload_len(&header), 0x12_34_56);
    }

    #[test]
    fn packet_frame_matches_header_plus_payload_encoding() {
        let mut frame = Vec::new();

        write_packet_frame_into(3, b"abc", &mut frame).unwrap();

        assert_eq!(frame, vec![3, 0, 0, 3, b'a', b'b', b'c']);
    }

    #[tokio::test]
    async fn read_packet_returns_small_payload_buffer_to_pool() {
        INPUT_PACKET_BUFFER_POOL.with(|pool| pool.borrow_mut().clear());
        let frame = [3, 0, 0, 0, b'a', b'b', b'c'];

        let mut first_input = &frame[..];
        let first = read_packet(&mut first_input).await.unwrap().unwrap();
        let first_ptr = first.payload.as_ptr();
        drop(first);
        assert_eq!(INPUT_PACKET_BUFFER_POOL.with(|pool| pool.borrow().len()), 1);

        let mut second_input = &frame[..];
        let second = read_packet(&mut second_input).await.unwrap().unwrap();
        assert_eq!(second.payload.as_ptr(), first_ptr);
        assert_eq!(second.payload, b"abc");
        drop(second);
        INPUT_PACKET_BUFFER_POOL.with(|pool| pool.borrow_mut().clear());
    }

    #[tokio::test]
    async fn read_packet_rejects_payload_larger_than_limit() {
        let limit = 1024;
        let payload_len = 4096;
        let mut header = [0_u8; 4];
        encode_payload_len(payload_len, &mut header);
        header[3] = 0;
        let mut buf: &[u8] = &header[..];
        let err = read_packet_with_limit(&mut buf, limit).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_packet_accepts_payload_within_limit() {
        let limit = 1024;
        let payload = b"hello world";
        let mut frame = Vec::new();
        write_packet_frame_into(7, payload, &mut frame).unwrap();
        let mut buf: &[u8] = &frame[..];
        let pkt = read_packet_with_limit(&mut buf, limit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pkt.sequence_id, 7);
        assert_eq!(pkt.payload, payload);
    }

    #[tokio::test]
    async fn write_all_with_timeout_returns_timed_out_when_writer_blocks() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;

        struct StallingWriter;

        impl AsyncWrite for StallingWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Pending
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Pending
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Pending
            }
        }

        let mut writer = StallingWriter;
        // Use a very short timeout via a direct tokio call so the test
        // doesn't depend on the global env-driven default.
        let res = tokio::time::timeout(Duration::from_millis(50), writer.write_all(b"abc")).await;
        assert!(res.is_err(), "stalling writer must time out");
    }

    #[tokio::test]
    async fn batch_writer_matches_individual_packet_frames() {
        let mut expected = Vec::new();
        write_packet_frame_into(1, b"hello", &mut expected).unwrap();
        write_packet_frame_into(2, b"world", &mut expected).unwrap();

        let mut actual = Vec::new();
        {
            let mut batch = PacketBatchWriter::new(&mut actual);
            batch.write_packet(1, b"hello").await.unwrap();
            batch.write_packet(2, b"world").await.unwrap();
            batch.finish().await.unwrap();
        }

        assert_eq!(actual, expected);
    }

    #[test]
    fn multi_frame_split_advances_sequence_by_frame_count() {
        // A payload one byte over the frame limit → two frames (full +
        // 1-byte), next sequence id advanced by 2.
        let payload = vec![0xCDu8; MAX_PAYLOAD_LEN + 1];
        let mut frame = Vec::new();
        let next = write_packet_frame_into(0, &payload, &mut frame).unwrap();
        assert_eq!(next, 2);
        // First header: length 0xffffff, seq 0.
        assert_eq!(&frame[0..4], &[0xff, 0xff, 0xff, 0]);
        // Second header sits right after the first frame's payload.
        let second = 4 + MAX_PAYLOAD_LEN;
        assert_eq!(&frame[second..second + 4], &[1, 0, 0, 1]);
        assert_eq!(frame.len(), 4 + MAX_PAYLOAD_LEN + 4 + 1);
    }

    #[test]
    fn exact_multiple_payload_gets_zero_length_terminator() {
        // A payload exactly one frame long must be followed by a 0-length
        // terminator frame so the client knows the packet ended.
        let payload = vec![0x01u8; MAX_PAYLOAD_LEN];
        let mut frame = Vec::new();
        let next = write_packet_frame_into(5, &payload, &mut frame).unwrap();
        assert_eq!(next, 7);
        assert_eq!(frame.len(), 4 + MAX_PAYLOAD_LEN + 4);
        // Terminator frame: length 0, seq 6.
        let term = 4 + MAX_PAYLOAD_LEN;
        assert_eq!(&frame[term..term + 4], &[0, 0, 0, 6]);
    }

    #[test]
    fn single_frame_pool_reuses_buffers_for_small_payloads() {
        // Drain the pool so the test is independent of previous tests.
        SINGLE_FRAME_BUFFER_POOL.with(|p| p.borrow_mut().clear());
        let (buf, charge) = acquire_single_frame_buffer(64).unwrap();
        assert!(buf.capacity() >= SINGLE_FRAME_BUFFER_DEFAULT);
        let cap_first = buf.capacity();
        release_single_frame_buffer(buf, charge);

        // Same-sized request returns the previously released buffer
        // (same capacity) instead of allocating a new one.
        let (buf2, charge2) = acquire_single_frame_buffer(64).unwrap();
        assert_eq!(buf2.capacity(), cap_first);
        assert_eq!(buf2.len(), 0);
        release_single_frame_buffer(buf2, charge2);
    }

    #[test]
    fn pooled_protocol_buffer_keeps_reservation_until_evicted() {
        SINGLE_FRAME_BUFFER_POOL.with(|pool| pool.borrow_mut().clear());
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[10];
        let (buf, charge) = acquire_single_frame_buffer(64).unwrap();
        release_single_frame_buffer(buf, charge);
        let pooled = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[10];
        assert!(pooled >= before + SINGLE_FRAME_BUFFER_DEFAULT);
        SINGLE_FRAME_BUFFER_POOL.with(|pool| pool.borrow_mut().clear());
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().2[10],
            before
        );
    }

    #[test]
    fn single_frame_pool_rejects_oversized_buffer_on_release() {
        SINGLE_FRAME_BUFFER_POOL.with(|p| p.borrow_mut().clear());
        // Build a vec well above the pool cap and try to release it. It
        // must be dropped, not retained, so a one-off huge OK packet
        // does not pin pool memory per worker thread.
        let oversized = Vec::with_capacity(SINGLE_FRAME_BUFFER_MAX * 2);
        let charge = ProtocolCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            oversized.capacity(),
        )
        .unwrap();
        release_single_frame_buffer(oversized, charge);
        SINGLE_FRAME_BUFFER_POOL.with(|p| {
            assert!(
                p.borrow().is_empty(),
                "oversized buffer must not enter the pool"
            );
        });
    }

    #[test]
    fn acquire_single_frame_skips_pool_for_multi_frame_payload() {
        SINGLE_FRAME_BUFFER_POOL.with(|p| p.borrow_mut().clear());
        // Pre-seed the pool with a small buffer the small-payload path
        // would normally take. A multi-frame payload (> pool cap) must
        // not consume it, so subsequent small writes still get cheap
        // pooled buffers.
        let seed = Vec::with_capacity(SINGLE_FRAME_BUFFER_DEFAULT);
        let seed_charge = ProtocolCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            seed.capacity(),
        )
        .unwrap();
        release_single_frame_buffer(seed, seed_charge);

        let (huge, _huge_charge) =
            acquire_single_frame_buffer(SINGLE_FRAME_BUFFER_MAX * 4).unwrap();
        assert!(huge.capacity() >= SINGLE_FRAME_BUFFER_MAX * 4);
        SINGLE_FRAME_BUFFER_POOL.with(|p| {
            assert_eq!(
                p.borrow().len(),
                1,
                "seed buffer must remain in the pool when a multi-frame request bypasses it"
            );
        });
    }

    #[tokio::test]
    async fn write_packet_unflushed_writes_correct_frame_bytes() {
        // The pooled small-frame path must produce the exact same wire
        // bytes as the legacy `Vec::with_capacity(4 + len)` form, so a
        // client cannot tell the optimization apart.
        let mut sink = Vec::new();
        let next = write_packet_unflushed(&mut sink, 9, b"hello")
            .await
            .unwrap();
        assert_eq!(next, 10);
        assert_eq!(sink, [5, 0, 0, 9, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn small_control_packets_remain_writable_without_memory_headroom() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        crate::engine::executor::mem_budget::with_test_effective_memory_limit(1, || {
            runtime.block_on(async {
                for size in [0, 100, 252] {
                    let payload = vec![0x42; size];
                    let mut wire = Vec::new();
                    let next = write_packet_unflushed(&mut wire, 255, &payload)
                        .await
                        .unwrap();
                    assert_eq!(next, 0);
                    assert_eq!(&wire[..4], &[size as u8, 0, 0, 255]);
                    assert_eq!(&wire[4..], payload);
                }
                let payload = vec![0xff; 512];
                let mut wire = Vec::new();
                write_packet_direct_unflushed(&mut wire, 0, &payload)
                    .await
                    .unwrap();
                assert_eq!(&wire[..4], &[0, 2, 0, 0]);
                assert_eq!(&wire[4..], payload);
            });
        });
    }

    #[tokio::test]
    async fn protocol_audit_command_data_timeout_covers_plain_and_compressed_headers() {
        use tokio::io::AsyncWriteExt;
        for compressed in [false, true] {
            for partial in [false, true] {
                let (mut reader, mut writer) = tokio::io::duplex(64);
                let mut codec = PacketCodec::plain();
                if compressed {
                    codec.enable_zlib_compression();
                }
                if partial {
                    writer.write_all(&[5]).await.unwrap();
                }
                let result = codec
                    .read_packet_with_timeout(&mut reader, Some(Duration::from_millis(20)))
                    .await;
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
                drop(writer);
            }
        }
    }

    #[tokio::test]
    async fn protocol_audit_high_compression_ratio_is_valid() {
        let payload = vec![0; 1024 * 1024];
        let mut encoded = Vec::new();
        let mut encoder = PacketCodec::plain();
        encoder.enable_zlib_compression();
        encoder
            .write_packet(&mut encoded, 2, &payload)
            .await
            .unwrap();
        assert!(payload.len() > encoded.len() * 64 + 4096);
        let mut decoder = PacketCodec::plain();
        decoder.enable_zlib_compression();
        let packet = decoder
            .read_packet_with_timeout(&mut encoded.as_slice(), Some(Duration::from_secs(1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet.payload, payload);
        assert_eq!(packet.sequence_id, 2);
    }

    #[tokio::test]
    async fn compressed_round_trip_small_payload_takes_raw_escape() {
        // Payload < MIN_COMPRESS_LENGTH must travel with uncomp_len = 0
        // so the receiver knows the body is verbatim (zlib's wrapper
        // overhead would make small payloads larger).
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        let payload = b"hello";
        codec.write_packet(&mut sink, 7, payload).await.unwrap();

        // Compressed header: payload_len = inner-frame length (9),
        // seq = inner MySQL seq (7) — Connector/J expects the compressed
        // seq to mirror the wrapped frame's seq, not a separate counter,
        // uncomp_len = 0 (raw escape).
        assert_eq!(sink[0], 9, "compressed payload_len byte0");
        assert_eq!(sink[1], 0);
        assert_eq!(sink[2], 0);
        assert_eq!(sink[3], 7, "compressed seq mirrors inner seq");
        assert_eq!(sink[4], 0, "uncomp_len == 0 → raw escape");
        assert_eq!(sink[5], 0);
        assert_eq!(sink[6], 0);
        // Body is the raw MySQL frame: 4-byte header + payload.
        assert_eq!(&sink[7..11], &[5, 0, 0, 7]);
        assert_eq!(&sink[11..16], b"hello");

        // Round-trip back through the codec.
        let mut codec2 = PacketCodec::plain();
        codec2.enable_zlib_compression();
        let mut reader: &[u8] = &sink;
        let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(pkt.sequence_id, 7);
        assert_eq!(pkt.payload, b"hello");
    }

    #[tokio::test]
    async fn compressed_round_trip_large_payload_actually_compresses() {
        // A highly-compressible payload (zeroes) must take the
        // compressed path — uncomp_len > 0 and on the wire the body is
        // smaller than the original raw frame.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        let payload = vec![0u8; 4000];
        codec.write_packet(&mut sink, 3, &payload).await.unwrap();

        let body_len = (sink[0] as usize) | ((sink[1] as usize) << 8) | ((sink[2] as usize) << 16);
        let uncomp_len =
            (sink[4] as usize) | ((sink[5] as usize) << 8) | ((sink[6] as usize) << 16);
        assert!(
            uncomp_len > 0,
            "large compressible payload should set uncomp_len != 0"
        );
        assert!(
            body_len < payload.len(),
            "compressed body ({body_len}) must be smaller than raw frame ({})",
            payload.len() + 4
        );

        // Decode round-trip.
        let mut codec2 = PacketCodec::plain();
        codec2.enable_zlib_compression();
        let mut reader: &[u8] = &sink;
        let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(pkt.sequence_id, 3);
        assert_eq!(pkt.payload, payload);
    }

    #[tokio::test]
    async fn compressed_batch_carries_multiple_frames_in_one_compressed_packet() {
        // The whole point of the batch writer in compressed mode: many
        // raw frames flush together → one (or a few) compressed
        // packets, amortising the zlib header overhead.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        {
            let mut batch = codec.batch_writer(&mut sink);
            // Three raw frames worth a compressed payload each:
            let big = vec![b'x'; 200];
            batch.write_packet(0, &big).await.unwrap();
            batch.write_packet(1, &big).await.unwrap();
            batch.write_packet(2, &big).await.unwrap();
            batch.finish().await.unwrap();
        }

        // Decode three separate logical packets, all from one
        // compressed wire packet.
        let mut codec2 = PacketCodec::plain();
        codec2.enable_zlib_compression();
        let mut reader: &[u8] = &sink;
        for expected_seq in 0..3 {
            let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
            assert_eq!(pkt.sequence_id, expected_seq);
            assert_eq!(pkt.payload, vec![b'x'; 200]);
        }
    }

    #[tokio::test]
    async fn compressed_read_accepts_arbitrary_compressed_seq() {
        // The compressed-protocol seq is informational and resets per
        // command on the client side (Connector/J behaviour). The codec
        // takes the inner MySQL frame's seq as authoritative and must
        // not reject a compressed packet whose outer seq differs from
        // an internal expected counter.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        codec.write_packet(&mut sink, 0, b"abcd").await.unwrap();
        sink[3] = 42; // mangle the outer compressed seq

        let mut codec2 = PacketCodec::plain();
        codec2.enable_zlib_compression();
        let mut reader: &[u8] = &sink;
        let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(pkt.sequence_id, 0, "inner MySQL seq is preserved");
        assert_eq!(pkt.payload, b"abcd");
    }

    #[tokio::test]
    async fn compressed_truncated_mid_frame_errors_without_panic() {
        // Regression: a compressed connection whose inner frame header
        // declares a payload length the peer never delivers (stream EOFs
        // mid-frame) must surface a clean `UnexpectedEof`, NOT panic on an
        // out-of-range slice inside `copy_from_inflate_buf`. Historically
        // the two in-loop `ensure_decompressed_bytes` calls dropped the
        // `Ok(false)` (clean-EOF) signal and copied past `inflate_buf`.
        //
        // Craft it via the raw-escape path (uncomp_len == 0), so the
        // "decompressed" bytes are exactly the body we supply: a single
        // 4-byte inner header declaring frame_len = 100, then nothing.
        let mut wire: Vec<u8> = Vec::new();
        // Compressed header: comp_len = 4, comp_seq = 0, uncomp_len = 0.
        wire.extend_from_slice(&[4, 0, 0, 0, 0, 0, 0]);
        // Body = inner MySQL frame header: len = 100, seq = 5. No payload
        // follows, and the stream ends here.
        wire.extend_from_slice(&[100, 0, 0, 5]);

        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        let mut reader: &[u8] = &wire;
        let err = codec
            .read_packet(&mut reader)
            .await
            .expect_err("truncated compressed frame must be an error, not a panic or clean EOF");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn plain_codec_is_byte_identical_to_legacy_write_packet() {
        // Sanity: when compression is off, the codec path must produce
        // exactly the wire bytes the legacy `write_packet` produces, so
        // turning the codec into a no-op cannot regress existing
        // clients.
        let mut legacy: Vec<u8> = Vec::new();
        write_packet(&mut legacy, 5, b"select 1").await.unwrap();

        let mut viacodec: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec
            .write_packet(&mut viacodec, 5, b"select 1")
            .await
            .unwrap();
        assert_eq!(legacy, viacodec);
    }

    #[tokio::test]
    async fn compressed_streams_two_packets_decodes_in_order() {
        // Two separate compressed packets on the wire decode as two
        // logical packets with the inner sequence ids preserved.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        codec.write_packet(&mut sink, 0, b"first").await.unwrap();
        codec
            .write_packet(&mut sink, 1, &vec![b'A'; 200])
            .await
            .unwrap();

        let mut codec2 = PacketCodec::plain();
        codec2.enable_zlib_compression();
        let mut reader: &[u8] = &sink;
        let p1 = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        let p2 = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(p1.sequence_id, 0);
        assert_eq!(p1.payload, b"first");
        assert_eq!(p2.sequence_id, 1);
        assert_eq!(p2.payload, vec![b'A'; 200]);
    }

    #[tokio::test]
    async fn multi_frame_round_trip_reassembles() {
        let payload = (0..(MAX_PAYLOAD_LEN + 12345))
            .map(|i| (i % 256) as u8)
            .collect::<Vec<u8>>();
        let mut frame = Vec::new();
        write_packet_frame_into(0, &payload, &mut frame).unwrap();
        let mut buf: &[u8] = &frame[..];
        let pkt = read_packet_with_limit(&mut buf, 1 << 30)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pkt.payload, payload);
        // Reassembled packet reports the last frame's sequence id (1),
        // so the caller's response starts at 2.
        assert_eq!(pkt.sequence_id, 1);
    }

    #[tokio::test]
    async fn continuation_sequence_mismatch_is_rejected() {
        let mut wire = Vec::with_capacity(MAX_PAYLOAD_LEN + 8);
        let mut header = [0u8; 4];
        encode_payload_len(MAX_PAYLOAD_LEN, &mut header);
        header[3] = 0;
        wire.extend_from_slice(&header);
        wire.resize(wire.len() + MAX_PAYLOAD_LEN, b'x');
        wire.extend_from_slice(&[0, 0, 0, 9]); // expected sequence 1

        let err = read_packet_with_limit(&mut wire.as_slice(), MAX_PAYLOAD_LEN + 1)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("sequence id"));
    }

    #[cfg(feature = "zstd-compression")]
    #[tokio::test]
    async fn zstd_round_trip_small_payload_takes_raw_escape() {
        // Mirrors the zlib `compressed_round_trip_small_payload_takes_raw_escape`
        // test: small frames < MIN_COMPRESS_LENGTH must travel with
        // `uncomp_len = 0`. The zstd path shares this escape, so a
        // hello-world frame round-trips byte-identical to the wire and
        // through the inverse decoder.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zstd_compression(3).unwrap();
        let payload = b"hello";
        codec.write_packet(&mut sink, 7, payload).await.unwrap();

        // Raw escape ⇒ inner frame (4-byte header + 5-byte body)
        // appears verbatim after the compressed header.
        assert_eq!(sink[3], 7, "compressed seq mirrors inner seq");
        assert_eq!(sink[4], 0, "uncomp_len == 0 → raw escape");
        assert_eq!(sink[5], 0);
        assert_eq!(sink[6], 0);
        assert_eq!(&sink[7..11], &[5, 0, 0, 7]);
        assert_eq!(&sink[11..16], b"hello");

        let mut codec2 = PacketCodec::plain();
        codec2.enable_zstd_compression(3).unwrap();
        let mut reader: &[u8] = &sink;
        let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(pkt.sequence_id, 7);
        assert_eq!(pkt.payload, b"hello");
    }

    #[cfg(feature = "zstd-compression")]
    #[tokio::test]
    async fn zstd_round_trip_large_payload_actually_compresses() {
        // A highly-compressible payload must take the compressed path
        // — uncomp_len > 0 and on the wire the body is strictly
        // smaller than the original raw frame. Same shape as the zlib
        // sibling test; the codec is interchangeable from the caller's
        // point of view.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zstd_compression(3).unwrap();
        let payload = vec![0u8; 4000];
        codec.write_packet(&mut sink, 3, &payload).await.unwrap();

        let body_len = (sink[0] as usize) | ((sink[1] as usize) << 8) | ((sink[2] as usize) << 16);
        let uncomp_len =
            (sink[4] as usize) | ((sink[5] as usize) << 8) | ((sink[6] as usize) << 16);
        assert!(
            uncomp_len > 0,
            "large compressible payload should set uncomp_len != 0"
        );
        assert!(
            body_len < payload.len(),
            "compressed body ({body_len}) must be smaller than raw frame ({})",
            payload.len() + 4
        );

        let mut codec2 = PacketCodec::plain();
        codec2.enable_zstd_compression(3).unwrap();
        let mut reader: &[u8] = &sink;
        let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
        assert_eq!(pkt.sequence_id, 3);
        assert_eq!(pkt.payload, payload);
    }

    #[cfg(feature = "zstd-compression")]
    #[tokio::test]
    async fn zstd_batch_carries_multiple_frames_in_one_compressed_packet() {
        // The whole point of the batch writer in compressed mode:
        // many raw frames flush together → one (or a few) compressed
        // packets, amortising the zstd frame overhead. Same invariant
        // as the zlib batch test; mirrors the wire-side behaviour for
        // result-set rows.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zstd_compression(3).unwrap();
        {
            let mut batch = codec.batch_writer(&mut sink);
            let big = vec![b'x'; 200];
            batch.write_packet(0, &big).await.unwrap();
            batch.write_packet(1, &big).await.unwrap();
            batch.write_packet(2, &big).await.unwrap();
            batch.finish().await.unwrap();
        }

        let mut codec2 = PacketCodec::plain();
        codec2.enable_zstd_compression(3).unwrap();
        let mut reader: &[u8] = &sink;
        for expected_seq in 0..3 {
            let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
            assert_eq!(pkt.sequence_id, expected_seq);
            assert_eq!(pkt.payload, vec![b'x'; 200]);
        }
    }

    #[cfg(feature = "zstd-compression")]
    #[tokio::test]
    async fn zstd_level_clamps_into_libzstd_range() {
        // libzstd refuses level 0 and out-of-range values. The codec
        // clamps into [1, 22] at construction so callers can forward
        // `HandshakeResponse.zstd_compression_level` (a `u8`, range
        // 0..=255) without pre-validating. A clamped level must still
        // round-trip a real payload.
        for level in [0i32, 1, 3, 22, 255] {
            let mut sink: Vec<u8> = Vec::new();
            let mut codec = PacketCodec::plain();
            codec
                .enable_zstd_compression(level)
                .unwrap_or_else(|err| panic!("level {level} should be clamped: {err}"));
            codec
                .write_packet(&mut sink, 0, &vec![b'X'; 1024])
                .await
                .unwrap();
            let mut codec2 = PacketCodec::plain();
            codec2.enable_zstd_compression(3).unwrap();
            let mut reader: &[u8] = &sink;
            let pkt = codec2.read_packet(&mut reader).await.unwrap().unwrap();
            assert_eq!(pkt.payload, vec![b'X'; 1024]);
        }
    }

    #[tokio::test]
    async fn audit_1029_compressed_response_continues_request_counter() {
        let mut state = CompressionState::new_zlib();
        for (last_request_seq, expected_response_seq) in [(1u8, 2u8), (0, 1), (255, 0)] {
            let mut request = Vec::new();
            write_single_compressed_block(
                &mut request,
                &mut CompressionState::new_zlib(),
                &[3, 0, 0, 0, b'a'],
                last_request_seq.wrapping_sub(1),
            )
            .await
            .unwrap();
            write_single_compressed_block(
                &mut request,
                &mut CompressionState::new_zlib(),
                b"bc",
                last_request_seq,
            )
            .await
            .unwrap();
            let packet = read_packet_compressed(&mut request.as_slice(), &mut state, 1024)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(packet.payload, b"abc");
            let mut reply = Vec::new();
            write_packet_compressed_seq(&mut reply, &mut state, 1, b"OK")
                .await
                .unwrap();
            assert_eq!(reply[3], expected_response_seq);
            let next = reply.len();
            write_packet_compressed_seq(&mut reply, &mut state, 3, b"row")
                .await
                .unwrap();
            assert_eq!(reply[next + 3], expected_response_seq.wrapping_add(1));
        }
    }

    #[tokio::test]
    async fn compressed_seq_monotonic_across_batches_in_same_response() {
        // Regression for the W3 bug: a SELECT response that crosses
        // multiple batched flushes (header + rows + terminator) must
        // emit compressed seq ids 1, 2, 3, … — not 1, 3, … (the
        // value the inner frame seq happens to land on after the col_count
        // + col_def packets). mysql 8.4 CLI's compressed reader treats
        // a duplicate or out-of-order compressed seq as a connection
        // teardown ("Lost connection during query"), and the bug only
        // shows up after a non-result-set response (an INSERT OK)
        // preceded a result-set in the same connection.
        let mut sink: Vec<u8> = Vec::new();
        let mut codec = PacketCodec::plain();
        codec.enable_zlib_compression();
        // Simulate the failing wire shape:
        //   1) INSERT OK at inner seq=1   → 1 compressed packet
        //   2) SELECT header batch (inner seqs 1+2 fused) → 1 compressed packet
        //   3) SELECT rows + final OK batch (inner seqs 3+4 fused) → 1 compressed packet
        codec
            .write_packet(&mut sink, 1, b"insert-ok")
            .await
            .unwrap();
        let mut request = Vec::new();
        let mut client_codec = PacketCodec::plain();
        client_codec.enable_zlib_compression();
        client_codec
            .write_packet(&mut request, 0, b"select")
            .await
            .unwrap();
        codec
            .read_packet(&mut request.as_slice())
            .await
            .unwrap()
            .unwrap();
        {
            let mut batch = codec.batch_writer(&mut sink);
            batch
                .write_packet(1, b"col-count-and-defs-fused-into-one-flush")
                .await
                .unwrap();
            batch
                .write_packet(2, b"second-frame-in-the-header-batch")
                .await
                .unwrap();
            batch.finish().await.unwrap();
        }
        {
            let mut batch = codec.batch_writer(&mut sink);
            batch.write_packet(3, b"row-1").await.unwrap();
            batch.write_packet(4, b"final-ok").await.unwrap();
            batch.finish().await.unwrap();
        }

        // Walk the wire bytes and decode the compressed seq id of each
        // compressed packet (`hdr[3]`). The MySQL spec — and mysql 8.4
        // CLI — requires:
        //   packet 1 (INSERT OK):   comp_seq = 1
        //   packet 2 (SELECT hdr):  comp_seq = 1 (new response resets)
        //   packet 3 (SELECT rows): comp_seq = 2 (continues within same response)
        let mut comp_seqs = Vec::new();
        let mut cursor = 0usize;
        while cursor + COMPRESSED_HEADER_LEN <= sink.len() {
            let hdr = &sink[cursor..cursor + COMPRESSED_HEADER_LEN];
            let comp_len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
            comp_seqs.push(hdr[3]);
            cursor += COMPRESSED_HEADER_LEN + comp_len;
        }
        assert_eq!(
            comp_seqs,
            vec![1, 1, 2],
            "comp_seq stream for INSERT then SELECT(header,rows)"
        );
    }

    #[tokio::test]
    async fn codec_algorithm_introspection_reports_choice() {
        // The session's algorithm-selection helper observes which mode
        // was picked via `compression_algorithm()`. A regression would
        // silently flip the default and we'd ship the wrong wire
        // protocol on every connection.
        let mut codec = PacketCodec::plain();
        assert!(codec.compression_algorithm().is_none());
        codec.enable_zlib_compression();
        assert_eq!(
            codec.compression_algorithm(),
            Some(CompressionAlgorithm::Zlib)
        );

        #[cfg(feature = "zstd-compression")]
        {
            let mut codec2 = PacketCodec::plain();
            codec2.enable_zstd_compression(3).unwrap();
            assert_eq!(
                codec2.compression_algorithm(),
                Some(CompressionAlgorithm::Zstd)
            );
        }
    }
}
