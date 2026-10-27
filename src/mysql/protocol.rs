use std::collections::BTreeMap;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

use crate::types::{Column, ColumnType, QueryError, Value};

pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
/// Negotiate the zlib-compressed wire protocol. Per the MySQL 8.0
/// reference, a compressed packet wraps one or more raw protocol frames
/// in a 7-byte header (3-byte compressed payload length + 1-byte
/// compressed sequence id + 3-byte uncompressed payload length). When
/// the uncompressed length is `0`, the payload is sent verbatim — this
/// is the standard escape hatch for short messages that would not
/// benefit from zlib's per-block overhead. Hot small commands (COM_PING
/// reply, single-row OK) take this path; result-set rows and large
/// COM_QUERY payloads get the compressed path.
pub const CLIENT_COMPRESS: u32 = 0x0000_0020;
pub const CLIENT_LOCAL_FILES: u32 = 0x0000_0080;
/// MySQL Connector/J sets this bit by default. With it set, an UPDATE
/// statement's OK packet must report the *matched* row count rather than
/// the *changed* row count — important for ORMs implementing optimistic
/// locking on the executeUpdate() return value.
pub const CLIENT_FOUND_ROWS: u32 = 0x0000_0002;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
/// Client sets this when it is an interactive session (e.g. the `mysql`
/// CLI). The server then uses `interactive_timeout` instead of
/// `wait_timeout` as the idle disconnect threshold for the connection.
pub const CLIENT_INTERACTIVE: u32 = 0x0000_0400;
pub const CLIENT_SSL: u32 = 0x0000_0800;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
/// Client can consume multiple result sets returned by COM_QUERY (notably
/// stored procedures). This is distinct from CLIENT_MULTI_STATEMENTS.
pub const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
/// Prepared-statement counterpart of CLIENT_MULTI_RESULTS, including CALL
/// OUT/INOUT parameter result sets.
pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x0004_0000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_CONNECT_ATTRS: u32 = 0x0010_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x0040_0000;
pub const CLIENT_SESSION_TRACK: u32 = 0x0080_0000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;
/// Result-set metadata is sent only when the client opted in.
/// When set, every column-count packet is prefixed with a single
/// `metadata_follows` byte (0 = NONE, 1 = FULL). When NONE, the
/// column-definition rows are skipped and only the column count + rows
/// ship — Connector/J 8.0.32+ uses this to suppress metadata for
/// repeated executions of the same prepared statement.
pub const CLIENT_OPTIONAL_RESULTSET_METADATA: u32 = 0x0200_0000;
/// Negotiate the zstd-compressed wire protocol. Adds one byte
/// (`zstd_compression_level`, 1–22) to the handshake response so the
/// server knows what level the client requested for its OUTBOUND
/// stream. Per MySQL 8.0.18+ semantics, when both `CLIENT_COMPRESS`
/// and `CLIENT_ZSTD_COMPRESSION_ALGORITHM` are negotiated, ZSTD wins —
/// it dominates zlib on every ratio/CPU tradeoff we measured and is
/// the only path that mysql 8.4+ / mariadb 11+ CLI clients still ship.
pub const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 0x0400_0000;
/// Each COM_QUERY / COM_STMT_EXECUTE carries a leading query-attribute
/// block (parameter_count + null bitmap + types + values + names).
/// mydb accepts and silently discards the attributes — Connector/J
/// 8.0.26+ advertises this bit unconditionally, and refusing it would
/// force a connector downgrade; the values themselves (used by
/// `mysql_query_attribute_string()`) have no SQL surface in mydb yet.
pub const CLIENT_QUERY_ATTRIBUTES: u32 = 0x0800_0000;
pub const CLIENT_MULTI_FACTOR_AUTHENTICATION: u32 = 0x1000_0000;

/// `RESULTSET_METADATA_*` byte values: when CLIENT_OPTIONAL_RESULTSET_METADATA
/// is negotiated, the column-count packet of every result set carries
/// this one-byte prefix. `FULL` = column definitions follow as usual;
/// `NONE` = the rows arrive immediately after the column count and the
/// client is expected to reuse cached metadata from a previous round.
pub const RESULTSET_METADATA_NONE: u8 = 0;
pub const RESULTSET_METADATA_FULL: u8 = 1;

pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
/// The current prepared-statement result set contains stored-procedure
/// OUT/INOUT parameter values.
pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
/// MySQL 8.0 OK-packet status bit indicating that this packet carries a
/// `session_state_info` payload (CLIENT_SESSION_TRACK protocol). The bit
/// is only meaningful when the client negotiated `CLIENT_SESSION_TRACK`
/// during handshake; otherwise the trailing bytes after `info` are not
/// read by the client.
pub const SERVER_SESSION_STATE_CHANGED: u16 = 0x4000;

/// `SESSION_TRACK_SYSTEM_VARIABLES` — pair of `(variable name, new
/// value)`. Used by `SET SESSION x = ...` to notify the client of the
/// new effective value (connectors mirror it into their session map).
pub const SESSION_TRACK_SYSTEM_VARIABLES: u8 = 0x00;
/// `SESSION_TRACK_SCHEMA` — fired by `USE <db>` and by an OK packet
/// emitted after a successful COM_INIT_DB. Payload is the new schema
/// name as a length-encoded string.
pub const SESSION_TRACK_SCHEMA: u8 = 0x01;
/// `SESSION_TRACK_STATE_CHANGE` — set whenever any other session-track
/// channel fires this round, so clients that only care "did anything
/// change?" can poll one bit. Payload is the ASCII string `"1"`.
pub const SESSION_TRACK_STATE_CHANGE: u8 = 0x02;
/// `SESSION_TRACK_GTIDS` — committed GTID set delta for the statement.
pub const SESSION_TRACK_GTIDS: u8 = 0x03;
/// `SESSION_TRACK_TRANSACTION_CHARACTERISTICS` — `SET TRANSACTION ...`
/// characteristics for the next transaction.
pub const SESSION_TRACK_TRANSACTION_CHARACTERISTICS: u8 = 0x04;
/// `SESSION_TRACK_TRANSACTION_STATE` — 8-byte ASCII transaction-state
/// flag string (e.g. `T_______` for "in transaction, read tracked").
pub const SESSION_TRACK_TRANSACTION_STATE: u8 = 0x05;

pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
/// Legacy schema-discovery command (`mysql_list_fields`). MySQL 8.0
/// keeps the surface alive; some old mysqldump / ODBC / Perl-DBI
/// flows still probe table column shape with it before the first
/// SELECT, so we keep parity.
pub const COM_FIELD_LIST: u8 = 0x04;
pub const COM_REFRESH: u8 = 0x07;
pub const COM_SHUTDOWN: u8 = 0x08;
pub const COM_STATISTICS: u8 = 0x09;
pub const COM_PROCESS_INFO: u8 = 0x0a;
pub const COM_PROCESS_KILL: u8 = 0x0c;
pub const COM_DEBUG: u8 = 0x0d;
pub const COM_PING: u8 = 0x0e;
pub const COM_CHANGE_USER: u8 = 0x11;
pub const COM_BINLOG_DUMP: u8 = 0x12;
pub const COM_REGISTER_SLAVE: u8 = 0x15;
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
pub const COM_STMT_CLOSE: u8 = 0x19;
pub const COM_STMT_RESET: u8 = 0x1a;
pub const COM_SET_OPTION: u8 = 0x1b;
pub const COM_STMT_FETCH: u8 = 0x1c;
pub const COM_BINLOG_DUMP_GTID: u8 = 0x1e;
pub const COM_RESET_CONNECTION: u8 = 0x1f;

const MYSQL_TYPE_DECIMAL: u8 = 0x00;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_FLOAT: u8 = 0x04;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_NULL: u8 = 0x06;
const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
/// 3-byte signed integer (MEDIUMINT). MySQL puts the value in 4 bytes
/// LE on the wire, but advertises the column with the INT24 type byte
/// so clients/JDBC pick the right column metadata width.
const MYSQL_TYPE_INT24: u8 = 0x09;
const MYSQL_TYPE_DATE: u8 = 0x0a;
const MYSQL_TYPE_TIME: u8 = 0x0b;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
/// 2-byte unsigned integer (YEAR), advertised as MYSQL_TYPE_YEAR so
/// JDBC's ResultSetMetaData reports the correct column type — the
/// previous code borrowed MYSQL_TYPE_TINY, making clients interpret
/// `YEAR` as `TINYINT`.
const MYSQL_TYPE_YEAR: u8 = 0x0d;
const MYSQL_TYPE_NEWDATE: u8 = 0x0e;
const MYSQL_TYPE_VARCHAR: u8 = 0x0f;
const MYSQL_TYPE_TIMESTAMP2: u8 = 0x11;
const MYSQL_TYPE_DATETIME2: u8 = 0x12;
const MYSQL_TYPE_TIME2: u8 = 0x13;
const MYSQL_TYPE_SHORT: u8 = 0x02;
const MYSQL_TYPE_TINY: u8 = 0x01;
/// `MYSQL_TYPE_BIT` — the wire type for `BIT(M)` columns. The value is
/// carried as a `ceil(M/8)`-byte big-endian binary string in both the text
/// and binary protocols (never a decimal integer).
const MYSQL_TYPE_BIT: u8 = 0x10;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
const MYSQL_TYPE_ENUM: u8 = 0xf7;
const MYSQL_TYPE_SET: u8 = 0xf8;
const MYSQL_TYPE_JSON: u8 = 0xf5;
const MYSQL_TYPE_GEOMETRY: u8 = 0xff;
const MYSQL_TYPE_VECTOR: u8 = 0xf2;
const MYSQL_TYPE_TINY_BLOB: u8 = 0xf9;
const MYSQL_TYPE_MEDIUM_BLOB: u8 = 0xfa;
const MYSQL_TYPE_LONG_BLOB: u8 = 0xfb;
pub(crate) const MYSQL_TYPE_BLOB: u8 = 0xfc;
pub(crate) const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
pub(crate) const MYSQL_TYPE_STRING: u8 = 0xfe;
const DEFAULT_UTF8MB4_COLLATION: u16 = crate::charset::UTF8MB4_0900_AI_CI_ID;
/// MySQL `binary` collation id. BLOB-family columns must advertise this
/// so clients treat the value as raw bytes rather than decoding it as
/// text (which would corrupt non-UTF-8 binary data).
const BINARY_COLLATION: u16 = 63;

const SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_LOCAL_FILES
    | CLIENT_PROTOCOL_41
    | CLIENT_INTERACTIVE
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PS_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
    | CLIENT_SESSION_TRACK
    | CLIENT_DEPRECATE_EOF;

/// Capability state for one classic-protocol connection. Behaviour must use
/// the intersected `negotiated`/`runtime` flags, never raw client bits. The
/// `COM_SET_OPTION` command is the protocol-defined exception: it may enable
/// `CLIENT_MULTI_STATEMENTS` after the handshake when the server advertised
/// support, even if the client did not initially request the capability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NegotiatedCapabilities {
    pub requested: u32,
    pub advertised: u32,
    pub negotiated: u32,
    pub runtime: u32,
}

impl NegotiatedCapabilities {
    pub fn new(requested: u32, advertised: u32) -> Self {
        let negotiated = requested & advertised;
        Self {
            requested,
            advertised,
            negotiated,
            runtime: negotiated,
        }
    }

    #[inline]
    pub fn has(self, capability: u32) -> bool {
        self.runtime & capability != 0
    }

    #[inline]
    pub fn runtime(self) -> u32 {
        self.runtime
    }

    pub fn set_multi_statements(&mut self, enabled: bool) {
        if enabled && self.advertised & CLIENT_MULTI_STATEMENTS != 0 {
            self.runtime |= CLIENT_MULTI_STATEMENTS;
        } else {
            self.runtime &= !CLIENT_MULTI_STATEMENTS;
        }
    }
}

pub const LOCAL_INFILE_REQUEST: u8 = 0xFB;

/// Length of the MySQL scramble ("auth-plugin-data"). MySQL 8.0 always uses
/// 20 random bytes split 8 + 12 across the initial handshake packet.
pub const AUTH_PLUGIN_DATA_LEN: usize = 20;

/// Fixed nonce used **only** by the legacy `QueryEngine::authenticate`
/// convenience method (`AuthRequest::legacy`) and its unit tests. Production
/// logins never use this: each connection generates a fresh random nonce via
/// [`generate_auth_nonce`] stored on the session, so a captured scramble on
/// one connection cannot be replayed on the next. Keeping a fixed value here
/// lets the legacy verify-logic tests build a deterministic scramble.
const LEGACY_TEST_AUTH_NONCE: &[u8] = b"mydb-auth-salt-12345";

pub fn auth_plugin_data() -> &'static [u8] {
    LEGACY_TEST_AUTH_NONCE
}

/// Generate a fresh 20-byte scramble for a connection. Bytes are drawn from
/// the printable-ASCII range [33, 126] so none is `0x00` — the handshake
/// packet writes the second scramble chunk as a nul-terminated string, and a
/// null byte inside it would truncate the salt on the wire (the same reason
/// MySQL's `create_random_string` avoids null bytes).
pub fn generate_auth_nonce() -> [u8; AUTH_PLUGIN_DATA_LEN] {
    use rand::{rngs::OsRng, RngCore};
    let mut raw = [0u8; AUTH_PLUGIN_DATA_LEN];
    OsRng.fill_bytes(&mut raw);
    let mut out = [0u8; AUTH_PLUGIN_DATA_LEN];
    for (dst, src) in out.iter_mut().zip(raw.iter()) {
        *dst = 33 + (src % 94);
    }
    out
}

pub fn auth_switch_request_packet(plugin: &str, auth_plugin_data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + plugin.len() + 1 + auth_plugin_data.len() + 1);
    payload.push(0xfe);
    write_null_string(&mut payload, plugin.as_bytes());
    payload.extend_from_slice(auth_plugin_data);
    payload.push(0x00);
    payload
}

pub const CACHING_SHA2_FAST_AUTH_SUCCESS: u8 = 0x03;
pub const CACHING_SHA2_PERFORM_FULL_AUTH: u8 = 0x04;
pub const CACHING_SHA2_REQUEST_PUBLIC_KEY: u8 = 0x02;

pub fn auth_more_data_packet(payload_byte: u8) -> Vec<u8> {
    vec![0x01, payload_byte]
}

pub fn auth_more_data_with_payload(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(0x01);
    out.extend_from_slice(payload);
    out
}

pub fn auth_more_data_fast_success() -> Vec<u8> {
    auth_more_data_packet(CACHING_SHA2_FAST_AUTH_SUCCESS)
}

pub fn auth_more_data_perform_full_auth() -> Vec<u8> {
    auth_more_data_packet(CACHING_SHA2_PERFORM_FULL_AUTH)
}

pub fn uses_caching_sha2(plugin: Option<&str>) -> bool {
    matches!(plugin, Some("caching_sha2_password"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeResponse {
    pub capability_flags: u32,
    pub max_packet_size: u32,
    pub character_set: u8,
    pub username: String,
    pub database: Option<String>,
    pub auth_response: Vec<u8>,
    pub auth_plugin_name: Option<String>,
    pub connection_attrs: BTreeMap<String, String>,
    /// The zstd compression level requested by the client. Only
    /// meaningful when `capability_flags & CLIENT_ZSTD_COMPRESSION_ALGORITHM`
    /// is non-zero — clients ship it as one trailing byte after the
    /// connection attributes block.
    pub zstd_compression_level: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SslRequest {
    pub capability_flags: u32,
    pub max_packet_size: u32,
    pub character_set: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeUserRequest {
    pub username: String,
    pub database: Option<String>,
    pub auth_response: Vec<u8>,
    pub auth_plugin_name: Option<String>,
    pub character_set: Option<u16>,
    pub connection_attrs: BTreeMap<String, String>,
}

#[cfg(test)]
pub fn initial_handshake_packet(connection_id: u32, ssl_supported: bool, nonce: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    initial_handshake_packet_into(connection_id, ssl_supported, nonce, &mut payload);
    payload
}

pub fn initial_handshake_packet_into(
    connection_id: u32,
    ssl_supported: bool,
    nonce: &[u8],
    payload: &mut Vec<u8>,
) {
    payload.clear();
    let capabilities = server_capabilities(ssl_supported);
    let lower_caps = (capabilities & 0xffff) as u16;
    let upper_caps = (capabilities >> 16) as u16;
    let auth_plugin_name = crate::engine::auth::default_auth_plugin();
    let auth_plugin_data_len = (nonce.len() + 1) as u8;
    let auth_part_1 = &nonce[..8];
    let auth_part_2 = &nonce[8..];

    payload.push(0x0a);
    write_null_string(
        payload,
        crate::server_version::MYSQL_COMPAT_VERSION.as_bytes(),
    );
    payload.extend_from_slice(&connection_id.to_le_bytes());
    payload.extend_from_slice(auth_part_1);
    payload.push(0x00);
    write_u16_le(payload, lower_caps);
    payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
    write_u16_le(payload, SERVER_STATUS_AUTOCOMMIT);
    write_u16_le(payload, upper_caps);
    payload.push(auth_plugin_data_len);
    payload.extend_from_slice(&[0_u8; 10]);
    payload.extend_from_slice(auth_part_2);
    payload.push(0x00);
    write_null_string(payload, auth_plugin_name.as_bytes());
}

pub fn server_capabilities(ssl_supported: bool) -> u32 {
    let mut caps = SERVER_CAPABILITIES;
    if ssl_supported {
        caps |= CLIENT_SSL;
    }
    // Compression capabilities are gated independently so an operator
    // can roll out zstd (mysql 8.4+ CLI default) without the legacy
    // zlib path, or vice versa. `MYDB_COMPRESS_PROTOCOL=off` kills
    // both simultaneously and is the safe kill switch for any client
    // that misbehaves on the compressed path.
    if crate::mysql::packet::compression_protocol_enabled() {
        if crate::mysql::packet::zlib_compression_enabled() {
            caps |= CLIENT_COMPRESS;
        }
        if crate::mysql::packet::zstd_compression_enabled() {
            caps |= CLIENT_ZSTD_COMPRESSION_ALGORITHM;
        }
    }
    // CLIENT_QUERY_ATTRIBUTES — accept and silently discard. Connector/J
    // 8.0.26+ sets this bit unconditionally; if we don't advertise it,
    // the connector falls back to a plain-COM_QUERY path that some users
    // configure off (`useAttributes=false`). Refusing the capability
    // would force every JDBC user to set the property, which is exactly
    // the kind of "tiny but undocumented" footgun we want to avoid.
    caps |= CLIENT_QUERY_ATTRIBUTES;
    // CLIENT_OPTIONAL_RESULTSET_METADATA — accept the bit and honour
    // the per-session `resultset_metadata` toggle (FULL/NONE). Default
    // stays FULL so existing clients see the same wire shape they do
    // today; clients that opt in via `SET resultset_metadata=NONE` get
    // the leaner column-count-only path.
    caps |= CLIENT_OPTIONAL_RESULTSET_METADATA;
    caps
}

pub fn parse_ssl_request(payload: &[u8]) -> Result<SslRequest, String> {
    if payload.len() != 32 {
        return Err(format!(
            "SSLRequest packet must be 32 bytes, got {}",
            payload.len()
        ));
    }

    let mut cursor = Cursor::new(payload);
    let capability_flags = cursor.read_u32_le()?;
    if capability_flags & CLIENT_PROTOCOL_41 == 0 {
        return Err("SSLRequest is missing CLIENT_PROTOCOL_41".to_string());
    }
    if capability_flags & CLIENT_SSL == 0 {
        return Err("SSLRequest is missing CLIENT_SSL".to_string());
    }
    let max_packet_size = cursor.read_u32_le()?;
    let character_set = cursor.read_u8()?;
    cursor.skip(23)?;

    Ok(SslRequest {
        capability_flags,
        max_packet_size,
        character_set,
    })
}

fn parse_connection_attributes(
    cursor: &mut Cursor<'_>,
    payload_len: usize,
    boundary_error: &str,
) -> Result<BTreeMap<String, String>, String> {
    let attrs_len = cursor.read_lenenc_int()? as usize;
    let attrs_end = cursor
        .position
        .checked_add(attrs_len)
        .ok_or_else(|| "connection attributes length overflow".to_string())?;
    if attrs_end > payload_len {
        return Err("connection attributes exceed payload length".to_string());
    }

    let mut attrs = BTreeMap::new();
    while cursor.position < attrs_end {
        let key = cursor.read_lenenc_string()?;
        let value = cursor.read_lenenc_string()?;
        attrs.insert(key, value);
    }
    if cursor.position != attrs_end {
        return Err(boundary_error.to_string());
    }
    Ok(attrs)
}

pub fn parse_handshake_response(payload: &[u8]) -> Result<HandshakeResponse, String> {
    let mut cursor = Cursor::new(payload);
    let capability_flags = cursor.read_u32_le()?;
    let max_packet_size = cursor.read_u32_le()?;
    let character_set = cursor.read_u8()?;
    cursor.skip(23)?;

    let username = cursor.read_null_string()?;
    let auth_response = if capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let len = cursor.read_lenenc_int()? as usize;
        cursor.read_bytes(len)?.to_vec()
    } else if capability_flags & CLIENT_SECURE_CONNECTION != 0 {
        let len = cursor.read_u8()? as usize;
        cursor.read_bytes(len)?.to_vec()
    } else {
        cursor.read_null_string()?.into_bytes()
    };

    let database = if capability_flags & CLIENT_CONNECT_WITH_DB != 0 {
        Some(cursor.read_null_string()?)
    } else {
        None
    };

    let auth_plugin_name = if capability_flags & CLIENT_PLUGIN_AUTH != 0 && cursor.remaining() > 0 {
        Some(cursor.read_null_string()?)
    } else {
        None
    };

    let connection_attrs = if capability_flags & CLIENT_CONNECT_ATTRS != 0 && cursor.remaining() > 0
    {
        parse_connection_attributes(
            &mut cursor,
            payload.len(),
            "connection attribute crossed declared block boundary",
        )?
    } else {
        BTreeMap::new()
    };

    // Trailing `zstd_compression_level` byte (MySQL 8.0.18+). Clients
    // that advertise CLIENT_ZSTD_COMPRESSION_ALGORITHM MUST append it
    // after the connection attributes; mariadb 11+ omits it sometimes,
    // so we tolerate a missing byte and fall back to MySQL's default
    // level of 3.
    let zstd_compression_level =
        if capability_flags & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0 && cursor.remaining() > 0 {
            Some(cursor.read_u8()?)
        } else {
            None
        };
    if cursor.remaining() != 0 {
        return Err(format!(
            "handshake response has {} unexpected trailing byte(s)",
            cursor.remaining()
        ));
    }

    Ok(HandshakeResponse {
        capability_flags,
        max_packet_size,
        character_set,
        username,
        database,
        auth_response,
        auth_plugin_name,
        connection_attrs,
        zstd_compression_level,
    })
}

pub fn parse_change_user_request(
    payload: &[u8],
    capability_flags: u32,
) -> Result<ChangeUserRequest, String> {
    let mut cursor = Cursor::new(payload);
    let username = cursor.read_null_string()?;
    let auth_response = if capability_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let len = cursor.read_lenenc_int()? as usize;
        cursor.read_bytes(len)?.to_vec()
    } else if capability_flags & CLIENT_SECURE_CONNECTION != 0 {
        let len = cursor.read_u8()? as usize;
        cursor.read_bytes(len)?.to_vec()
    } else {
        cursor.read_null_string()?.into_bytes()
    };
    let database = if cursor.remaining() > 0 {
        let db = cursor.read_null_string()?;
        (!db.is_empty()).then_some(db)
    } else {
        None
    };
    let character_set = if capability_flags & CLIENT_PROTOCOL_41 != 0 && cursor.remaining() >= 2 {
        Some(cursor.read_u16_le()?)
    } else {
        None
    };
    let auth_plugin_name = if capability_flags & CLIENT_PLUGIN_AUTH != 0 && cursor.remaining() > 0 {
        Some(cursor.read_null_string()?)
    } else {
        None
    };
    let connection_attrs = if capability_flags & CLIENT_CONNECT_ATTRS != 0 && cursor.remaining() > 0
    {
        parse_connection_attributes(
            &mut cursor,
            payload.len(),
            "COM_CHANGE_USER attribute crossed declared block boundary",
        )?
    } else {
        BTreeMap::new()
    };
    if cursor.remaining() != 0 {
        return Err(format!(
            "COM_CHANGE_USER has {} unexpected trailing byte(s)",
            cursor.remaining()
        ));
    }
    Ok(ChangeUserRequest {
        username,
        database,
        auth_response,
        auth_plugin_name,
        character_set,
        connection_attrs,
    })
}

#[cfg(test)]
pub fn ok_packet() -> Vec<u8> {
    ok_packet_with_header(0x00, 0)
}

#[cfg(test)]
pub fn ok_packet_with_affected_rows(affected_rows: u64) -> Vec<u8> {
    ok_packet_with_header(0x00, affected_rows)
}

#[cfg(test)]
pub fn ok_packet_with_affected_rows_insert_id_status_warnings(
    affected_rows: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
) -> Vec<u8> {
    ok_packet_with_header_insert_id_status_warnings(
        0x00,
        affected_rows,
        insert_id,
        status,
        warnings,
    )
}

pub fn stmt_prepare_ok_packet(
    statement_id: u32,
    columns: usize,
    params: usize,
    warnings: u16,
    client_capabilities: u32,
    metadata_follows: u8,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0x00);
    write_u32_le(&mut payload, statement_id);
    write_u16_le(&mut payload, columns as u16);
    write_u16_le(&mut payload, params as u16);
    payload.push(0x00);
    write_u16_le(&mut payload, warnings);
    if client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0 {
        payload.push(metadata_follows);
    }
    payload
}

pub fn parameter_definition_packet(index: usize) -> Vec<u8> {
    column_definition_packet(&format!("?{index}"), ColumnType::VarString)
}

#[cfg(test)]
pub fn eof_as_ok_packet() -> Vec<u8> {
    ok_packet_with_header(0xfe, 0)
}

pub fn eof_as_ok_packet_with_status_warnings(status: u16, warnings: u16) -> Vec<u8> {
    ok_packet_with_header_status_warnings(0xfe, 0, status, warnings)
}

#[cfg(test)]
fn ok_packet_with_header(header: u8, affected_rows: u64) -> Vec<u8> {
    ok_packet_with_header_and_status(header, affected_rows, SERVER_STATUS_AUTOCOMMIT)
}

#[cfg(test)]
fn ok_packet_with_header_and_status(header: u8, affected_rows: u64, status: u16) -> Vec<u8> {
    ok_packet_with_header_status_warnings(header, affected_rows, status, 0)
}

fn ok_packet_with_header_status_warnings(
    header: u8,
    affected_rows: u64,
    status: u16,
    warnings: u16,
) -> Vec<u8> {
    ok_packet_with_header_insert_id_status_warnings(header, affected_rows, 0, status, warnings)
}

fn ok_packet_with_header_insert_id_status_warnings(
    header: u8,
    affected_rows: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(header);
    write_lenenc_int(&mut payload, affected_rows);
    write_lenenc_int(&mut payload, insert_id);
    write_u16_le(&mut payload, status);
    write_u16_le(&mut payload, warnings);
    payload
}

#[cfg(test)]
/// Extended OK packet variant carrying the MySQL `info` trailer — the
/// `Rows matched: N Changed: M Warnings: W` (UPDATE) / `Records: N
/// Duplicates: D Warnings: W` (multi-row INSERT) text that MySQL clients
/// rely on. The trailer is encoded as a lenenc-string and only appended
/// when the caller has something to say; an empty `info` produces the
/// same bytes as `ok_packet_with_affected_rows_insert_id_status_warnings`.
///
/// When `session_state_info` is non-empty *and* the caller already added
/// `SERVER_SESSION_STATE_CHANGED` to `status`, the OK packet appends a
/// length-encoded session-state-info block per the CLIENT_SESSION_TRACK
/// protocol. The block is the concatenation of one or more `(type,
/// lenenc-string data)` entries already pre-encoded by the caller via
/// [`encode_session_track_schema`] etc.
pub fn ok_packet_with_full_payload(
    affected_rows: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
    info: &str,
    session_state_info: &[u8],
) -> Vec<u8> {
    let mut payload = Vec::new();
    ok_packet_with_full_payload_into(
        affected_rows,
        insert_id,
        status,
        warnings,
        info,
        session_state_info,
        &mut payload,
    );
    payload
}

#[cfg(test)]
pub fn ok_packet_with_full_payload_into(
    affected_rows: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
    info: &str,
    session_state_info: &[u8],
    payload: &mut Vec<u8>,
) {
    ok_packet_with_full_payload_capabilities_into(
        affected_rows,
        insert_id,
        status,
        warnings,
        info,
        session_state_info,
        CLIENT_SESSION_TRACK,
        payload,
    )
}

pub fn ok_packet_with_full_payload_capabilities_into(
    affected_rows: u64,
    insert_id: u64,
    status: u16,
    warnings: u16,
    info: &str,
    session_state_info: &[u8],
    client_capabilities: u32,
    payload: &mut Vec<u8>,
) {
    payload.clear();
    payload.push(0x00);
    write_lenenc_int(payload, affected_rows);
    write_lenenc_int(payload, insert_id);
    write_u16_le(payload, status);
    write_u16_le(payload, warnings);
    let emit_state = !session_state_info.is_empty() && (status & SERVER_SESSION_STATE_CHANGED) != 0;
    let session_track = client_capabilities & CLIENT_SESSION_TRACK != 0;
    // CLIENT_SESSION_TRACK requires `info` to be present as a lenenc-string
    // BEFORE the session-state-info block, so the client can read past
    // it deterministically. When we have no `info` text but DO have a
    // state-info block, emit an empty lenenc-string (`0x00`) so the
    // wire layout stays parseable.
    // MySQL 8.0's net_send_ok writes a lenenc info field even when
    // CLIENT_SESSION_TRACK is absent (despite older protocol prose calling
    // that branch string<EOF>). libmysqlclient/mysql_common implement the
    // server behaviour; emitting raw EOF text makes Rust mysql clients fail
    // after a successful multi-row INSERT.
    if !info.is_empty() || (session_track && emit_state) {
        let bytes = info.as_bytes();
        write_lenenc_int(payload, bytes.len() as u64);
        payload.extend_from_slice(bytes);
    }
    if session_track && emit_state {
        write_lenenc_int(payload, session_state_info.len() as u64);
        payload.extend_from_slice(session_state_info);
    }
}

/// Encode one `SESSION_TRACK_SCHEMA` entry into the running
/// `session_state_info` buffer. Each entry is
/// `[type byte] [lenenc data-length] [lenenc schema-name]`, where the
/// data segment is itself `lenenc(schema_name)` (MySQL's spec nests one
/// lenenc-string inside another length-encoded data block — this is what
/// real MySQL 8.0 emits and what connector/J et al. parse).
pub fn encode_session_track_schema(buf: &mut Vec<u8>, schema: &str) {
    buf.push(SESSION_TRACK_SCHEMA);
    let name_bytes = schema.as_bytes();
    let mut inner = Vec::with_capacity(name_bytes.len() + 9);
    write_lenenc_int(&mut inner, name_bytes.len() as u64);
    inner.extend_from_slice(name_bytes);
    write_lenenc_int(buf, inner.len() as u64);
    buf.extend_from_slice(&inner);
}

/// Encode one `SESSION_TRACK_SYSTEM_VARIABLES` entry: the variable name
/// and its new value, each a length-encoded string, wrapped in an outer
/// length-encoded data block (matching MySQL 8.0 so connector/J et al.
/// mirror the value into their session map).
pub fn encode_session_track_system_variables(buf: &mut Vec<u8>, name: &str, value: &str) {
    buf.push(SESSION_TRACK_SYSTEM_VARIABLES);
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let mut inner = Vec::with_capacity(name_bytes.len() + value_bytes.len() + 18);
    write_lenenc_int(&mut inner, name_bytes.len() as u64);
    inner.extend_from_slice(name_bytes);
    write_lenenc_int(&mut inner, value_bytes.len() as u64);
    inner.extend_from_slice(value_bytes);
    write_lenenc_int(buf, inner.len() as u64);
    buf.extend_from_slice(&inner);
}

/// Encode one `SESSION_TRACK_STATE_CHANGE` entry. Payload is the ASCII
/// byte `'1'`, signalling that *some* tracked state changed this round
/// — clients use this as a single bit for "did anything change?".
pub fn encode_session_track_state_change(buf: &mut Vec<u8>) {
    buf.push(SESSION_TRACK_STATE_CHANGE);
    // data = lenenc("1") → 0x01 0x31. Outer length wraps that two-byte
    // block, so the full entry on the wire is [02 02 01 31].
    let mut inner = Vec::with_capacity(2);
    write_lenenc_int(&mut inner, 1);
    inner.push(b'1');
    write_lenenc_int(buf, inner.len() as u64);
    buf.extend_from_slice(&inner);
}

pub fn err_packet(err: &QueryError) -> Vec<u8> {
    assert_ne!(
        err.spec().map(|spec| spec.domain()),
        Some(crate::error::ErrorDomain::MysqlClient),
        "CR_* client conditions must never be encoded as server ERR packets"
    );
    let mut payload = Vec::new();
    payload.push(0xff);
    write_u16_le(&mut payload, err.code());
    payload.push(b'#');
    payload.extend_from_slice(err.sql_state().as_bytes());
    payload.extend_from_slice(err.message().as_bytes());
    payload
}

/// ERR packet sent before capability negotiation (connection refusal before
/// HandshakeV10). No SQLSTATE marker may be included because the peer has not
/// yet announced CLIENT_PROTOCOL_41.
pub fn pre_handshake_err_packet(
    spec: crate::error::ErrorSpec,
    message: impl Into<String>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0xff);
    payload.extend_from_slice(&spec.code().get().to_le_bytes());
    payload.extend_from_slice(message.into().as_bytes());
    payload
}

pub fn command_err_packet_for_spec(
    spec: crate::mysql::errors::MysqlErrorSpec,
    message: impl Into<String>,
) -> Vec<u8> {
    err_packet(&crate::mysql::errors::error(spec, message))
}

#[allow(dead_code)]
/// Per-result-set ZEROFILL pad widths, or `None` when no projected
/// column carries `ZEROFILL_FLAG` — the common case, so the per-row
/// text encode path stays branch-free. Widths follow MySQL's default
/// display widths for the unsigned integer types (mydb does not
/// preserve explicit display widths) and `precision (+1 for the dot)`
/// for DECIMAL.
pub fn zerofill_pad_spec(columns: &[crate::types::Column]) -> Option<Vec<u16>> {
    let mut any = false;
    let spec: Vec<u16> = columns
        .iter()
        .map(|column| {
            if column.source.flags & ZEROFILL_FLAG == 0 {
                return 0;
            }
            let width = match column.column_type {
                crate::types::ColumnType::Tiny => 3,
                crate::types::ColumnType::Short => 5,
                crate::types::ColumnType::MediumInt => 8,
                crate::types::ColumnType::Long => 10,
                crate::types::ColumnType::LongLong => 20,
                crate::types::ColumnType::Float => {
                    column.source.length.unwrap_or(12).min(u32::from(u16::MAX)) as u16
                }
                crate::types::ColumnType::Double => {
                    column.source.length.unwrap_or(22).min(u32::from(u16::MAX)) as u16
                }
                crate::types::ColumnType::Decimal { precision, scale } => {
                    u16::from(precision) + u16::from(scale > 0)
                }
                _ => 0,
            };
            if width > 0 {
                any = true;
            }
            width
        })
        .collect();
    any.then_some(spec)
}

/// Render a row with ZEROFILL display padding applied — numeric values
/// in flagged columns become zero-padded strings (text protocol only;
/// the binary protocol ships raw numerics plus the flag, like MySQL).
pub fn zerofill_padded_row(row: &[Value], spec: &[u16]) -> Vec<Value> {
    row.iter()
        .enumerate()
        .map(|(idx, value)| {
            let width = spec.get(idx).copied().unwrap_or(0) as usize;
            if width == 0 {
                return value.clone();
            }
            let text = match value {
                Value::Int(n) if *n >= 0 => n.to_string(),
                Value::UInt(n) => n.to_string(),
                Value::Float(n) if n.is_sign_positive() || *n == 0.0 => {
                    crate::types::mysql_f32_text(*n)
                }
                Value::Double(n) if n.is_sign_positive() || *n == 0.0 => {
                    let mut buf = ryu::Buffer::new();
                    crate::types::ryu_to_mysql_text(buf.format(*n), || n.to_string()).into_owned()
                }
                Value::Decimal(s) if !s.starts_with('-') => s.clone(),
                Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
                    let s = crate::storage::value_to_string(value);
                    if s.starts_with('-') {
                        return value.clone();
                    }
                    s
                }
                _ => return value.clone(),
            };
            if text.len() >= width {
                return value.clone();
            }
            let mut padded = String::with_capacity(width);
            for _ in 0..(width - text.len()) {
                padded.push('0');
            }
            padded.push_str(&text);
            Value::String(padded)
        })
        .collect()
}

#[cfg(test)]
pub fn text_row_packet(row: &[Value]) -> Vec<u8> {
    let mut payload = Vec::new();
    text_row_packet_into(row, &mut payload);
    payload
}

#[cfg(test)]
pub fn text_row_packet_into(row: &[Value], payload: &mut Vec<u8>) {
    text_row_packet_into_with_charset(row, payload, None);
}

#[cfg(test)]
pub fn text_row_packet_into_with_charset(
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&str>,
) {
    text_row_packet_into_with_columns(row, payload, charset, None);
}

#[cfg(test)]
/// Column-aware variant: temporal cells use the column's declared `fsp`
/// (from `Column.source.decimals`) so a `DATETIME(3)` value `.830` renders
/// with the trailing zero, matching MySQL behaviour. Pass `None` for
/// `columns` to keep the legacy (trim-trailing-zero) shape.
///
/// This resolves the charset name once per call and delegates to
/// `text_row_packet_into_with_columns_info`. On the result-set hot path
/// (per-row calls) the caller should resolve the charset ONCE for the
/// whole result set with `crate::charset::resolve_result_charset` and call
/// the `_info` worker directly, so the resolve does not repeat per row.
pub fn text_row_packet_into_with_columns(
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&str>,
    columns: Option<&[Column]>,
) {
    let charset = crate::charset::resolve_result_charset(charset);
    text_row_packet_into_with_columns_info(row, payload, charset, columns);
}

/// Worker for `text_row_packet_into_with_columns` that takes an
/// already-resolved charset descriptor. The result-set encoder resolves
/// the connection's `character_set_results` exactly once per result set
/// and calls this per row, so a wide string-heavy result set no longer
/// pays two heap allocations + a hash lookup *per string cell* just to
/// re-derive the same descriptor.
pub fn text_row_packet_into_with_columns_info(
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&'static crate::charset::CharsetInfo>,
    columns: Option<&[Column]>,
) {
    write_text_row::<false>(row, payload, charset, columns);
}

pub(crate) fn vector_text_row_packet_into(
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&'static crate::charset::CharsetInfo>,
    columns: &[Column],
) {
    write_text_row::<true>(row, payload, charset, Some(columns));
}

fn write_text_row<const VECTOR_TEXT: bool>(
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&'static crate::charset::CharsetInfo>,
    columns: Option<&[Column]>,
) {
    payload.clear();
    let estimate = if VECTOR_TEXT {
        estimate_vector_text_row_packet_bytes(row, charset, columns)
    } else {
        estimate_text_row_packet_bytes(row)
    };
    payload.reserve(estimate);
    for (idx, value) in row.iter().enumerate() {
        let col = columns.and_then(|c| c.get(idx));
        let fsp = col.and_then(|c| c.source.decimals);
        let col_type = col.map(|c| c.column_type);
        if VECTOR_TEXT {
            if let Value::Vector(vector) = value {
                ROW_TEXT_SCRATCH.with(|scratch| {
                    let mut scratch = scratch.borrow_mut();
                    crate::engine::vector::codec::to_text_into(vector, &mut scratch);
                    write_text_string(payload, &scratch, result_charset_for_column(charset, col));
                });
                continue;
            }
        }
        write_text_value(
            payload,
            value,
            result_charset_for_column(charset, col),
            fsp,
            col_type,
        );
    }
}

/// Tight upper-bound byte estimate for a single `Value` written by
/// `write_text_value`. Used by `text_row_packet_into_with_charset` to
/// pre-reserve the per-row payload buffer and skip 2–4 `Vec::grow`
/// doublings on wide result rows.
///
/// The estimates are conservative (lenenc prefix counted as 9 bytes —
/// the max — even for short strings) so the reserve never under-counts;
/// the payload `Vec` is reused across rows so any overshoot lives a
/// single row.
#[inline]
fn estimate_text_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        // Integer text: max 20 digits (i64::MIN/u64::MAX) + sign + 1 lenenc byte.
        Value::Int(_) | Value::UInt(_) => 22,
        Value::Float(_) => 16,
        Value::Double(_) => 26,
        Value::Decimal(s) => 9 + s.len(),
        Value::DecimalCompact(_, _) => 48,
        Value::Decimal128(_, _) => 58,
        Value::DecimalWide(_) => 76,
        Value::String(s) => 9 + s.len(),
        Value::SmallString(b) => 9 + b.len(),
        Value::Bytes(b) => 9 + b.len(),
        Value::Date(_) | Value::Year(_) => 12,
        Value::Time(_) => 18,
        Value::DateTime(_) | Value::Timestamp(_) => 28,
        // JSON uses canonical text; VECTOR exports FLOAT32 coordinates.
        Value::Json(b) => 9 + b.len(),
        Value::Geometry(b) => 9 + b.len(),
        Value::Vector(b) => 9 + b.dimension as usize * 4,
    }
}

pub(crate) fn estimate_text_row_packet_bytes(row: &[Value]) -> usize {
    row.iter().fold(0usize, |total, value| {
        total.saturating_add(estimate_text_value_bytes(value))
    })
}

pub(crate) fn estimate_vector_text_row_packet_bytes(
    row: &[Value],
    charset: Option<&'static crate::charset::CharsetInfo>,
    columns: Option<&[Column]>,
) -> usize {
    row.iter()
        .enumerate()
        .fold(0usize, |total, (index, value)| {
            let bytes = match value {
                Value::Vector(vector) => {
                    let width = match vector.datatype {
                        crate::vector::VectorDataType::Binary => 2,
                        crate::vector::VectorDataType::Int8 => 5,
                        // Include the separator and sign of subnormal FLOAT32 text.
                        _ => 50,
                    };
                    let charset =
                        result_charset_for_column(charset, columns.and_then(|c| c.get(index)));
                    let unit = match charset.map(|c| c.name) {
                        Some("ucs2" | "utf16" | "utf16le") => 2,
                        Some("utf32") => 4,
                        _ => 1,
                    };
                    9 + (2 + vector.dimension as usize * width) * unit
                }
                _ => estimate_text_value_bytes(value),
            };
            total.saturating_add(bytes)
        })
}

/// Adapt only outgoing text-protocol metadata; execution keeps native vectors.
pub(crate) fn vector_text_result_columns(
    columns: &[Column],
    enabled: bool,
) -> std::borrow::Cow<'_, [Column]> {
    if !enabled
        || !columns
            .iter()
            .any(|c| matches!(c.column_type, ColumnType::Vector))
    {
        return std::borrow::Cow::Borrowed(columns);
    }
    let mut columns = columns.to_vec();
    for column in &mut columns {
        if matches!(column.column_type, ColumnType::Vector) {
            column.column_type = ColumnType::VarString;
            column.source = crate::types::ColumnSource {
                schema: column.source.schema.clone(),
                table: column.source.table.clone(),
                org_table: column.source.org_table.clone(),
                org_name: column.source.org_name.clone(),
                flags: column.source.flags & NOT_NULL_FLAG,
                length: Some(u32::MAX),
                ..Default::default()
            };
        }
    }
    std::borrow::Cow::Owned(columns)
}

/// Tight upper-bound byte estimate for a `Value` written by
/// `binary_row_packet_into`. Same role as `estimate_text_value_bytes`
/// but for the binary-protocol shape — fixed-width values count their
/// exact wire size and variable-width values count their lenenc length
/// + bytes.
#[inline]
fn estimate_binary_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0, // counted only in the NULL bitmap
        Value::Int(_) | Value::UInt(_) => 8,
        Value::Float(_) => 4,
        Value::Double(_) => 8,
        Value::Decimal(s) => 9 + s.len(),
        Value::DecimalCompact(_, _) => 48,
        Value::Decimal128(_, _) => 58,
        Value::DecimalWide(_) => 76,
        Value::String(s) => 9 + s.len(),
        Value::SmallString(b) => 9 + b.len(),
        Value::Bytes(b) => 9 + b.len(),
        Value::Date(_) => 5,
        Value::Year(_) => 2,
        Value::Time(_) => 13,
        Value::DateTime(_) | Value::Timestamp(_) => 12,
        Value::Json(b) => 9 + b.len(),
        Value::Geometry(b) => 9 + b.len(),
        Value::Vector(b) => 9 + b.dimension as usize * 4,
    }
}

pub(crate) fn estimate_binary_row_packet_bytes(row: &[Value]) -> usize {
    1usize.saturating_add((row.len() + 9) / 8).saturating_add(
        row.iter().fold(0usize, |total, value| {
            total.saturating_add(estimate_binary_value_bytes(value))
        }),
    )
}

#[cfg(test)]
/// Allocating variant of [`binary_row_packet_into`]. Used by the
/// server-side cursor FETCH path which encodes each row inside
/// `spawn_blocking` and returns the owned packet payloads to the
/// async task; trying to reuse a single buffer there would require
/// either moving the buffer back across tasks per row or cloning
/// each slice anyway, so a fresh `Vec<u8>` per row is the simplest
/// and well within amortized growth costs.
pub fn binary_row_packet(columns: &[Column], row: &[Value]) -> Vec<u8> {
    let mut payload = Vec::new();
    binary_row_packet_into(columns, row, &mut payload);
    payload
}

pub fn binary_row_packet_into(columns: &[Column], row: &[Value], payload: &mut Vec<u8>) {
    binary_row_packet_into_with_charset(columns, row, payload, None);
}

pub fn binary_row_packet_into_with_charset(
    columns: &[Column],
    row: &[Value],
    payload: &mut Vec<u8>,
    charset: Option<&'static crate::charset::CharsetInfo>,
) {
    payload.clear();
    // Pre-reserve based on a Value-aware byte estimate: 1B header
    // + null bitmap + sum of per-value wire sizes. Wide binary rows
    // would otherwise pay 2–4 `Vec::grow` doublings on every row.
    let null_bitmap_len = (row.len() + 7 + 2) / 8;
    payload.reserve(estimate_binary_row_packet_bytes(row));
    payload.push(0x00);
    let null_bitmap_start = payload.len();
    payload.resize(payload.len() + null_bitmap_len, 0);
    // First sweep: populate the NULL bitmap in the just-zeroed slice.
    // Using `bit >> 3` / `bit & 7` makes the bit-arithmetic explicit so
    // codegen doesn't depend on LLVM's div/mod-by-constant peephole, and
    // borrowing the bitmap as a mut slice lets the bounds check elide
    // (LLVM proves the index is in range from `null_bitmap_len`).
    {
        let bitmap = &mut payload[null_bitmap_start..null_bitmap_start + null_bitmap_len];
        for (idx, value) in row.iter().enumerate() {
            if matches!(value, Value::Null) {
                let bit = idx + 2;
                bitmap[bit >> 3] |= 1u8 << (bit & 7);
            }
        }
    }
    // Second sweep: write non-NULL values. The Value::Null branch is
    // kept as a no-op `continue` so the match remains exhaustive without
    // re-reading the bitmap.
    for (idx, value) in row.iter().enumerate() {
        if matches!(value, Value::Null) {
            continue;
        }
        let column_type = columns.get(idx).map(|column| column.column_type);
        if let Some(integral) = integral_decimal_for_integer_column(column_type, value) {
            // A DECIMAL-typed runtime value under INTEGER metadata must
            // still use the advertised fixed-width integer encoding.
            match i64::try_from(integral) {
                Ok(signed) => write_binary_signed_integer(payload, column_type, signed),
                Err(_) => write_binary_unsigned_integer(payload, column_type, integral as u64),
            }
            continue;
        }
        match value {
            Value::Null => unreachable!("Null filtered above"),
            Value::Int(value) => write_binary_signed_integer(
                payload,
                columns.get(idx).map(|column| column.column_type),
                *value,
            ),
            Value::UInt(value) => write_binary_unsigned_integer(
                payload,
                columns.get(idx).map(|column| column.column_type),
                *value,
            ),
            Value::Float(number) => write_binary_floating(
                payload,
                columns.get(idx).map(|column| column.column_type),
                value,
                f64::from(*number),
            ),
            Value::Double(number) => write_binary_floating(
                payload,
                columns.get(idx).map(|column| column.column_type),
                value,
                *number,
            ),
            Value::String(text) => write_binary_string(
                payload,
                text,
                columns.get(idx).map(|column| column.column_type),
                result_charset_for_column(charset, columns.get(idx)),
            ),
            value @ Value::SmallString(_) => write_binary_string(
                payload,
                crate::types::value_str(value).expect("matched string value"),
                columns.get(idx).map(|column| column.column_type),
                result_charset_for_column(charset, columns.get(idx)),
            ),
            Value::Decimal(value)
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Float)
                ) =>
            {
                let parsed = value.parse::<f32>().unwrap_or(0.0);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            Value::Decimal(value)
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Double)
                ) =>
            {
                let parsed = value.parse::<f64>().unwrap_or(0.0);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            Value::Decimal(value) => write_lenenc_string(payload, value.as_bytes()),
            value @ (Value::Decimal128(..) | Value::DecimalWide(..))
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Float)
                ) =>
            {
                let parsed = value.to_display_string().parse::<f32>().unwrap_or(0.0);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            value @ (Value::Decimal128(..) | Value::DecimalWide(..))
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Double)
                ) =>
            {
                let parsed = value.to_display_string().parse::<f64>().unwrap_or(0.0);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            value @ (Value::Decimal128(..) | Value::DecimalWide(..)) => {
                write_lenenc_string(payload, value.to_display_string().as_bytes())
            }
            Value::Bytes(value) => write_lenenc_string(payload, value),
            // Binary temporal / decimal — emit the MySQL binary
            // protocol shape directly. Avoids the format-then-parse
            // round-trip that a `to_display_string()` + Value::String
            // fallback would incur, which is the whole point of
            // carrying these as binary variants in the first place.
            Value::DecimalCompact(mag, scale)
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Float)
                ) =>
            {
                let parsed = (*mag as f32) / 10f32.powi(*scale as i32);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            Value::DecimalCompact(mag, scale)
                if matches!(
                    columns.get(idx).map(|column| column.column_type),
                    Some(ColumnType::Double)
                ) =>
            {
                let parsed = (*mag as f64) / 10f64.powi(*scale as i32);
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            Value::DecimalCompact(mag, scale) => {
                // MySQL sends DECIMAL as a length-encoded string in
                // both text and binary result sets. Encode straight
                // into a stack buffer to avoid the `String` alloc
                // that `to_display_string()` would incur per row.
                let mut buf = [0u8; 48];
                let bytes = crate::types::write_decimal_compact_canonical(*mag, *scale, &mut buf);
                write_lenenc_string(payload, bytes);
            }
            Value::Date(d) => {
                write_binary_date_from_days(payload, *d);
            }
            Value::DateTime(dt) | Value::Timestamp(dt) => {
                write_binary_datetime_from_micros(payload, *dt);
            }
            Value::Time(t) => {
                write_binary_time_from_micros(payload, *t);
            }
            Value::Year(y) => {
                let year_u16 = if *y == 0 { 0 } else { 1900 + *y as u16 };
                payload.extend_from_slice(&year_u16.to_le_bytes());
            }
            // JSON: length-encoded canonical text, like VARCHAR.
            Value::Json(b) => write_lenenc_json_text(payload, b),
            // GEOMETRY: length-encoded internal `SRID + WKB` bytes (clients
            // parse them or call ST_AsText), like a BLOB.
            Value::Geometry(b) => write_lenenc_string(payload, b),
            // Native VECTOR clients expect little-endian FLOAT32 coordinates.
            Value::Vector(b) => write_lenenc_vector(payload, b),
        }
    }
}

/// String storage representation must not affect the wire type advertised in
/// column metadata. Borrow both heap and inline strings before dispatching.
#[inline]
fn result_charset_for_column(
    charset: Option<&'static crate::charset::CharsetInfo>,
    column: Option<&Column>,
) -> Option<&'static crate::charset::CharsetInfo> {
    charset.or_else(|| {
        column
            .and_then(|c| c.source.collation_id)
            .and_then(crate::charset::resolve_collation_id)
            .and_then(|c| crate::charset::resolve_charset(c.charset).ok())
    })
}

#[inline]
fn write_binary_string(
    out: &mut Vec<u8>,
    value: &str,
    column_type: Option<ColumnType>,
    charset: Option<&'static crate::charset::CharsetInfo>,
) {
    if matches!(
        column_type,
        Some(ColumnType::Date | ColumnType::Time | ColumnType::DateTime | ColumnType::Timestamp)
    ) {
        if column_type == Some(ColumnType::Time) {
            write_binary_time(out, value);
        } else {
            write_binary_temporal(out, value, column_type == Some(ColumnType::Date));
        }
    } else {
        write_lenenc_string(
            out,
            &crate::charset::encode_string_lossy_cow_info(charset, value),
        );
    }
}

fn write_binary_date_fields(out: &mut Vec<u8>, year: u32, month: u32, day: u32) {
    write_u16_le(out, year as u16);
    out.push(month as u8);
    out.push(day as u8);
}

fn write_binary_date_from_days(out: &mut Vec<u8>, days: i32) {
    if days == crate::types::ZERO_DATE {
        out.push(0);
        return;
    }
    let (y, m, d) = crate::types::date_components(days);
    out.push(4);
    write_binary_date_fields(out, y, m, d);
}

fn write_binary_datetime_parts(
    out: &mut Vec<u8>,
    date: (u32, u32, u32),
    time: (u32, u32, u32),
    frac: u32,
) {
    out.push(if frac == 0 { 7 } else { 11 });
    write_binary_date_fields(out, date.0, date.1, date.2);
    out.push(time.0 as u8);
    out.push(time.1 as u8);
    out.push(time.2 as u8);
    if frac != 0 {
        out.extend_from_slice(&frac.to_le_bytes());
    }
}

fn write_binary_datetime_from_micros(out: &mut Vec<u8>, micros: i64) {
    if micros == crate::types::ZERO_DATETIME {
        out.push(0);
        return;
    }
    let (y, m, d, hh, mm, ss, frac) = crate::types::datetime_components(micros);
    write_binary_datetime_parts(out, (y, m, d), (hh, mm, ss), frac);
}

fn write_binary_time_parts(
    out: &mut Vec<u8>,
    negative: bool,
    days: u32,
    time: (u8, u8, u8),
    frac: u32,
) {
    let days = days.to_le_bytes();
    out.extend_from_slice(&[
        if frac == 0 { 8 } else { 12 },
        u8::from(negative),
        days[0],
        days[1],
        days[2],
        days[3],
        time.0,
        time.1,
        time.2,
    ]);
    if frac != 0 {
        out.extend_from_slice(&frac.to_le_bytes());
    }
}

fn write_binary_time_from_micros(out: &mut Vec<u8>, micros: i64) {
    if micros == 0 {
        out.push(0);
        return;
    }
    let m = micros.unsigned_abs();
    let total_secs = m / 1_000_000;
    let frac = (m % 1_000_000) as u32;
    let days = (total_secs / 86_400) as u32;
    let secs_of_day = (total_secs % 86_400) as u32;
    let hh = (secs_of_day / 3600) as u8;
    let mm = ((secs_of_day / 60) % 60) as u8;
    let ss = (secs_of_day % 60) as u8;
    write_binary_time_parts(out, micros < 0, days, (hh, mm, ss), frac);
}

/// Encode an integer runtime value according to the *advertised* result-column
/// type. Derived tables / CTEs can conservatively expose a textual type while
/// retaining a typed integer payload. COM_STMT_EXECUTE clients decode binary
/// rows exclusively from the advertised metadata, so emitting fixed-width
/// integer bytes for a VARCHAR column makes the integer's low byte look like a
/// length-encoded string prefix and corrupts the rest of the row.
/// An integral DECIMAL runtime value whose column advertises an integer wire
/// type. Clients decode such a column as fixed-width little-endian bytes, so
/// a length-encoded decimal string would desynchronize the rest of the row.
fn integral_decimal_for_integer_column(
    column_type: Option<ColumnType>,
    value: &Value,
) -> Option<i128> {
    if !matches!(
        column_type,
        Some(
            ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong
                | ColumnType::Year
        )
    ) {
        return None;
    }
    let text = match value {
        Value::DecimalCompact(mag, 0) => return Some(i128::from(*mag)),
        Value::DecimalCompact(..) | Value::Decimal128(..) | Value::DecimalWide(..) => {
            value.to_display_string()
        }
        Value::Decimal(text) => text.clone(),
        _ => return None,
    };
    let integral = text
        .split_once('.')
        .map_or(text.as_str(), |(whole, fraction)| {
            if fraction.bytes().all(|b| b == b'0') {
                whole
            } else {
                ""
            }
        });
    let integral = integral.parse::<i128>().ok()?;
    (i128::from(i64::MIN)..=i128::from(u64::MAX))
        .contains(&integral)
        .then_some(integral)
}

fn write_binary_signed_integer(out: &mut Vec<u8>, column_type: Option<ColumnType>, value: i64) {
    match column_type {
        Some(
            ColumnType::VarString
            | ColumnType::Blob
            | ColumnType::Json
            | ColumnType::Geometry
            | ColumnType::Vector
            | ColumnType::Decimal { .. },
        ) => {
            let mut buf = itoa::Buffer::new();
            write_lenenc_string(out, buf.format(value).as_bytes());
        }
        Some(ColumnType::Float) => out.extend_from_slice(&(value as f32).to_le_bytes()),
        Some(ColumnType::Double) => out.extend_from_slice(&(value as f64).to_le_bytes()),
        Some(ColumnType::Date) => write_binary_date_from_days(out, value as i32),
        Some(ColumnType::Time) => write_binary_time_from_micros(out, value),
        Some(ColumnType::DateTime | ColumnType::Timestamp) => {
            write_binary_datetime_from_micros(out, value)
        }
        _ => write_binary_integer_bits(out, column_type, value as u64),
    }
}

/// FLOAT and DOUBLE differ in binary width (4 vs 8 bytes). Encode a runtime
/// f32/f64 by the advertised column type: a width that disagrees with the
/// metadata shifts every following field of the row (e.g. `SELECT *` over a
/// FLOAT column whose rows carry widened f64 values).
fn write_binary_floating(
    out: &mut Vec<u8>,
    column_type: Option<ColumnType>,
    value: &Value,
    number: f64,
) {
    match column_type {
        Some(ColumnType::Float) => out.extend_from_slice(&(number as f32).to_le_bytes()),
        Some(ColumnType::Double) => out.extend_from_slice(&number.to_le_bytes()),
        Some(
            ColumnType::VarString
            | ColumnType::Blob
            | ColumnType::Json
            | ColumnType::Geometry
            | ColumnType::Vector
            | ColumnType::Decimal { .. },
        ) => write_lenenc_string(out, value.to_display_string().as_bytes()),
        _ => match value {
            Value::Float(number) => out.extend_from_slice(&number.to_le_bytes()),
            _ => out.extend_from_slice(&number.to_le_bytes()),
        },
    }
}

fn write_binary_unsigned_integer(out: &mut Vec<u8>, column_type: Option<ColumnType>, value: u64) {
    match column_type {
        Some(
            ColumnType::VarString
            | ColumnType::Blob
            | ColumnType::Json
            | ColumnType::Geometry
            | ColumnType::Vector
            | ColumnType::Decimal { .. },
        ) => {
            let mut buf = itoa::Buffer::new();
            write_lenenc_string(out, buf.format(value).as_bytes());
        }
        Some(ColumnType::Float) => out.extend_from_slice(&(value as f32).to_le_bytes()),
        Some(ColumnType::Double) => out.extend_from_slice(&(value as f64).to_le_bytes()),
        Some(ColumnType::Date) => write_binary_date_from_days(out, value as i32),
        Some(ColumnType::Time) => write_binary_time_from_micros(out, value as i64),
        Some(ColumnType::DateTime | ColumnType::Timestamp) => {
            write_binary_datetime_from_micros(out, value as i64)
        }
        _ => write_binary_integer_bits(out, column_type, value),
    }
}

fn write_binary_integer_bits(out: &mut Vec<u8>, column_type: Option<ColumnType>, value: u64) {
    match column_type {
        // BIT(M): the binary protocol transports the value as a
        // length-encoded binary string of `ceil(M/8)` big-endian bytes,
        // not a fixed-width integer. Encoding it as a 2-byte SHORT (the
        // old behaviour) silently truncated BIT(M > 16) on the wire.
        Some(ColumnType::Bit { bits }) => {
            let width = (bits.max(1) as usize).div_ceil(8);
            let be = value.to_be_bytes();
            write_lenenc_int(out, width as u64);
            out.extend_from_slice(&be[be.len() - width..]);
        }
        Some(ColumnType::Tiny) => out.push(value as u8),
        Some(ColumnType::Short) | Some(ColumnType::Year) => {
            // YEAR is wire-encoded as u16 (full year, not year-1900),
            // matching MySQL. `Value::Year` writes it directly in
            // binary_row_packet_into; this arm covers the rarer path
            // where a YEAR column receives a plain integer value.
            out.extend_from_slice(&(value as u16).to_le_bytes())
        }
        // INT24 (MEDIUMINT) is wire-encoded as 4 bytes LE despite only
        // 3 bytes being significant — same as MySQL.
        Some(ColumnType::MediumInt) | Some(ColumnType::Long) => {
            out.extend_from_slice(&(value as u32).to_le_bytes())
        }
        Some(ColumnType::LongLong) | None => out.extend_from_slice(&value.to_le_bytes()),
        _ => out.extend_from_slice(&value.to_le_bytes()),
    }
}

fn write_binary_temporal(out: &mut Vec<u8>, value: &str, date_only: bool) {
    if date_only {
        if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            out.push(4);
            write_binary_date_fields(out, date.year() as u32, date.month(), date.day());
            return;
        }
    }
    if let Some(dt) = parse_temporal_value(value) {
        write_binary_datetime_parts(
            out,
            (dt.year() as u32, dt.month(), dt.day()),
            (dt.hour(), dt.minute(), dt.second()),
            dt.and_utc().timestamp_subsec_micros(),
        );
    } else {
        out.push(0);
    }
}

fn write_binary_time(out: &mut Vec<u8>, value: &str) {
    let value = value.trim();
    let negative = value.starts_with('-');
    let v = value.trim_start_matches('-');
    let mut parts = v.split(':');
    let (Some(hours), Some(minutes), Some(seconds), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        out.push(0);
        return;
    };
    let hours: u32 = hours.parse().unwrap_or(0);
    let minutes: u8 = minutes.parse().unwrap_or(0);
    // The seconds field may carry a fractional tail ("SS.ffffff").
    let (sec_str, frac) = match seconds.split_once('.') {
        Some((sec, frac_str)) => {
            // Right-pad/truncate the digits to micros (".5" → 500000).
            let mut micros: u32 = 0;
            for i in 0..6 {
                let d = frac_str.as_bytes().get(i).map_or(0, |b| {
                    if b.is_ascii_digit() {
                        (b - b'0') as u32
                    } else {
                        0
                    }
                });
                micros = micros * 10 + d;
            }
            (sec, micros)
        }
        None => (seconds, 0),
    };
    let seconds: u8 = sec_str.parse().unwrap_or(0);
    write_binary_time_parts(
        out,
        negative,
        hours / 24,
        ((hours % 24) as u8, minutes, seconds),
        frac,
    );
}

fn parse_temporal_value(value: &str) -> Option<NaiveDateTime> {
    let value = value.trim();
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, fmt) {
            return Some(dt);
        }
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
}

#[derive(Debug, Clone, PartialEq)]
pub enum StmtParamValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    /// Pre-parsed `DECIMAL` parameter. JDBC sends `setBigDecimal` as
    /// `MYSQL_TYPE_NEWDECIMAL`, which arrives on the wire as ASCII.
    /// Parsing it once here (rather than once per arithmetic / codec
    /// touch downstream) lets the engine treat the value as a
    /// `Value::DecimalCompact` end-to-end. Falls back to
    /// `StmtParamValue::Bytes` if the input doesn't parse cleanly.
    Decimal {
        magnitude: i64,
        scale: u8,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StmtParamType {
    ty: u8,
    unsigned: bool,
}

pub struct ParsedStmtExecute {
    pub flags: u8,
    pub params: smallvec::SmallVec<[StmtParamValue; 8]>,
    pub param_types: Option<Vec<StmtParamType>>,
    pub query_attributes: Vec<(String, StmtParamValue)>,
}

pub struct ParsedQueryAttributes<'a> {
    pub sql: &'a [u8],
    pub attributes: Vec<(String, StmtParamValue)>,
}

/// Skip past a CLIENT_QUERY_ATTRIBUTES preamble in a COM_QUERY payload,
/// returning the SQL byte slice. When the capability is negotiated, the
/// wire layout is `[lenenc parameter_count][lenenc parameter_set_count]
/// (+ optional null_bitmap / types / names / values when count > 0) +
/// [SQL bytes]`. mydb has no SQL-surface for query attributes today, so
/// the block is parsed for protocol compliance and the values are
/// dropped. `payload` must NOT include the leading `0x03` command byte.
///
/// The smallest legal preamble — what Connector/J 8.0+ sends when no
/// `Statement.setQueryAttribute(...)` calls were made — is `0x00 0x01`
/// (`parameter_count=0, parameter_set_count=1`). The two-byte fast
/// path skips the heavier parser for that case.
///
/// Upper bound on how many parameter-type slots we *pre-reserve* from a
/// wire-supplied count before reading any of them. The count is a lenenc
/// int the client controls; with a large `max_allowed_packet` it can name
/// billions of parameters, so reserving it verbatim is a per-connection
/// memory-amplification DoS. We reserve at most this many and let the Vec
/// grow on demand — a genuinely large bind still works, it just doesn't
/// get to force a giant up-front allocation that the subsequent reads
/// (which run out of payload bytes and error) would never have filled.
const MAX_PARAM_PREALLOC: usize = 4096;

pub fn parse_query_attributes(payload: &[u8]) -> Result<ParsedQueryAttributes<'_>, String> {
    let mut cursor = Cursor::new(payload);
    let parameter_count = usize::try_from(cursor.read_lenenc_int()?)
        .map_err(|_| "parameter count exceeds address space".to_string())?;
    let parameter_set_count = cursor.read_lenenc_int()?;
    if parameter_set_count != 1 {
        return Err(format!(
            "COM_QUERY parameter_set_count must be 1, got {parameter_set_count}"
        ));
    }
    if parameter_count == 0 {
        return Ok(ParsedQueryAttributes {
            sql: &payload[cursor.position..],
            attributes: Vec::new(),
        });
    }
    let null_bitmap_len = parameter_count.div_ceil(8);
    let null_bitmap = cursor.read_bytes(null_bitmap_len)?.to_vec();
    let new_params_bind_flag = cursor.read_u8()?;
    // COM_QUERY with parameter_count > 0 spec requires new_params_bind_flag = 1
    // and the type loop to be present; there is no cached-types path
    // (statement is not prepared).
    if new_params_bind_flag == 0 {
        return Err(
            "COM_QUERY with query_attributes requires new_params_bind_flag = 1".to_string(),
        );
    }
    let mut types: Vec<(u8, bool, String)> =
        Vec::with_capacity(parameter_count.min(MAX_PARAM_PREALLOC));
    for _ in 0..parameter_count {
        let ty = cursor.read_u8()?;
        let unsigned = cursor.read_u8()? != 0;
        let name = std::str::from_utf8(cursor.read_lenenc_bytes()?)
            .map_err(|_| "query attribute name is not valid UTF-8".to_string())?
            .to_string();
        types.push((ty, unsigned, name));
    }
    let mut attributes = Vec::with_capacity(parameter_count.min(MAX_PARAM_PREALLOC));
    for (idx, (ty, unsigned, name)) in types.iter().enumerate() {
        if null_bitmap[idx / 8] & (1 << (idx % 8)) != 0 {
            attributes.push((name.clone(), StmtParamValue::Null));
            continue;
        }
        attributes.push((
            name.clone(),
            read_parameter_value(&mut cursor, *ty, *unsigned, "query attribute")?,
        ));
    }
    Ok(ParsedQueryAttributes {
        sql: &payload[cursor.position..],
        attributes,
    })
}

#[cfg(test)]
pub fn strip_query_attributes(payload: &[u8]) -> Result<&[u8], String> {
    parse_query_attributes(payload).map(|parsed| parsed.sql)
}

// Expand the shared wire match directly into each caller. This preserves
// the prepared path's value construction without an extra Result boundary.
macro_rules! parameter_value {
    ($cursor:ident, $temporal_cursor:expr, $ty:expr, $unsigned:expr, $context:expr) => {{
match $ty {
            MYSQL_TYPE_TINY => {
                let v = $cursor.read_u8()?;
                if $unsigned {
                    StmtParamValue::UInt(v as u64)
                } else {
                    StmtParamValue::Int(v as i8 as i64)
                }
            }
            MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => {
                let b = $cursor.read_bytes(2)?;
                let u = u16::from_le_bytes([b[0], b[1]]);
                if $unsigned {
                    StmtParamValue::UInt(u as u64)
                } else {
                    StmtParamValue::Int(u as i16 as i64)
                }
            }
            MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
                let b = $cursor.read_bytes(4)?;
                let u = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                if $unsigned {
                    StmtParamValue::UInt(u as u64)
                } else {
                    StmtParamValue::Int(u as i32 as i64)
                }
            }
            MYSQL_TYPE_LONGLONG => {
                let b = $cursor.read_bytes(8)?;
                let u = u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
                if $unsigned {
                    StmtParamValue::UInt(u)
                } else {
                    StmtParamValue::Int(u as i64)
                }
            }
            MYSQL_TYPE_FLOAT => {
                let b = $cursor.read_bytes(4)?;
                StmtParamValue::Float(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            }
            MYSQL_TYPE_DOUBLE => {
                let b = $cursor.read_bytes(8)?;
                StmtParamValue::Double(f64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            }
            MYSQL_TYPE_DATE
            | MYSQL_TYPE_NEWDATE
            | MYSQL_TYPE_DATETIME
            | MYSQL_TYPE_TIMESTAMP
            | MYSQL_TYPE_TIMESTAMP2
            | MYSQL_TYPE_DATETIME2 => {
                StmtParamValue::String(parse_binary_temporal($temporal_cursor)?)
            }
            MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => {
                StmtParamValue::String(parse_binary_time($temporal_cursor)?)
            }
            MYSQL_TYPE_NULL => StmtParamValue::Null,
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
                let bytes = $cursor.read_lenenc_bytes()?;
                // JDBC's `setBigDecimal` sends NEWDECIMAL as
                // length-encoded ASCII (e.g. "100.50"). Pre-parse it
                // into the `(magnitude, scale)` shape the executor's
                // arithmetic and codec fast paths consume, so the
                // string-to-i64 conversion happens once per execute
                // rather than on every operator touch.
                if let Some((magnitude, scale)) = parse_param_decimal(bytes) {
                    StmtParamValue::Decimal { magnitude, scale }
                } else {
                    StmtParamValue::Bytes(bytes.to_vec())
                }
            }
            MYSQL_TYPE_VARCHAR
            | MYSQL_TYPE_VAR_STRING
            | MYSQL_TYPE_STRING
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB
            | MYSQL_TYPE_BLOB
            | MYSQL_TYPE_ENUM
            | MYSQL_TYPE_SET
            | MYSQL_TYPE_JSON
            | MYSQL_TYPE_GEOMETRY
            | MYSQL_TYPE_VECTOR
            // BIT parameters arrive as a length-encoded big-endian byte
            // string; the BIT column coercion (`constraints`) reads
            // `Value::Bytes` as the big-endian bit value, so passing the raw
            // bytes through is correct.
            | MYSQL_TYPE_BIT => StmtParamValue::Bytes($cursor.read_lenenc_bytes()?.to_vec()),
            other => {
                return Err(format!(
                    "unsupported {} parameter type: 0x{other:02x}", $context
                ))
            }
        }
    }};
}

fn read_parameter_value(
    cursor: &mut Cursor<'_>,
    ty: u8,
    unsigned: bool,
    context: &str,
) -> Result<StmtParamValue, String> {
    Ok(parameter_value!(cursor, cursor, ty, unsigned, context))
}

pub fn parse_stmt_execute_params(
    payload: &[u8],
    param_count: usize,
    cached_types: Option<&[StmtParamType]>,
    long_data: Option<&[Option<Vec<u8>>]>,
    with_query_attributes: bool,
) -> Result<ParsedStmtExecute, String> {
    let mut cursor = Cursor::new(payload);
    let _statement_id = cursor.read_u32_le()?;
    let flags = cursor.read_u8()?;
    let iteration_count = cursor.read_u32_le()?;
    if iteration_count != 1 {
        return Err(format!(
            "COM_STMT_EXECUTE iteration_count must be 1, got {iteration_count}"
        ));
    }
    // CLIENT_QUERY_ATTRIBUTES: a lenenc parameter count follows
    // iteration_count. The spec gates this on the
    // PARAMETER_COUNT_AVAILABLE flag bit (0x08), and Connector/J sets
    // it — but mysql-connector-python sends the count with flags=0x00
    // (no bit), and the real MySQL server accepts that, so the count's
    // presence is in practice tied to the *capability*, not the flag.
    // Reading it whenever query attributes are negotiated matches both
    // clients; gating on 0x08 misreads the python connector's packet
    // (count byte parsed as the null bitmap → new_params_bound reads 0
    // → spurious "without cached parameter types"). mydb has no SQL
    // surface for query attributes, so any extra params beyond the
    // statement's own appear as trailing values the bind loop discards.
    let effective_param_count = if with_query_attributes && cursor.remaining() != 0 {
        usize::try_from(cursor.read_lenenc_int()?)
            .map_err(|_| "parameter count exceeds address space".to_string())?
    } else {
        param_count
    };
    if effective_param_count < param_count {
        return Err(format!(
            "COM_STMT_EXECUTE supplied {effective_param_count} parameters for {param_count} placeholders"
        ));
    }
    if effective_param_count == 0 {
        // Connector/J (without query attributes) still writes the one-byte
        // new_params_bound flag after the empty NULL bitmap; MySQL ignores it.
        let new_params_bound_only =
            cursor.remaining() == 1 && payload.last().is_some_and(|flag| *flag <= 1);
        if cursor.remaining() != 0 && !new_params_bound_only {
            return Err("COM_STMT_EXECUTE has trailing parameter bytes".to_string());
        }
        return Ok(ParsedStmtExecute {
            flags,
            params: smallvec::SmallVec::new(),
            param_types: None,
            query_attributes: Vec::new(),
        });
    }
    let null_bitmap_len = effective_param_count.div_ceil(8);
    // Borrowed straight from the payload — `read_bytes` hands back a
    // slice tied to the payload's lifetime, not the cursor borrow, so
    // the old per-execute `.to_vec()` copy was pure malloc churn.
    let null_bitmap = cursor.read_bytes(null_bitmap_len)?;
    let new_params_bound = cursor.read_u8()?;
    let mut parsed_types = None;
    let mut parameter_names = Vec::new();
    // Borrow the session's cached types on the steady-state path
    // (`new_params_bound == 0`, i.e. every execute after the first)
    // instead of cloning the whole Vec per execute.
    let types: &[StmtParamType] = if new_params_bound != 0 {
        let mut types = Vec::with_capacity(effective_param_count.min(MAX_PARAM_PREALLOC));
        for _ in 0..effective_param_count {
            let ty = cursor.read_u8()?;
            let unsigned = cursor.read_u8()? != 0;
            // CLIENT_QUERY_ATTRIBUTES adds a lenenc parameter_name per
            // param between the type pair and the value loop. We read
            // and discard it (mydb has no `mysql_query_attribute_string`
            // surface yet).
            if with_query_attributes {
                let name = std::str::from_utf8(cursor.read_lenenc_bytes()?)
                    .map_err(|_| "query attribute name is not valid UTF-8".to_string())?;
                parameter_names.push(name.to_string());
            }
            types.push(StmtParamType { ty, unsigned });
        }
        parsed_types = Some(types);
        parsed_types
            .as_deref()
            .expect("parameter types were just decoded")
    } else {
        cached_types.ok_or_else(|| "COM_STMT_EXECUTE without cached parameter types".to_string())?
    };
    // Only the leading `param_count` of `effective_param_count` get
    // bound; the extras are query attributes the caller asked us to
    // discard. They still need to be *consumed* from the wire so the
    // cursor lands on the trailing bytes (none for COM_STMT_EXECUTE).
    let bound_count = param_count.min(effective_param_count);
    let mut values = smallvec::SmallVec::<[StmtParamValue; 8]>::with_capacity(bound_count);
    let mut query_attributes = Vec::new();
    for idx in 0..effective_param_count {
        let is_null = null_bitmap[idx / 8] & (1 << (idx % 8)) != 0;
        if idx >= bound_count {
            let StmtParamType { ty, unsigned } = *types.get(idx).ok_or_else(|| {
                "COM_STMT_EXECUTE declared more parameters than bound types".to_string()
            })?;
            let value = if is_null {
                StmtParamValue::Null
            } else {
                read_parameter_value(&mut cursor, ty, unsigned, "query attribute")?
            };
            if let Some(name) = parameter_names.get(idx).filter(|name| !name.is_empty()) {
                query_attributes.push((name.clone(), value));
            }
            continue;
        }
        if is_null {
            let value = StmtParamValue::Null;
            if let Some(name) = parameter_names.get(idx).filter(|name| !name.is_empty()) {
                query_attributes.push((name.clone(), value.clone()));
            }
            values.push(value);
            continue;
        }
        let StmtParamType { ty, unsigned } = *types.get(idx).ok_or_else(|| {
            "COM_STMT_EXECUTE declared more parameters than bound types".to_string()
        })?;
        if let Some(bytes) = long_data
            .and_then(|items| items.get(idx))
            .and_then(|item| item.as_ref())
        {
            let value = StmtParamValue::Bytes(bytes.clone());
            if let Some(name) = parameter_names.get(idx).filter(|name| !name.is_empty()) {
                query_attributes.push((name.clone(), value.clone()));
            }
            values.push(value);
            continue;
        }
        let value = parameter_value!(cursor, &mut cursor, ty, unsigned, "prepared");
        if let Some(name) = parameter_names.get(idx).filter(|name| !name.is_empty()) {
            query_attributes.push((name.clone(), value.clone()));
        }
        values.push(value);
    }
    if cursor.remaining() != 0 {
        return Err(format!(
            "COM_STMT_EXECUTE has {} unexpected trailing byte(s)",
            cursor.remaining()
        ));
    }
    Ok(ParsedStmtExecute {
        flags,
        params: values,
        param_types: parsed_types,
        query_attributes,
    })
}

/// Parse an ASCII decimal parameter (e.g. `b"100.50"`) into
/// `(magnitude, scale)` so the engine can route it as a binary
/// `Value::DecimalCompact`. Returns `None` for input that doesn't
/// parse cleanly or whose magnitude wouldn't fit in `i64` — callers
/// fall back to passing the raw bytes through as a string.
fn parse_param_decimal(bytes: &[u8]) -> Option<(i64, u8)> {
    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let mut negative = false;
    match bytes[i] {
        b'-' => {
            negative = true;
            i += 1;
        }
        b'+' => i += 1,
        _ => {}
    }
    let mut magnitude: u128 = 0;
    let mut any_digit = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        any_digit = true;
        magnitude = magnitude.checked_mul(10)?;
        magnitude = magnitude.checked_add((bytes[i] - b'0') as u128)?;
        i += 1;
    }
    let mut scale = 0u8;
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            if scale < 30 {
                magnitude = magnitude.checked_mul(10)?;
                magnitude = magnitude.checked_add((bytes[i] - b'0') as u128)?;
                scale += 1;
                any_digit = true;
            }
            i += 1;
        }
    }
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if !any_digit || i != bytes.len() {
        return None;
    }
    if magnitude > i64::MAX as u128 {
        return None;
    }
    let signed = if negative {
        -(magnitude as i64)
    } else {
        magnitude as i64
    };
    Some((signed, scale))
}

/// Append `d` to `out` as a zero-padded ASCII number of width `width`.
/// `width <= 4` covers every component of MySQL's binary temporal layout.
#[inline]
fn push_pad_u32(out: &mut String, value: u32, width: usize) {
    debug_assert!(width <= 8);
    // 0..=9999 fits a u16, but the helper is u32 to avoid casts at call
    // sites where the parsed byte / u16 is already wider (microseconds
    // need width 6).
    let mut buf = [0u8; 8];
    let mut n = value;
    for slot in buf.iter_mut().rev().take(width) {
        *slot = b'0' + (n % 10) as u8;
        n /= 10;
    }
    // Safety: ASCII digits are valid UTF-8.
    out.push_str(unsafe { std::str::from_utf8_unchecked(&buf[(8 - width)..]) });
}

fn parse_binary_temporal(cursor: &mut Cursor<'_>) -> Result<String, String> {
    let len = cursor.read_u8()? as usize;
    if len == 0 {
        return Ok("0000-00-00".to_string());
    }
    let b = cursor.read_bytes(len)?;
    match len {
        4 => {
            // YYYY-MM-DD: exactly 10 bytes, allocate once.
            let mut out = String::with_capacity(10);
            push_pad_u32(&mut out, u16::from_le_bytes([b[0], b[1]]) as u32, 4);
            out.push('-');
            push_pad_u32(&mut out, b[2] as u32, 2);
            out.push('-');
            push_pad_u32(&mut out, b[3] as u32, 2);
            Ok(out)
        }
        7 | 11 => {
            // YYYY-MM-DD HH:MM:SS[.ffffff]: 19 or 26 bytes. len==11 carries
            // a 4-byte LE microseconds tail which must survive into the
            // rewritten literal — DATETIME(n) columns would otherwise lose
            // sub-second precision on every prepared-statement write.
            let mut out = String::with_capacity(if len == 11 { 26 } else { 19 });
            push_pad_u32(&mut out, u16::from_le_bytes([b[0], b[1]]) as u32, 4);
            out.push('-');
            push_pad_u32(&mut out, b[2] as u32, 2);
            out.push('-');
            push_pad_u32(&mut out, b[3] as u32, 2);
            out.push(' ');
            push_pad_u32(&mut out, b[4] as u32, 2);
            out.push(':');
            push_pad_u32(&mut out, b[5] as u32, 2);
            out.push(':');
            push_pad_u32(&mut out, b[6] as u32, 2);
            if len == 11 {
                let micros = u32::from_le_bytes([b[7], b[8], b[9], b[10]]);
                out.push('.');
                push_pad_u32(&mut out, micros, 6);
            }
            Ok(out)
        }
        _ => Err("invalid temporal parameter length".to_string()),
    }
}

fn parse_binary_time(cursor: &mut Cursor<'_>) -> Result<String, String> {
    let len = cursor.read_u8()? as usize;
    if len == 0 {
        return Ok("00:00:00".to_string());
    }
    let b = cursor.read_bytes(len)?;
    if !matches!(len, 8 | 12) {
        return Err("invalid time parameter length".to_string());
    }
    let negative = b[0] != 0;
    let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]);
    let hours = days
        .checked_mul(24)
        .and_then(|hours| hours.checked_add(u32::from(b[5])))
        .ok_or_else(|| "time parameter hours out of range".to_string())?;
    let minutes = b[6] as u32;
    let seconds = b[7] as u32;
    // "HH:MM:SS" is 8 chars; "-HH:MM:SS" / "HHH:MM:SS" / ".ffffff" can
    // run longer, allocate with a small margin.
    let mut out = String::with_capacity(if len >= 12 { 19 } else { 12 });
    if negative {
        out.push('-');
    }
    if hours >= 100 {
        // For oversized hours, fall back to itoa (rare path).
        let mut buf = itoa::Buffer::new();
        out.push_str(buf.format(hours));
    } else {
        push_pad_u32(&mut out, hours, 2);
    }
    out.push(':');
    push_pad_u32(&mut out, minutes, 2);
    out.push(':');
    push_pad_u32(&mut out, seconds, 2);
    if len >= 12 {
        // 4-byte LE microseconds tail (len==12) — keep sub-second
        // precision for TIME(n) parameters.
        let micros = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
        out.push('.');
        push_pad_u32(&mut out, micros, 6);
    }
    Ok(out)
}

pub fn stmt_param_to_sql(value: &StmtParamValue, no_backslash_escapes: bool) -> String {
    match value {
        StmtParamValue::Null => "NULL".to_string(),
        StmtParamValue::Int(value) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*value).to_string()
        }
        StmtParamValue::UInt(value) => {
            let mut buf = itoa::Buffer::new();
            buf.format(*value).to_string()
        }
        StmtParamValue::Float(value) => crate::types::protocol_f32_text(*value),
        StmtParamValue::Double(value) => {
            let mut buf = ryu::Buffer::new();
            crate::types::ryu_to_mysql_text(buf.format(*value), || value.to_string()).into_owned()
        }
        StmtParamValue::Bytes(value) => {
            quote_sql_string(&String::from_utf8_lossy(value), no_backslash_escapes)
        }
        StmtParamValue::String(value) => quote_sql_string(value, no_backslash_escapes),
        // SQL literal-rewriting path only used for unrecognised
        // statements (rare). Emit the canonical decimal form.
        StmtParamValue::Decimal { magnitude, scale } => {
            let v = crate::types::Value::DecimalCompact(*magnitude, *scale);
            v.to_display_string()
        }
    }
}

fn quote_sql_string(value: &str, no_backslash_escapes: bool) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push('\'');
        } else if ch == '\\' && !no_backslash_escapes {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

#[cfg(test)]
fn result_set_packets(
    result_set: &crate::engine::api::ResultSet,
    client_capabilities: u32,
) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let deprecate_eof = client_capabilities & CLIENT_DEPRECATE_EOF != 0;

    let mut column_count = Vec::new();
    write_lenenc_int(&mut column_count, result_set.columns.len() as u64);
    packets.push(column_count);

    for column in &result_set.columns {
        packets.push(column_definition_packet_full(
            &column.name,
            column.column_type,
            &column.source,
        ));
    }

    if !deprecate_eof {
        packets.push(eof_packet().to_vec());
    }

    for row in &result_set.rows {
        packets.push(text_row_packet(row));
    }

    if deprecate_eof {
        packets.push(eof_as_ok_packet());
    } else {
        packets.push(eof_packet().to_vec());
    }
    packets
}

thread_local! {
    /// Reused scratch for JSON cell text. These values serialise to
    /// a `String` before the lenenc copy into the payload; without a shared
    /// buffer that is one heap allocation per JSON cell on the
    /// result-set hot path. The buffer is cleared by `to_text_into` on each
    /// use and never held across a call that re-enters these encoders, so
    /// the `borrow_mut` cannot alias.
    static ROW_TEXT_SCRATCH: std::cell::RefCell<String> =
        const { std::cell::RefCell::new(String::new()) };
}

/// Write a JSON cell as canonical lenenc text, reusing the per-thread
/// scratch buffer (no per-cell `String` allocation).
#[inline]
fn write_lenenc_json_text(payload: &mut Vec<u8>, jsonb: &[u8]) {
    ROW_TEXT_SCRATCH.with(|s| {
        let mut s = s.borrow_mut();
        crate::engine::json::to_text_into(jsonb, &mut s);
        write_lenenc_string(payload, s.as_bytes());
    });
}

/// MySQL's VECTOR wire value contains little-endian FLOAT32 coordinates.
/// Other native storage datatypes have exactly representable FLOAT32 values.
#[inline]
fn write_lenenc_vector(payload: &mut Vec<u8>, internal: &crate::vector::VectorValue) {
    if internal.datatype == crate::vector::VectorDataType::Float32 {
        write_lenenc_string(payload, &internal.payload);
    } else {
        write_lenenc_int(payload, internal.dimension as u64 * 4);
        for index in 0..internal.dimension as usize {
            payload.extend_from_slice(&(internal.element(index) as f32).to_le_bytes());
        }
    }
}

fn write_text_value(
    payload: &mut Vec<u8>,
    value: &Value,
    charset: Option<&'static crate::charset::CharsetInfo>,
    fsp: Option<u8>,
    col_type: Option<ColumnType>,
) {
    if matches!(col_type, Some(ColumnType::Float)) {
        if let Value::Double(value) = value {
            return write_text_value_inner(payload, &Value::Float(*value as f32), charset, fsp);
        }
    }
    // BIT(M): MySQL's text protocol transports the value as a raw
    // big-endian byte string of `ceil(M/8)` bytes, not a decimal integer.
    // The value rides through the engine as `Value::UInt`; only the column
    // type distinguishes it from a plain unsigned integer, so render bytes
    // here when the column is BIT. (When `col_type` is unavailable — paths
    // that don't pass columns — it degrades to the decimal integer form.)
    if let Some(ColumnType::Bit { bits }) = col_type {
        let n = match value {
            Value::UInt(v) => *v,
            Value::Int(v) => *v as u64,
            Value::Null => {
                payload.push(0xfb);
                return;
            }
            _ => {
                // Non-integer value in a BIT column (e.g. an expression
                // result) — fall through to the normal encoders below.
                write_text_value_inner(payload, value, charset, fsp);
                return;
            }
        };
        let width = (bits.max(1) as usize).div_ceil(8);
        let be = n.to_be_bytes();
        write_lenenc_string(payload, &be[be.len() - width..]);
        return;
    }
    write_text_value_inner(payload, value, charset, fsp);
}

#[inline]
fn write_text_string(
    payload: &mut Vec<u8>,
    text: &str,
    charset: Option<&'static crate::charset::CharsetInfo>,
) {
    let bytes = crate::charset::encode_string_lossy_cow_info(charset, text);
    write_lenenc_string(payload, bytes.as_ref());
}

fn write_text_value_inner(
    payload: &mut Vec<u8>,
    value: &Value,
    charset: Option<&'static crate::charset::CharsetInfo>,
    fsp: Option<u8>,
) {
    match value {
        Value::Null => payload.push(0xfb),
        // Integer text encoding via itoa — stack-resident 22-byte buffer,
        // zero heap allocation. itoa's output matches Rust's `to_string`
        // byte-for-byte so this is a pure perf change, no protocol drift.
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            write_lenenc_string(payload, buf.format(*v).as_bytes());
        }
        Value::UInt(v) => {
            let mut buf = itoa::Buffer::new();
            write_lenenc_string(payload, buf.format(*v).as_bytes());
        }
        // Floating-point text encoding via ryu — stack-resident
        // 25-byte buffer, zero heap allocation in the hot path.
        // `ryu_to_mysql_text` reshapes ryu's "1.0" / "1e20" output to
        // the MySQL/Rust `to_string + trim_float` byte sequence so
        // existing tests and clients see no format change.
        Value::Float(v) => {
            if let Some(decimals) = fsp.filter(|decimals| *decimals != 0x1f) {
                let text = format!("{v:.decimals$}", decimals = usize::from(decimals));
                write_lenenc_string(payload, text.as_bytes());
                return;
            }
            let text = crate::types::mysql_f32_text(*v);
            write_lenenc_string(payload, text.as_bytes());
        }
        Value::Double(v) => {
            if let Some(decimals) = fsp.filter(|decimals| *decimals != 0x1f) {
                let text = format!("{v:.decimals$}", decimals = usize::from(decimals));
                write_lenenc_string(payload, text.as_bytes());
                return;
            }
            let mut buf = ryu::Buffer::new();
            let text = crate::types::ryu_to_mysql_text(buf.format(*v), || v.to_string());
            write_lenenc_string(payload, text.as_bytes());
        }
        Value::Decimal(value) => write_lenenc_string(payload, value.as_bytes()),
        Value::Bytes(value) => write_lenenc_string(payload, value),
        // String text encoding — borrow when the connection charset
        // doesn't require re-encoding (the dominant case: default
        // utf8mb4 client, or `charset = None`). The previous
        // `encode_string_lossy` returned a freshly allocated `Vec<u8>`
        // per cell, which the lenenc writer then memcpy'd into the
        // payload. The Cow path borrows the source bytes and the
        // lenenc writer copies them straight in — saves one alloc +
        // one memcpy per string cell on the result-set hot path.
        Value::String(text) => write_text_string(payload, text, charset),
        value @ Value::SmallString(_) => write_text_string(
            payload,
            crate::types::value_str(value).expect("matched string value"),
            charset,
        ),
        // Binary temporal / decimal — emit canonical ASCII directly
        // into a stack buffer to avoid the per-row `String` allocation
        // that `to_display_string()` would incur.
        Value::DecimalCompact(mag, scale) => {
            let mut buf = [0u8; 48];
            let bytes = crate::types::write_decimal_compact_canonical(*mag, *scale, &mut buf);
            write_lenenc_string(payload, bytes);
        }
        Value::Decimal128(..) | Value::DecimalWide(..) => {
            let text = value.to_display_string();
            write_lenenc_string(payload, text.as_bytes());
        }
        Value::Date(d) => {
            let mut buf = [0u8; 32];
            let bytes = crate::types::write_date_canonical(*d, &mut buf);
            write_lenenc_string(payload, bytes);
        }
        Value::Time(t) => {
            let mut buf = [0u8; 32];
            let bytes = crate::types::write_time_canonical_with_fsp(*t, fsp, &mut buf);
            write_lenenc_string(payload, bytes);
        }
        Value::DateTime(dt) | Value::Timestamp(dt) => {
            let mut buf = [0u8; 32];
            let bytes = crate::types::write_datetime_canonical_with_fsp(*dt, fsp, &mut buf);
            write_lenenc_string(payload, bytes);
        }
        Value::Year(y) => {
            let mut buf = [0u8; 32];
            let bytes = crate::types::write_year_canonical(*y, &mut buf);
            write_lenenc_string(payload, bytes);
        }
        // JSON is sent to the client as canonical text (MySQL serialises the
        // binary form before transmitting).
        Value::Json(b) => write_lenenc_json_text(payload, b),
        // GEOMETRY is sent as its raw internal `SRID + WKB` bytes (MySQL does
        // the same; clients render via ST_AsText / ST_AsWKB).
        Value::Geometry(b) => write_lenenc_string(payload, b),
        Value::Vector(b) => write_lenenc_vector(payload, b),
    }
}

// MySQL column-definition flags. Only type-derived flags (NUM/BLOB/BINARY)
// are set from the ColumnType alone. Catalog-derived flags (NOT_NULL,
// PRI_KEY, AUTO_INCREMENT, UNSIGNED) and the real schema/table/org names
// require Column to carry its schema source — that is the remaining P0-7
// work (a planner→protocol metadata flow touching ~139 Column construction
// sites), tracked separately.
// MySQL column-definition flags. See `include/mysql_com.h` in the
// upstream tree. These mirror the upstream bit positions exactly so a
// flag value computed here can be OR-ed into the wire packet verbatim.
pub const NOT_NULL_FLAG: u16 = 0x0001;
pub const PRI_KEY_FLAG: u16 = 0x0002;
pub const UNIQUE_KEY_FLAG: u16 = 0x0004;
pub const MULTIPLE_KEY_FLAG: u16 = 0x0008;
pub const BLOB_FLAG: u16 = 0x0010;
pub const UNSIGNED_FLAG: u16 = 0x0020;
pub const ZEROFILL_FLAG: u16 = 0x0040;
pub const BINARY_FLAG: u16 = 0x0080;
pub const ENUM_FLAG: u16 = 0x0100;
pub const AUTO_INCREMENT_FLAG: u16 = 0x0200;
pub const TIMESTAMP_FLAG: u16 = 0x0400;
pub const SET_FLAG: u16 = 0x0800;
pub const NO_DEFAULT_VALUE_FLAG: u16 = 0x1000;
pub const ON_UPDATE_NOW_FLAG: u16 = 0x2000;
pub const NUM_FLAG: u16 = 0x8000;
/// `decimals = 0x1f` marks a floating-point column with a non-fixed number
/// of decimals (MySQL's NOT_FIXED_DEC), as opposed to a DECIMAL's exact scale.
const NOT_FIXED_DEC: u8 = 31;

/// Per-type column metadata for the result-set column-definition packet:
/// `(display_length, decimals, type_flags)`. Drives the precision/scale and
/// column-type information ORM/JDBC clients read via `ResultSetMetaData`
/// (`getPrecision`/`getScale`/`getColumnDisplaySize`). Previously these were
/// hardcoded to `len=1024, decimals=0, flags=0`, so e.g. `DECIMAL(10,2)`
/// reported scale 0 and `DOUBLE` reported a fixed scale.
fn column_type_metadata(column_type: ColumnType) -> (u32, u8, u16) {
    match column_type {
        ColumnType::Tiny => (4, 0, NUM_FLAG | BINARY_FLAG),
        ColumnType::Short => (6, 0, NUM_FLAG | BINARY_FLAG),
        ColumnType::MediumInt => (9, 0, NUM_FLAG | BINARY_FLAG),
        ColumnType::Long => (11, 0, NUM_FLAG | BINARY_FLAG),
        ColumnType::LongLong => (20, 0, NUM_FLAG | BINARY_FLAG),
        ColumnType::Float => (12, NOT_FIXED_DEC, NUM_FLAG | BINARY_FLAG),
        ColumnType::Double => (22, NOT_FIXED_DEC, NUM_FLAG | BINARY_FLAG),
        ColumnType::Decimal { precision, scale } => {
            // Display width: significant digits + sign + (decimal point if any).
            let len = precision as u32 + if scale > 0 { 2 } else { 1 };
            (len, scale, NUM_FLAG | BINARY_FLAG)
        }
        ColumnType::Date => (10, 0, BINARY_FLAG | UNSIGNED_FLAG),
        ColumnType::Time => (10, 0, BINARY_FLAG | UNSIGNED_FLAG),
        ColumnType::DateTime => (19, 0, BINARY_FLAG | UNSIGNED_FLAG),
        ColumnType::Timestamp => (19, 0, BINARY_FLAG | UNSIGNED_FLAG),
        // YEAR is reported as a 4-character display width per MySQL, and
        // is always unsigned (0 / 1901..=2155) — MySQL sets UNSIGNED_FLAG
        // on YEAR columns regardless of the column definition.
        ColumnType::Year => (4, 0, NUM_FLAG | BINARY_FLAG | UNSIGNED_FLAG | ZEROFILL_FLAG),
        ColumnType::Blob => (65_535, 0, BLOB_FLAG | BINARY_FLAG),
        ColumnType::Json => (u32::MAX, 0, BLOB_FLAG),
        ColumnType::Geometry => (u32::MAX, 0, BLOB_FLAG | BINARY_FLAG),
        ColumnType::Vector => (u32::MAX, 0, BLOB_FLAG | BINARY_FLAG),
        // BIT(M): display length = M (bit count), no decimals. MySQL marks
        // BIT columns UNSIGNED; the value travels as a binary byte string
        // (see the BIT arms in the text/binary row encoders and the
        // binary-charset default below).
        ColumnType::Bit { bits } => (bits as u32, 0, BINARY_FLAG | UNSIGNED_FLAG),
        ColumnType::VarString => (65_535, 0, 0),
    }
}

/// Construct a column-definition packet from a column's name and wire
/// type alone. Bare-bones shape — no per-column source flags / decimals
/// override. Use [`column_definition_packet_full`] for the result-set
/// hot path, which carries the catalog-derived flags and length.
pub fn column_definition_packet(name: &str, column_type: ColumnType) -> Vec<u8> {
    column_definition_packet_full(name, column_type, &crate::types::ColumnSource::default())
}

/// Construct a column-definition packet using both the wire `column_type`
/// and the catalog-derived [`ColumnSource`]. The source carries:
///   * `flags` — OR-ed into the wire flags so ENUM/SET/UNSIGNED/etc.
///     reach the client. ENUM/SET pick up `MYSQL_TYPE_STRING` per
///     MySQL convention (ENUM/SET don't have dedicated wire bytes;
///     clients detect them through `ENUM_FLAG` / `SET_FLAG`).
///   * `length` — overrides the type-derived display width (e.g.
///     `VARCHAR(50)` reports 50 rather than 65535).
pub fn column_definition_packet_full(
    name: &str,
    column_type: ColumnType,
    source: &crate::types::ColumnSource,
) -> Vec<u8> {
    column_definition_packet_full_with_fallback(name, column_type, source, 0)
}

pub fn column_definition_packet_full_with_fallback(
    name: &str,
    column_type: ColumnType,
    source: &crate::types::ColumnSource,
    fallback_collation: u16,
) -> Vec<u8> {
    let mut payload = Vec::new();
    column_definition_packet_full_into_with_fallback(
        &mut payload,
        name,
        column_type,
        source,
        fallback_collation,
    );
    payload
}

pub fn column_definition_packet_full_into_with_fallback(
    payload: &mut Vec<u8>,
    name: &str,
    column_type: ColumnType,
    source: &crate::types::ColumnSource,
    fallback_collation: u16,
) {
    payload.clear();
    write_lenenc_string(payload, b"def");
    // Metadata identifiers obey character_set_results even for numeric and
    // binary result columns, whose value collation is always binary.
    let metadata_charset = crate::charset::resolve_collation_id(fallback_collation)
        .filter(|info| !matches!(info.charset, "utf8mb4" | "binary"))
        .and_then(|info| crate::charset::resolve_charset(info.charset).ok());
    for text in [
        &*source.schema,
        &*source.table,
        &*source.org_table,
        name,
        &*source.org_name,
    ] {
        if let Some(charset) = metadata_charset {
            write_lenenc_string(
                payload,
                &crate::charset::encode_string_lossy_cow_info(Some(charset), text),
            );
        } else {
            write_lenenc_string(payload, text.as_bytes());
        }
    }
    payload.push(0x0c);
    // Per-column catalog collation when known (real text collation, or
    // 63 for BINARY/VARBINARY/BLOB). Without a catalog source: BLOB /
    // GEOMETRY wire types report binary so clients keep raw bytes,
    // everything else the connection's utf8mb4 collation.
    let collation = if matches!(column_type, ColumnType::Json) {
        // MySQL always reports a JSON result column with the binary (63)
        // collation, regardless of any catalog collation stored for it.
        BINARY_COLLATION
    } else {
        source.collation_id.unwrap_or(match column_type {
            ColumnType::VarString => {
                if fallback_collation == 0 {
                    DEFAULT_UTF8MB4_COLLATION
                } else {
                    fallback_collation
                }
            }
            // Numeric and temporal result fields have no character
            // repertoire. MySQL advertises them with the binary collation
            // (63), including computed MIN/MAX(YEAR) expressions.
            _ => BINARY_COLLATION,
        })
    };
    // Zero means character_set_results=NULL: retain the source encoding.
    // Binary fields never acquire the session's text result charset.
    let collation = if collation != BINARY_COLLATION && fallback_collation != 0 {
        fallback_collation
    } else {
        collation
    };
    write_u16_le(payload, collation);
    let (default_length, decimals, default_flags) = column_type_metadata(column_type);
    let length = source.length.unwrap_or_else(|| {
        if matches!(
            column_type,
            ColumnType::Time | ColumnType::DateTime | ColumnType::Timestamp
        ) {
            default_length
                + source
                    .decimals
                    .filter(|fsp| *fsp > 0)
                    .map_or(0, |fsp| u32::from(fsp) + 1)
        } else {
            default_length
        }
    });
    write_u32_le(payload, length);
    let computed_lob = matches!(column_type, ColumnType::Blob)
        && source.org_table.is_empty()
        && source.decimals == Some(31);
    let group_concat_text_blob = computed_lob && collation != BINARY_COLLATION;
    payload.push(source.wire_type.unwrap_or_else(|| match column_type {
        ColumnType::Tiny => MYSQL_TYPE_TINY,
        ColumnType::Short => MYSQL_TYPE_SHORT,
        ColumnType::MediumInt => MYSQL_TYPE_INT24,
        ColumnType::Long => MYSQL_TYPE_LONG,
        ColumnType::LongLong => MYSQL_TYPE_LONGLONG,
        ColumnType::Float => MYSQL_TYPE_FLOAT,
        ColumnType::Double => MYSQL_TYPE_DOUBLE,
        ColumnType::Decimal { .. } => MYSQL_TYPE_NEWDECIMAL,
        ColumnType::Date => MYSQL_TYPE_DATE,
        ColumnType::Time => MYSQL_TYPE_TIME,
        ColumnType::DateTime => MYSQL_TYPE_DATETIME,
        ColumnType::Timestamp => MYSQL_TYPE_TIMESTAMP,
        ColumnType::Year => MYSQL_TYPE_YEAR,
        ColumnType::Blob if group_concat_text_blob => MYSQL_TYPE_LONG_BLOB,
        ColumnType::Blob if computed_lob && length <= 255 => MYSQL_TYPE_TINY_BLOB,
        ColumnType::Blob if computed_lob && length <= 65_535 => MYSQL_TYPE_BLOB,
        ColumnType::Blob if computed_lob && length <= 16_777_215 => MYSQL_TYPE_MEDIUM_BLOB,
        ColumnType::Blob if computed_lob => MYSQL_TYPE_LONG_BLOB,
        ColumnType::Blob => MYSQL_TYPE_BLOB,
        ColumnType::Json => MYSQL_TYPE_JSON,
        ColumnType::Geometry => MYSQL_TYPE_GEOMETRY,
        ColumnType::Vector => MYSQL_TYPE_VECTOR,
        ColumnType::Bit { .. } => MYSQL_TYPE_BIT,
        ColumnType::VarString if source.flags & (ENUM_FLAG | SET_FLAG) != 0 => MYSQL_TYPE_STRING,
        ColumnType::VarString => MYSQL_TYPE_VAR_STRING,
    }));
    // Source flags take precedence — they carry ENUM_FLAG / SET_FLAG /
    // UNSIGNED_FLAG / NOT_NULL_FLAG / PRI_KEY_FLAG / AUTO_INCREMENT_FLAG
    // computed from the catalog row. OR with type defaults so e.g. a
    // BLOB column still reports BLOB_FLAG | BINARY_FLAG.
    let flags = if computed_lob {
        source.flags
    } else {
        let type_flags = if matches!(
            source.wire_type,
            Some(MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING)
        ) {
            default_flags & !BLOB_FLAG
        } else {
            default_flags
        };
        type_flags | source.flags
    };
    write_u16_le(payload, flags);
    // Decimals override: FLOAT(M,D)/DOUBLE(M,D)'s D, or TIME/DATETIME/
    // TIMESTAMP's fsp, replace the type-derived NOT_FIXED_DEC / 0. DECIMAL
    // is handled by ColumnType::Decimal.scale and never sets source.decimals.
    let decimals = source.decimals.unwrap_or(decimals);
    payload.push(decimals);
    payload.extend_from_slice(&[0_u8; 2]);
}

#[cfg(test)]
pub fn eof_packet() -> [u8; 5] {
    eof_packet_with_status(SERVER_STATUS_AUTOCOMMIT)
}

/// EOF_Packet payload — always exactly 5 bytes (marker + warnings +
/// status), so a fixed array avoids two heap round-trips per result
/// set (header EOF + terminator EOF on every SELECT response).
pub fn eof_packet_with_status(status: u16) -> [u8; 5] {
    eof_packet_with_status_warnings(status, 0)
}

pub fn eof_packet_with_status_warnings(status: u16, warnings: u16) -> [u8; 5] {
    let status = status.to_le_bytes();
    let warnings = warnings.to_le_bytes();
    [0xfe, warnings[0], warnings[1], status[0], status[1]]
}

fn write_u16_le(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_null_string(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(value);
    out.push(0x00);
}

fn write_lenenc_string(out: &mut Vec<u8>, value: &[u8]) {
    if value.len() <= 250 {
        let start = out.len();
        let additional = value.len() + 1;
        out.reserve(additional);
        // SAFETY: reserve checks start + additional for overflow and provides
        // capacity for both the one-byte prefix and the entire payload. The
        // source slice cannot overlap a uniquely borrowed destination Vec.
        // All new bytes are initialized before publishing the new length.
        unsafe {
            let dst = out.as_mut_ptr().add(start);
            dst.write(value.len() as u8);
            std::ptr::copy_nonoverlapping(value.as_ptr(), dst.add(1), value.len());
            out.set_len(start + additional);
        }
    } else {
        write_lenenc_int(out, value.len() as u64);
        out.extend_from_slice(value);
    }
}

pub fn write_lenenc_int(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=250 => out.push(value as u8),
        251..=0xffff => {
            out.push(0xfc);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        0x1_0000..=0xff_ffff => {
            out.push(0xfd);
            out.push((value & 0xff) as u8);
            out.push(((value >> 8) & 0xff) as u8);
            out.push(((value >> 16) & 0xff) as u8);
        }
        _ => {
            out.push(0xfe);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_u16_le(&mut self) -> Result<u16, String> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32_le(&mut self) -> Result<u32, String> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| "cursor overflow".to_string())?;
        if end > self.input.len() {
            return Err("packet ended unexpectedly".to_string());
        }

        let bytes = &self.input[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn skip(&mut self, len: usize) -> Result<(), String> {
        self.read_bytes(len).map(|_| ())
    }

    fn read_null_string(&mut self) -> Result<String, String> {
        let Some(relative_end) = self.input[self.position..]
            .iter()
            .position(|byte| *byte == 0)
        else {
            return Err("null-terminated string is missing terminator".to_string());
        };
        let end = self.position + relative_end;
        let value = String::from_utf8_lossy(&self.input[self.position..end]).to_string();
        self.position = end + 1;
        Ok(value)
    }

    fn read_lenenc_string(&mut self) -> Result<String, String> {
        let len = self.read_lenenc_int()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8_lossy(bytes).to_string())
    }

    fn read_lenenc_bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.read_lenenc_int()? as usize;
        self.read_bytes(len)
    }

    fn read_lenenc_int(&mut self) -> Result<u64, String> {
        let first = self.read_u8()?;
        match first {
            0x00..=0xfa => Ok(first as u64),
            0xfc => {
                let bytes = self.read_bytes(2)?;
                Ok(u16::from_le_bytes([bytes[0], bytes[1]]) as u64)
            }
            0xfd => {
                let bytes = self.read_bytes(3)?;
                Ok((bytes[0] as u64) | ((bytes[1] as u64) << 8) | ((bytes[2] as u64) << 16))
            }
            0xfe => {
                let bytes = self.read_bytes(8)?;
                Ok(u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]))
            }
            0xfb => Err("NULL length-encoded integer is not valid here".to_string()),
            0xff => Err("0xff is not a valid length-encoded integer".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refactor_query_and_execute_parameter_wire_matrix() {
        let mut cases = Vec::new();
        for (ty, width) in [
            (MYSQL_TYPE_TINY, 1),
            (MYSQL_TYPE_SHORT, 2),
            (MYSQL_TYPE_YEAR, 2),
            (MYSQL_TYPE_LONG, 4),
            (MYSQL_TYPE_INT24, 4),
            (MYSQL_TYPE_LONGLONG, 8),
        ] {
            for unsigned in [false, true] {
                cases.push((ty, unsigned, vec![0xff; width]));
            }
        }
        cases.push((MYSQL_TYPE_FLOAT, false, 1.25f32.to_le_bytes().to_vec()));
        cases.push((MYSQL_TYPE_DOUBLE, false, (-2.5f64).to_le_bytes().to_vec()));
        cases.push((MYSQL_TYPE_NULL, false, vec![]));
        for ty in [
            MYSQL_TYPE_DATE,
            MYSQL_TYPE_NEWDATE,
            MYSQL_TYPE_DATETIME,
            MYSQL_TYPE_TIMESTAMP,
            MYSQL_TYPE_DATETIME2,
            MYSQL_TYPE_TIMESTAMP2,
            MYSQL_TYPE_TIME,
            MYSQL_TYPE_TIME2,
        ] {
            cases.push((ty, false, vec![0]));
        }
        for ty in [
            MYSQL_TYPE_DECIMAL,
            MYSQL_TYPE_NEWDECIMAL,
            MYSQL_TYPE_VARCHAR,
            MYSQL_TYPE_VAR_STRING,
            MYSQL_TYPE_STRING,
            MYSQL_TYPE_TINY_BLOB,
            MYSQL_TYPE_MEDIUM_BLOB,
            MYSQL_TYPE_LONG_BLOB,
            MYSQL_TYPE_BLOB,
            MYSQL_TYPE_ENUM,
            MYSQL_TYPE_SET,
            MYSQL_TYPE_BIT,
            MYSQL_TYPE_JSON,
            MYSQL_TYPE_GEOMETRY,
            MYSQL_TYPE_VECTOR,
        ] {
            cases.push((ty, false, b"\x04-1.5".to_vec()));
        }
        for (ty, unsigned, bytes) in cases {
            let mut query = vec![1, 1, 0, 1, ty, u8::from(unsigned), 1, b'a'];
            query.extend_from_slice(&bytes);
            query.extend_from_slice(b"SELECT 1");
            let attributes = parse_query_attributes(&query).unwrap();
            assert_eq!(attributes.sql, b"SELECT 1", "type {ty}");

            let mut execute = vec![7, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, ty, u8::from(unsigned)];
            execute.extend_from_slice(&bytes);
            let prepared = parse_stmt_execute_params(&execute, 1, None, None, false).unwrap();
            assert_eq!(
                attributes.attributes[0].1, prepared.params[0],
                "type {ty} unsigned {unsigned}"
            );
            for truncated in 0..bytes.len() {
                let mut cursor = Cursor::new(&bytes[..truncated]);
                assert!(
                    read_parameter_value(&mut cursor, ty, unsigned, "prepared").is_err(),
                    "type {ty} length {truncated}"
                );
            }
        }
        for context in ["prepared", "query attribute"] {
            let error =
                read_parameter_value(&mut Cursor::new(&[]), 0xf1, false, context).unwrap_err();
            assert_eq!(error, format!("unsupported {context} parameter type: 0xf1"));
        }
    }
    use crate::engine::api::ResultSet;

    #[test]
    fn protocol_audit_extreme_parameter_counts_are_rejected() {
        for count in [u64::MAX, u64::MAX - 7, u32::MAX as u64] {
            let mut query = Vec::new();
            write_lenenc_int(&mut query, count);
            query.push(1);
            assert!(parse_query_attributes(&query).is_err());
            let mut execute = vec![0, 0, 0, 0, 8, 1, 0, 0, 0];
            write_lenenc_int(&mut execute, count);
            assert!(parse_stmt_execute_params(&execute, 1, None, None, true).is_err());
        }
    }

    /// Locate the MySQL `type` byte in a column-definition packet. Layout:
    ///   lenenc("def") lenenc(schema) lenenc(table) lenenc(org_table)
    ///   lenenc(name) lenenc(org_name) lenenc(0x0c) u16 collation
    ///   u32 length **u8 type** u16 flags u8 decimals [u8;2] filler
    /// All lenenc strings start with a 1-byte length when ≤250, which is
    /// what these tests construct.
    fn extract_type_byte(packet: &[u8]) -> u8 {
        let mut pos = 0;
        for _ in 0..6 {
            let len = packet[pos] as usize;
            pos += 1 + len;
        }
        // skip the 0x0c marker + collation(u16) + length(u32) = 7 bytes
        pos += 7;
        packet[pos]
    }

    fn extract_flags(packet: &[u8]) -> u16 {
        // The type byte sits at the index above; flags are the next 2 bytes.
        let mut pos = 0;
        for _ in 0..6 {
            let len = packet[pos] as usize;
            pos += 1 + len;
        }
        pos += 7; // skip the 0x0c filler + collation(u16) + length(u32)
        pos += 1; // skip the type byte
        u16::from_le_bytes([packet[pos], packet[pos + 1]])
    }

    #[test]
    fn err_packet_uses_mysql_protocol_error_shape() {
        let packet = command_err_packet_for_spec(
            crate::mysql::errors::ER_PARSE_ERROR,
            "syntax problem near 'x'",
        );
        assert_eq!(packet[0], 0xff);
        assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1064);
        assert_eq!(packet[3], b'#');
        assert_eq!(&packet[4..9], b"42000");
        assert_eq!(&packet[9..], b"syntax problem near 'x'");
    }

    #[test]
    fn out_of_memory_packet_is_mysql_1037_hy001() {
        let packet =
            err_packet(&crate::error::catalog::ER_OUTOFMEMORY.error("query memory limit exceeded"));
        assert_eq!(packet[0], 0xff);
        assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1037);
        assert_eq!(&packet[3..9], b"#HY001");
    }

    #[test]
    fn err_packet_preserves_validated_signal_sqlstate_exactly() {
        let error = QueryError::user_defined(1644, "45A01", "raised by SIGNAL").unwrap();
        let packet = err_packet(&error);
        assert_eq!(&packet[..9], b"\xffl\x06#45A01");
        assert_eq!(&packet[9..], b"raised by SIGNAL");
    }

    #[test]
    #[should_panic(expected = "CR_* client conditions")]
    fn client_error_domain_cannot_be_sent_as_server_err_packet() {
        let _ = err_packet(
            &crate::error::catalog::CR_SERVER_LOST.error("internal replica client lost source"),
        );
    }

    #[test]
    fn column_def_emits_year_type_byte() {
        let packet = column_definition_packet("y", ColumnType::Year);
        assert_eq!(
            extract_type_byte(&packet),
            MYSQL_TYPE_YEAR,
            "YEAR must serialise as 0x0d, not borrow TINY 0x01"
        );
    }

    #[test]
    fn column_def_emits_int24_for_mediumint() {
        let packet = column_definition_packet("m", ColumnType::MediumInt);
        assert_eq!(extract_type_byte(&packet), MYSQL_TYPE_INT24);
    }

    #[test]
    fn column_def_source_flags_or_into_packet() {
        // Pretend the catalog flagged this column as ENUM (0x0100) and
        // NOT NULL (0x0001). Both must appear in the wire flags.
        let source = crate::types::ColumnSource {
            flags: 0x0100 | 0x0001,
            ..Default::default()
        };
        let packet = column_definition_packet_full("e", ColumnType::VarString, &source);
        let flags = extract_flags(&packet);
        assert!(
            flags & 0x0100 != 0,
            "ENUM_FLAG must reach the wire (got {:#x})",
            flags
        );
        assert!(flags & 0x0001 != 0, "NOT_NULL_FLAG must reach the wire");
    }

    #[test]
    fn column_def_source_length_overrides_default_width() {
        let source = crate::types::ColumnSource {
            length: Some(50),
            ..Default::default()
        };
        let packet = column_definition_packet_full("v", ColumnType::VarString, &source);
        // Length field is 4 bytes LE just before the type byte; find it
        // the same way extract_type_byte does, but back up 4.
        let mut pos = 0;
        for _ in 0..6 {
            let len = packet[pos] as usize;
            pos += 1 + len;
        }
        pos += 1 + 2; // 0x0c + collation(u16)
        let length = u32::from_le_bytes(packet[pos..pos + 4].try_into().unwrap());
        assert_eq!(length, 50);
    }

    #[test]
    fn computed_binary_lob_uses_declared_width_for_wire_subtype() {
        let source = crate::types::ColumnSource {
            collation_id: Some(BINARY_COLLATION),
            length: Some(2_147_483_648),
            decimals: Some(31),
            flags: BINARY_FLAG,
            ..Default::default()
        };
        let packet = column_definition_packet_full("UNHEX(x)", ColumnType::Blob, &source);
        assert_eq!(extract_type_byte(&packet), MYSQL_TYPE_LONG_BLOB);
        assert_eq!(extract_flags(&packet), BINARY_FLAG);
    }

    #[test]
    fn column_type_metadata_reflects_precision_scale_and_flags() {
        // DECIMAL(10,2): display width 12 (10 digits + sign + point), scale 2.
        assert_eq!(
            column_type_metadata(ColumnType::Decimal {
                precision: 10,
                scale: 2
            }),
            (12, 2, NUM_FLAG | BINARY_FLAG)
        );
        // FLOAT/DOUBLE report NOT_FIXED_DEC (0x1f), not a fixed scale of 0.
        assert_eq!(column_type_metadata(ColumnType::Double).1, NOT_FIXED_DEC);
        assert_eq!(column_type_metadata(ColumnType::Float).1, NOT_FIXED_DEC);
        // Integers carry NUM_FLAG with a sensible display width and 0 decimals.
        assert_eq!(
            column_type_metadata(ColumnType::Long),
            (11, 0, NUM_FLAG | BINARY_FLAG)
        );
        // BLOB carries BLOB|BINARY; plain strings carry no type flags.
        assert_eq!(
            column_type_metadata(ColumnType::Blob).2,
            BLOB_FLAG | BINARY_FLAG
        );
        assert_eq!(column_type_metadata(ColumnType::VarString).2, 0);
    }

    #[test]
    fn bit_column_advertises_mysql_type_bit_with_unsigned_binary() {
        // B2/B10: a BIT(M) column must go out as MYSQL_TYPE_BIT (0x10) with
        // UNSIGNED_FLAG, binary charset (63), and length = M — not as a
        // signed SMALLINT (which truncated BIT(M>16) on the binary protocol).
        let packet = column_definition_packet("b", ColumnType::Bit { bits: 40 });
        assert_eq!(extract_type_byte(&packet), MYSQL_TYPE_BIT);
        assert!(
            extract_flags(&packet) & UNSIGNED_FLAG != 0,
            "BIT must carry UNSIGNED_FLAG"
        );
        // Collation sits right after the 0x0c filler; expect binary (63).
        let mut pos = 0;
        for _ in 0..6 {
            let len = packet[pos] as usize;
            pos += 1 + len;
        }
        pos += 1; // 0x0c filler
        assert_eq!(
            u16::from_le_bytes([packet[pos], packet[pos + 1]]),
            BINARY_COLLATION
        );
        // Metadata: length = M, 0 decimals, UNSIGNED.
        assert_eq!(
            column_type_metadata(ColumnType::Bit { bits: 40 }),
            (40, 0, UNSIGNED_FLAG | BINARY_FLAG)
        );
    }

    #[test]
    fn vector_wire_payload_matches_native_float32_clients_for_all_storage_types() {
        use crate::vector::{VectorDataType as T, VectorValue};
        let columns = [Column {
            name: "v".into(),
            column_type: ColumnType::Vector,
            source: Default::default(),
        }];
        let cases = [
            (
                T::Float32,
                vec![
                    1.0f32.to_le_bytes(),
                    (-2.0f32).to_le_bytes(),
                    3.0f32.to_le_bytes(),
                ]
                .concat(),
                vec![1.0f32, -2.0, 3.0],
            ),
            (
                T::Float16,
                vec![
                    half::f16::from_f32(1.0).to_le_bytes(),
                    half::f16::from_f32(-2.0).to_le_bytes(),
                    half::f16::from_f32(3.0).to_le_bytes(),
                ]
                .concat(),
                vec![1.0f32, -2.0, 3.0],
            ),
            (
                T::BFloat16,
                vec![
                    half::bf16::from_f32(1.0).to_le_bytes(),
                    half::bf16::from_f32(-2.0).to_le_bytes(),
                    half::bf16::from_f32(3.0).to_le_bytes(),
                ]
                .concat(),
                vec![1.0f32, -2.0, 3.0],
            ),
            (T::Int8, vec![1, 254, 3], vec![1.0f32, -2.0, 3.0]),
            (T::Binary, vec![5], vec![1.0f32, 0.0, 1.0]),
        ];
        for (datatype, payload, values) in cases {
            let row = [Value::Vector(VectorValue::new(datatype, 3, payload))];
            let expected: Vec<u8> = values.into_iter().flat_map(f32::to_le_bytes).collect();
            let binary = binary_row_packet(&columns, &row);
            assert_eq!(&binary[..3], &[0, 0, 12]);
            assert_eq!(&binary[3..], expected);
            let mut text = Vec::new();
            text_row_packet_into_with_columns(&row, &mut text, None, Some(&columns));
            assert_eq!(text[0], 12);
            assert_eq!(&text[1..], expected);
            assert!(estimate_binary_row_packet_bytes(&row) >= binary.len());
            assert!(estimate_text_row_packet_bytes(&row) >= text.len());

            let text_columns = vector_text_result_columns(&columns, true);
            vector_text_row_packet_into(&row, &mut text, None, &text_columns);
            let expected = if datatype == T::Binary {
                "[1,0,1]"
            } else {
                "[1,-2,3]"
            };
            assert_eq!(text[0] as usize, expected.len());
            assert_eq!(&text[1..], expected.as_bytes());
            assert!(
                estimate_vector_text_row_packet_bytes(&row, None, Some(&text_columns))
                    >= text.len()
            );
        }
    }

    #[test]
    fn vector_text_encoding_handles_extremes_without_changing_native_strings() {
        let values = [f32::MAX, f32::MIN, f32::MIN_POSITIVE, -f32::from_bits(1)];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let row = [Value::Vector(bytes.clone().into())];
        // Mixed expressions may declare VARCHAR while retaining a VECTOR value.
        // Their default wire representation must remain unchanged.
        let columns = [Column {
            column_type: ColumnType::VarString,
            ..Default::default()
        }];
        let mut packet = Vec::new();
        text_row_packet_into_with_columns(&row, &mut packet, None, Some(&columns));
        assert_eq!(Cursor::new(&packet).read_lenenc_bytes().unwrap(), bytes);
        for name in ["utf8mb4", "latin1", "ucs2", "utf16", "utf16le", "utf32"] {
            let charset = crate::charset::resolve_result_charset(Some(name));
            vector_text_row_packet_into(&row, &mut packet, charset, &columns);
            assert!(
                estimate_vector_text_row_packet_bytes(&row, charset, Some(&columns))
                    >= packet.len()
            );
            let mut cursor = Cursor::new(&packet);
            let text =
                crate::charset::decode_bytes(name, cursor.read_lenenc_bytes().unwrap()).unwrap();
            let decoded: Vec<f32> = serde_json::from_str(&text).unwrap();
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn vector_text_result_metadata_and_nulls_are_consistent() {
        let columns = [Column {
            name: "embedding".into(),
            column_type: ColumnType::Vector,
            source: crate::types::ColumnSource {
                schema: "test".into(),
                table: "t".into(),
                org_table: "t".into(),
                org_name: "v".into(),
                wire_type: Some(MYSQL_TYPE_VECTOR),
                collation_id: Some(BINARY_COLLATION),
                flags: BLOB_FLAG | BINARY_FLAG | NOT_NULL_FLAG,
                length: Some(12),
                vector_type: Some((crate::vector::VectorDataType::Float32, Some(3))),
                ..Default::default()
            },
        }];
        assert!(matches!(
            vector_text_result_columns(&columns, false),
            std::borrow::Cow::Borrowed(_)
        ));
        let converted = vector_text_result_columns(&columns, true);
        assert_eq!(converted[0].source.org_name, "v");
        assert_eq!(columns[0].column_type, ColumnType::Vector);
        let packet = column_definition_packet_full_with_fallback(
            &converted[0].name,
            converted[0].column_type,
            &converted[0].source,
            45,
        );
        assert_eq!(extract_type_byte(&packet), MYSQL_TYPE_VAR_STRING);
        assert_eq!(extract_flags(&packet), NOT_NULL_FLAG);
        assert_eq!(converted[0].source.collation_id, None);
        let mut payload = Vec::new();
        vector_text_row_packet_into(&[Value::Null], &mut payload, None, &converted);
        assert_eq!(payload, [0xfb]);
        assert!(matches!(
            vector_text_result_columns(&converted, true),
            std::borrow::Cow::Borrowed(_)
        ));
    }
    #[test]
    fn bit_value_binary_and_text_encode_as_bytes_not_truncated() {
        // B2: BIT(40) value 0x01_0000_0005 must survive the binary protocol
        // as a 5-byte length-encoded big-endian string, not a truncated u16.
        let columns = [Column {
            name: "b".to_string(),
            column_type: ColumnType::Bit { bits: 40 },
            source: crate::types::ColumnSource::default(),
        }];
        let value = 0x01_0000_0005u64;
        let row = [Value::UInt(value)];
        let mut payload = Vec::new();
        binary_row_packet_into(&columns, &row, &mut payload);
        // header(1) + null-bitmap(1) + lenenc(1)=5 + 5 big-endian bytes.
        assert_eq!(
            &payload[2..],
            &[5, 0x01, 0x00, 0x00, 0x00, 0x05],
            "binary BIT must be a 5-byte big-endian lenenc string"
        );

        // Text protocol: raw big-endian bytes (ceil(40/8) = 5), not decimal.
        let mut text = Vec::new();
        text_row_packet_into_with_columns(&row, &mut text, None, Some(&columns));
        assert_eq!(text, vec![5, 0x01, 0x00, 0x00, 0x00, 0x05]);
    }

    #[test]
    fn text_row_packet_encodes_strings_with_result_charset() {
        let row = [Value::String("é".to_string())];
        let mut payload = Vec::new();
        text_row_packet_into_with_charset(&row, &mut payload, Some("latin1"));
        assert_eq!(payload, vec![1, 0xe9]);

        text_row_packet_into_with_charset(&row, &mut payload, Some("utf8mb4"));
        assert_eq!(payload, vec![2, 0xc3, 0xa9]);
    }

    #[test]
    fn parses_handshake_response_with_zstd_compression_level() {
        // Clients advertising CLIENT_ZSTD_COMPRESSION_ALGORITHM append
        // a one-byte `zstd_compression_level` after the connection
        // attributes block. The level is what the *client* asks the
        // server to use for its outbound stream; mydb honours it via
        // `enable_negotiated_compression`.
        let mut payload = Vec::new();
        let flags = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_CONNECT_ATTRS
            | CLIENT_ZSTD_COMPRESSION_ALGORITHM;
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&1024_u32.to_le_bytes());
        payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
        payload.extend_from_slice(&[0_u8; 23]);
        payload.extend_from_slice(b"root\0");
        payload.push(3);
        payload.extend_from_slice(b"abc");
        // No CONNECT_WITH_DB so we go straight to plugin / attrs.
        // (CLIENT_PLUGIN_AUTH not set, so no plugin name either.)
        // attrs = empty
        write_lenenc_int(&mut payload, 0);
        // Trailing zstd level byte.
        payload.push(7);

        let parsed = parse_handshake_response(&payload).unwrap();
        assert_eq!(parsed.zstd_compression_level, Some(7));
    }

    #[test]
    fn parses_handshake_response_without_zstd_when_flag_absent() {
        // Conversely, when the capability bit is not set, the level
        // byte MUST stay `None`. A regression that always reads the
        // byte would consume part of the next packet.
        let mut payload = Vec::new();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&1024_u32.to_le_bytes());
        payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
        payload.extend_from_slice(&[0_u8; 23]);
        payload.extend_from_slice(b"root\0");
        payload.push(0); // empty auth response

        let parsed = parse_handshake_response(&payload).unwrap();
        assert!(parsed.zstd_compression_level.is_none());
    }

    #[test]
    fn parses_handshake_response_with_username_database_plugin_and_attrs() {
        let mut payload = Vec::new();
        let flags = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PLUGIN_AUTH
            | CLIENT_CONNECT_ATTRS;
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&1024_u32.to_le_bytes());
        payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
        payload.extend_from_slice(&[0_u8; 23]);
        payload.extend_from_slice(b"root\0");
        payload.push(3);
        payload.extend_from_slice(b"abc");
        payload.extend_from_slice(b"test\0");
        payload.extend_from_slice(b"mysql_native_password\0");

        let mut attrs = Vec::new();
        write_lenenc_string(&mut attrs, b"_client_name");
        write_lenenc_string(&mut attrs, b"mysql-connector-j");
        write_lenenc_int(&mut payload, attrs.len() as u64);
        payload.extend_from_slice(&attrs);

        let parsed = parse_handshake_response(&payload).unwrap();

        assert_eq!(parsed.username, "root");
        assert_eq!(parsed.database.as_deref(), Some("test"));
        assert_eq!(parsed.auth_response, b"abc");
        assert_eq!(
            parsed.auth_plugin_name.as_deref(),
            Some("mysql_native_password")
        );
        assert_eq!(
            parsed
                .connection_attrs
                .get("_client_name")
                .map(String::as_str),
            Some("mysql-connector-j")
        );
    }

    #[test]
    fn parses_change_user_request_with_plugin_charset_and_attrs() {
        let flags = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CLIENT_CONNECT_ATTRS;
        let mut payload = Vec::new();
        payload.extend_from_slice(b"app\0");
        payload.push(3);
        payload.extend_from_slice(b"tok");
        payload.extend_from_slice(b"appdb\0");
        payload.extend_from_slice(&crate::charset::UTF8MB4_0900_AI_CI_ID.to_le_bytes());
        payload.extend_from_slice(b"caching_sha2_password\0");

        let mut attrs = Vec::new();
        write_lenenc_string(&mut attrs, b"_client_name");
        write_lenenc_string(&mut attrs, b"mysql-connector-j");
        write_lenenc_int(&mut payload, attrs.len() as u64);
        payload.extend_from_slice(&attrs);

        let parsed = parse_change_user_request(&payload, flags).unwrap();

        assert_eq!(parsed.username, "app");
        assert_eq!(parsed.database.as_deref(), Some("appdb"));
        assert_eq!(parsed.auth_response, b"tok");
        assert_eq!(
            parsed.auth_plugin_name.as_deref(),
            Some("caching_sha2_password")
        );
        assert_eq!(
            parsed.character_set,
            Some(crate::charset::UTF8MB4_0900_AI_CI_ID)
        );
        assert_eq!(
            parsed
                .connection_attrs
                .get("_client_name")
                .map(String::as_str),
            Some("mysql-connector-j")
        );
    }

    #[test]
    fn negotiated_capabilities_intersect_and_set_option_uses_advertised_support() {
        let advertised = server_capabilities(false);
        assert_ne!(advertised & CLIENT_FOUND_ROWS, 0);
        assert_ne!(advertised & CLIENT_INTERACTIVE, 0);
        assert_ne!(advertised & CLIENT_MULTI_RESULTS, 0);
        assert_ne!(advertised & CLIENT_PS_MULTI_RESULTS, 0);
        assert_eq!(advertised & CLIENT_MULTI_FACTOR_AUTHENTICATION, 0);

        let mut caps = NegotiatedCapabilities::new(
            advertised | CLIENT_MULTI_FACTOR_AUTHENTICATION,
            advertised,
        );
        assert!(!caps.has(CLIENT_MULTI_FACTOR_AUTHENTICATION));
        caps.set_multi_statements(false);
        assert!(!caps.has(CLIENT_MULTI_STATEMENTS));
        caps.set_multi_statements(true);
        assert!(caps.has(CLIENT_MULTI_STATEMENTS));

        let mut connector_j_caps =
            NegotiatedCapabilities::new(advertised & !CLIENT_MULTI_STATEMENTS, advertised);
        assert!(!connector_j_caps.has(CLIENT_MULTI_STATEMENTS));
        connector_j_caps.set_multi_statements(true);
        assert!(connector_j_caps.has(CLIENT_MULTI_STATEMENTS));

        let mut unsupported_caps = NegotiatedCapabilities::new(
            CLIENT_MULTI_STATEMENTS,
            advertised & !CLIENT_MULTI_STATEMENTS,
        );
        unsupported_caps.set_multi_statements(true);
        assert!(!unsupported_caps.has(CLIENT_MULTI_STATEMENTS));
    }

    #[test]
    fn handshake_lenenc_auth_response_takes_precedence_over_secure_connection() {
        let flags = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        let token = vec![b'x'; 300];
        let mut payload = Vec::new();
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
        payload.extend_from_slice(&[0_u8; 23]);
        payload.extend_from_slice(b"root\0");
        write_lenenc_int(&mut payload, token.len() as u64);
        payload.extend_from_slice(&token);
        payload.extend_from_slice(b"caching_sha2_password\0");

        let parsed = parse_handshake_response(&payload).unwrap();
        assert_eq!(parsed.auth_response, token);
    }

    #[test]
    fn ok_info_matches_mysql_lenenc_runtime_without_session_track() {
        let mut payload = Vec::new();
        ok_packet_with_full_payload_capabilities_into(
            0,
            0,
            SERVER_STATUS_AUTOCOMMIT,
            0,
            "Rows matched: 1",
            &[],
            CLIENT_PROTOCOL_41,
            &mut payload,
        );
        assert!(payload.ends_with(b"Rows matched: 1"));
        assert_eq!(payload[7] as usize, "Rows matched: 1".len());
        assert_eq!(payload[8], b'R');
    }

    #[test]
    fn prepare_ok_appends_optional_metadata_flag() {
        let packet = stmt_prepare_ok_packet(
            7,
            1,
            2,
            3,
            CLIENT_OPTIONAL_RESULTSET_METADATA,
            RESULTSET_METADATA_FULL,
        );
        assert_eq!(packet.len(), 13);
        assert_eq!(packet[10..12], 3_u16.to_le_bytes());
        assert_eq!(packet[12], RESULTSET_METADATA_FULL);
    }

    #[test]
    fn pre_handshake_error_has_no_protocol41_sqlstate_marker() {
        let packet = pre_handshake_err_packet(
            crate::error::catalog::ER_CON_COUNT_ERROR,
            "Too many connections",
        );
        assert_eq!(packet[0], 0xff);
        assert!(!packet[3..].starts_with(b"#HY000"));
        assert_eq!(&packet[3..], b"Too many connections");
    }

    #[test]
    fn stmt_execute_uses_long_data_without_inline_value_bytes() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7_u32.to_le_bytes());
        payload.push(0); // no cursor
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.push(0); // NULL bitmap
        payload.push(1); // new params bound
        payload.push(MYSQL_TYPE_BLOB);
        payload.push(0);

        let long_data = vec![Some(b"hello world".to_vec())];
        let parsed = parse_stmt_execute_params(&payload, 1, None, Some(&long_data), false).unwrap();

        assert_eq!(parsed.flags, 0);
        assert_eq!(
            parsed.params.as_slice(),
            &[StmtParamValue::Bytes(b"hello world".to_vec())]
        );
        assert!(parsed.param_types.is_some());
    }

    #[test]
    fn stmt_execute_exposes_cursor_flag() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7_u32.to_le_bytes());
        payload.push(1); // CURSOR_TYPE_READ_ONLY
        payload.extend_from_slice(&1_u32.to_le_bytes());

        let parsed = parse_stmt_execute_params(&payload, 0, None, None, false).unwrap();

        assert_eq!(parsed.flags, 1);
        assert!(parsed.params.is_empty());
    }

    #[test]
    fn release_audit_zero_parameter_execute_rejects_trailing_bytes() {
        for attributes in [false, true] {
            let mut payload = Vec::new();
            payload.extend_from_slice(&7_u32.to_le_bytes());
            payload.push(0);
            payload.extend_from_slice(&1_u32.to_le_bytes());
            if attributes {
                payload.push(0);
            }
            assert!(parse_stmt_execute_params(&payload, 0, None, None, attributes).is_ok());
            // Connector/J's trailing new_params_bound flag for zero parameters.
            let mut flagged = payload.clone();
            flagged.push(0);
            assert!(parse_stmt_execute_params(&flagged, 0, None, None, attributes).is_ok());
            flagged.push(0);
            assert!(parse_stmt_execute_params(&flagged, 0, None, None, attributes).is_err());
            payload.extend_from_slice(b"GARBAGE");
            assert!(parse_stmt_execute_params(&payload, 0, None, None, attributes).is_err());
        }
    }

    #[test]
    fn release_audit_metadata_uses_result_charset_for_numeric_columns() {
        let source = crate::types::ColumnSource::default();
        for (collation, text, expected) in [
            ("latin1_swedish_ci", "ä", vec![0xe4]),
            ("gbk_chinese_ci", "列", vec![0xc1, 0xd0]),
            ("utf8mb4_0900_ai_ci", "ä", vec![0xc3, 0xa4]),
        ] {
            let payload = column_definition_packet_full_with_fallback(
                text,
                ColumnType::LongLong,
                &source,
                crate::charset::collation_id(collation),
            );
            let mut cursor = Cursor::new(&payload);
            for _ in 0..4 {
                cursor.read_lenenc_bytes().unwrap();
            }
            assert_eq!(cursor.read_lenenc_bytes().unwrap(), expected.as_slice());
        }
    }

    #[test]
    fn strip_query_attributes_fast_path_empty_preamble() {
        // Connector/J 8.0+ default: `parameter_count=0,
        // parameter_set_count=1` shipped as two `0x00 / 0x01` lenenc
        // bytes, then the SQL. This is the only attribute-related
        // overhead the typical JDBC user incurs.
        let mut payload = Vec::new();
        payload.push(0x00); // lenenc parameter_count = 0
        payload.push(0x01); // lenenc parameter_set_count = 1
        payload.extend_from_slice(b"SELECT 1");
        let sql = strip_query_attributes(&payload).unwrap();
        assert_eq!(sql, b"SELECT 1");
    }

    #[test]
    fn strip_query_attributes_skips_single_string_attr() {
        // One attribute (`@traceparent='ABC'`) → param_count=1,
        // bitmap=0, new_params_bind_flag=1, type=VAR_STRING+unsigned=0,
        // name="traceparent", value=lenenc("ABC"), then SQL.
        let mut payload = Vec::new();
        payload.push(0x01); // parameter_count = 1
        payload.push(0x01); // parameter_set_count = 1
        payload.push(0x00); // null_bitmap (1 byte for 1 param)
        payload.push(0x01); // new_params_bind_flag
        payload.push(MYSQL_TYPE_VAR_STRING); // type
        payload.push(0x00); // unsigned flag
                            // name = "traceparent" (lenenc string)
        let name = b"traceparent";
        payload.push(name.len() as u8);
        payload.extend_from_slice(name);
        // value = "ABC"
        let value = b"ABC";
        payload.push(value.len() as u8);
        payload.extend_from_slice(value);
        payload.extend_from_slice(b"SELECT @@version");
        let parsed = parse_query_attributes(&payload).unwrap();
        assert_eq!(parsed.sql, b"SELECT @@version");
        assert_eq!(
            parsed.attributes,
            vec![(
                "traceparent".to_string(),
                StmtParamValue::Bytes(b"ABC".to_vec())
            )]
        );
    }

    #[test]
    fn strip_query_attributes_handles_null_value() {
        // NULL-valued attribute: bitmap bit set, NO value bytes on the
        // wire. Mis-parsing this would consume part of the SQL.
        let mut payload = Vec::new();
        payload.push(0x01); // parameter_count = 1
        payload.push(0x01); // parameter_set_count = 1
        payload.push(0x01); // null_bitmap: param 0 is NULL
        payload.push(0x01); // new_params_bind_flag
        payload.push(MYSQL_TYPE_VAR_STRING);
        payload.push(0x00);
        let name = b"attr";
        payload.push(name.len() as u8);
        payload.extend_from_slice(name);
        // no value bytes!
        payload.extend_from_slice(b"SELECT 1");
        let sql = strip_query_attributes(&payload).unwrap();
        assert_eq!(sql, b"SELECT 1");
    }

    #[test]
    fn strip_query_attributes_rejects_bad_parameter_set_count() {
        // parameter_set_count > 1 has never been defined for COM_QUERY
        // — the server must error out rather than guess at a layout.
        let payload = [0x00u8, 0x02]; // count=0, set_count=2
        assert!(strip_query_attributes(&payload).is_err());
    }

    #[test]
    fn stmt_execute_with_query_attributes_skips_trailing_attr() {
        // Statement has 1 prepared parameter; client also sends 1 query
        // attribute, so `effective_param_count = 2` arrives via
        // PARAMETER_COUNT_AVAILABLE (flag bit 0x08). The trailing
        // attribute value must be CONSUMED so cached_types / future
        // EXECUTEs don't see stale bytes.
        let mut payload = Vec::new();
        payload.extend_from_slice(&42u32.to_le_bytes()); // statement_id
        payload.push(0x08); // flags: PARAMETER_COUNT_AVAILABLE
        payload.extend_from_slice(&1u32.to_le_bytes()); // iteration_count
        payload.push(0x02); // lenenc effective_param_count = 2
        payload.push(0x00); // null_bitmap for 2 params
        payload.push(0x01); // new_params_bind_flag
                            // type loop: type + unsigned + lenenc name per param
        payload.push(MYSQL_TYPE_LONG);
        payload.push(0x00);
        payload.push(0x00); // empty name (lenenc 0)
        payload.push(MYSQL_TYPE_VAR_STRING);
        payload.push(0x00);
        let attr_name = b"trace";
        payload.push(attr_name.len() as u8);
        payload.extend_from_slice(attr_name);
        // value loop: param 0 (LONG = 4 bytes), param 1 (VAR_STRING = lenenc)
        payload.extend_from_slice(&7i32.to_le_bytes());
        let attr_val = b"hello";
        payload.push(attr_val.len() as u8);
        payload.extend_from_slice(attr_val);

        let parsed = parse_stmt_execute_params(&payload, 1, None, None, true).unwrap();
        assert_eq!(parsed.params.len(), 1, "only the bound param survives");
        assert_eq!(parsed.params[0], StmtParamValue::Int(7));
    }

    #[test]
    fn stmt_execute_query_attrs_count_without_flag_bit() {
        // Regression: mysql-connector-python negotiates CLIENT_QUERY_ATTRIBUTES
        // and sends the lenenc parameter_count in COM_STMT_EXECUTE *without*
        // setting the PARAMETER_COUNT_AVAILABLE flag bit (flags=0x00). The real
        // MySQL server accepts this; mydb used to gate the count read on the
        // flag, misread the count byte as the null bitmap, decode
        // new_params_bound as 0, and reject with "without cached parameter
        // types" (error 1047) — breaking every connector prepared statement.
        // This is the exact two-param INSERT(?, STRING_TO_VECTOR(?)) packet
        // captured off the wire (TINY id=1, VAR_STRING "[1,2,3,4]").
        let payload: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // statement_id = 1
            0x00, // flags = 0 (no PARAMETER_COUNT_AVAILABLE bit)
            0x01, 0x00, 0x00, 0x00, // iteration_count = 1
            0x02, // lenenc parameter_count = 2 (present despite flags=0)
            0x00, // null_bitmap (2 params, none null)
            0x01, // new_params_bind_flag = 1
            0x01, 0x80, 0x00, // param0: type=TINY, flag, empty name (QA layout)
            0xfe, 0x00, 0x00, // param1: type=VAR_STRING, flag, empty name
            0x01, // value param0 = 1
            0x09, b'[', b'1', b',', b'2', b',', b'3', b',', b'4', b']', // value param1
        ];
        let parsed = parse_stmt_execute_params(payload, 2, None, None, true).unwrap();
        assert_eq!(parsed.params.len(), 2);
        // param0's flag byte is 0x80 (unsigned), so the TINY decodes as UInt.
        assert_eq!(parsed.params[0], StmtParamValue::UInt(1));
        assert_eq!(
            parsed.params[1],
            StmtParamValue::Bytes(b"[1,2,3,4]".to_vec())
        );
        assert!(
            parsed.param_types.is_some(),
            "types must be cached for reuse"
        );
    }

    #[test]
    fn stmt_execute_query_attrs_zero_params_without_count() {
        // Oracle libmysqlclient negotiates CLIENT_QUERY_ATTRIBUTES, but for a
        // zero-parameter statement such as sysbench's prepared BEGIN it sends
        // only the fixed COM_STMT_EXECUTE header. There is no lenenc parameter
        // count after iteration_count, even though the capability is active.
        // Treat end-of-packet as the prepared statement's known count (zero)
        // instead of trying to read past the packet and returning ER 1047.
        let payload: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // statement_id = 1
            0x00, // flags
            0x01, 0x00, 0x00, 0x00, // iteration_count = 1
        ];
        let parsed = parse_stmt_execute_params(payload, 0, None, None, true).unwrap();
        assert!(parsed.params.is_empty());
        assert!(parsed.param_types.is_none());
    }

    #[test]
    fn stmt_execute_cached_types_shorter_than_effective_count_errors_without_panic() {
        // Regression: on the cached-types path (`new_params_bound == 0`),
        // `types` holds exactly `param_count` entries, but a malformed or
        // hostile client can declare a larger `effective_param_count` via
        // the query-attributes count. The trailing-attribute branch then
        // indexed `types[idx]` with `idx >= types.len()` — an
        // attacker-controlled server-side panic. It must now be a clean
        // protocol error.
        let cached = [StmtParamType {
            ty: MYSQL_TYPE_LONG,
            unsigned: false,
        }];
        // statement_id, flags=0, iteration_count=1, effective_param_count=2,
        // null_bitmap = 0b01 (idx0 NULL so no value byte is needed; idx1
        // NON-null so the decoder must consult types[1] to skip it),
        // new_params_bound = 0 (use cache).
        let payload: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // statement_id
            0x00, // flags
            0x01, 0x00, 0x00, 0x00, // iteration_count
            0x02, // lenenc effective_param_count = 2
            0x01, // null_bitmap: idx0 null, idx1 not null
            0x00, // new_params_bound = 0 → cached types
        ];
        match parse_stmt_execute_params(payload, 1, Some(&cached), None, true) {
            Ok(_) => panic!("expected a protocol error, got Ok"),
            Err(err) => assert!(
                err.contains("more parameters than bound types"),
                "unexpected error text: {err}"
            ),
        }
    }

    #[test]
    fn auth_switch_request_packet_names_plugin_and_seed() {
        let packet = auth_switch_request_packet("mysql_native_password", b"seed-123");
        assert_eq!(packet[0], 0xfe);
        assert!(packet[1..].starts_with(b"mysql_native_password\0"));
        assert!(packet.ends_with(b"seed-123\0"));
    }

    #[test]
    fn handshake_capabilities_follow_tls_acceptor() {
        assert_eq!(server_capabilities(false) & CLIENT_SSL, 0);
        assert_ne!(server_capabilities(true) & CLIENT_SSL, 0);
        assert_ne!(server_capabilities(false) & CLIENT_MULTI_STATEMENTS, 0);
        assert_ne!(server_capabilities(false) & CLIENT_SESSION_TRACK, 0);

        let plain = initial_handshake_packet(1, false, auth_plugin_data());
        let tls = initial_handshake_packet(1, true, auth_plugin_data());

        assert_eq!(handshake_capabilities(&plain) & CLIENT_SSL, 0);
        assert_ne!(handshake_capabilities(&tls) & CLIENT_SSL, 0);
    }

    #[test]
    fn parses_standard_ssl_request() {
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SSL | CLIENT_SECURE_CONNECTION;
        let mut payload = Vec::new();
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&1024_u32.to_le_bytes());
        payload.push(DEFAULT_UTF8MB4_COLLATION as u8);
        payload.extend_from_slice(&[0_u8; 23]);

        let parsed = parse_ssl_request(&payload).unwrap();

        assert_eq!(parsed.capability_flags, flags);
        assert_eq!(parsed.max_packet_size, 1024);
        assert_eq!(parsed.character_set, DEFAULT_UTF8MB4_COLLATION as u8);
    }

    #[test]
    fn rejects_invalid_ssl_request() {
        let mut missing_ssl = Vec::new();
        missing_ssl.extend_from_slice(&CLIENT_PROTOCOL_41.to_le_bytes());
        missing_ssl.extend_from_slice(&1024_u32.to_le_bytes());
        missing_ssl.push(DEFAULT_UTF8MB4_COLLATION as u8);
        missing_ssl.extend_from_slice(&[0_u8; 23]);

        assert!(parse_ssl_request(&missing_ssl).is_err());
        assert!(parse_ssl_request(&missing_ssl[..31]).is_err());
    }

    #[test]
    fn select_one_result_set_has_column_count_column_row_and_eofs() {
        let packets = result_set_packets(
            &ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "1".to_string(),
                    column_type: ColumnType::Long,
                }],
                rows: vec![vec![Value::Int(1)]],
            },
            0,
        );

        assert_eq!(packets.len(), 5);
        assert_eq!(packets[0], vec![1]);
        assert_eq!(packets[2][0], 0xfe);
        assert_eq!(packets[3], vec![1, b'1']);
        assert_eq!(packets[4][0], 0xfe);
    }

    #[test]
    fn binary_decimal_compact_respects_float_metadata() {
        let double_col = [Column {
            source: Default::default(),
            name: "sum_d".to_string(),
            column_type: ColumnType::Double,
        }];
        let packet = binary_row_packet(&double_col, &[Value::DecimalCompact(123, 2)]);
        assert_eq!(packet.len(), 10);
        assert_eq!(&packet[..2], &[0x00, 0x00]);
        let actual = f64::from_le_bytes(packet[2..10].try_into().unwrap());
        assert_eq!(actual, 1.23);

        let decimal_col = [Column {
            source: Default::default(),
            name: "sum_d".to_string(),
            column_type: ColumnType::Decimal {
                precision: crate::types::MAX_DECIMAL_PRECISION,
                scale: 2,
            },
        }];
        let packet = binary_row_packet(&decimal_col, &[Value::DecimalCompact(123, 2)]);
        assert_eq!(&packet, b"\x00\x00\x041.23");
    }

    /// AP Q10 regression: the derived CTE conservatively advertised `id` as
    /// VARCHAR while the executor retained `Value::Int`. The old encoder wrote
    /// eight little-endian integer bytes; Connector/J treated the low byte
    /// (0xdc for 397788) as a lenenc-string length and read past the packet.
    #[test]
    fn binary_row_encodes_integer_runtime_value_as_advertised_varchar() {
        let columns = [
            Column {
                source: Default::default(),
                name: "id".to_string(),
                column_type: ColumnType::VarString,
            },
            Column {
                source: Default::default(),
                name: "name".to_string(),
                column_type: ColumnType::VarString,
            },
            Column {
                source: Default::default(),
                name: "revenue".to_string(),
                column_type: ColumnType::Double,
            },
        ];
        let packet = binary_row_packet(
            &columns,
            &[
                Value::Int(397_788),
                Value::String("Qian Dan".to_string()),
                Value::Double(89.97),
            ],
        );

        assert_eq!(&packet[..3], b"\x00\x00\x06");
        assert_eq!(&packet[3..9], b"397788");
        assert_eq!(packet[9], 8);
        assert_eq!(&packet[10..18], b"Qian Dan");
        assert_eq!(
            f64::from_le_bytes(packet[18..26].try_into().unwrap()),
            89.97
        );
    }

    #[test]
    fn binary_row_coerces_signed_and_unsigned_integers_from_metadata() {
        let textual = [Column {
            source: Default::default(),
            name: "v".to_string(),
            column_type: ColumnType::VarString,
        }];
        assert_eq!(
            binary_row_packet(&textual, &[Value::Int(-42)]),
            b"\x00\x00\x03-42"
        );
        assert_eq!(
            binary_row_packet(&textual, &[Value::UInt(42)]),
            b"\x00\x00\x0242"
        );

        let double = [Column {
            source: Default::default(),
            name: "v".to_string(),
            column_type: ColumnType::Double,
        }];
        let packet = binary_row_packet(&double, &[Value::Int(42)]);
        assert_eq!(f64::from_le_bytes(packet[2..10].try_into().unwrap()), 42.0);
    }

    #[test]
    fn binary_row_encodes_integral_decimals_with_integer_metadata() {
        let column = |column_type| Column {
            source: Default::default(),
            name: "v".to_string(),
            column_type,
        };
        let long_long = [column(ColumnType::LongLong), column(ColumnType::LongLong)];
        let packet = binary_row_packet(&long_long, &[Value::DecimalCompact(1, 0), Value::Int(200)]);
        assert_eq!(&packet[2..10], &1i64.to_le_bytes());
        assert_eq!(&packet[10..18], &200i64.to_le_bytes());
        let packet = binary_row_packet(&long_long[..1], &[Value::Decimal("-7.000".into())]);
        assert_eq!(&packet[2..10], &(-7i64).to_le_bytes());
        let packet = binary_row_packet(
            &long_long[..1],
            &[Value::Decimal("18446744073709551615".into())],
        );
        assert_eq!(&packet[2..10], &u64::MAX.to_le_bytes());
        let long = [column(ColumnType::Long)];
        let packet = binary_row_packet(&long, &[Value::DecimalCompact(3000, 3)]);
        assert_eq!(&packet[2..6], &3i32.to_le_bytes());
        // A fractional decimal is never silently truncated to an integer.
        let packet = binary_row_packet(&long, &[Value::DecimalCompact(25, 1)]);
        assert_eq!(&packet[2..], b"\x032.5");
    }

    #[test]
    fn ok_packet_has_protocol_41_status_and_warnings() {
        let packet = ok_packet();

        assert_eq!(packet, vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn ok_packet_encodes_affected_rows() {
        let packet = ok_packet_with_affected_rows(300);

        assert_eq!(
            packet,
            vec![0x00, 0xfc, 0x2c, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn ok_packet_encodes_last_insert_id() {
        let packet = ok_packet_with_affected_rows_insert_id_status_warnings(
            2,
            300,
            SERVER_STATUS_AUTOCOMMIT,
            0,
        );

        assert_eq!(
            packet,
            vec![0x00, 0x02, 0xfc, 0x2c, 0x01, 0x02, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn session_track_schema_entry_layout() {
        // SESSION_TRACK_SCHEMA: type byte (0x01), then a length-encoded
        // data block. The data block itself is a lenenc-string of the
        // schema name. For "test" that's lenenc(4) + "test" = 5 bytes,
        // so the outer lenenc data-length is 5.
        let mut buf = Vec::new();
        encode_session_track_schema(&mut buf, "test");
        assert_eq!(buf, vec![0x01, 0x05, 0x04, b't', b'e', b's', b't']);
    }

    #[test]
    fn session_track_state_change_entry_layout() {
        let mut buf = Vec::new();
        encode_session_track_state_change(&mut buf);
        // Type byte 0x02, lenenc(2), lenenc("1") = [0x01, '1'].
        assert_eq!(buf, vec![0x02, 0x02, 0x01, b'1']);
    }

    #[test]
    fn session_track_system_variables_entry_layout() {
        // SESSION_TRACK_SYSTEM_VARIABLES: type byte (0x00), outer lenenc
        // wrapping lenenc-string(name) + lenenc-string(value). For
        // ("autocommit", "OFF"): inner = [10]"autocommit"[3]"OFF" = 15 B.
        let mut buf = Vec::new();
        encode_session_track_system_variables(&mut buf, "autocommit", "OFF");
        let mut expected = vec![0x00, 0x0f, 0x0a];
        expected.extend_from_slice(b"autocommit");
        expected.push(0x03);
        expected.extend_from_slice(b"OFF");
        assert_eq!(buf, expected);
    }

    #[test]
    fn ok_packet_with_full_payload_omits_state_info_when_status_bit_unset() {
        // Even if the caller passes a non-empty state-info block,
        // unless SERVER_SESSION_STATE_CHANGED is in the status word
        // the bytes must NOT make it onto the wire — the client won't
        // parse past `info` and would mis-frame the next packet.
        let mut state_info = Vec::new();
        encode_session_track_schema(&mut state_info, "test");
        let packet =
            ok_packet_with_full_payload(0, 0, SERVER_STATUS_AUTOCOMMIT, 0, "", &state_info);
        // Header + affected (0) + insert_id (0) + status (LE u16) +
        // warnings (LE u16). 7 bytes, no info, no state-info.
        assert_eq!(packet, vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn ok_packet_with_full_payload_emits_state_info_when_bit_set() {
        let mut state_info = Vec::new();
        encode_session_track_schema(&mut state_info, "test");
        encode_session_track_state_change(&mut state_info);
        let status = SERVER_STATUS_AUTOCOMMIT | SERVER_SESSION_STATE_CHANGED;
        let packet = ok_packet_with_full_payload(0, 0, status, 0, "", &state_info);
        // Expected layout:
        //   00          header
        //   00          affected_rows lenenc
        //   00          insert_id lenenc
        //   42 00       status (LE u16) = 0x4002 = 0x4000 | 0x0002
        //   00 00       warnings (LE u16)
        //   00          info length (lenenc 0, empty info)
        //   0b          state_info length (lenenc 11 bytes)
        //   01 05 04 't' 'e' 's' 't'       SCHEMA entry
        //   02 02 01 '1'                   STATE_CHANGE entry
        assert_eq!(
            packet,
            vec![
                0x00, 0x00, 0x00, 0x02, 0x40, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x05, 0x04, b't', b'e',
                b's', b't', 0x02, 0x02, 0x01, b'1',
            ]
        );
    }

    #[test]
    fn ok_packet_with_full_payload_keeps_info_before_state_info() {
        // The MySQL wire spec requires `info` (lenenc-string) BEFORE the
        // session-state-info block. A client reading linearly must see
        // info first, so even when info is empty and we want to emit
        // state-info, the empty info length byte (0x00) appears first.
        let mut state_info = Vec::new();
        encode_session_track_state_change(&mut state_info);
        let status = SERVER_STATUS_AUTOCOMMIT | SERVER_SESSION_STATE_CHANGED;
        let packet = ok_packet_with_full_payload(
            5,
            0,
            status,
            0,
            "Records: 5 Duplicates: 0 Warnings: 0",
            &state_info,
        );
        // Confirm info length precedes the state-info block.
        // Layout offsets (lenenc bytes for affected_rows/insert_id < 251):
        //   [0]  OK header                        = 0x00
        //   [1]  affected_rows lenenc-int (5)     = 0x05
        //   [2]  insert_id lenenc-int (0)         = 0x00
        //   [3]  status low byte (LE u16 of 0x4002) = 0x02
        //   [4]  status high byte                 = 0x40
        //   [5]  warnings low byte                = 0x00
        //   [6]  warnings high byte               = 0x00
        //   [7]  info lenenc-length               = 0x24 (= 36)
        //   [8..44] info text
        //   [44] state_info lenenc-length
        //   [45..] state_info bytes
        assert_eq!(packet[0], 0x00);
        assert_eq!(packet[1], 0x05);
        assert_eq!(packet[2], 0x00);
        assert_eq!(packet[3], 0x02);
        assert_eq!(packet[4], 0x40);
        assert_eq!(packet[5], 0x00);
        assert_eq!(packet[6], 0x00);
        assert_eq!(packet[7], 0x24);
        let info_end = 8 + 36;
        assert_eq!(packet[info_end], state_info.len() as u8);
        assert_eq!(&packet[info_end + 1..], state_info.as_slice());
    }

    #[test]
    fn result_set_uses_ok_terminator_when_client_deprecates_eof() {
        let packets = result_set_packets(
            &ResultSet {
                columns: vec![Column {
                    source: Default::default(),
                    name: "1".to_string(),
                    column_type: ColumnType::Long,
                }],
                rows: vec![vec![Value::Int(1)]],
            },
            CLIENT_DEPRECATE_EOF,
        );

        assert_eq!(packets.len(), 4);
        assert_eq!(packets[0], vec![1]);
        assert_eq!(packets[2], vec![1, b'1']);
        assert_eq!(packets[3], vec![0xfe, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn binary_row_uses_column_integer_widths_before_temporal_values() {
        let packet = binary_row_packet(
            &[
                Column {
                    source: Default::default(),
                    name: "id".to_string(),
                    column_type: ColumnType::Long,
                },
                Column {
                    source: Default::default(),
                    name: "created_at".to_string(),
                    column_type: ColumnType::Timestamp,
                },
            ],
            &[
                Value::Int(3001),
                Value::String("2026-05-04 14:14:47".to_string()),
            ],
        );

        assert_eq!(packet[0], 0x00);
        assert_eq!(packet[1], 0x00);
        assert_eq!(&packet[2..6], &3001_u32.to_le_bytes());
        assert_eq!(packet[6], 7);
        assert_eq!(&packet[7..9], &2026_u16.to_le_bytes());
        assert_eq!(&packet[9..14], &[5, 4, 14, 14, 47]);
    }

    #[test]
    fn binary_row_floating_width_follows_advertised_type() {
        let column = |name: &str, column_type| Column {
            source: Default::default(),
            name: name.to_string(),
            column_type,
        };
        let packet = binary_row_packet(
            &[
                column("f", ColumnType::Float),
                column("d", ColumnType::Double),
                column(
                    "w",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                ),
            ],
            &[Value::Double(1.5), Value::Float(-2.5), Value::Double(0.25)],
        );
        assert_eq!(&packet[0..2], &[0x00, 0x00]);
        assert_eq!(&packet[2..6], &1.5_f32.to_le_bytes());
        assert_eq!(&packet[6..14], &(-2.5_f64).to_le_bytes());
        assert_eq!(&packet[14..], &[4, b'0', b'.', b'2', b'5']);
    }

    #[test]
    fn binary_temporal_param_keeps_microseconds() {
        // DATETIME len==11: 2026-05-04 14:14:47.123456
        let mut payload = vec![11u8];
        payload.extend_from_slice(&2026_u16.to_le_bytes());
        payload.extend_from_slice(&[5, 4, 14, 14, 47]);
        payload.extend_from_slice(&123456_u32.to_le_bytes());
        let mut cursor = Cursor::new(&payload);
        assert_eq!(
            parse_binary_temporal(&mut cursor).unwrap(),
            "2026-05-04 14:14:47.123456"
        );

        // len==7 stays second-resolution.
        let mut payload = vec![7u8];
        payload.extend_from_slice(&2026_u16.to_le_bytes());
        payload.extend_from_slice(&[5, 4, 14, 14, 47]);
        let mut cursor = Cursor::new(&payload);
        assert_eq!(
            parse_binary_temporal(&mut cursor).unwrap(),
            "2026-05-04 14:14:47"
        );
    }

    #[test]
    fn fuzz_round1_binary_time_rejects_hour_overflow() {
        for (days, hours) in [(u32::MAX, 0), (u32::MAX / 24, 16)] {
            let mut payload = vec![8, 0];
            payload.extend_from_slice(&days.to_le_bytes());
            payload.extend_from_slice(&[hours, 0, 0]);
            assert!(parse_binary_time(&mut Cursor::new(&payload)).is_err());
        }
    }

    #[test]
    fn fuzz_round1_binary_time_rejects_invalid_lengths() {
        for len in [1, 7, 9, 10, 11, 13, 255] {
            let mut payload = vec![0; len as usize + 1];
            payload[0] = len;
            assert!(
                parse_binary_time(&mut Cursor::new(&payload)).is_err(),
                "length {len}"
            );
        }
    }

    #[test]
    fn fuzz_round1_binary_time_execute_and_boundaries() {
        for (days, hours, expected) in [
            (0, 0, "00:59:59"),
            (34, 22, "838:59:59"),
            (u32::MAX / 24, 15, "4294967295:59:59"),
        ] {
            let mut value = vec![8, 0];
            value.extend_from_slice(&days.to_le_bytes());
            value.extend_from_slice(&[hours, 59, 59]);
            assert_eq!(
                parse_binary_time(&mut Cursor::new(&value)).unwrap(),
                expected
            );
            for end in 0..value.len() {
                assert!(parse_binary_time(&mut Cursor::new(&value[..end])).is_err());
            }
        }
        assert_eq!(
            parse_binary_time(&mut Cursor::new(&[0])).unwrap(),
            "00:00:00"
        );
        let mut execute = 7_u32.to_le_bytes().to_vec();
        execute.push(0);
        execute.extend_from_slice(&1_u32.to_le_bytes());
        execute.extend_from_slice(&[0, 1, MYSQL_TYPE_TIME, 0, 8, 0]);
        execute.extend_from_slice(&u32::MAX.to_le_bytes());
        execute.extend_from_slice(&[0, 0, 0]);
        let error = parse_stmt_execute_params(&execute, 1, None, None, false)
            .err()
            .expect("invalid TIME must fail");
        assert!(
            error.contains("time parameter hours out of range"),
            "{error}"
        );
    }

    #[test]
    fn binary_time_param_keeps_microseconds() {
        // TIME len==12, negative, 1 day + 02:03:04.000500
        let mut payload = vec![12u8, 1];
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.extend_from_slice(&[2, 3, 4]);
        payload.extend_from_slice(&500_u32.to_le_bytes());
        let mut cursor = Cursor::new(&payload);
        assert_eq!(parse_binary_time(&mut cursor).unwrap(), "-26:03:04.000500");

        // len==8 stays second-resolution.
        let mut payload = vec![8u8, 0];
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&[2, 3, 4]);
        let mut cursor = Cursor::new(&payload);
        assert_eq!(parse_binary_time(&mut cursor).unwrap(), "02:03:04");
    }

    #[test]
    fn binary_row_string_temporal_keeps_microseconds() {
        let packet = binary_row_packet(
            &[Column {
                source: Default::default(),
                name: "created_at".to_string(),
                column_type: ColumnType::DateTime,
            }],
            &[Value::String("2026-05-04 14:14:47.123456".to_string())],
        );
        assert_eq!(packet[1], 0x00);
        assert_eq!(packet[2], 11);
        assert_eq!(&packet[3..5], &2026_u16.to_le_bytes());
        assert_eq!(&packet[5..10], &[5, 4, 14, 14, 47]);
        assert_eq!(&packet[10..14], &123456_u32.to_le_bytes());
    }

    #[test]
    fn binary_row_string_time_keeps_microseconds() {
        let packet = binary_row_packet(
            &[Column {
                source: Default::default(),
                name: "t".to_string(),
                column_type: ColumnType::Time,
            }],
            &[Value::String("26:03:04.5".to_string())],
        );
        assert_eq!(packet[2], 12);
        assert_eq!(packet[3], 0); // positive
        assert_eq!(&packet[4..8], &1_u32.to_le_bytes()); // 1 day
        assert_eq!(&packet[8..11], &[2, 3, 4]);
        assert_eq!(&packet[11..15], &500000_u32.to_le_bytes());
    }

    #[test]
    fn handshake_packet_into_matches_allocating_helper() {
        let nonce = auth_plugin_data();
        let expected = initial_handshake_packet(99, false, nonce);
        let mut out = Vec::with_capacity(128);
        initial_handshake_packet_into(99, false, nonce, &mut out);
        assert_eq!(out, expected);

        initial_handshake_packet_into(100, true, nonce, &mut out);
        assert_eq!(out, initial_handshake_packet(100, true, nonce));
    }

    fn handshake_capabilities(payload: &[u8]) -> u32 {
        let mut cursor = Cursor::new(payload);
        cursor.read_u8().unwrap();
        cursor.read_null_string().unwrap();
        cursor.read_u32_le().unwrap();
        cursor.read_bytes(8).unwrap();
        cursor.read_u8().unwrap();
        let lower = cursor.read_bytes(2).unwrap();
        cursor.read_u8().unwrap();
        cursor.read_bytes(2).unwrap();
        let upper = cursor.read_bytes(2).unwrap();
        u16::from_le_bytes([lower[0], lower[1]]) as u32
            | ((u16::from_le_bytes([upper[0], upper[1]]) as u32) << 16)
    }
}

#[cfg(test)]
#[test]
fn stmt_execute_small_and_large_cached_bindings_preserve_nulls_and_unsigned_values() {
    for count in [1usize, 8, 9, 32] {
        let bitmap_len = count.div_ceil(8);
        let mut fresh = vec![1, 0, 0, 0, 0, 1, 0, 0, 0];
        let mut bitmap = vec![0u8; bitmap_len];
        for i in 0..count {
            if i % 5 == 0 {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        fresh.extend_from_slice(&bitmap);
        fresh.push(1);
        for i in 0..count {
            fresh.extend_from_slice(&[MYSQL_TYPE_LONGLONG, u8::from(i % 2 != 0)]);
        }
        let values_start = fresh.len();
        let mut expected = Vec::new();
        for i in 0..count {
            if i % 5 == 0 {
                expected.push(StmtParamValue::Null);
            } else if i % 2 == 0 {
                let value = -(i as i64);
                fresh.extend_from_slice(&value.to_le_bytes());
                expected.push(StmtParamValue::Int(value));
            } else {
                let value = (1u64 << 63) + i as u64;
                fresh.extend_from_slice(&value.to_le_bytes());
                expected.push(StmtParamValue::UInt(value));
            }
        }
        let parsed = parse_stmt_execute_params(&fresh, count, None, None, false).unwrap();
        assert_eq!(parsed.params.as_slice(), expected.as_slice());
        let mut cached = fresh[..9 + bitmap_len].to_vec();
        cached.push(0);
        cached.extend_from_slice(&fresh[values_start..]);
        let repeated =
            parse_stmt_execute_params(&cached, count, parsed.param_types.as_deref(), None, false)
                .unwrap();
        assert_eq!(repeated.params, parsed.params);
        assert!(repeated.param_types.is_none());
    }
}

#[cfg(test)]
#[path = "protocol/temporal_encoding_tests.rs"]
mod temporal_encoding_tests;

#[cfg(test)]
#[path = "protocol/string_encoding_tests.rs"]
mod string_encoding_tests;
