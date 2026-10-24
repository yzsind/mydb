use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

pub use crate::error::QueryError;

/// Inline byte buffer for `Value::SmallString`. Holds up to 31 bytes inline
/// (no heap allocation) which covers CHAR(1)..CHAR(31) / short VARCHAR
/// payloads — the bulk of decode_var traffic on TPC-H and BenchmarkSQL
/// fact tables. Longer payloads spill to `Value::String` instead.
///
/// The 31-byte cap keeps common short text such as `c_name` (18 chars in TPC-H
/// `Customer#XXXXXX` form), `c_phone` (15 chars), `r_name` / `n_name`
/// (≤ 15 chars), 24-byte codes/descriptions, and many short addresses inline.
/// SmallVec's representation grows to 40 bytes at this capacity, but `Value`
/// remains 48 bytes on 64-bit targets because another variant already fixes
/// that enum size. Thus 24..31-byte strings avoid one heap allocation without
/// enlarging the ubiquitous value cell.
pub type SmallStringBuf = SmallVec<[u8; 31]>;

/// Allocation-backed coefficient for the cold DECIMAL(39..65) lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecimalWide {
    pub magnitude: num_bigint::BigInt,
    pub scale: u8,
}

/// Inline cap for `SmallStringBuf`. Decoder picks the SmallString lane
/// when payload length ≤ this; longer strings go to `Value::String`.
pub const SMALL_STRING_INLINE_CAP: usize = 31;

/// Calendar and UTC bounds shared by temporal assignment and functions.
pub const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;
pub const MAX_DATETIME_MICROS: i64 = MAX_TIMESTAMP_SECONDS * 1_000_000 + 999_999;
pub const MIN_DATETIME_MICROS: i64 = -62_167_219_200_000_000;
pub const MIN_TIMESTAMP_MICROS: i64 = 1_000_000;

#[inline]
pub(crate) fn datetime_micros_in_calendar_range(micros: i64) -> bool {
    (MIN_DATETIME_MICROS..=MAX_DATETIME_MICROS).contains(&micros)
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    /// Catalog source metadata for the result-set column-definition packet.
    /// Default (empty) when the column has no single-table origin
    /// (expressions, SHOW/metadata results) — the protocol layer then falls
    /// back to type-derived metadata. Filled for plain column references so
    /// ORM/JDBC `ResultSetMetaData` reflects the real table and constraints.
    pub source: ColumnSource,
}

/// Per-column catalog origin for protocol column definitions. All fields are
/// empty/zero by default; a `Default` value means "no schema source, use
/// type-derived metadata".
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ColumnSource {
    /// An untyped parameter projection obtains its domain at execution time.
    /// This provenance survives derived/CTE passthrough metadata.
    pub dynamic_parameter: bool,
    /// Catalog wire representation when the execution type intentionally
    /// groups several SQL domains (e.g. BINARY/VARBINARY as byte vectors).
    pub wire_type: Option<u8>,
    /// Schema/database name for the result-set column (`schema`).
    pub schema: String,
    /// Table name/alias as referenced in the query (the `table` field).
    pub table: String,
    /// Real underlying table name (`org_table`).
    pub org_table: String,
    /// Real underlying column name (`org_name`); differs from `name` on alias.
    pub org_name: String,
    /// MySQL column-definition flags (NOT_NULL / PRI_KEY / UNSIGNED /
    /// AUTO_INCREMENT / ...). Most flags are catalog-derived, but semantic
    /// result flags such as UNSIGNED are also populated for expressions with
    /// no catalog origin. Zero means no additional flags.
    pub flags: u16,
    /// Real declared length in bytes, overriding the type-derived default
    /// (e.g. VARCHAR(50)). `None` keeps the type default.
    pub length: Option<u32>,
    /// Declared numeric precision for legacy FLOAT(M,D) / DOUBLE(M,D).
    /// Kept separate from wire `length`: set-operation and CTAS type merging
    /// need the decimal domain, while clients need the protocol display size.
    pub numeric_precision: Option<u8>,
    /// Catalog-declared decimals override for FLOAT(M,D) / DOUBLE(M,D)
    /// / TIME(N) / DATETIME(N) / TIMESTAMP(N). `None` falls back to the
    /// type-derived default (NOT_FIXED_DEC for FLOAT/DOUBLE, 0 for
    /// temporal types). DECIMAL goes through `ColumnType::Decimal.scale`
    /// directly and ignores this override.
    pub decimals: Option<u8>,
    /// Catalog-declared collation id for the column-definition packet:
    /// the column's real collation for text types, 63 (binary) for
    /// BINARY/VARBINARY/BLOB. `None` falls back to the type-derived
    /// default (utf8mb4 for text, binary for BLOB/GEOMETRY wire types).
    pub collation_id: Option<u16>,
    /// Expression derivation survives set-operation and derived-table metadata.
    pub collation_coercibility: Option<u8>,
}

impl ColumnSource {
    /// MIN/MAX preserve the value domain and origin, but the aggregate may
    /// be NULL and does not inherit the input column's key/default constraints.
    pub(crate) fn nullable_aggregate(mut self) -> Self {
        use crate::mysql::protocol::{
            AUTO_INCREMENT_FLAG, MULTIPLE_KEY_FLAG, NOT_NULL_FLAG, NO_DEFAULT_VALUE_FLAG,
            PRI_KEY_FLAG, UNIQUE_KEY_FLAG,
        };
        self.flags &= !(NOT_NULL_FLAG
            | PRI_KEY_FLAG
            | UNIQUE_KEY_FLAG
            | MULTIPLE_KEY_FLAG
            | AUTO_INCREMENT_FLAG
            | NO_DEFAULT_VALUE_FLAG);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ColumnType {
    Tiny,
    Short,
    /// `MEDIUMINT` — distinct wire type so the protocol layer emits
    /// `MYSQL_TYPE_INT24 (0x09)` rather than borrowing `MYSQL_TYPE_LONG`.
    /// JDBC/ORM clients read this as `INTEGER` with the correct
    /// 8-/24-bit display width.
    MediumInt,
    Long,
    LongLong,
    Float,
    Double,
    /// `DECIMAL(P, S)` — precision (total digits, max 38 in MySQL) and
    /// scale (digits after the decimal point). Carrying these on the
    /// type lets the vectorized executor pick a `Decimal{32,64,128}`
    /// lane at compile time instead of inferring it from runtime
    /// magnitudes per batch. `MAX_DECIMAL_PRECISION` (38, 0) is the
    /// conservative default for callers that don't have catalog
    /// metadata.
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Time,
    DateTime,
    Timestamp,
    Blob,
    /// `YEAR` — distinct wire type so the protocol emits
    /// `MYSQL_TYPE_YEAR (0x0d)` rather than borrowing `MYSQL_TYPE_TINY`.
    /// MySQL clients use this byte to distinguish a 2-byte year from
    /// a 1-byte TINYINT.
    Year,
    Json,
    /// `GEOMETRY` and its subtypes (POINT/LINESTRING/POLYGON/…). All map to
    /// the MySQL wire type `MYSQL_TYPE_GEOMETRY (0xff)`; the declared subtype
    /// is carried separately in the catalog (`DataTypeKind::Geometry`). The
    /// value form is [`Value::Geometry`].
    Geometry,
    /// `VECTOR(n)` — a fixed-width array of 32-bit floats for similarity
    /// search. Maps to the MySQL wire type `MYSQL_TYPE_VECTOR (0xf2)`; the
    /// declared dimension is carried in the catalog (`DataTypeKind::Vector`).
    /// The value form is [`Value::Vector`].
    Vector,
    /// `BIT(M)` — a fixed-width bit field of `M` bits (1..=64). Maps to the
    /// MySQL wire type `MYSQL_TYPE_BIT (0x10)`; the declared bit count is
    /// carried here so the protocol layer can emit the value as a
    /// `ceil(M/8)`-byte big-endian binary string (text and binary protocol
    /// alike) rather than a decimal integer, and advertise `UNSIGNED_FLAG` +
    /// binary charset. The value form is [`Value::UInt`] (BIT is numeric in
    /// expression context; `bitcol + 0` yields the integer).
    Bit {
        bits: u8,
    },
    #[default]
    VarString,
}

/// Maximum precision MySQL allows for `DECIMAL(P, S)`.
/// MySQL's declared DECIMAL precision limit. Execution keeps i64/i128 lanes
/// for values up to 38 digits and uses the allocation-backed wide lane only
/// beyond that boundary.
pub const MAX_DECIMAL_PRECISION: u8 = 65;

impl ColumnType {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// ASCII string form of a decimal value (legacy / fallback shape).
    /// Wire / SQL literals arrive here; the codec round-trips DECIMAL
    /// columns via `DecimalCompact` whenever the value fits, which is
    /// the case for every TPC-C / TPC-H DECIMAL column.
    Decimal(String),
    Bytes(Vec<u8>),
    String(String),
    /// Inline-byte short string ≤ `SMALL_STRING_INLINE_CAP` bytes —
    /// semantically identical to `Value::String` (UTF-8 invariant) but
    /// avoids the `String::from_utf8(payload.to_vec())` allocation per row
    /// on CHAR(N) / short VARCHAR decode paths. Constructed only by the
    /// row codec on read; equality, hashing, comparison, and protocol
    /// serialisation all treat `SmallString(b)` and `String(s)` with the
    /// same bytes as equal.
    SmallString(SmallStringBuf),
    /// Binary DECIMAL: `(magnitude, scale)` where `magnitude` is the
    /// signed integer value × 10^scale (i.e. the decimal point shifted
    /// right by `scale` digits). Used when the value fits in `i64`,
    /// which covers DECIMAL(P, S) up to about 18 total digits — every
    /// DECIMAL column TPC-C and TPC-H actually use. Eliminates the
    /// `String` ↔ binary round-trip on heap read/write and comparisons.
    DecimalCompact(i64, u8),
    /// Inline fixed-point coefficient for DECIMAL values up to 38 digits.
    Decimal128(i128, u8),
    /// Allocation-backed fixed-point coefficient for DECIMAL(39..65).
    DecimalWide(Box<DecimalWide>),
    /// `DATE` as days since 1970-01-01. `i32::MIN` is the sentinel for
    /// MySQL's zero date `0000-00-00`.
    Date(i32),
    /// `TIME` as signed microseconds; MySQL allows ±838:59:59 which
    /// fits comfortably in i64.
    Time(i64),
    /// `DATETIME` as a naive wall-clock value encoded as microseconds since
    /// 1970-01-01 (as-if UTC). `i64::MIN` is the MySQL zero-datetime sentinel.
    DateTime(i64),
    /// `TIMESTAMP` as an absolute UTC instant in microseconds since the Unix
    /// epoch. Keeping a distinct value lane is essential: DATETIME/string
    /// operands are interpreted in the session time zone, while TIMESTAMP is
    /// already normalized and must not be interpreted a second time.
    /// `i64::MIN` is the shared MySQL zero-datetime sentinel.
    Timestamp(i64),
    /// `YEAR` stored as `year - 1900`. `0` is the MySQL zero year
    /// (`0000`), valid range thereafter maps `1901..=2155` → `1..=255`.
    Year(u8),
    /// Binary JSON document. Holds the compact, self-describing `jsonb`
    /// byte string defined in `engine::json` — *not* text. Reading a JSON
    /// column off the heap therefore costs zero parsing; the bytes are
    /// already navigable. Serialised to MySQL canonical text only at the
    /// protocol boundary (`json::to_text`) and compared with MySQL's JSON
    /// type-precedence ordering (`json::compare`).
    Json(Vec<u8>),
    /// A spatial value in MySQL's internal `SRID(4, little-endian) + WKB`
    /// byte form (see `engine::geo`). Like JSON, this stays binary on disk and
    /// over the wire — clients receive the raw geometry bytes and call
    /// `ST_AsText`/`ST_AsWKB` to render them. Parsed into a geometry tree only
    /// when a spatial function computes over it.
    Geometry(Vec<u8>),
    /// A `VECTOR(n)` value as a bare little-endian `f32` array (see
    /// `engine::vector`). Like JSON/GEOMETRY it stays binary on disk and over
    /// the wire, rendered to `[a,b,c]` text only at the protocol boundary
    /// (`vector::to_text`). It has no natural total order; similarity is
    /// computed by `DISTANCE()` and the HNSW index, and `ORDER BY` on the raw
    /// column falls back to internal-byte order (`vector::compare`).
    Vector(Vec<u8>),
}

/// Sentinel value of `Value::Date` for the MySQL zero date `0000-00-00`.
pub const ZERO_DATE: i32 = i32::MIN;
/// Sentinel value of `Value::DateTime` for the MySQL zero datetime
/// `0000-00-00 00:00:00`.
pub const ZERO_DATETIME: i64 = i64::MIN;

// Reserved negative ranges used for MySQL's component-valid but
// calendar-invalid DATE/DATETIME values (for example 2009-02-31 under
// ALLOW_INVALID_DATES, or a zero month/day when NO_ZERO_IN_DATE is absent).
// Real DATE day numbers and real DATETIME microseconds are many orders of
// magnitude above these ranges, so the tags cannot collide. Keeping the
// representation inline preserves Value's hot-path size and lets the heap
// codec continue using its fixed 3/8-byte lanes.
// DATE is persisted in a signed 3-byte lane. -8_388_608 is already the
// on-disk zero sentinel; the following 5.12M values fit the lane and remain
// below the smallest real date day number (-719_528).
const TAGGED_DATE_BASE: i32 = -8_388_607;
const TAGGED_DATE_PAYLOAD_MAX: i32 = (9999 << 9) | (12 << 5) | 31;
const TAGGED_DATETIME_BASE: i64 = i64::MIN + 1;
const TAGGED_DATETIME_PAYLOAD_MAX: i64 = (1i64 << 60) - 1;

pub(crate) fn encode_tagged_date(year: u32, month: u32, day: u32) -> Option<i32> {
    if year > 9999 || month > 12 || day > 31 || (year == 0 && month == 0 && day == 0) {
        return None;
    }
    let payload = ((year as i32) << 9) | ((month as i32) << 5) | day as i32;
    Some(TAGGED_DATE_BASE + payload)
}

pub(crate) fn decode_tagged_date(value: i32) -> Option<(u32, u32, u32)> {
    if !(TAGGED_DATE_BASE..=TAGGED_DATE_BASE + TAGGED_DATE_PAYLOAD_MAX).contains(&value) {
        return None;
    }
    let payload = value - TAGGED_DATE_BASE;
    Some((
        ((payload >> 9) & 0x3fff) as u32,
        ((payload >> 5) & 0x0f) as u32,
        (payload & 0x1f) as u32,
    ))
}

pub(crate) fn encode_tagged_datetime(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    micros: u32,
) -> Option<i64> {
    if year > 9999
        || month > 12
        || day > 31
        || hour > 23
        || minute > 59
        || second > 59
        || micros > 999_999
        || (year == 0
            && month == 0
            && day == 0
            && hour == 0
            && minute == 0
            && second == 0
            && micros == 0)
    {
        return None;
    }
    let payload =
        (((((((year as i64) << 4 | month as i64) << 5 | day as i64) << 5 | hour as i64) << 6
            | minute as i64)
            << 6
            | second as i64)
            << 20)
            | micros as i64;
    Some(TAGGED_DATETIME_BASE + payload)
}

pub(crate) fn decode_tagged_datetime(value: i64) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    if !(TAGGED_DATETIME_BASE..=TAGGED_DATETIME_BASE + TAGGED_DATETIME_PAYLOAD_MAX).contains(&value)
    {
        return None;
    }
    let mut payload = value - TAGGED_DATETIME_BASE;
    let micros = (payload & ((1 << 20) - 1)) as u32;
    payload >>= 20;
    let second = (payload & 0x3f) as u32;
    payload >>= 6;
    let minute = (payload & 0x3f) as u32;
    payload >>= 6;
    let hour = (payload & 0x1f) as u32;
    payload >>= 5;
    let day = (payload & 0x1f) as u32;
    payload >>= 5;
    let month = (payload & 0x0f) as u32;
    payload >>= 4;
    Some((payload as u32, month, day, hour, minute, second, micros))
}

pub(crate) fn date_components(value: i32) -> (u32, u32, u32) {
    if value == ZERO_DATE {
        return (0, 0, 0);
    }
    if let Some(parts) = decode_tagged_date(value) {
        return parts;
    }
    let (year, month, day) = civil_from_days(value);
    (year.clamp(0, 9999) as u32, month, day)
}

pub(crate) fn datetime_components(value: i64) -> (u32, u32, u32, u32, u32, u32, u32) {
    if value == ZERO_DATETIME {
        return (0, 0, 0, 0, 0, 0, 0);
    }
    if let Some(parts) = decode_tagged_datetime(value) {
        return parts;
    }
    let secs = value.div_euclid(1_000_000);
    let micros = value.rem_euclid(1_000_000) as u32;
    let days = secs.div_euclid(86_400) as i32;
    let secs_of_day = secs.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    (
        year.clamp(0, 9999) as u32,
        month,
        day,
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60,
        micros,
    )
}

/// `&str` view of a string-valued `Value::String` / `Value::SmallString`.
/// Returns `None` for any other variant. Used by code paths that need to
/// read both string lanes uniformly (DD-row parsing, `if let
/// Some(Value::String(..))` patterns over data-dictionary rows where
/// short payloads land in the SmallString lane).
#[inline]
pub(crate) fn value_str(v: &Value) -> Option<&str> {
    match v {
        Value::String(s) => Some(s.as_str()),
        // SAFETY: SmallString is constructed only from validated UTF-8 input
        // (the row codec calls `String::from_utf8` then routes to SmallString,
        // or callers construct it from an already-validated `&str`). The
        // invariant is upheld at every construction site.
        Value::SmallString(b) => Some(unsafe { std::str::from_utf8_unchecked(b.as_slice()) }),
        _ => None,
    }
}

#[cfg(test)]
/// Clone `src` into `dst`, re-using `dst`'s existing heap allocation when
/// both sides carry the same buffer-backed variant. Streaming scan paths
/// call this per row on long-lived output slots, where a plain
/// `dst = src.clone()` costs a malloc+free pair per row for every
/// `String`/`Vec<u8>`-backed value.
///
/// A `String` destination also absorbs a `SmallString` source (the two
/// lanes are semantically interchangeable — see [`Value::SmallString`]) so
/// a column whose lengths straddle the inline cap doesn't bounce the slot
/// between lanes and drop its buffer.
#[inline]
pub(crate) fn assign_value_reusing(dst: &mut Value, src: &Value) {
    match (&mut *dst, src) {
        (Value::String(d), Value::String(s)) => {
            d.clear();
            d.push_str(s);
        }
        (Value::String(d), Value::SmallString(s)) => {
            if s.is_empty() {
                // ENUM uses the empty SmallString lane to retain a declared
                // empty member's ordinal separately from ordinal 0.
                *dst = Value::SmallString(s.clone());
            } else {
                d.clear();
                // SAFETY: same invariant as `value_str` — SmallString is only
                // constructed from validated UTF-8.
                d.push_str(unsafe { std::str::from_utf8_unchecked(s.as_slice()) });
            }
        }
        (Value::Bytes(d), Value::Bytes(s)) => {
            d.clear();
            d.extend_from_slice(s);
        }
        (Value::Json(d), Value::Json(s)) => {
            d.clear();
            d.extend_from_slice(s);
        }
        (Value::Geometry(d), Value::Geometry(s)) => {
            d.clear();
            d.extend_from_slice(s);
        }
        (Value::Vector(d), Value::Vector(s)) => {
            d.clear();
            d.extend_from_slice(s);
        }
        (Value::Decimal(d), Value::Decimal(s)) => {
            d.clear();
            d.push_str(s);
        }
        (Value::Decimal128(dm, ds), Value::Decimal128(sm, ss)) => {
            *dm = *sm;
            *ds = *ss;
        }
        (Value::DecimalWide(d), Value::DecimalWide(s)) => d.clone_from(s),
        (d, s) => *d = s.clone(),
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        // String/SmallString equivalence: byte-identical content compares
        // equal across the two variants. Decimal stays a separate variant
        // even though it shares String's payload type — equality between
        // Decimal and String would break MySQL's numeric/string distinction.
        match (self, other) {
            (Value::String(a), Value::String(b)) => a == b,
            (Value::SmallString(a), Value::SmallString(b)) => a == b,
            (Value::String(a), Value::SmallString(b))
            | (Value::SmallString(b), Value::String(a)) => a.as_bytes() == b.as_slice(),
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::UInt(a), Value::UInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::DecimalCompact(am, ascale), Value::DecimalCompact(bm, bscale)) => {
                am == bm && ascale == bscale
            }
            (Value::Decimal128(am, ascale), Value::Decimal128(bm, bscale)) => {
                am == bm && ascale == bscale
            }
            (Value::DecimalWide(a), Value::DecimalWide(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Year(a), Value::Year(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Geometry(a), Value::Geometry(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => a == b,
            _ => false,
        }
    }
}

impl Value {
    /// Heap capacity owned by this value, excluding its inline representation.
    #[inline]
    pub(crate) fn heap_capacity(&self) -> usize {
        match self {
            Value::Decimal(value) | Value::String(value) => value.capacity(),
            Value::SmallString(value) if value.spilled() => value.capacity(),
            Value::DecimalWide(_) => self.decimal_heap_bytes(),
            Value::Bytes(value)
            | Value::Json(value)
            | Value::Geometry(value)
            | Value::Vector(value) => value.capacity(),
            _ => 0,
        }
    }

    /// Convert any exact numeric runtime lane into the unified decimal
    /// representation. Legacy textual Decimal remains accepted at SQL and
    /// protocol boundaries, but execution results never need to stringify.
    pub fn exact_decimal(&self) -> Option<crate::util::decimal::ExactDecimal> {
        use crate::util::decimal::ExactDecimal;
        match self {
            Value::Int(value) => Some(ExactDecimal::I64(*value, 0)),
            Value::UInt(value) => Some(ExactDecimal::from_big((*value).into(), 0)),
            Value::DecimalCompact(value, scale) => Some(ExactDecimal::I64(*value, *scale)),
            Value::Decimal128(value, scale) => Some(ExactDecimal::I128(*value, *scale)),
            Value::DecimalWide(value) => {
                Some(ExactDecimal::Big(value.magnitude.clone(), value.scale))
            }
            Value::Decimal(text) => crate::util::decimal::parse_exact_decimal_big(text)
                .map(|(value, scale)| ExactDecimal::from_big(value, scale)),
            _ => None,
        }
    }

    #[inline]
    pub fn from_exact_decimal(value: crate::util::decimal::ExactDecimal) -> Self {
        match value {
            crate::util::decimal::ExactDecimal::I64(value, scale) => {
                Value::DecimalCompact(value, scale)
            }
            crate::util::decimal::ExactDecimal::I128(value, scale) => {
                Value::Decimal128(value, scale)
            }
            crate::util::decimal::ExactDecimal::Big(magnitude, scale) => {
                Value::DecimalWide(Box::new(DecimalWide { magnitude, scale }))
            }
        }
    }

    /// Account the allocation-backed coefficient by its bounded limb storage.
    /// `num-bigint` does not expose spare capacity, so this uses the exact
    /// occupied limb ceiling; the surrounding reservation adds container
    /// capacity separately.
    pub fn decimal_heap_bytes(&self) -> usize {
        match self {
            Value::Decimal(text) => text.capacity(),
            Value::DecimalWide(value) => {
                let limb_bits = usize::BITS as u64;
                let limbs = value.magnitude.bits().div_ceil(limb_bits) as usize;
                // `num-bigint` does not expose its Vec capacity. Account the
                // allocator growth class conservatively so reservation never
                // trails a likely limb allocation after promotion/growth.
                let limb_capacity = limbs.max(1).next_power_of_two();
                std::mem::size_of::<DecimalWide>()
                    .saturating_add(limb_capacity.saturating_mul(std::mem::size_of::<usize>()))
            }
            _ => 0,
        }
    }

    /// Canonical human-readable form of this value — used by SHOW /
    /// information_schema output and by callers that haven't been
    /// taught about the binary temporal / DecimalCompact variants.
    /// Returns an owned `String`; callers in hot paths should prefer
    /// the variant-specific helpers below.
    pub fn to_display_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(v) => v.to_string(),
            Value::UInt(v) => v.to_string(),
            Value::Float(v) => format_f32_compact(*v),
            Value::Double(v) => format_f64_compact(*v),
            Value::Decimal(s) | Value::String(s) => s.clone(),
            Value::SmallString(b) => {
                // SAFETY: SmallString invariant — bytes are valid UTF-8.
                unsafe { std::str::from_utf8_unchecked(b.as_slice()) }.to_string()
            }
            Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            Value::DecimalCompact(mag, scale) => format_decimal_compact(*mag, *scale),
            Value::Decimal128(mag, scale) => {
                crate::util::decimal::format_decimal_parts(*mag, *scale)
            }
            Value::DecimalWide(value) => {
                crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
            }
            Value::Date(days) => format_date_compact(*days),
            Value::Time(micros) => format_time_compact(*micros),
            Value::DateTime(micros) => format_datetime_compact(*micros),
            // Session-aware externalization happens at the QueryResponse /
            // protocol boundary. Internal/debug callers render UTC rather
            // than silently applying an unavailable session offset.
            Value::Timestamp(micros) => format_datetime_compact(*micros),
            Value::Year(b) => format_year_compact(*b),
            Value::Json(b) => crate::engine::json::to_text(b),
            Value::Geometry(b) => crate::engine::geo::to_text(b),
            Value::Vector(b) => crate::engine::vector::to_text(b),
        }
    }

    /// Allocation-free canonical byte form, used by the comparison hot
    /// path (`compare_values`) so a `DATE`/`DATETIME`/`TIME`/`YEAR` value
    /// compared against a string literal does not allocate a `String`
    /// per row.
    ///
    /// For every `Some(_)` return the bytes are **identical** to
    /// `to_display_string().as_bytes()`. Returns `None` for the numeric
    /// variants (`Int`/`UInt`/`Float`/`Double`/`DecimalCompact`) and
    /// non-UTF-8 `Bytes`, where the caller falls back to the allocating
    /// path; those variants almost never reach the byte comparison
    /// fallback because they are caught by the numeric fast lanes.
    pub fn canonical_cmp_bytes<'a>(&'a self, buf: &'a mut [u8; 32]) -> Option<&'a [u8]> {
        match self {
            Value::Null => Some(b"NULL"),
            Value::Decimal(s) | Value::String(s) => Some(s.as_bytes()),
            Value::SmallString(b) => Some(b.as_slice()),
            Value::Bytes(b) => {
                if std::str::from_utf8(b).is_ok() {
                    Some(b.as_slice())
                } else {
                    None
                }
            }
            Value::Date(d) => Some(write_date_canonical(*d, buf)),
            Value::Time(t) => Some(write_time_canonical(*t, buf)),
            Value::DateTime(dt) => Some(write_datetime_canonical(*dt, buf)),
            Value::Timestamp(ts) => Some(write_datetime_canonical(*ts, buf)),
            Value::Year(y) => Some(write_year_canonical(*y, buf)),
            Value::Int(_)
            | Value::UInt(_)
            | Value::Float(_)
            | Value::Double(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(..) => None,
            // JSON is ordered by `json::compare` (type-precedence aware),
            // never by canonical byte comparison — `compare_values` handles
            // it before reaching this fallback.
            Value::Json(_) => None,
            // Geometry is ordered by `geo::compare` (internal-binary order),
            // handled by `compare_values` before this fallback.
            Value::Geometry(_) => None,
            // Vector is ordered by `vector::compare` (internal-binary order),
            // handled by `compare_values` before this fallback.
            Value::Vector(_) => None,
        }
    }
}

// ---- Canonical formatting helpers (used by Value::to_display_string) ----
//
// These mirror the hand-rolled fast paths in `storage::core::row_codec`
// so that `Value::Date(d).to_display_string()` and the codec produce the
// same byte sequence. Kept here so the binding from on-disk form to
// human-readable form lives next to the Value enum itself rather than
// being scattered across modules.

fn format_decimal_compact(magnitude: i64, scale: u8) -> String {
    crate::util::decimal::format_decimal_parts(i128::from(magnitude), scale)
}

fn format_date_compact(days: i32) -> String {
    let (y, m, d) = date_components(days);
    let mut buf = [0u8; 10];
    write_4_digit_ascii(&mut buf[0..4], y);
    buf[4] = b'-';
    write_2_digit_ascii(&mut buf[5..7], m);
    buf[7] = b'-';
    write_2_digit_ascii(&mut buf[8..10], d);
    unsafe { String::from_utf8_unchecked(buf.to_vec()) }
}

fn format_time_compact(micros: i64) -> String {
    let neg = micros < 0;
    let m = micros.unsigned_abs();
    let total_secs = m / 1_000_000;
    let frac = (m % 1_000_000) as u32;
    let hours = total_secs / 3600;
    let minutes = (total_secs / 60) % 60;
    let secs = total_secs % 60;
    let prefix = if neg { "-" } else { "" };
    if frac == 0 {
        format!("{}{:02}:{:02}:{:02}", prefix, hours, minutes, secs)
    } else {
        let s = format!("{:06}", frac);
        let s = s.trim_end_matches('0');
        format!("{}{:02}:{:02}:{:02}.{}", prefix, hours, minutes, secs, s)
    }
}

fn format_datetime_compact(micros: i64) -> String {
    let (y, m, d, hh, mm, ss, frac) = datetime_components(micros);
    if frac == 0 {
        let mut buf = [0u8; 19];
        write_4_digit_ascii(&mut buf[0..4], y);
        buf[4] = b'-';
        write_2_digit_ascii(&mut buf[5..7], m);
        buf[7] = b'-';
        write_2_digit_ascii(&mut buf[8..10], d);
        buf[10] = b' ';
        write_2_digit_ascii(&mut buf[11..13], hh);
        buf[13] = b':';
        write_2_digit_ascii(&mut buf[14..16], mm);
        buf[16] = b':';
        write_2_digit_ascii(&mut buf[17..19], ss);
        unsafe { String::from_utf8_unchecked(buf.to_vec()) }
    } else {
        let mut buf = [0u8; 26];
        write_4_digit_ascii(&mut buf[0..4], y);
        buf[4] = b'-';
        write_2_digit_ascii(&mut buf[5..7], m);
        buf[7] = b'-';
        write_2_digit_ascii(&mut buf[8..10], d);
        buf[10] = b' ';
        write_2_digit_ascii(&mut buf[11..13], hh);
        buf[13] = b':';
        write_2_digit_ascii(&mut buf[14..16], mm);
        buf[16] = b':';
        write_2_digit_ascii(&mut buf[17..19], ss);
        buf[19] = b'.';
        let mut frac_digits = [b'0'; 6];
        let mut f = frac;
        for i in (0..6).rev() {
            frac_digits[i] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut last_non_zero = 6;
        while last_non_zero > 0 && frac_digits[last_non_zero - 1] == b'0' {
            last_non_zero -= 1;
        }
        buf[20..20 + last_non_zero].copy_from_slice(&frac_digits[..last_non_zero]);
        let total_len = 20 + last_non_zero;
        unsafe { String::from_utf8_unchecked(buf[..total_len].to_vec()) }
    }
}

fn format_year_compact(b: u8) -> String {
    if b == 0 {
        "0000".to_string()
    } else {
        format!("{}", 1900 + b as i32)
    }
}

/// MySQL's "temporal → number" coercion, decomposed into an integer part and
/// a sub-second microsecond fraction. This is the single source of truth for
/// every *user-facing* numeric context over a temporal value — `CAST(… AS
/// SIGNED/UNSIGNED/DECIMAL)`, arithmetic (`d + 0`), and mixed comparison
/// (`d = 20240531`, `dt > 20200101000000`). MySQL renders a DATE to the
/// decimal integer `YYYYMMDD`, a DATETIME/TIMESTAMP to `YYYYMMDDHHMMSS`, a
/// TIME to (±)`HHMMSS`, and a YEAR to the plain year; the fractional seconds
/// ride along in `frac_micros`. The zero-date / zero-datetime sentinels
/// render to 0 (not the internal `i32::MIN` / `i64::MIN`).
///
/// Note this is deliberately distinct from the *internal* days-since-epoch /
/// microsecond representation used by temporal math (`value_to_date_days`,
/// `value_to_datetime_micros`, `civil_from_days`); those keep their own paths.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TemporalNumber {
    /// Non-negative magnitude of the integer part (`YYYYMMDD[HHMMSS]` /
    /// `HHMMSS` / year). The sign lives in `negative` so a value like
    /// `-00:00:00.5` (integer part 0) still carries its sign.
    pub int_part: i64,
    /// Sub-second microseconds (0..1_000_000), always the magnitude.
    pub frac_micros: u32,
    /// Only TIME can be negative.
    pub negative: bool,
}

impl TemporalNumber {
    /// Integer-context value: MySQL rounds half-away-from-zero on the
    /// fractional seconds (`CAST(TIME'10:20:30.5' AS SIGNED)` = 102031).
    pub(crate) fn to_i64(&self) -> i64 {
        let mut v = self.int_part;
        if self.frac_micros >= 500_000 {
            v += 1;
        }
        if self.negative {
            -v
        } else {
            v
        }
    }

    /// Real-context value: integer part plus fractional seconds as a decimal
    /// (`TIME'10:20:30.5' + 0.0` = 102030.5). A 14-digit `YYYYMMDDHHMMSS`
    /// stays inside f64's exact-integer range (< 2^53).
    pub(crate) fn to_f64(&self) -> f64 {
        let v = self.int_part as f64 + self.frac_micros as f64 / 1_000_000.0;
        if self.negative {
            -v
        } else {
            v
        }
    }
}

/// Decompose a temporal `Value` into its MySQL decimal-number form. Returns
/// `None` for non-temporal values (the caller keeps its own coercion path).
pub(crate) fn temporal_to_mysql_number(value: &Value) -> Option<TemporalNumber> {
    match value {
        Value::Date(d) => {
            let (y, m, day) = date_components(*d);
            Some(TemporalNumber {
                int_part: y as i64 * 10_000 + m as i64 * 100 + day as i64,
                frac_micros: 0,
                negative: false,
            })
        }
        Value::DateTime(micros) | Value::Timestamp(micros) => {
            let (y, m, day, hh, mm, ss, frac) = datetime_components(*micros);
            let int_part =
                ((((y as i64 * 10_000 + m as i64 * 100 + day as i64) * 100 + hh as i64) * 100
                    + mm as i64)
                    * 100)
                    + ss as i64;
            Some(TemporalNumber {
                int_part,
                frac_micros: frac,
                negative: false,
            })
        }
        Value::Time(micros) => {
            let neg = *micros < 0;
            let mag = micros.unsigned_abs();
            let total_secs = (mag / 1_000_000) as i64;
            let frac = (mag % 1_000_000) as u32;
            let hours = total_secs / 3600;
            let minutes = (total_secs / 60) % 60;
            let secs = total_secs % 60;
            Some(TemporalNumber {
                int_part: hours * 10_000 + minutes * 100 + secs,
                frac_micros: frac,
                negative: neg,
            })
        }
        Value::Year(y) => Some(TemporalNumber {
            int_part: if *y == 0 { 0 } else { 1900 + *y as i64 },
            frac_micros: 0,
            negative: false,
        }),
        _ => None,
    }
}

fn format_f32_compact(v: f32) -> String {
    mysql_f32_text(v)
}

/// Parameter SQL literals preserve the exact promoted f32 value. This is
/// separate from FLOAT result display, which uses `mysql_f32_text`.
pub(crate) fn protocol_f32_text(value: f32) -> String {
    let mut buffer = ryu::Buffer::new();
    let promoted = f64::from(value);
    ryu_to_mysql_text(buffer.format(promoted), || promoted.to_string()).into_owned()
}

/// MySQL renders an unconstrained FLOAT with approximately `FLT_DIG` (six)
/// significant decimal digits. Rust/ryu deliberately emits the shortest
/// round-trippable f32 instead, exposing storage noise such as
/// `1234567936`. Keep fixed notation across MySQL's ordinary display range
/// and switch to scientific notation only outside it.
pub(crate) fn mysql_f32_text(v: f32) -> String {
    if !v.is_finite() {
        return v.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }
    let abs = v.abs() as f64;
    let exponent = abs.log10().floor() as i32;
    if abs >= f64::from(1e15_f32) || exponent < -15 {
        let scientific = format!("{v:.5e}");
        return compact_scientific_notation(&scientific);
    }
    let decimals = (5 - exponent).max(0) as usize;
    let rounded: f64 = if exponent > 5 {
        let factor = 10f64.powi(exponent - 5);
        ((v as f64) / factor).round() * factor
    } else {
        v as f64
    };
    let mut fixed = format!("{rounded:.decimals$}");
    trim_fraction_zeros(&mut fixed);
    fixed
}

fn format_f64_compact(v: f64) -> String {
    let mut buf = ryu::Buffer::new();
    ryu_to_mysql_text(buf.format(v), || v.to_string()).into_owned()
}

/// Render an approximate numeric value for assignment to a bounded character
/// column. MySQL keeps its ordinary fixed representation when that fits, but
/// falls back to the shorter scientific spelling before reporting truncation.
/// Keep this separate from result-set rendering: column width must not change
/// the textual form returned by an unconstrained SELECT.
pub(crate) fn float_text_for_char_width(value: &Value, max_chars: usize) -> Option<String> {
    let ordinary = match value {
        Value::Float(value) => mysql_f32_text(*value),
        Value::Double(value) => format_f64_compact(*value),
        _ => return None,
    };
    if ordinary.chars().count() <= max_chars {
        return Some(ordinary);
    }
    // Only pay for a second formatting pass on the truncation path.
    let scientific = match value {
        Value::Float(value) => compact_scientific_notation(&format!("{value:e}")),
        Value::Double(value) => compact_scientific_notation(&format!("{value:e}")),
        _ => unreachable!(),
    };
    Some(if scientific.chars().count() < ordinary.chars().count() {
        scientific
    } else {
        ordinary
    })
}

fn trim_fraction_zeros(text: &mut String) {
    if !text.contains('.') {
        return;
    }
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
}

fn compact_scientific_notation(text: &str) -> String {
    let Some((mantissa, exponent)) = text.split_once(['e', 'E']) else {
        return text.to_string();
    };
    let mut mantissa = mantissa.to_string();
    trim_fraction_zeros(&mut mantissa);
    let exponent = exponent.parse::<i32>().unwrap_or(0);
    format!("{mantissa}e{exponent}")
}

fn expand_scientific_notation(text: &str) -> Option<String> {
    let (mantissa, exponent) = text.split_once(['e', 'E'])?;
    let exponent = exponent.parse::<i32>().ok()?;
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.trim_start_matches(['-', '+']);
    let dot = unsigned.find('.').unwrap_or(unsigned.len());
    let digits: String = unsigned.chars().filter(|ch| *ch != '.').collect();
    let decimal_pos = dot as i32 + exponent;
    let mut out = String::with_capacity(
        digits
            .len()
            .saturating_add(exponent.unsigned_abs() as usize)
            .saturating_add(3),
    );
    if negative {
        out.push('-');
    }
    if decimal_pos <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-decimal_pos) as usize));
        out.push_str(&digits);
    } else if decimal_pos as usize >= digits.len() {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n(
            '0',
            decimal_pos as usize - digits.len(),
        ));
    } else {
        let decimal_pos = decimal_pos as usize;
        out.push_str(&digits[..decimal_pos]);
        out.push('.');
        out.push_str(&digits[decimal_pos..]);
    }
    Some(out)
}

/// Massage a ryu-produced "shortest round-trip" string into the MySQL/Rust
/// `to_string + trim_float` shape, returning a borrowed slice when
/// possible so the protocol-layer hot path stays allocation-free:
///   * `"1.0"` → `"1"`              (drop integral ".0")
///   * `"-0.0"` → `"-0"`            (preserve sign on integral zero)
/// Scientific notation and all other shapes (`"1e20"`, `"1.5"`, `"NaN"`,
/// `"inf"`, `"-inf"`) pass through. MySQL switches to exponent notation for
/// large/small approximate values just as ryu does.
pub(crate) fn ryu_to_mysql_text<'a>(
    s: &'a str,
    _fallback: impl FnOnce() -> String,
) -> std::borrow::Cow<'a, str> {
    use std::borrow::Cow;
    if !s.contains(['e', 'E']) {
        if let Ok(value) = s.parse::<f64>() {
            if value.abs() >= 1e15 {
                return Cow::Owned(compact_scientific_notation(&format!("{value:e}")));
            }
        }
    }
    if let Some(stripped) = s.strip_suffix(".0") {
        return Cow::Borrowed(stripped);
    }
    if let Some((_, exponent)) = s.split_once(['e', 'E']) {
        if exponent
            .parse::<i32>()
            .is_ok_and(|exponent| (-15..15).contains(&exponent))
        {
            if let Some(expanded) = expand_scientific_notation(s) {
                return Cow::Owned(expanded);
            }
        }
    }
    Cow::Borrowed(s)
}

#[inline]
fn write_2_digit_ascii(buf: &mut [u8], v: u32) {
    buf[0] = b'0' + (v / 10) as u8;
    buf[1] = b'0' + (v % 10) as u8;
}

#[inline]
fn write_4_digit_ascii(buf: &mut [u8], v: u32) {
    buf[0] = b'0' + (v / 1000) as u8;
    buf[1] = b'0' + ((v / 100) % 10) as u8;
    buf[2] = b'0' + ((v / 10) % 10) as u8;
    buf[3] = b'0' + (v % 10) as u8;
}

// ---- Allocation-free canonical byte writers ----
//
// Each writes the exact byte sequence that the matching `format_*`
// helper (and hence `Value::to_display_string`) produces, but into a
// caller-provided stack buffer. Used by `Value::canonical_cmp_bytes`.

pub(crate) fn write_date_canonical(days: i32, buf: &mut [u8; 32]) -> &[u8] {
    let (y, m, d) = date_components(days);
    write_4_digit_ascii(&mut buf[0..4], y);
    buf[4] = b'-';
    write_2_digit_ascii(&mut buf[5..7], m);
    buf[7] = b'-';
    write_2_digit_ascii(&mut buf[8..10], d);
    &buf[..10]
}

pub(crate) fn write_year_canonical(b: u8, buf: &mut [u8; 32]) -> &[u8] {
    if b == 0 {
        buf[..4].copy_from_slice(b"0000");
        return &buf[..4];
    }
    write_4_digit_ascii(&mut buf[0..4], 1900 + b as u32);
    &buf[..4]
}

pub(crate) fn write_datetime_canonical(micros: i64, buf: &mut [u8; 32]) -> &[u8] {
    write_datetime_canonical_with_fsp(micros, None, buf)
}

/// Render `Value::DateTime` to canonical MySQL text. When `fsp` is `Some`,
/// the fractional-second field is emitted with exactly `fsp` digits —
/// including trailing zeros — matching MySQL's behaviour for typed
/// `DATETIME(N)` / `TIMESTAMP(N)` result columns. When `fsp` is `None`, we
/// fall back to the trailing-zero-trimmed shape (legacy callers that don't
/// know the column's declared precision, e.g. casts, `Value::to_string()`).
pub(crate) fn write_datetime_canonical_with_fsp(
    micros: i64,
    fsp: Option<u8>,
    buf: &mut [u8; 32],
) -> &[u8] {
    let (y, m, d, hh, mm, ss, frac) = datetime_components(micros);
    write_4_digit_ascii(&mut buf[0..4], y);
    buf[4] = b'-';
    write_2_digit_ascii(&mut buf[5..7], m);
    buf[7] = b'-';
    write_2_digit_ascii(&mut buf[8..10], d);
    buf[10] = b' ';
    write_2_digit_ascii(&mut buf[11..13], hh);
    buf[13] = b':';
    write_2_digit_ascii(&mut buf[14..16], mm);
    buf[16] = b':';
    write_2_digit_ascii(&mut buf[17..19], ss);
    if let Some(fsp) = fsp {
        let fsp = fsp.min(6);
        if fsp == 0 {
            return &buf[..19];
        }
        buf[19] = b'.';
        let scale = 10u32.pow(6 - fsp as u32);
        let mut f = frac / scale;
        let n = fsp as usize;
        for i in (0..n).rev() {
            buf[20 + i] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        &buf[..20 + n]
    } else {
        if frac == 0 {
            return &buf[..19];
        }
        buf[19] = b'.';
        let mut frac_digits = [b'0'; 6];
        let mut f = frac;
        for slot in frac_digits.iter_mut().rev() {
            *slot = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut last_non_zero = 6;
        while last_non_zero > 0 && frac_digits[last_non_zero - 1] == b'0' {
            last_non_zero -= 1;
        }
        buf[20..20 + last_non_zero].copy_from_slice(&frac_digits[..last_non_zero]);
        &buf[..20 + last_non_zero]
    }
}

pub(crate) fn write_time_canonical(micros: i64, buf: &mut [u8; 32]) -> &[u8] {
    write_time_canonical_with_fsp(micros, None, buf)
}

/// Render `Value::Time` to canonical MySQL text. See
/// [`write_datetime_canonical_with_fsp`] for the `fsp` semantics.
pub(crate) fn write_time_canonical_with_fsp(
    micros: i64,
    fsp: Option<u8>,
    buf: &mut [u8; 32],
) -> &[u8] {
    let neg = micros < 0;
    let m = micros.unsigned_abs();
    let total_secs = m / 1_000_000;
    let frac = (m % 1_000_000) as u32;
    let hours = total_secs / 3600;
    let minutes = ((total_secs / 60) % 60) as u32;
    let secs = (total_secs % 60) as u32;
    let mut pos = 0usize;
    if neg {
        buf[0] = b'-';
        pos = 1;
    }
    // Hours: variable width, minimum two digits.
    let mut tmp = [0u8; 20];
    let mut tp = tmp.len();
    let mut h = hours;
    loop {
        tp -= 1;
        tmp[tp] = b'0' + (h % 10) as u8;
        h /= 10;
        if h == 0 {
            break;
        }
    }
    let mut hdigits = tmp.len() - tp;
    while hdigits < 2 {
        tp -= 1;
        tmp[tp] = b'0';
        hdigits += 1;
    }
    buf[pos..pos + hdigits].copy_from_slice(&tmp[tp..]);
    pos += hdigits;
    buf[pos] = b':';
    write_2_digit_ascii(&mut buf[pos + 1..pos + 3], minutes);
    buf[pos + 3] = b':';
    write_2_digit_ascii(&mut buf[pos + 4..pos + 6], secs);
    pos += 6;
    if let Some(fsp) = fsp {
        let fsp = fsp.min(6);
        if fsp == 0 {
            return &buf[..pos];
        }
        buf[pos] = b'.';
        pos += 1;
        let scale = 10u32.pow(6 - fsp as u32);
        let mut f = frac / scale;
        let n = fsp as usize;
        for i in (0..n).rev() {
            buf[pos + i] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        &buf[..pos + n]
    } else {
        if frac == 0 {
            return &buf[..pos];
        }
        buf[pos] = b'.';
        pos += 1;
        let mut frac_digits = [b'0'; 6];
        let mut f = frac;
        for slot in frac_digits.iter_mut().rev() {
            *slot = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut last_non_zero = 6;
        while last_non_zero > 0 && frac_digits[last_non_zero - 1] == b'0' {
            last_non_zero -= 1;
        }
        buf[pos..pos + last_non_zero].copy_from_slice(&frac_digits[..last_non_zero]);
        &buf[..pos + last_non_zero]
    }
}

/// Allocation-free MySQL canonical decimal text writer used by the
/// protocol hot path. Writes the same byte sequence
/// `format_decimal_compact` returns into a 48-byte caller-owned stack
/// buffer. 48 bytes covers the worst case of `i64::MIN` (20 chars)
/// with MySQL's maximum `DECIMAL(P, 30)` scale — `(sign 1) + (int 1)
/// + (dot 1) + (scale 30)` = 33 plus headroom.
pub(crate) fn write_decimal_compact_canonical(
    magnitude: i64,
    scale: u8,
    buf: &mut [u8; 48],
) -> &[u8] {
    let negative = magnitude < 0;
    let mut mag = magnitude.unsigned_abs() as u128;
    let scale_usize = scale as usize;
    // Build the digit sequence right-to-left in a scratch buffer so
    // we know the integer/fraction split before laying it into `buf`.
    let mut tmp = [0u8; 48];
    let mut pos = tmp.len();
    let mut digits = 0usize;
    loop {
        pos -= 1;
        tmp[pos] = b'0' + (mag % 10) as u8;
        mag /= 10;
        digits += 1;
        if mag == 0 {
            break;
        }
    }
    while digits < scale_usize + 1 {
        pos -= 1;
        tmp[pos] = b'0';
        digits += 1;
    }
    let int_len = digits - scale_usize;
    let digit_bytes = &tmp[pos..pos + digits];
    let mut out_pos = 0usize;
    if negative && magnitude != 0 {
        buf[out_pos] = b'-';
        out_pos += 1;
    }
    if scale_usize == 0 {
        buf[out_pos..out_pos + digits].copy_from_slice(digit_bytes);
        out_pos += digits;
    } else {
        buf[out_pos..out_pos + int_len].copy_from_slice(&digit_bytes[..int_len]);
        out_pos += int_len;
        buf[out_pos] = b'.';
        out_pos += 1;
        buf[out_pos..out_pos + scale_usize].copy_from_slice(&digit_bytes[int_len..]);
        out_pos += scale_usize;
    }
    &buf[..out_pos]
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 → (Y, M, D)
/// in the proleptic Gregorian calendar. Duplicated here (and in
/// `storage::core::row_codec`) so both modules can format binary
/// temporal values without depending on each other or on chrono.
#[inline]
pub(crate) fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

/// Gregorian year without computing the unused month/day divisions. Wide
/// intermediates also make the arithmetic total over the i32 day domain.
#[inline]
pub(crate) fn civil_year_from_days(days: i32) -> i32 {
    let z = i64::from(days) + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    (i64::from(yoe) + era * 400 + i64::from(doy >= 306)) as i32
}

#[cfg(test)]
mod civil_year_tests {
    #[test]
    fn year_only_matches_complete_calendar_for_every_sql_date() {
        for day in -719528..=2932896 {
            assert_eq!(
                super::civil_year_from_days(day),
                super::civil_from_days(day).0,
                "day={day}"
            );
        }
    }
}

#[cfg(test)]
mod float_text_tests {
    use super::{
        float_text_for_char_width, mysql_f32_text, ryu_to_mysql_text, SmallStringBuf, Value,
    };

    #[test]
    fn critical_audit_protocol_float_preserves_stored_value() {
        for value in [
            0.1_f32,
            978903.375,
            f32::MAX,
            f32::MIN_POSITIVE,
            1e15,
            1e-15,
            1e-20,
            140.36,
            -0.0,
        ] {
            let text = super::protocol_f32_text(value);
            assert_eq!(
                text.parse::<f64>().unwrap().to_bits(),
                f64::from(value).to_bits(),
                "{text}"
            );
        }
    }

    #[test]
    fn thirty_one_byte_small_strings_remain_inline_without_growing_value() {
        let value = SmallStringBuf::from_slice(b"1234567890123456789012345678901");
        assert!(!value.spilled());
        assert_eq!(std::mem::size_of::<SmallStringBuf>(), 40);
        assert_eq!(std::mem::size_of::<Value>(), 48);
    }

    #[test]
    fn mysql_float_uses_six_significant_digits() {
        assert_eq!(mysql_f32_text(1_234_567_936.0), "1234570000");
        assert_eq!(mysql_f32_text(3.4028235e38), "3.40282e38");
        assert_eq!(mysql_f32_text(1e15), "1e15");
        assert_eq!(mysql_f32_text(1e-15), "0.000000000000001");
        assert_eq!(mysql_f32_text(1e-20), "1e-20");
        assert_eq!(mysql_f32_text(140.36), "140.36");
    }

    #[test]
    fn mysql_double_expands_ordinary_negative_exponents() {
        assert_eq!(
            ryu_to_mysql_text("3.4785054261852176e-7", String::new),
            "0.00000034785054261852176"
        );
        assert_eq!(ryu_to_mysql_text("1e-15", String::new), "0.000000000000001");
        assert_eq!(ryu_to_mysql_text("1e-20", String::new), "1e-20");
        assert_eq!(ryu_to_mysql_text("1000000000000000.0", String::new), "1e15");
        assert_eq!(ryu_to_mysql_text("1e20", String::new), "1e20");
    }

    #[test]
    fn bounded_char_float_text_uses_scientific_only_when_needed() {
        assert_eq!(
            float_text_for_char_width(&Value::Double(2e5), 6).as_deref(),
            Some("200000")
        );
        assert_eq!(
            float_text_for_char_width(&Value::Double(2e6), 6).as_deref(),
            Some("2e6")
        );
        assert_eq!(
            float_text_for_char_width(&Value::Double(2e-4), 6).as_deref(),
            Some("0.0002")
        );
        assert_eq!(
            float_text_for_char_width(&Value::Double(2e-5), 6).as_deref(),
            Some("2e-5")
        );
    }
}
