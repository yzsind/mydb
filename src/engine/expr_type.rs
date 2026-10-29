//! Bind-time SQL expression type descriptors.
//!
//! `Value` is deliberately absent from this module.  Runtime payload shape is
//! not a SQL type: constant folding, an empty input, or a branch selected at
//! runtime must never change result-set metadata.  The descriptor is compact,
//! allocation-free and can therefore be copied into compiled plans and batch
//! schemas without adding hot-path ownership work.

pub(crate) mod column_type;

use crate::parser::{BinaryOp, Expr, Literal, UnaryOp};
use crate::storage::schema::GeometrySubtype;
use crate::storage::DataTypeKind;
use crate::types::{ColumnSource, ColumnType, Value, MAX_DECIMAL_PRECISION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IntegerWidth {
    Tiny,
    Small,
    Medium,
    Int,
    Big,
}

impl IntegerWidth {
    pub(crate) const fn decimal_precision(self) -> u8 {
        match self {
            Self::Tiny => 3,
            Self::Small => 5,
            Self::Medium => 8,
            Self::Int => 10,
            // BIGINT UNSIGNED needs 20 digits.  Keeping the complete domain
            // here is also conservative for a signed BIGINT expression.
            Self::Big => 20,
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Tiny => 0,
            Self::Small => 1,
            Self::Medium => 2,
            Self::Int => 3,
            Self::Big => 4,
        }
    }

    const fn max(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }

    const fn next(self) -> Self {
        match self {
            Self::Tiny => Self::Small,
            Self::Small => Self::Medium,
            Self::Medium => Self::Int,
            Self::Int | Self::Big => Self::Big,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StringKind {
    Character,
    Binary,
    Text,
    Blob,
    Enum,
    Set,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TemporalKind {
    Date,
    Time,
    DateTime,
    Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ExprTypeKind {
    Null,
    Integer {
        width: IntegerWidth,
        unsigned: bool,
    },
    Decimal {
        precision: u8,
        scale: u8,
        unsigned: bool,
    },
    Float {
        single: bool,
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Bit {
        bits: u8,
    },
    Year,
    String {
        kind: StringKind,
    },
    Temporal {
        kind: TemporalKind,
        fsp: u8,
    },
    Json,
    Geometry {
        subtype: GeometrySubtype,
        srid: Option<u32>,
    },
    Vector {
        dim: Option<u32>,
        datatype: crate::vector::VectorDataType,
    },
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ExprType {
    /// True only while the result domain depends on an unbound parameter.
    pub(crate) dynamic_parameter: bool,
    pub(crate) kind: ExprTypeKind,
    pub(crate) nullable: bool,
    /// Maximum result bytes when statically known.  Character lengths are
    /// converted to bytes at binding using the selected charset.
    pub(crate) max_octets: Option<u32>,
    pub(crate) collation_id: Option<u16>,
    /// MySQL collation coercibility (lower wins).
    pub(crate) coercibility: u8,
    /// Precision retained when a temporal branch enters a character domain.
    pub(crate) temporal_string_fsp: Option<u8>,
}

/// Static part of a built-in's bind-time contract.  Argument-dependent
/// functions (CAST, CASE-family, arithmetic, DATE_ADD, JSON_VALUE, aggregates)
/// refine this through the common type algebra; every fixed-contract built-in
/// is described here instead of being independently guessed by protocol,
/// CTAS and executor code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuiltinSpec {
    pub(crate) id: BuiltinId,
    pub(crate) min_arity: u8,
    pub(crate) max_arity: u8,
    pub(crate) volatility: Volatility,
    pub(crate) result: ExprType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BuiltinId {
    HttpRead,
    HttpWrite,
    AiGenerate,
    AiDetail,
    AiEmbed,
    ConnectionId,
    UuidShort,
    Crc32,
    Period,
    YearWeek,
    Coercibility,
    JsonStorage,
    VectorDim,
    StatementCounter,
    Temporal,
    CurrentTemporal,
    Database,
    InetNtoa,
    Bin,
    Hex,
    JsonType,
    Version,
    Uuid,
    IsUuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

/// All session- and scope-dependent inputs used while binding an expression.
/// Keeping these explicit makes type inference deterministic and ensures plan
/// cache keys can account for every variable that changes a descriptor.
pub(crate) struct TypeContext<'a> {
    pub(crate) resolve_column: &'a dyn Fn(&Expr) -> Option<ExprType>,
    pub(crate) resolve_placeholder: &'a dyn Fn(usize) -> Option<ExprType>,
    pub(crate) resolve_subquery: &'a dyn Fn(&crate::parser::Select) -> Option<ExprType>,
    pub(crate) div_precision_increment: u8,
    pub(crate) group_concat_max_len: u64,
    pub(crate) connection_collation_id: u16,
    pub(crate) connection_mbmaxlen: u8,
    pub(crate) no_unsigned_subtraction: bool,
}

impl<'a> TypeContext<'a> {
    pub(crate) fn context_free(
        resolve_column: &'a dyn Fn(&Expr) -> Option<ExprType>,
        resolve_placeholder: &'a dyn Fn(usize) -> Option<ExprType>,
    ) -> Self {
        Self {
            resolve_column,
            resolve_placeholder,
            resolve_subquery: &crate::engine::executor::select::eval::current_scalar_subquery_type,
            div_precision_increment: crate::engine::expr::session_div_precision_increment(),
            group_concat_max_len: crate::engine::executor::select::aggregate::group_concat_max_len()
                as u64,
            connection_collation_id: crate::engine::expr::connection_collation_id(),
            connection_mbmaxlen: crate::engine::expr::connection_charset_max_len(),
            no_unsigned_subtraction: crate::engine::expr::session_no_unsigned_subtraction(),
        }
    }
}

pub(crate) fn fixed_integer_result(
    width: IntegerWidth,
    unsigned: bool,
    nullable: bool,
) -> ExprType {
    ExprType::new(ExprTypeKind::Integer { width, unsigned }, nullable)
}

pub(crate) fn fixed_temporal_result(kind: TemporalKind, nullable: bool) -> ExprType {
    ExprType::new(ExprTypeKind::Temporal { kind, fsp: 0 }, nullable)
}

pub(crate) fn fixed_string_result(max_octets: Option<u32>, nullable: bool) -> ExprType {
    let mut result = ExprType::new(
        ExprTypeKind::String {
            kind: StringKind::Character,
        },
        nullable,
    );
    result.max_octets = max_octets;
    result
}

fn fixed_ai_text_result() -> ExprType {
    let mut result = fixed_string_result(Some(8 * 1024 * 1024), true);
    result.kind = ExprTypeKind::String {
        kind: StringKind::Text,
    };
    result.collation_id = Some(46);
    result
}
fn fixed_ai_vector_result() -> ExprType {
    ExprType::new(
        ExprTypeKind::Vector {
            dim: None,
            datatype: crate::vector::VectorDataType::Float32,
        },
        true,
    )
}

// Fixed bind-time contracts are emitted from the shared declarations;
// argument-dependent inference stays in the type algebra.
crate::engine::builtin_catalog::declare_builtin_spec!();

impl ExprType {
    pub(crate) const fn new(kind: ExprTypeKind, nullable: bool) -> Self {
        Self {
            temporal_string_fsp: None,
            dynamic_parameter: false,
            kind,
            nullable,
            max_octets: None,
            collation_id: None,
            coercibility: 5,
        }
    }

    pub(crate) fn from_column_type(column_type: ColumnType, source: &ColumnSource) -> Self {
        if source.dynamic_parameter {
            let mut result = Self::new(ExprTypeKind::Unknown, true);
            result.dynamic_parameter = true;
            return result;
        }
        use crate::mysql::protocol::{ENUM_FLAG, SET_FLAG, UNSIGNED_FLAG};
        if source.wire_type == Some(6) {
            return Self::new(ExprTypeKind::Null, true);
        }
        let unsigned = source.flags & UNSIGNED_FLAG != 0;
        let kind = match column_type {
            ColumnType::Tiny => ExprTypeKind::Integer {
                width: IntegerWidth::Tiny,
                unsigned,
            },
            ColumnType::Short => ExprTypeKind::Integer {
                width: IntegerWidth::Small,
                unsigned,
            },
            ColumnType::MediumInt => ExprTypeKind::Integer {
                width: IntegerWidth::Medium,
                unsigned,
            },
            ColumnType::Long => ExprTypeKind::Integer {
                width: IntegerWidth::Int,
                unsigned,
            },
            ColumnType::LongLong => ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned,
            },
            ColumnType::Float | ColumnType::Double => ExprTypeKind::Float {
                single: column_type == ColumnType::Float,
                precision: source.numeric_precision,
                scale: source.decimals.filter(|scale| *scale != 31),
            },
            ColumnType::Decimal { precision, scale } => ExprTypeKind::Decimal {
                precision,
                scale,
                unsigned,
            },
            ColumnType::Date => ExprTypeKind::Temporal {
                kind: TemporalKind::Date,
                fsp: 0,
            },
            ColumnType::Time => ExprTypeKind::Temporal {
                kind: TemporalKind::Time,
                fsp: source.decimals.unwrap_or(0).min(6),
            },
            ColumnType::DateTime => ExprTypeKind::Temporal {
                kind: TemporalKind::DateTime,
                fsp: source.decimals.unwrap_or(0).min(6),
            },
            ColumnType::Timestamp => ExprTypeKind::Temporal {
                kind: TemporalKind::Timestamp,
                fsp: source.decimals.unwrap_or(0).min(6),
            },
            ColumnType::Year => ExprTypeKind::Year,
            ColumnType::Bit { bits } => ExprTypeKind::Bit { bits },
            ColumnType::VarString => ExprTypeKind::String {
                kind: if source.flags & ENUM_FLAG != 0 {
                    StringKind::Enum
                } else if source.flags & SET_FLAG != 0 {
                    StringKind::Set
                } else {
                    StringKind::Character
                },
            },
            ColumnType::Blob => ExprTypeKind::String {
                // The wire BLOB tag represents both TEXT and binary BLOB.
                // Preserve the semantic family from flags/collation when a
                // derived or set-operation schema is rebound.
                kind: if source.flags & crate::mysql::protocol::BINARY_FLAG != 0
                    || source.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
                {
                    StringKind::Blob
                } else {
                    StringKind::Text
                },
            },
            ColumnType::Json => ExprTypeKind::Json,
            ColumnType::Geometry => ExprTypeKind::Geometry {
                subtype: GeometrySubtype::Geometry,
                srid: None,
            },
            ColumnType::Vector => {
                let (datatype, dim) = source
                    .vector_type
                    .unwrap_or((crate::vector::VectorDataType::Float32, None));
                ExprTypeKind::Vector { dim, datatype }
            }
        };
        Self {
            temporal_string_fsp: None,
            dynamic_parameter: false,
            kind,
            nullable: source.flags & crate::mysql::protocol::NOT_NULL_FLAG == 0,
            max_octets: source.length,
            collation_id: source.collation_id,
            coercibility: source
                .collation_coercibility
                .unwrap_or(if source.org_name.is_empty() { 4 } else { 2 }),
        }
    }

    pub(crate) fn from_data_type(
        data_type: &DataTypeKind,
        unsigned: bool,
        nullable: bool,
        max_octets: Option<u32>,
        collation_id: Option<u16>,
    ) -> Self {
        let kind = match data_type {
            DataTypeKind::TinyInt | DataTypeKind::Bool => ExprTypeKind::Integer {
                width: IntegerWidth::Tiny,
                unsigned,
            },
            DataTypeKind::SmallInt => ExprTypeKind::Integer {
                width: IntegerWidth::Small,
                unsigned,
            },
            DataTypeKind::MediumInt => ExprTypeKind::Integer {
                width: IntegerWidth::Medium,
                unsigned,
            },
            DataTypeKind::Int => ExprTypeKind::Integer {
                width: IntegerWidth::Int,
                unsigned,
            },
            DataTypeKind::BigInt => ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned,
            },
            DataTypeKind::Float { precision, scale } => ExprTypeKind::Float {
                single: true,
                precision: *precision,
                scale: *scale,
            },
            DataTypeKind::Double { precision, scale } => ExprTypeKind::Float {
                single: false,
                precision: *precision,
                scale: *scale,
            },
            DataTypeKind::Decimal { precision, scale } => ExprTypeKind::Decimal {
                precision: precision.unwrap_or(MAX_DECIMAL_PRECISION),
                scale: scale.unwrap_or(0),
                unsigned,
            },
            DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } => ExprTypeKind::String {
                kind: StringKind::Character,
            },
            DataTypeKind::Text { .. } => ExprTypeKind::String {
                kind: StringKind::Text,
            },
            DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } => ExprTypeKind::String {
                kind: StringKind::Binary,
            },
            DataTypeKind::Blob { .. } => ExprTypeKind::String {
                kind: StringKind::Blob,
            },
            DataTypeKind::Date => ExprTypeKind::Temporal {
                kind: TemporalKind::Date,
                fsp: 0,
            },
            DataTypeKind::Time { fsp } => ExprTypeKind::Temporal {
                kind: TemporalKind::Time,
                fsp: *fsp,
            },
            DataTypeKind::DateTime { fsp } => ExprTypeKind::Temporal {
                kind: TemporalKind::DateTime,
                fsp: *fsp,
            },
            DataTypeKind::Timestamp { fsp } => ExprTypeKind::Temporal {
                kind: TemporalKind::Timestamp,
                fsp: *fsp,
            },
            DataTypeKind::Year => ExprTypeKind::Year,
            DataTypeKind::Json => ExprTypeKind::Json,
            DataTypeKind::Geometry { subtype, srid } => ExprTypeKind::Geometry {
                subtype: *subtype,
                srid: *srid,
            },
            DataTypeKind::Vector { dim, datatype } => ExprTypeKind::Vector {
                dim: *dim,
                datatype: *datatype,
            },
            DataTypeKind::Enum { .. } => ExprTypeKind::String {
                kind: StringKind::Enum,
            },
            DataTypeKind::Set { .. } => ExprTypeKind::String {
                kind: StringKind::Set,
            },
            DataTypeKind::Bit { len } => ExprTypeKind::Bit {
                bits: len.unwrap_or(1).clamp(1, 64) as u8,
            },
        };
        Self {
            temporal_string_fsp: None,
            dynamic_parameter: false,
            kind,
            nullable,
            max_octets,
            collation_id,
            coercibility: 2,
        }
    }

    pub(crate) const fn column_type(self) -> ColumnType {
        match self.kind {
            ExprTypeKind::Integer { width, .. } => match width {
                IntegerWidth::Tiny => ColumnType::Tiny,
                IntegerWidth::Small => ColumnType::Short,
                IntegerWidth::Medium => ColumnType::MediumInt,
                IntegerWidth::Int => ColumnType::Long,
                IntegerWidth::Big => ColumnType::LongLong,
            },
            ExprTypeKind::Decimal {
                precision, scale, ..
            } => ColumnType::Decimal { precision, scale },
            ExprTypeKind::Float { single: true, .. } => ColumnType::Float,
            ExprTypeKind::Float { single: false, .. } => ColumnType::Double,
            ExprTypeKind::Bit { bits } => ColumnType::Bit { bits },
            ExprTypeKind::Year => ColumnType::Year,
            ExprTypeKind::String {
                kind: StringKind::Binary | StringKind::Text | StringKind::Blob,
            } => ColumnType::Blob,
            ExprTypeKind::String { .. } | ExprTypeKind::Null | ExprTypeKind::Unknown => {
                ColumnType::VarString
            }
            ExprTypeKind::Temporal {
                kind: TemporalKind::Date,
                ..
            } => ColumnType::Date,
            ExprTypeKind::Temporal {
                kind: TemporalKind::Time,
                ..
            } => ColumnType::Time,
            ExprTypeKind::Temporal {
                kind: TemporalKind::DateTime,
                ..
            } => ColumnType::DateTime,
            ExprTypeKind::Temporal {
                kind: TemporalKind::Timestamp,
                ..
            } => ColumnType::Timestamp,
            ExprTypeKind::Json => ColumnType::Json,
            ExprTypeKind::Geometry { .. } => ColumnType::Geometry,
            ExprTypeKind::Vector { .. } => ColumnType::Vector,
        }
    }

    pub(crate) fn apply_to_source(self, source: &mut ColumnSource) {
        source.vector_type = if let ExprTypeKind::Vector { datatype, dim } = self.kind {
            Some((datatype, dim))
        } else {
            None
        };
        source.dynamic_parameter =
            self.dynamic_parameter && matches!(self.kind, ExprTypeKind::Unknown);
        use crate::mysql::protocol::{BINARY_FLAG, NOT_NULL_FLAG, UNSIGNED_FLAG};
        if matches!(self.kind, ExprTypeKind::Null) {
            source.wire_type = Some(6);
        } else if source.wire_type == Some(6) {
            source.wire_type = None;
        }
        source.flags &= !(UNSIGNED_FLAG | NOT_NULL_FLAG);
        if !self.nullable {
            source.flags |= NOT_NULL_FLAG;
        }
        match self.kind {
            ExprTypeKind::Integer { unsigned, .. } | ExprTypeKind::Decimal { unsigned, .. } => {
                source.flags |= BINARY_FLAG;
                if unsigned {
                    source.flags |= UNSIGNED_FLAG;
                }
            }
            ExprTypeKind::Float { .. } => source.flags |= BINARY_FLAG,
            ExprTypeKind::Bit { .. } | ExprTypeKind::Year => {
                source.flags |= BINARY_FLAG | UNSIGNED_FLAG;
            }
            ExprTypeKind::String {
                kind: StringKind::Binary | StringKind::Blob,
            } => {
                source.flags |= BINARY_FLAG;
            }
            _ => {}
        }
        if let Some(max_octets) = self.max_octets {
            // Numeric display widths are semantic (a comparison is one
            // digit), so the descriptor replaces legacy BIGINT defaults.
            // Strings can have an independently computed charset-expanded
            // bound, where retaining the larger complete bound is required.
            source.length = Some(if matches!(self.kind, ExprTypeKind::String { .. }) {
                source.length.map_or(max_octets, |old| old.max(max_octets))
            } else {
                max_octets
            });
        } else if let ExprTypeKind::Decimal {
            precision, scale, ..
        } = self.kind
        {
            source.length = Some(u32::from(precision) + if scale > 0 { 2 } else { 1 });
        }
        if matches!(
            self.kind,
            ExprTypeKind::Integer { .. }
                | ExprTypeKind::Decimal { .. }
                | ExprTypeKind::Float { .. }
                | ExprTypeKind::Bit { .. }
                | ExprTypeKind::Year
        ) {
            // Numeric domains have no text collation. The protocol defaults
            // them to binary; storing binary here would contaminate UNION's
            // character-domain collation aggregation.
            source.collation_id = None;
            source.collation_coercibility = None;
            source.decimals = Some(match self.kind {
                ExprTypeKind::Decimal { scale, .. } | ExprTypeKind::Temporal { fsp: scale, .. } => {
                    scale
                }
                ExprTypeKind::Float { scale, .. } => scale.unwrap_or(31),
                _ => 0,
            });
        } else if self.collation_id.is_some() {
            source.collation_id = self.collation_id;
            source.collation_coercibility = Some(self.coercibility);
        }
        match self.kind {
            ExprTypeKind::Float {
                precision, scale, ..
            } => {
                if precision.is_some() {
                    source.numeric_precision = precision;
                }
                if scale.is_some() {
                    source.decimals = scale;
                }
            }
            ExprTypeKind::Decimal { scale, .. } | ExprTypeKind::Temporal { fsp: scale, .. } => {
                source.decimals = Some(scale)
            }
            _ => {}
        }
    }
}

/// MySQL CASE/IF/COALESCE type aggregation for the semantic type families.
/// Literal NULL operands are omitted by callers.
pub(crate) fn aggregate_branch_types(left: ExprType, right: ExprType) -> ExprType {
    use ExprTypeKind::*;
    let nullable = left.nullable || right.nullable;
    let mut out = match (left.kind, right.kind) {
        (Null, other) => ExprType::new(other, true),
        (other, Null) => ExprType::new(other, true),
        (a, b) if a == b => ExprType::new(a, nullable),
        (
            Vector {
                dim: ld,
                datatype: lt,
            },
            Vector {
                dim: rd,
                datatype: rt,
            },
        ) => ExprType::new(
            Vector {
                dim: if ld == rd { ld } else { None },
                datatype: if lt == rt {
                    lt
                } else {
                    crate::vector::VectorDataType::Float32
                },
            },
            nullable,
        ),
        (
            Integer {
                width: lw,
                unsigned: lu,
            },
            Integer {
                width: rw,
                unsigned: ru,
            },
        ) => {
            let width = lw.max(rw);
            if lu == ru {
                ExprType::new(
                    Integer {
                        width,
                        unsigned: lu,
                    },
                    nullable,
                )
            } else if width == IntegerWidth::Big && (lu || ru) {
                ExprType::new(
                    Decimal {
                        precision: 21,
                        scale: 0,
                        unsigned: false,
                    },
                    nullable,
                )
            } else {
                ExprType::new(
                    Integer {
                        width: width.next(),
                        unsigned: false,
                    },
                    nullable,
                )
            }
        }
        (
            Decimal {
                precision: lp,
                scale: ls,
                ..
            },
            Decimal {
                precision: rp,
                scale: rs,
                ..
            },
        ) => {
            let scale = ls.max(rs);
            let integer_digits = lp.saturating_sub(ls).max(rp.saturating_sub(rs));
            ExprType::new(
                Decimal {
                    precision: integer_digits
                        .saturating_add(scale)
                        .min(MAX_DECIMAL_PRECISION),
                    scale,
                    unsigned: false,
                },
                nullable,
            )
        }
        (
            Decimal {
                precision, scale, ..
            },
            Integer { width, .. },
        )
        | (
            Integer { width, .. },
            Decimal {
                precision, scale, ..
            },
        ) => {
            let integer_digits = precision
                .saturating_sub(scale)
                .max(width.decimal_precision());
            ExprType::new(
                Decimal {
                    precision: integer_digits
                        .saturating_add(scale)
                        .min(MAX_DECIMAL_PRECISION),
                    scale,
                    unsigned: false,
                },
                nullable,
            )
        }
        (Float { .. }, Integer { .. } | Decimal { .. } | Float { .. })
        | (Integer { .. } | Decimal { .. }, Float { .. }) => ExprType::new(
            Float {
                single: false,
                precision: None,
                scale: None,
            },
            nullable,
        ),
        (Bit { bits: l }, Bit { bits: r }) => ExprType::new(Bit { bits: l.max(r) }, nullable),
        (
            Year,
            Integer {
                width, unsigned, ..
            },
        )
        | (
            Integer {
                width, unsigned, ..
            },
            Year,
        ) => ExprType::new(
            Integer {
                width: width.max(IntegerWidth::Small),
                unsigned,
            },
            nullable,
        ),
        (Year, Year) => ExprType::new(Year, nullable),
        (Temporal { kind: lk, fsp: lf }, Temporal { kind: rk, fsp: rf }) => {
            let kind = if lk == rk { lk } else { TemporalKind::DateTime };
            ExprType::new(
                Temporal {
                    kind,
                    fsp: lf.max(rf),
                },
                nullable,
            )
        }
        (Json, Json) => ExprType::new(Json, nullable),
        (Geometry { .. }, Geometry { .. }) => ExprType::new(
            Geometry {
                subtype: GeometrySubtype::Geometry,
                srid: None,
            },
            nullable,
        ),
        (String { kind: lk }, String { kind: rk }) => {
            let kind = if lk == rk {
                lk
            } else if matches!(lk, StringKind::Binary | StringKind::Blob)
                || matches!(rk, StringKind::Binary | StringKind::Blob)
            {
                StringKind::Binary
            } else {
                StringKind::Character
            };
            ExprType::new(String { kind }, nullable)
        }
        _ => ExprType::new(
            String {
                kind: StringKind::Character,
            },
            nullable,
        ),
    };
    out.max_octets = match out.kind {
        Decimal {
            precision, scale, ..
        } => Some(u32::from(precision) + if scale > 0 { 2 } else { 1 }),
        Temporal { kind, fsp } => {
            let base = match kind {
                TemporalKind::Date => 10,
                TemporalKind::Time => 10,
                TemporalKind::DateTime | TemporalKind::Timestamp => 19,
            };
            Some(base + if fsp > 0 { u32::from(fsp) + 1 } else { 0 })
        }
        String { .. } => Some(match (left.kind, right.kind) {
            // NULL contributes no bytes. An unknown non-NULL branch, however,
            // must not borrow a shorter known branch's bound: CTAS/derived
            // consumers need a capacity valid for every possible result.
            (Null, _) => right.max_octets.unwrap_or(u32::MAX),
            (_, Null) => left.max_octets.unwrap_or(u32::MAX),
            _ => left
                .max_octets
                .unwrap_or(u32::MAX)
                .max(right.max_octets.unwrap_or(u32::MAX)),
        }),
        _ => match (left.max_octets, right.max_octets) {
            (Some(l), Some(r)) => Some(l.max(r)),
            (l, r) => l.or(r),
        },
    };
    out.collation_id = if left.coercibility <= right.coercibility {
        left.collation_id
    } else {
        right.collation_id
    };
    out.coercibility = left.coercibility.min(right.coercibility);
    if matches!(out.kind, String { .. }) {
        let fsp = |value: ExprType| match value.kind {
            Temporal { fsp, .. } => Some(fsp),
            _ => value.temporal_string_fsp,
        };
        out.temporal_string_fsp = fsp(left).into_iter().chain(fsp(right)).max();
    }
    out.dynamic_parameter =
        matches!(out.kind, Unknown) && (left.dynamic_parameter || right.dynamic_parameter);
    out
}

/// Resolve character domains using the same fallible collation algebra as
/// scalar comparisons. Descriptors, not runtime rows, determine the domain.
pub(crate) fn checked_branch_types(
    left: ExprType,
    right: ExprType,
) -> Result<ExprType, crate::types::QueryError> {
    if let (ExprTypeKind::Vector { datatype: l, .. }, ExprTypeKind::Vector { datatype: r, .. }) =
        (left.kind, right.kind)
    {
        if (l == crate::vector::VectorDataType::Binary)
            != (r == crate::vector::VectorDataType::Binary)
        {
            return Err(crate::engine::vector::vector_err(
                "cannot merge BINARY and numeric vector types",
            ));
        }
    }
    let mut result = aggregate_branch_types(left, right);
    if let ExprTypeKind::Vector {
        dim: Some(dim),
        datatype,
    } = result.kind
    {
        if dim > datatype.max_dimension() {
            return Err(crate::engine::vector::vector_err(
                "merged vector exceeds target datatype dimension limit",
            ));
        }
    }
    if matches!(result.kind, ExprTypeKind::String { .. }) {
        let operands: Vec<Expr> = [left, right]
            .iter()
            .filter_map(|ty| {
                if !matches!(ty.kind, ExprTypeKind::String { .. }) {
                    return None;
                }
                let info = ty
                    .collation_id
                    .and_then(crate::charset::resolve_collation_id)?;
                Some(Expr::Function {
                    name: "__COLLATED_CONST".into(),
                    args: vec![
                        Expr::Literal(Literal::String(String::new())),
                        Expr::Literal(Literal::String(info.name.into())),
                        Expr::Literal(Literal::Int(i64::from(ty.coercibility))),
                    ],
                    distinct: false,
                })
            })
            .collect();
        if !operands.is_empty() {
            result.coercibility = [left, right]
                .iter()
                .filter(|ty| matches!(ty.kind, ExprTypeKind::String { .. }))
                .map(|ty| ty.coercibility)
                .min()
                .unwrap_or(4);
            result.collation_id = crate::engine::expr::aggregate_static_expr_collations(
                &operands.iter().collect::<Vec<_>>(),
                "UNION",
            )?;
        }
    }
    Ok(result)
}

fn literal_type(literal: &Literal, context: &TypeContext<'_>) -> ExprType {
    match literal {
        Literal::Null => ExprType::new(ExprTypeKind::Null, true),
        Literal::Bool(_) => integer_result(IntegerWidth::Big, false, false, Some(1)),
        Literal::Int(value) => integer_result(
            IntegerWidth::Big,
            false,
            false,
            Some(value.to_string().len().min(u32::MAX as usize) as u32),
        ),
        Literal::UInt(value) => integer_result(
            IntegerWidth::Big,
            true,
            false,
            Some(value.to_string().len().min(u32::MAX as usize) as u32),
        ),
        Literal::Float(_) => {
            let mut result = ExprType::new(
                ExprTypeKind::Float {
                    single: false,
                    precision: None,
                    scale: None,
                },
                false,
            );
            result.max_octets = Some(23);
            result
        }
        Literal::Decimal(_) => {
            let ColumnType::Decimal { precision, scale } =
                column_type::expr_result_type(&Expr::Literal(literal.clone()))
            else {
                unreachable!("decimal literal must have an exact descriptor")
            };
            ExprType::new(
                ExprTypeKind::Decimal {
                    precision,
                    scale,
                    unsigned: false,
                },
                false,
            )
        }
        Literal::String(value) => {
            let mut result = ExprType::new(
                ExprTypeKind::String {
                    kind: StringKind::Character,
                },
                false,
            );
            result.max_octets = Some(
                u32::try_from(value.chars().count())
                    .unwrap_or(u32::MAX)
                    .saturating_mul(u32::from(context.connection_mbmaxlen)),
            );
            result.collation_id = Some(context.connection_collation_id);
            result.coercibility = 4;
            result
        }
        Literal::HexBytes(value) | Literal::BitBytes(value) => {
            let mut result = ExprType::new(
                ExprTypeKind::String {
                    kind: StringKind::Binary,
                },
                false,
            );
            result.max_octets = Some(u32::try_from(value.len()).unwrap_or(u32::MAX));
            result.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
            result.coercibility = 4;
            result
        }
    }
}

fn integer_result(
    width: IntegerWidth,
    unsigned: bool,
    nullable: bool,
    display_length: Option<u32>,
) -> ExprType {
    let mut result = ExprType::new(ExprTypeKind::Integer { width, unsigned }, nullable);
    result.max_octets = display_length;
    result
}

fn boolean_result(nullable: bool) -> ExprType {
    integer_result(IntegerWidth::Big, false, nullable, Some(1))
}

fn bound_value_type(value: &Value, context: &TypeContext<'_>) -> ExprType {
    let mut source = ColumnSource::default();
    if let Value::Vector(v) = value {
        source.vector_type = Some((v.datatype, Some(v.dimension)));
    }
    source.flags = if matches!(value, Value::Null) {
        0
    } else {
        crate::mysql::protocol::NOT_NULL_FLAG
    };
    let mut result = ExprType::from_column_type(
        column_type::expr_result_type(&Expr::BoundValue(value.clone())),
        &source,
    );
    match value {
        Value::Null => return ExprType::new(ExprTypeKind::Null, true),
        Value::Int(value) => result.max_octets = Some(value.to_string().len() as u32),
        Value::UInt(value) => {
            result.max_octets = Some(value.to_string().len() as u32);
            if let ExprTypeKind::Integer { unsigned, .. } | ExprTypeKind::Decimal { unsigned, .. } =
                &mut result.kind
            {
                *unsigned = true;
            }
        }
        Value::String(value) => {
            result.max_octets = Some(
                u32::try_from(value.chars().count())
                    .unwrap_or(u32::MAX)
                    .saturating_mul(u32::from(context.connection_mbmaxlen)),
            );
            result.collation_id = Some(context.connection_collation_id);
        }
        Value::Decimal(value) => {
            result.max_octets = Some(u32::try_from(value.len()).unwrap_or(u32::MAX));
        }
        Value::SmallString(value) => {
            result.max_octets = Some(u32::try_from(value.len()).unwrap_or(u32::MAX));
            result.collation_id = Some(context.connection_collation_id);
        }
        Value::Bytes(value) => {
            result.kind = ExprTypeKind::String {
                kind: StringKind::Blob,
            };
            result.max_octets = Some(u32::try_from(value.len()).unwrap_or(u32::MAX));
            result.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
        }
        _ => {}
    }
    result
}

/// Stable identity of the currently bound parameter descriptors.
///
/// `infer_expr_type(Placeholder)` observes the live parameter values, so a
/// compiled program carrying that inferred descriptor must never be reused by
/// a binding with a different descriptor.  Hash descriptors rather than raw
/// payloads: equal SQL types/widths share programs while signedness, DECIMAL
/// scale, string/binary width and literal-vs-wire provenance remain isolated.
pub(crate) fn current_parameter_type_fingerprint() -> u64 {
    use std::cell::Cell;
    use std::hash::{BuildHasher, Hash, Hasher};
    use std::sync::OnceLock;

    // Every compiled expression asks for this; hashing all descriptors each
    // time is quadratic in the number of (auto-)parameters of a statement.
    // The fingerprint is constant for one installation and provenance.
    thread_local! {
        static CACHED: Cell<Option<(u64, bool, u64)>> = const { Cell::new(None) };
    }
    let generation = crate::engine::params::current_params_generation();
    let auto_parameterized = crate::engine::params::auto_parameterized_active();
    if let Some((cached_generation, cached_auto, fingerprint)) = CACHED.with(Cell::get) {
        if cached_generation == generation && cached_auto == auto_parameterized {
            return fingerprint;
        }
    }

    static STATE: OnceLock<ahash::RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        ahash::RandomState::with_seeds(
            0x4D59_4442_5041_5241,
            0x4D54_5950_4553_4947,
            0x243F_6A88_85A3_08D3,
            0x1319_8A2E_0370_7344,
        )
    });
    let fingerprint = crate::engine::params::with_current_params(|params, auto_parameterized| {
        let mut hasher = state.build_hasher();
        params.is_some().hash(&mut hasher);
        auto_parameterized.hash(&mut hasher);
        let Some(params) = params else {
            return hasher.finish();
        };
        params.len().hash(&mut hasher);
        let no_column = |_: &Expr| None;
        let no_placeholder = |_: usize| None;
        let context = TypeContext::context_free(&no_column, &no_placeholder);
        for value in params {
            bound_value_type(value, &context).hash(&mut hasher);
            let shape = match value {
                Value::String(s) => crate::vector::text_parameter_shape(s.as_bytes()),
                Value::Bytes(b) => crate::vector::text_parameter_shape(b),
                Value::SmallString(s) => crate::vector::text_parameter_shape(s.as_ref()),
                _ => None,
            };
            shape.hash(&mut hasher);
        }
        hasher.finish()
    });
    CACHED.with(|cached| cached.set(Some((generation, auto_parameterized, fingerprint))));
    fingerprint
}

fn numeric_decimal_spec(kind: ExprTypeKind) -> Option<crate::util::decimal::DecimalResultSpec> {
    match kind {
        ExprTypeKind::Integer { width, .. } => Some(crate::util::decimal::DecimalResultSpec::new(
            width.decimal_precision(),
            0,
        )),
        ExprTypeKind::Decimal {
            precision, scale, ..
        } => Some(crate::util::decimal::DecimalResultSpec::new(
            precision, scale,
        )),
        ExprTypeKind::Bit { bits } => Some(crate::util::decimal::DecimalResultSpec::new(
            if bits == 64 {
                20
            } else {
                ((u32::from(bits) * 30_103 + 99_999) / 100_000) as u8
            },
            0,
        )),
        ExprTypeKind::Temporal { kind, fsp } => Some(crate::util::decimal::DecimalResultSpec::new(
            match kind {
                TemporalKind::Date => 8,
                TemporalKind::Time => 7,
                _ => 14,
            } + fsp,
            fsp,
        )),
        ExprTypeKind::Year => Some(crate::util::decimal::DecimalResultSpec::new(4, 0)),
        _ => None,
    }
}

fn binary_type(
    op: BinaryOp,
    mut left: ExprType,
    mut right: ExprType,
    context: &TypeContext<'_>,
) -> ExprType {
    use crate::util::decimal::DecimalResultSpec;
    use ExprTypeKind::*;
    if matches!(
        op,
        BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::IntDiv
            | BinaryOp::Mod
    ) {
        for operand in [&mut left, &mut right] {
            if let String {
                kind: kind @ (StringKind::Enum | StringKind::Set),
            } = operand.kind
            {
                operand.kind = Integer {
                    width: IntegerWidth::Big,
                    unsigned: kind == StringKind::Set,
                };
            }
        }
    }
    // Division-family operators have a NULL result in their declared domain
    // even when the currently bound denominator is a non-zero literal: a
    // parameter, later rebind, or constant-folding decision must not change
    // result metadata. MySQL exposes `/`, `DIV` and `MOD` as nullable for this
    // reason (division by zero produces NULL under the applicable sql_mode).
    let nullable = left.nullable
        || right.nullable
        || matches!(op, BinaryOp::Div | BinaryOp::IntDiv | BinaryOp::Mod);
    match op {
        BinaryOp::NullSafeEq => boolean_result(false),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Xor
        | BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => boolean_result(nullable),
        BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => integer_result(IntegerWidth::Big, true, nullable, Some(21)),
        BinaryOp::IntDiv => integer_result(
            IntegerWidth::Big,
            matches!(left.kind, Integer { unsigned: true, .. })
                || matches!(right.kind, Integer { unsigned: true, .. }),
            true,
            Some(
                left.max_octets
                    .unwrap_or(1)
                    .max(right.max_octets.unwrap_or(1))
                    .saturating_add(1),
            ),
        ),
        BinaryOp::Arrow => ExprType::new(Json, true),
        BinaryOp::LongArrow => {
            let mut result = ExprType::new(
                String {
                    kind: StringKind::Character,
                },
                true,
            );
            result.collation_id = Some(context.connection_collation_id);
            result
        }
        BinaryOp::Concat => {
            let mut result = ExprType::new(
                String {
                    kind: StringKind::Character,
                },
                nullable,
            );
            result.max_octets = left
                .max_octets
                .zip(right.max_octets)
                .map(|(left, right)| left.saturating_add(right));
            result.collation_id = left.collation_id.or(right.collation_id);
            result.coercibility = left.coercibility.min(right.coercibility);
            result
        }
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            if matches!(left.kind, Float { .. }) || matches!(right.kind, Float { .. }) {
                let mut result = ExprType::new(
                    Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    nullable,
                );
                result.max_octets = Some(23);
                return result;
            }
            let Some(left_spec) = numeric_decimal_spec(left.kind) else {
                let mut result = ExprType::new(
                    Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    nullable,
                );
                result.max_octets = Some(23);
                return result;
            };
            let Some(right_spec) = numeric_decimal_spec(right.kind) else {
                let mut result = ExprType::new(
                    Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    nullable,
                );
                result.max_octets = Some(23);
                return result;
            };
            if matches!(
                left.kind,
                Integer { .. } | Year | Bit { .. } | Temporal { fsp: 0, .. }
            ) && matches!(
                right.kind,
                Integer { .. } | Year | Bit { .. } | Temporal { fsp: 0, .. }
            ) && !matches!(op, BinaryOp::Div)
            {
                let unsigned = match op {
                    BinaryOp::Sub if context.no_unsigned_subtraction => false,
                    BinaryOp::Mod => matches!(left.kind, Integer { unsigned: true, .. }),
                    _ => {
                        matches!(left.kind, Integer { unsigned: true, .. })
                            || matches!(right.kind, Integer { unsigned: true, .. })
                    }
                };
                let mut result = ExprType::new(
                    Integer {
                        width: IntegerWidth::Big,
                        unsigned,
                    },
                    nullable,
                );
                result.max_octets = match op {
                    BinaryOp::Add | BinaryOp::Sub => Some(
                        left.max_octets
                            .unwrap_or(20)
                            .max(right.max_octets.unwrap_or(20))
                            .saturating_add(1),
                    ),
                    BinaryOp::Mul => Some(
                        left.max_octets
                            .unwrap_or(20)
                            .saturating_add(right.max_octets.unwrap_or(20)),
                    ),
                    BinaryOp::Mod => left.max_octets,
                    _ => None,
                };
                return result;
            }
            let spec: DecimalResultSpec = match op {
                BinaryOp::Add | BinaryOp::Sub => DecimalResultSpec::add_sub(left_spec, right_spec),
                BinaryOp::Mul => DecimalResultSpec::multiply(left_spec, right_spec),
                BinaryOp::Div => DecimalResultSpec::divide(
                    left_spec,
                    right_spec,
                    context.div_precision_increment,
                ),
                BinaryOp::Mod => DecimalResultSpec::modulo(left_spec, right_spec),
                _ => unreachable!(),
            };
            ExprType::new(
                Decimal {
                    precision: spec.precision,
                    scale: spec.scale,
                    unsigned: false,
                },
                nullable,
            )
        }
    }
}

fn cast_type(args: &[Expr], input: ExprType, context: &TypeContext<'_>) -> ExprType {
    let target = match args.get(1) {
        Some(Expr::Literal(Literal::String(target))) => target.to_ascii_uppercase(),
        _ => return ExprType::new(ExprTypeKind::Unknown, true),
    };
    let nullable = input.nullable;
    let integer_arg = |index: usize| match args.get(index) {
        Some(Expr::Literal(Literal::Int(value))) => u8::try_from(*value).ok(),
        Some(Expr::Literal(Literal::UInt(value))) => u8::try_from(*value).ok(),
        _ => None,
    };
    let kind = match target.as_str() {
        "SIGNED" => ExprTypeKind::Integer {
            width: IntegerWidth::Big,
            unsigned: false,
        },
        "UNSIGNED" => ExprTypeKind::Integer {
            width: IntegerWidth::Big,
            unsigned: true,
        },
        "YEAR" => ExprTypeKind::Year,
        "DECIMAL" => ExprTypeKind::Decimal {
            precision: integer_arg(2).unwrap_or(10).clamp(1, MAX_DECIMAL_PRECISION),
            scale: integer_arg(3)
                .unwrap_or(0)
                .min(crate::util::decimal::MAX_DECIMAL_SCALE),
            unsigned: false,
        },
        "FLOAT" => ExprTypeKind::Float {
            single: true,
            precision: None,
            scale: None,
        },
        "DOUBLE" | "REAL" => ExprTypeKind::Float {
            single: false,
            precision: None,
            scale: None,
        },
        "DATE" => ExprTypeKind::Temporal {
            kind: TemporalKind::Date,
            fsp: 0,
        },
        "TIME" => ExprTypeKind::Temporal {
            kind: TemporalKind::Time,
            fsp: integer_arg(2).unwrap_or(0).min(6),
        },
        "DATETIME" => ExprTypeKind::Temporal {
            kind: TemporalKind::DateTime,
            fsp: integer_arg(2).unwrap_or(0).min(6),
        },
        "JSON" => ExprTypeKind::Json,
        "VECTOR" => {
            let dim = args.get(2).and_then(|e| match e {
                Expr::Literal(Literal::Int(v)) => u32::try_from(*v).ok(),
                _ => None,
            });
            let datatype = args
                .get(3)
                .and_then(|e| match e {
                    Expr::Literal(Literal::String(s)) => crate::vector::VectorDataType::parse(s),
                    _ => None,
                })
                .unwrap_or_default();
            ExprTypeKind::Vector { dim, datatype }
        }
        "BINARY" => ExprTypeKind::String {
            kind: StringKind::Binary,
        },
        _ => ExprTypeKind::String {
            kind: StringKind::Character,
        },
    };
    let mut result = ExprType::new(kind, nullable);
    if matches!(
        kind,
        ExprTypeKind::String {
            kind: StringKind::Character
        }
    ) {
        result.collation_id = Some(context.connection_collation_id);
    } else if matches!(
        kind,
        ExprTypeKind::String {
            kind: StringKind::Binary
        }
    ) {
        result.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
    }
    result
}

/// Shared aggregate/window result algebra.  Both row and vector executors use
/// this function so their output schemas cannot drift from each other.
pub(crate) fn aggregate_result_type(
    name: &str,
    argument: Option<ExprType>,
    context: &TypeContext<'_>,
) -> ExprType {
    let normalized = crate::engine::sys_schema::NormalizedFuncName::new(name);
    match normalized.as_str() {
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => argument
            .filter(|arg| {
                matches!(
                    arg.kind,
                    ExprTypeKind::String {
                        kind: StringKind::Binary | StringKind::Blob
                    }
                )
            })
            .unwrap_or_else(|| fixed_integer_result(IntegerWidth::Big, true, false)),
        "COUNT" => ExprType::new(
            ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned: false,
            },
            false,
        ),
        "MIN" | "MAX" | "__HIDDEN_FIRST_VALUE" => {
            let mut result = argument.unwrap_or_else(|| ExprType::new(ExprTypeKind::Unknown, true));
            result.nullable = true;
            result
        }
        "SUM" | "AVG" => {
            let avg = normalized.as_str() == "AVG";
            let argument = argument.unwrap_or_else(|| ExprType::new(ExprTypeKind::Unknown, true));
            match argument.kind {
                ExprTypeKind::Float { .. } => ExprType::new(
                    ExprTypeKind::Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    true,
                ),
                ExprTypeKind::Decimal {
                    precision, scale, ..
                } => {
                    let scale = if avg {
                        scale
                            .saturating_add(context.div_precision_increment)
                            .min(crate::util::decimal::MAX_DECIMAL_SCALE)
                    } else {
                        scale
                    };
                    let precision = if avg {
                        precision.saturating_add(context.div_precision_increment)
                    } else {
                        precision.saturating_add(22)
                    }
                    .min(MAX_DECIMAL_PRECISION)
                    .max(scale);
                    ExprType::new(
                        ExprTypeKind::Decimal {
                            precision,
                            scale,
                            unsigned: false,
                        },
                        true,
                    )
                }
                ExprTypeKind::Integer { width, .. } => {
                    let scale = if avg {
                        context.div_precision_increment
                    } else {
                        0
                    };
                    let precision = if avg {
                        width.decimal_precision().saturating_add(scale)
                    } else {
                        width.decimal_precision().saturating_add(22)
                    }
                    .min(MAX_DECIMAL_PRECISION);
                    ExprType::new(
                        ExprTypeKind::Decimal {
                            precision,
                            scale,
                            unsigned: false,
                        },
                        true,
                    )
                }
                ExprTypeKind::Year => ExprType::new(
                    ExprTypeKind::Decimal {
                        precision: 4,
                        scale: 0,
                        unsigned: false,
                    },
                    true,
                ),
                _ => ExprType::new(
                    ExprTypeKind::Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    true,
                ),
            }
        }
        "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            ExprType::new(
                ExprTypeKind::Float {
                    single: false,
                    precision: None,
                    scale: None,
                },
                true,
            )
        }
        "GROUP_CONCAT" => {
            let binary = argument.is_some_and(|argument| {
                matches!(
                    argument.kind,
                    ExprTypeKind::String {
                        kind: StringKind::Binary | StringKind::Blob
                    }
                )
            });
            let mut result = ExprType::new(
                ExprTypeKind::String {
                    kind: if context.group_concat_max_len > 512 {
                        if binary {
                            StringKind::Blob
                        } else {
                            StringKind::Text
                        }
                    } else if binary {
                        StringKind::Binary
                    } else {
                        StringKind::Character
                    },
                },
                true,
            );
            let protocol_octets = if binary {
                context.group_concat_max_len
            } else if context.group_concat_max_len <= 512 {
                context
                    .group_concat_max_len
                    .saturating_mul(u64::from(context.connection_mbmaxlen))
            } else {
                // MySQL's BLOB-family GROUP_CONCAT metadata expands the byte
                // cap by mbmaxlen three times (utf8mb4: 64x, utf8mb3: 27x,
                // gbk: 8x). This is observable in ColumnDefinition41 and is
                // distinct from the actual execution truncation cap.
                let mbmax = u64::from(context.connection_mbmaxlen);
                context
                    .group_concat_max_len
                    .saturating_mul(mbmax)
                    .saturating_mul(mbmax)
                    .saturating_mul(mbmax)
            };
            result.max_octets = Some(protocol_octets.min(u64::from(u32::MAX)) as u32);
            result.collation_id = Some(if binary {
                63
            } else {
                argument
                    .and_then(|arg| arg.collation_id)
                    .unwrap_or(context.connection_collation_id)
            });
            result
        }
        "ST_COLLECT" => ExprType::new(
            ExprTypeKind::Geometry {
                subtype: GeometrySubtype::Geometry,
                srid: None,
            },
            true,
        ),
        _ => ExprType::new(ExprTypeKind::Unknown, true),
    }
}

fn temporal_argument_fsp(expr: &Expr, context: &TypeContext<'_>) -> u8 {
    match infer_expr_type(expr, context).kind {
        ExprTypeKind::Temporal { fsp, .. } => fsp,
        ExprTypeKind::Decimal { scale, .. } => scale.min(6),
        ExprTypeKind::Integer { .. } | ExprTypeKind::Year => 0,
        ExprTypeKind::String { .. } => {
            let text = match expr {
                Expr::Literal(Literal::String(text)) | Expr::BoundValue(Value::String(text)) => {
                    Some(text.as_str())
                }
                _ => None,
            };
            text.map(|text| {
                text.rsplit_once('.')
                    .map_or(0, |(_, fraction)| fraction.len().min(6) as u8)
            })
            .unwrap_or(6)
        }
        _ => 6,
    }
}

/// Conservative presentation bounds, including scalar-to-string coercions.
/// Unknown JSON/LOB/geometry domains deliberately keep the byte-bounded oracle.
fn display_octet_bound(value: &ExprType) -> Option<u32> {
    use ExprTypeKind::*;
    match value.kind {
        Null => Some(0),
        Integer { width, .. } => Some(u32::from(width.decimal_precision()) + 1),
        Year => Some(4),
        Bit { bits } => Some(u32::from(bits).div_ceil(8).max(20)),
        Decimal {
            precision, scale, ..
        } => Some(u32::from(precision) + 1 + u32::from(scale > 0)),
        Float { .. } => Some(384),
        Temporal { kind, fsp } => Some(match kind {
            TemporalKind::Date => 10,
            TemporalKind::Time => 11 + if fsp > 0 { 7 } else { 0 },
            _ => 19 + if fsp > 0 { 7 } else { 0 },
        }),
        String { .. } => value.max_octets,
        _ => None,
    }
}

/// Infer a complete expression descriptor once during binding.  This routine
/// is intentionally allocation-free; recursive calls only copy descriptors.
pub(crate) fn infer_expr_type(expr: &Expr, context: &TypeContext<'_>) -> ExprType {
    crate::util::stack::ensure_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || infer_expr_type_checked(expr, context),
    )
}

fn infer_expr_type_checked(expr: &Expr, context: &TypeContext<'_>) -> ExprType {
    use ExprTypeKind::*;
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            (context.resolve_column)(expr).unwrap_or_else(|| ExprType::new(Unknown, true))
        }
        Expr::Placeholder(index) => (context.resolve_placeholder)(*index)
            .or_else(|| {
                crate::engine::params::current_param(*index)
                    .as_ref()
                    .map(|value| bound_value_type(value, context))
            })
            .unwrap_or_else(|| {
                // An unbound protocol parameter has a dynamic scalar domain.
                // MySQL reserves a 21-character presentation width when a
                // string/control-flow context consumes it (e.g. IFNULL(?,
                // 'test') => 84 bytes under utf8mb4). Numeric CASTs replace
                // this Unknown kind before the width is observed.
                let mut result = ExprType::new(Unknown, true);
                result.dynamic_parameter = true;
                result.max_octets =
                    Some(21_u32.saturating_mul(u32::from(context.connection_mbmaxlen)));
                result.collation_id = Some(context.connection_collation_id);
                result
            }),
        Expr::Literal(literal) => literal_type(literal, context),
        Expr::BoundValue(value) => bound_value_type(value, context),
        Expr::UserVariable(name) => crate::engine::expr::session_user_variable(name)
            .map(|value| bound_value_type(&value, context))
            .unwrap_or_else(|| ExprType::new(Unknown, true)),
        Expr::GroupKeyRef { expr, .. } => infer_expr_type(expr, context),
        Expr::Grouping(_) => ExprType::new(
            Integer {
                width: IntegerWidth::Big,
                unsigned: true,
            },
            false,
        ),
        Expr::Unary { op, expr } => {
            let input = infer_expr_type(expr, context);
            match op {
                UnaryOp::Not => boolean_result(input.nullable),
                UnaryOp::BitNot => ExprType::new(
                    Integer {
                        width: IntegerWidth::Big,
                        unsigned: true,
                    },
                    input.nullable,
                ),
                UnaryOp::Neg => {
                    let mut result = input;
                    match &mut result.kind {
                        Integer { unsigned, .. } | Decimal { unsigned, .. } => *unsigned = false,
                        String { .. } => {
                            result.kind = Float {
                                single: false,
                                precision: None,
                                scale: None,
                            }
                        }
                        _ => {}
                    }
                    result
                }
            }
        }
        Expr::Binary { left, op, right } => {
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) {
                let l = crate::engine::expr::prepared_numeric_operand(left);
                let r = crate::engine::expr::prepared_numeric_operand(right);
                if l.is_some() || r.is_some() {
                    return binary_type(
                        *op,
                        l.map(|value| infer_expr_type(&Expr::BoundValue(value), context))
                            .unwrap_or_else(|| infer_expr_type(left, context)),
                        r.map(|value| infer_expr_type(&Expr::BoundValue(value), context))
                            .unwrap_or_else(|| infer_expr_type(right, context)),
                        context,
                    );
                }
            }

            // Hex/bit literals have a binary-string domain normally, but in
            // arithmetic MySQL binds them as the smallest unsigned integer
            // family that can contain their bytes.  Reuse the canonical
            // classifier so `0x9999 + 0` remains INT UNSIGNED, not DOUBLE.
            if matches!(
                *op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod
            ) && (crate::engine::expr::expr_is_numeric_hexbit_literal(left)
                || crate::engine::expr::expr_is_numeric_hexbit_literal(right))
            {
                let mut source = ColumnSource::default();
                if crate::engine::expr::expr_result_unsigned(expr, &|_| None) {
                    source.flags |= crate::mysql::protocol::UNSIGNED_FLAG;
                }
                let mut result =
                    ExprType::from_column_type(column_type::expr_result_type(expr), &source);
                result.nullable = infer_expr_type(left, context).nullable
                    || infer_expr_type(right, context).nullable;
                result
            } else {
                binary_type(
                    *op,
                    infer_expr_type(left, context),
                    infer_expr_type(right, context),
                    context,
                )
            }
        }
        Expr::Case {
            results,
            else_result,
            ..
        } => {
            let mut branches = results.iter().chain(else_result.iter().map(Box::as_ref));
            let mut result = branches
                .next()
                .map(|branch| infer_expr_type(branch, context))
                .unwrap_or_else(|| ExprType::new(Null, true));
            for branch in branches {
                result = aggregate_branch_types(result, infer_expr_type(branch, context));
            }
            if else_result.is_none() {
                result.nullable = true;
            }
            if matches!(result.kind, Temporal { .. }) {
                // MySQL exposes control-flow temporal domains using the
                // connection charset's presentation width (DATETIME(6):
                // 26 chars => 104 bytes under utf8mb4) and conservatively
                // keeps CASE nullable even when every literal branch is not.
                result.max_octets = result
                    .max_octets
                    .map(|length| length.saturating_mul(u32::from(context.connection_mbmaxlen)));
                result.collation_id = Some(context.connection_collation_id);
                result.nullable = true;
            }
            result
        }
        Expr::Function { name, args, .. } => {
            if let Some(descriptor) = crate::engine::expr::resolved_stored_function_type(name) {
                return descriptor;
            }
            let normalized = crate::engine::sys_schema::NormalizedFuncName::new(name);
            if crate::engine::vector::sql::returns_vector(normalized.as_str()) {
                let string = |expr: &Expr| -> Option<std::string::String> {
                    match expr {
                        Expr::Literal(Literal::String(s)) => Some(s.clone()),
                        Expr::BoundValue(Value::String(s)) => Some(s.clone()),
                        Expr::Placeholder(i) => {
                            crate::engine::params::current_param(*i).map(|v| v.to_display_string())
                        }
                        _ => None,
                    }
                };
                let integer = |expr: &Expr| -> Option<u32> {
                    let value_integer = |value: &Value| match value {
                        Value::Int(i) => u32::try_from(*i).ok(),
                        Value::UInt(i) => u32::try_from(*i).ok(),
                        Value::String(text) | Value::Decimal(text) => text.trim().parse().ok(),
                        Value::SmallString(_) => {
                            crate::types::value_str(value)?.trim().parse().ok()
                        }
                        _ => None,
                    };
                    match expr {
                        Expr::Literal(Literal::Int(i)) => u32::try_from(*i).ok(),
                        Expr::Literal(Literal::UInt(i)) => u32::try_from(*i).ok(),
                        Expr::Literal(Literal::String(text)) => text.trim().parse().ok(),
                        Expr::BoundValue(value) => value_integer(value),
                        Expr::Placeholder(i) => crate::engine::params::current_param(*i)
                            .and_then(|value| value_integer(&value)),
                        _ => None,
                    }
                };
                let (datatype, dim) = match normalized.as_str() {
                    "SUBVECTOR" => {
                        let ty = args.first().map(|a| infer_expr_type(a, context));
                        let datatype = ty
                            .and_then(|t| {
                                if let Vector { datatype, .. } = t.kind {
                                    Some(datatype)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();
                        (datatype, args.get(2).and_then(integer))
                    }
                    "L2_NORMALIZE" => {
                        let dim = args.first().and_then(|a| {
                            if let Vector { dim, .. } = infer_expr_type(a, context).kind {
                                dim
                            } else {
                                None
                            }
                        });
                        (crate::vector::VectorDataType::Float32, dim)
                    }
                    _ => {
                        let datatype = args
                            .get(1)
                            .and_then(string)
                            .and_then(|s| crate::vector::VectorDataType::parse(&s))
                            .unwrap_or_default();
                        let dim = if normalized.as_str() == "VECTOR_FROM_BINARY" {
                            args.get(2).and_then(integer)
                        } else {
                            args.first()
                                .and_then(string)
                                .map(|s| s.bytes().filter(|b| *b == b',').count() as u32 + 1)
                        };
                        (datatype, dim)
                    }
                };
                return ExprType::new(
                    Vector { dim, datatype },
                    args.iter().any(|a| infer_expr_type(a, context).nullable),
                );
            }
            match normalized.as_str() {
                "__TEMPORAL_COMPARISON_KEY" if args.len() == 1 => ExprType::new(
                    Temporal {
                        kind: TemporalKind::DateTime,
                        fsp: 6,
                    },
                    infer_expr_type(&args[0], context).nullable,
                ),
                "COLLATE" if args.len() == 2 => {
                    let mut descriptor = infer_expr_type(&args[0], context);
                    if let Expr::Literal(Literal::String(name)) = &args[1] {
                        descriptor.collation_id =
                            crate::charset::resolve_collation(name).ok().map(|c| c.id);
                    }
                    descriptor.coercibility = 0;
                    descriptor
                }
                "__COLLATED_CONST" if args.len() == 3 => {
                    let mut descriptor = infer_expr_type(&args[0], context);
                    descriptor.collation_id =
                        crate::engine::expr::expression_metadata_collation_id(expr);
                    if let Expr::Literal(Literal::Int(coercibility)) = &args[2] {
                        descriptor.coercibility = (*coercibility).clamp(0, 6) as u8;
                    }
                    descriptor
                }
                "MOD" if args.len() == 2 => binary_type(
                    BinaryOp::Mod,
                    infer_expr_type(&args[0], context),
                    infer_expr_type(&args[1], context),
                    context,
                ),
                "ANY_VALUE" | "NAME_CONST" => args
                    .last()
                    .map(|arg| infer_expr_type(arg, context))
                    .unwrap_or_else(|| ExprType::new(Unknown, true)),
                "GREATEST" | "LEAST" => args
                    .iter()
                    .map(|arg| infer_expr_type(arg, context))
                    .reduce(aggregate_branch_types)
                    .unwrap_or_else(|| ExprType::new(Null, true)),
                "INTERVAL"
                | "__INTERVAL_LIST"
                | "FIND_IN_SET"
                | "__FIND_IN_SET_SET"
                | "__FIND_IN_SET_SET_BINARY"
                | "VALIDATE_PASSWORD_STRENGTH"
                | "__ENUM_CMP"
                | "__SET_CMP"
                | "__SET_CMP_BINARY" => ExprType::new(
                    Integer {
                        width: IntegerWidth::Big,
                        unsigned: false,
                    },
                    true,
                ),
                "__ENUM_ORDINAL" | "__SET_ORDINAL" | "__SET_ORDINAL_BINARY" => ExprType::new(
                    Integer {
                        width: IntegerWidth::Big,
                        unsigned: name != "__ENUM_ORDINAL",
                    },
                    true,
                ),
                "CHAR" => {
                    let mut result = ExprType::new(
                        String {
                            kind: StringKind::Binary,
                        },
                        true,
                    );
                    result.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
                    result
                }
                "ST_ASTEXT" | "ST_ASWKT" => {
                    let mut result = ExprType::new(
                        String {
                            kind: StringKind::Text,
                        },
                        true,
                    );
                    result.max_octets = Some(u32::MAX);
                    result.collation_id = Some(context.connection_collation_id);
                    result
                }
                "AES_ENCRYPT" | "AES_DECRYPT" | "COMPRESS" | "UNCOMPRESS" | "RANDOM_BYTES" => {
                    let mut result = ExprType::new(
                        String {
                            kind: StringKind::Blob,
                        },
                        true,
                    );
                    result.collation_id = Some(crate::charset::BINARY_COLLATION_ID);
                    result
                }
                "EXPORT_SET" => {
                    let strings = args.get(1..args.len().min(4)).unwrap_or_default();
                    let binary =
                        strings
                            .iter()
                            .map(|arg| infer_expr_type(arg, context))
                            .any(|arg| {
                                matches!(
                                    arg.kind,
                                    String {
                                        kind: StringKind::Binary | StringKind::Blob
                                    }
                                )
                            });
                    let mut result = ExprType::new(
                        String {
                            kind: if binary {
                                StringKind::Binary
                            } else {
                                StringKind::Character
                            },
                        },
                        true,
                    );
                    result.collation_id = Some(if binary {
                        crate::charset::BINARY_COLLATION_ID
                    } else {
                        context.connection_collation_id
                    });
                    result
                }
                "BIN_TO_UUID" | "INET6_NTOA" => {
                    let mut result = ExprType::new(
                        String {
                            kind: StringKind::Character,
                        },
                        true,
                    );
                    result.collation_id = Some(context.connection_collation_id);
                    result
                }
                "CONCAT" | "CONCAT_WS" | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "LOWER"
                | "LCASE" | "UPPER" | "UCASE" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE"
                | "SUBSTRING_INDEX" => {
                    // Infer each string operand once: nested string functions
                    // would otherwise re-infer every subtree per pass.
                    let operands: smallvec::SmallVec<[ExprType; 4]> = args
                        .iter()
                        .take(if matches!(normalized.as_str(), "CONCAT" | "CONCAT_WS") {
                            args.len()
                        } else {
                            1
                        })
                        .map(|arg| infer_expr_type(arg, context))
                        .collect();
                    let binary = operands.iter().any(|arg| {
                        matches!(
                            arg.kind,
                            String {
                                kind: StringKind::Binary | StringKind::Blob
                            }
                        )
                    });
                    let mut result = ExprType::new(
                        String {
                            kind: if binary {
                                StringKind::Binary
                            } else {
                                StringKind::Character
                            },
                        },
                        true,
                    );
                    let widths: Option<smallvec::SmallVec<[u32; 4]>> =
                        operands.iter().map(display_octet_bound).collect();
                    result.max_octets = widths.map(|widths| match normalized.as_str() {
                        "CONCAT" => widths.into_iter().fold(0u32, u32::saturating_add),
                        "CONCAT_WS" => {
                            let separators = widths.len().saturating_sub(2) as u32;
                            widths
                                .iter()
                                .skip(1)
                                .copied()
                                .fold(0u32, u32::saturating_add)
                                .saturating_add(
                                    widths
                                        .first()
                                        .copied()
                                        .unwrap_or(0)
                                        .saturating_mul(separators),
                                )
                        }
                        // Unicode case mappings can expand one scalar to three.
                        "LOWER" | "LCASE" | "UPPER" | "UCASE" if !binary => {
                            widths.first().copied().unwrap_or(0).saturating_mul(3)
                        }
                        _ => widths.first().copied().unwrap_or(0),
                    });
                    let collation = operands
                        .into_iter()
                        .filter(|arg| matches!(arg.kind, String { .. }))
                        .reduce(aggregate_branch_types);
                    result.collation_id = Some(if binary {
                        crate::charset::BINARY_COLLATION_ID
                    } else {
                        collation
                            .and_then(|c| c.collation_id)
                            .unwrap_or(context.connection_collation_id)
                    });
                    result
                }
                "REPLACE" if args.len() == 3 => {
                    let input = infer_expr_type(&args[0], context);
                    let replacement = infer_expr_type(&args[2], context);
                    let mut result = ExprType::new(
                        String {
                            kind: if input.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
                            {
                                StringKind::Binary
                            } else {
                                StringKind::Character
                            },
                        },
                        true,
                    );
                    result.collation_id =
                        input.collation_id.or(Some(context.connection_collation_id));
                    result.max_octets = display_octet_bound(&input)
                        .zip(display_octet_bound(&replacement))
                        .map(|(input, replacement)| input.saturating_mul(replacement.max(1)));
                    result
                }
                "DATE_FORMAT" | "TIME_FORMAT" => {
                    let bound = args.get(1).and_then(|format| match format {
                        Expr::Literal(Literal::String(format))
                        | Expr::BoundValue(Value::String(format)) => {
                            Some(if normalized.as_str() == "TIME_FORMAT" {
                                (format.len().min(u32::MAX as usize) as u32).saturating_mul(64)
                            } else {
                                crate::engine::expr::date_format::DateFormatPlan::max_format_bytes(
                                    format,
                                )
                            })
                        }
                        other => display_octet_bound(&infer_expr_type(other, context))
                            .map(|bytes| bytes.saturating_mul(64)),
                    });
                    let mut result = fixed_string_result(bound, true);
                    result.collation_id = Some(context.connection_collation_id);
                    result
                }
                "MD5" | "SHA" | "SHA1" | "SHA2" => {
                    let digits = match normalized.as_str() {
                        "MD5" => 32,
                        "SHA" | "SHA1" => 40,
                        _ => 128,
                    };
                    let mut result = fixed_string_result(
                        Some(digits * u32::from(context.connection_mbmaxlen)),
                        true,
                    );
                    result.collation_id = Some(context.connection_collation_id);
                    result
                }
                "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => {
                    let input = args
                        .first()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Unknown, true));
                    let unit = match args.get(1) {
                        Some(Expr::Function { name, args, .. })
                            if name.eq_ignore_ascii_case("__INTERVAL") =>
                        {
                            args.get(1).and_then(|arg| match arg {
                                Expr::Literal(Literal::String(unit)) => Some(unit.as_str()),
                                _ => None,
                            })
                        }
                        _ => Some("DAY"),
                    };
                    let dynamic_first = !crate::engine::params::auto_parameterized_active()
                        && args.first().is_some_and(|arg| {
                            matches!(arg, Expr::Placeholder(_) | Expr::BoundValue(_))
                        });
                    if dynamic_first && !matches!(input.kind, Temporal { .. }) {
                        ExprType::new(
                            Temporal {
                                kind: TemporalKind::DateTime,
                                fsp: 6,
                            },
                            true,
                        )
                    } else if let Temporal { kind, fsp } = input.kind {
                        let date_unit = unit.is_some_and(|unit| {
                            ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]
                                .iter()
                                .any(|part| unit.contains(part))
                        });
                        let time_unit = unit.is_some_and(|unit| {
                            ["HOUR", "MINUTE", "SECOND"]
                                .iter()
                                .any(|part| unit.contains(part))
                        });
                        let kind = match kind {
                            TemporalKind::Time if !date_unit => TemporalKind::Time,
                            TemporalKind::Date if !time_unit => TemporalKind::Date,
                            _ => TemporalKind::DateTime,
                        };
                        ExprType::new(
                            Temporal {
                                kind,
                                fsp: fsp.max(
                                    crate::engine::expr::expression_metadata_decimals(expr)
                                        .unwrap_or(0),
                                ),
                            },
                            true,
                        )
                    } else {
                        let mut result = ExprType::new(
                            String {
                                kind: StringKind::Character,
                            },
                            true,
                        );
                        result.collation_id = Some(context.connection_collation_id);
                        result
                    }
                }
                "TIMESTAMP" if (1..=2).contains(&args.len()) => ExprType::new(
                    Temporal {
                        kind: TemporalKind::DateTime,
                        fsp: args
                            .iter()
                            .map(|arg| temporal_argument_fsp(arg, context))
                            .max()
                            .unwrap_or(0),
                    },
                    true,
                ),
                "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
                    let binary = args
                        .first()
                        .map(|arg| infer_expr_type(arg, context))
                        .filter(|arg| {
                            matches!(
                                arg.kind,
                                String {
                                    kind: StringKind::Binary | StringKind::Blob
                                }
                            )
                        })
                        .filter(|_| {
                            !matches!(
                                args.first(),
                                Some(Expr::Literal(Literal::HexBytes(_) | Literal::BitBytes(_)))
                            )
                        });
                    binary.unwrap_or_else(|| fixed_integer_result(IntegerWidth::Big, true, false))
                }
                "ADDTIME" | "SUBTIME" => {
                    let input = args
                        .first()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Unknown, true));
                    match input.kind {
                        Temporal { kind, fsp } => ExprType::new(
                            Temporal {
                                kind,
                                fsp: fsp.max(
                                    args.get(1)
                                        .map(|arg| temporal_argument_fsp(arg, context))
                                        .unwrap_or(0),
                                ),
                            },
                            true,
                        ),
                        _ => {
                            let mut result = ExprType::new(
                                String {
                                    kind: StringKind::Character,
                                },
                                true,
                            );
                            result.collation_id = Some(context.connection_collation_id);
                            result
                        }
                    }
                }
                "TIMEDIFF" => {
                    let fsp = args
                        .iter()
                        .map(|arg| temporal_argument_fsp(arg, context))
                        .max()
                        .unwrap_or(0);
                    ExprType::new(
                        Temporal {
                            kind: TemporalKind::Time,
                            fsp,
                        },
                        true,
                    )
                }
                "REPEAT" if args.len() == 2 => {
                    let input = infer_expr_type(&args[0], context);
                    let count = match &args[1] {
                        Expr::Literal(Literal::Int(n)) | Expr::BoundValue(Value::Int(n)) => {
                            Some(u32::try_from((*n).max(0)).unwrap_or(u32::MAX))
                        }
                        Expr::Literal(Literal::UInt(n)) | Expr::BoundValue(Value::UInt(n)) => {
                            Some(u32::try_from(*n).unwrap_or(u32::MAX))
                        }
                        _ => None,
                    };
                    let string_input = matches!(input.kind, String { .. });
                    let mut result = ExprType::new(
                        String {
                            kind: if string_input
                                && input.collation_id == Some(crate::charset::BINARY_COLLATION_ID)
                            {
                                StringKind::Binary
                            } else {
                                StringKind::Character
                            },
                        },
                        true, // Packet limits can produce NULL for non-NULL operands.
                    );
                    result.collation_id = if string_input {
                        input.collation_id.or(Some(context.connection_collation_id))
                    } else {
                        Some(context.connection_collation_id)
                    };
                    result.max_octets = Some(match (input.max_octets, count) {
                        (_, Some(0)) => 0,
                        (Some(bytes), Some(count)) => bytes.saturating_mul(count),
                        _ => u32::MAX,
                    });
                    result
                }
                "IFNULL" | "COALESCE" => {
                    let mut arguments = args.iter();
                    let mut result = arguments
                        .next()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Null, true));
                    let mut all_nullable = result.nullable;
                    for arg in arguments {
                        let argument = infer_expr_type(arg, context);
                        all_nullable &= argument.nullable;
                        result = aggregate_branch_types(result, argument);
                    }
                    result.nullable = all_nullable;
                    result
                }
                "IF" => {
                    let mut branches = args.iter().skip(1);
                    let mut result = branches
                        .next()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Null, true));
                    for branch in branches {
                        result = aggregate_branch_types(result, infer_expr_type(branch, context));
                    }
                    result
                }
                "NULLIF" => {
                    let mut result = args
                        .first()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Unknown, true));
                    result.nullable = true;
                    result
                }
                "CAST" => cast_type(
                    args,
                    args.first()
                        .map(|arg| infer_expr_type(arg, context))
                        .unwrap_or_else(|| ExprType::new(Unknown, true)),
                    context,
                ),
                "TIME" | "SEC_TO_TIME" | "MAKETIME" => {
                    let source = args
                        .get(if name.eq_ignore_ascii_case("MAKETIME") {
                            2
                        } else {
                            0
                        })
                        .map(|arg| infer_expr_type(arg, context));
                    let fsp = crate::engine::expr::expression_metadata_decimals(expr)
                        .unwrap_or_else(|| match source.map(|source| source.kind) {
                            Some(Temporal { fsp, .. }) => fsp,
                            Some(Decimal { scale, .. }) => scale.min(6),
                            Some(Integer { .. } | Year) => 0,
                            _ => 6,
                        });
                    ExprType::new(
                        Temporal {
                            kind: TemporalKind::Time,
                            fsp,
                        },
                        true,
                    )
                }
                "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE" | "UNIX_TIMESTAMP"
                | "STR_TO_DATE" => {
                    // These functions consult only their first operand's type;
                    // infer it once rather than once per derived property.
                    let first = args.first().map(|arg| infer_expr_type(arg, context));
                    let mut column_type = column_type::conditional_function_result_type(
                        name,
                        args,
                        |arg| match first {
                            Some(first) if std::ptr::eq(arg, &args[0]) => first.column_type(),
                            _ => infer_expr_type(arg, context).column_type(),
                        },
                    )
                    .unwrap_or_else(|| column_type::function_result_type(name));
                    if name.eq_ignore_ascii_case("UNIX_TIMESTAMP") {
                        if let Some(Temporal { fsp, .. }) = first.map(|first| first.kind) {
                            if fsp > 0 {
                                column_type = ColumnType::Decimal {
                                    precision: 20,
                                    scale: fsp,
                                };
                            }
                        }
                    }
                    let mut result =
                        ExprType::from_column_type(column_type, &ColumnSource::default());
                    if matches!(
                        name.to_ascii_uppercase().as_str(),
                        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE"
                    ) {
                        if let (
                            Integer { unsigned, .. },
                            Some(Integer {
                                unsigned: input_unsigned,
                                ..
                            }),
                        ) = (&mut result.kind, first.map(|first| first.kind))
                        {
                            *unsigned = input_unsigned;
                        }
                    }
                    if let Some(fsp) = crate::engine::expr::expression_metadata_decimals(expr) {
                        if let Temporal { fsp: output, .. } = &mut result.kind {
                            *output = fsp;
                        }
                    }
                    result
                }
                "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "__HIDDEN_FIRST_VALUE"
                | "GROUP_CONCAT"
                | "ST_COLLECT"
                | "STD"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP" => aggregate_result_type(
                    name,
                    args.first().map(|arg| infer_expr_type(arg, context)),
                    context,
                ),
                "__GC_ORDERED" => args
                    .first()
                    .map(|arg| infer_expr_type(arg, context))
                    .unwrap_or_else(|| ExprType::new(Unknown, true)),
                _ => {
                    if let Some(mut spec) = builtin_spec(name) {
                        if args.len() < usize::from(spec.min_arity)
                            || args.len() > usize::from(spec.max_arity)
                        {
                            return ExprType::new(Unknown, true);
                        }
                        if matches!(
                            spec.result.kind,
                            String {
                                kind: StringKind::Character
                            }
                        ) {
                            spec.result
                                .collation_id
                                .get_or_insert(context.connection_collation_id);
                        }
                        if let Some(fsp) = crate::engine::expr::expression_metadata_decimals(expr) {
                            if let Temporal {
                                fsp: result_fsp, ..
                            } = &mut spec.result.kind
                            {
                                *result_fsp = fsp;
                            }
                        }
                        spec.result
                    } else {
                        let mut source = ColumnSource::default();
                        source.flags = if crate::engine::expr::expr_result_unsigned(expr, &|_| None)
                        {
                            crate::mysql::protocol::UNSIGNED_FLAG
                        } else {
                            0
                        };
                        let mut result = ExprType::from_column_type(
                            column_type::expr_result_type(expr),
                            &source,
                        );
                        if let Some(fsp) = crate::engine::expr::expression_metadata_decimals(expr) {
                            if let Temporal {
                                fsp: result_fsp, ..
                            } = &mut result.kind
                            {
                                *result_fsp = fsp;
                            }
                        }
                        result
                    }
                }
            }
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) => boolean_result(false),
        Expr::InList { expr, list, .. } => boolean_result(
            infer_expr_type(expr, context).nullable
                || list
                    .iter()
                    .any(|item| infer_expr_type(item, context).nullable),
        ),
        Expr::Between {
            expr, low, high, ..
        } => boolean_result(
            infer_expr_type(expr, context).nullable
                || infer_expr_type(low, context).nullable
                || infer_expr_type(high, context).nullable,
        ),
        Expr::Like { expr, pattern, .. } => boolean_result(
            infer_expr_type(expr, context).nullable || infer_expr_type(pattern, context).nullable,
        ),
        Expr::Subquery(select) => {
            // Empty scalar subqueries yield NULL. Use the selected expression's
            // domain, with nullability independent of whether it is a literal.
            let mut result = (context.resolve_subquery)(select).unwrap_or_else(|| {
                match select.projection.as_slice() {
                    [crate::parser::SelectItem::Expr { expr, .. }] if select.from.is_empty() => {
                        infer_expr_type(expr, context)
                    }
                    [crate::parser::SelectItem::Expr { expr, .. }] => {
                        let no_column = |_: &Expr| None;
                        let inner_context = TypeContext {
                            resolve_column: &no_column,
                            ..*context
                        };
                        infer_expr_type(expr, &inner_context)
                    }
                    _ => ExprType::new(Unknown, true),
                }
            });
            result.nullable = true;
            result
        }
        Expr::Exists { .. } => boolean_result(false),
        Expr::InSubquery { .. } | Expr::QuantifiedComparison { .. } => boolean_result(true),
        Expr::WindowFunction { name, args, .. } => {
            let normalized = crate::engine::sys_schema::NormalizedFuncName::new(name);
            let argument = args.first().map(|arg| infer_expr_type(arg, context));
            match normalized.as_str() {
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "COUNT" => {
                    fixed_integer_result(IntegerWidth::Big, false, false)
                }
                "SUM" | "AVG" | "MIN" | "MAX" => {
                    aggregate_result_type(normalized.as_str(), argument, context)
                }
                "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                    let mut result = argument.unwrap_or_else(|| ExprType::new(Unknown, true));
                    result.nullable = true;
                    result
                }
                "PERCENT_RANK" | "CUME_DIST" | "STD" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP"
                | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => ExprType::new(
                    Float {
                        single: false,
                        precision: None,
                        scale: None,
                    },
                    !matches!(normalized.as_str(), "PERCENT_RANK" | "CUME_DIST"),
                ),
                "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
                    fixed_integer_result(IntegerWidth::Big, false, true)
                }
                _ => ExprType::new(Unknown, true),
            }
        }
        Expr::Wildcard
        | Expr::SystemVariable { .. }
        | Expr::Default(_)
        | Expr::DefaultQualified(_)
        | Expr::Tuple(_) => ExprType::new(Unknown, true),
    }
}

/// Schema-aware legacy/oracle adapter. New compiled paths retain the same
/// descriptor in their program; the row evaluator uses this only on fallback
/// so conditional branch coercion cannot lose declared FSP or signedness.
pub(crate) fn infer_expr_type_in_table(
    expr: &Expr,
    table: &crate::storage::TableSchema,
) -> ExprType {
    let resolve_column = |column_expr: &Expr| {
        let name = match column_expr {
            Expr::Identifier(name) => name.as_str(),
            Expr::CompoundIdentifier(parts) => parts.last()?.as_str(),
            _ => return None,
        };
        let index = table.column_index(name).ok()?;
        let column = &table.columns[index];
        let source = table.wire_column_source(index);
        Some(ExprType::from_data_type(
            &column.data_type,
            column.unsigned,
            column.nullable,
            source.length,
            source.collation_id,
        ))
    };
    let resolve_placeholder = |_: usize| None;
    let context = TypeContext::context_free(&resolve_column, &resolve_placeholder);
    infer_expr_type(expr, &context)
}

/// Hash keys must share the scalar comparison domain. Exact numeric pairs
/// already have compatible canonical encodings, so their hot path stays
/// unchanged. Normalize approximate and mixed temporal comparisons once
/// while binding, before either executor chooses its key representation.
pub(crate) fn normalize_equality_key_exprs(
    a: &mut Expr,
    b: &mut Expr,
    at: ExprTypeKind,
    bt: ExprTypeKind,
) {
    use ExprTypeKind as K;
    use TemporalKind as T;
    let numeric = |kind| {
        matches!(
            kind,
            K::Integer { .. } | K::Decimal { .. } | K::Float { .. } | K::Bit { .. } | K::Year
        )
    };
    let target: Option<(&str, Vec<i64>)> = match (at, bt) {
        // SQL compares integer/approximate numeric columns with text/binary
        // columns in the DOUBLE domain. Hash both sides in that same domain.
        (
            K::String { .. },
            K::Integer { .. } | K::Decimal { .. } | K::Float { .. } | K::Bit { .. } | K::Year,
        )
        | (
            K::Integer { .. } | K::Decimal { .. } | K::Float { .. } | K::Bit { .. } | K::Year,
            K::String { .. },
        ) => Some(("DOUBLE", vec![])),
        (K::String { .. }, K::Temporal { kind, .. })
        | (K::Temporal { kind, .. }, K::String { .. }) => {
            Some((if kind == T::Time { "TIME" } else { "DATETIME" }, vec![6]))
        }
        // A date/datetime compared with a number reads the number as a
        // compact `YYYYMMDD[hhmmss[.ffffff]]` value (`101` is 2000-01-01).
        (K::Temporal { kind, .. }, K::Integer { .. } | K::Decimal { .. } | K::Float { .. })
        | (K::Integer { .. } | K::Decimal { .. } | K::Float { .. }, K::Temporal { kind, .. })
            if matches!(kind, T::Date | T::DateTime | T::Timestamp) =>
        {
            Some(("DATETIME", vec![6]))
        }
        (K::Float { .. }, other) | (other, K::Float { .. }) if numeric(other) && at != bt => {
            Some(("DOUBLE", vec![]))
        }
        (K::Year, K::Decimal { scale, .. }) | (K::Decimal { scale, .. }, K::Year) => {
            Some(("DECIMAL", vec![65, i64::from(scale)]))
        }
        (K::Year, other) | (other, K::Year) if numeric(other) && at != bt => {
            Some(("DECIMAL", vec![20, 0]))
        }
        (K::Temporal { kind: ta, .. }, K::Temporal { kind: tb, .. })
            if ta != tb
                && matches!(ta, T::Date | T::DateTime | T::Timestamp)
                && matches!(tb, T::Date | T::DateTime | T::Timestamp) =>
        {
            Some(("DATETIME", vec![6]))
        }
        _ => None,
    };
    if let Some((target, parameters)) = target {
        for (key, kind) in [(a, at), (b, bt)] {
            if target == "DATETIME" {
                if matches!(
                    kind,
                    K::String { .. } | K::Integer { .. } | K::Decimal { .. } | K::Float { .. }
                ) {
                    *key = Expr::Function {
                        name: "CAST".into(),
                        args: vec![
                            key.clone(),
                            Expr::Literal(Literal::String("DATETIME".into())),
                            Expr::Literal(Literal::Int(6)),
                        ],
                        distinct: false,
                    };
                }
                // Comparison promotion must not apply CAST's NO_ZERO_DATE
                // validation to already stored temporal values.
                *key = Expr::Function {
                    name: "__TEMPORAL_COMPARISON_KEY".into(),
                    args: vec![key.clone()],
                    distinct: false,
                };
                continue;
            }
            let mut args = vec![
                key.clone(),
                Expr::Literal(Literal::String(target.to_owned())),
            ];
            args.extend(
                parameters
                    .iter()
                    .map(|&value| Expr::Literal(Literal::Int(value))),
            );
            *key = Expr::Function {
                name: "CAST".to_owned(),
                args,
                distinct: false,
            };
        }
    }
}

/// Approximate hash keys are a candidate filter for exact DECIMAL/text
/// equality. Recheck the original predicate to distinguish values above 2^53.
pub(crate) fn equality_key_needs_recheck(a: ExprTypeKind, b: ExprTypeKind) -> bool {
    use ExprTypeKind as K;
    let numeric = |kind| {
        matches!(
            kind,
            K::Integer { .. } | K::Decimal { .. } | K::Float { .. }
        )
    };
    matches!(
        (a, b),
        (K::Decimal { .. }, K::String { .. }) | (K::String { .. }, K::Decimal { .. })
    ) || matches!((a, b), (K::Temporal { .. }, other) | (other, K::Temporal { .. }) if numeric(other))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_metadata_preserves_nested_aggregate_and_input_domains() {
        let decimal = ExprType::new(
            ExprTypeKind::Decimal {
                precision: 7,
                scale: 2,
                unsigned: false,
            },
            true,
        );
        let unsigned = fixed_integer_result(IntegerWidth::Big, true, false);
        let date = fixed_temporal_result(TemporalKind::Date, false);
        let resolve = |expr: &Expr| match expr {
            Expr::Identifier(name) if name == "price" => Some(decimal),
            Expr::Identifier(name) if name == "u" => Some(unsigned),
            Expr::Identifier(name) if name == "d" => Some(date),
            _ => None,
        };
        let params = |_: usize| None;
        let context = TypeContext::context_free(&resolve, &params);
        let infer =
            |sql| infer_expr_type(&crate::parser::parse_expr_fragment(sql).unwrap(), &context);
        assert_eq!(
            infer("SUM(SUM(price)) OVER (ORDER BY d)").kind,
            ExprTypeKind::Decimal {
                precision: 51,
                scale: 2,
                unsigned: false
            }
        );
        let mut nullable_unsigned = unsigned;
        nullable_unsigned.nullable = true;
        assert_eq!(infer("MAX(u) OVER (ORDER BY d)"), nullable_unsigned);
        let mut nullable_date = date;
        nullable_date.nullable = true;
        assert_eq!(infer("LAG(d) OVER (ORDER BY u)"), nullable_date);
        assert_eq!(
            infer("ROW_NUMBER() OVER (ORDER BY d)"),
            fixed_integer_result(IntegerWidth::Big, false, false)
        );
    }

    #[test]
    fn signed_unsigned_bigint_aggregates_to_decimal_21() {
        let signed = ExprType::new(
            ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned: false,
            },
            false,
        );
        let unsigned = ExprType::new(
            ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned: true,
            },
            false,
        );
        assert_eq!(
            aggregate_branch_types(signed, unsigned).kind,
            ExprTypeKind::Decimal {
                precision: 21,
                scale: 0,
                unsigned: false
            }
        );
    }

    #[test]
    fn temporal_mixture_becomes_datetime_with_max_fsp() {
        let date = ExprType::new(
            ExprTypeKind::Temporal {
                kind: TemporalKind::Date,
                fsp: 0,
            },
            false,
        );
        let time = ExprType::new(
            ExprTypeKind::Temporal {
                kind: TemporalKind::Time,
                fsp: 6,
            },
            true,
        );
        let result = aggregate_branch_types(date, time);
        assert_eq!(
            result.kind,
            ExprTypeKind::Temporal {
                kind: TemporalKind::DateTime,
                fsp: 6
            }
        );
        assert!(result.nullable);
    }

    #[test]
    fn fixed_builtin_registry_carries_identity_arity_and_volatility() {
        let spec = builtin_spec("connection_id").unwrap();
        assert_eq!(spec.id, BuiltinId::ConnectionId);
        assert_eq!((spec.min_arity, spec.max_arity), (0, 0));
        assert_eq!(spec.volatility, Volatility::Stable);
        assert_eq!(
            spec.result.kind,
            ExprTypeKind::Integer {
                width: IntegerWidth::Big,
                unsigned: true
            }
        );
        assert!(!spec.result.nullable);
        assert_eq!(
            builtin_spec("from_unixtime").unwrap().volatility,
            Volatility::Stable
        );
        assert_eq!(
            builtin_spec("convert_tz").unwrap().volatility,
            Volatility::Stable
        );
        let str_to_date = builtin_spec("str_to_date").unwrap();
        assert_eq!((str_to_date.min_arity, str_to_date.max_arity), (2, 2));
        assert_eq!(str_to_date.volatility, Volatility::Immutable);
    }

    #[test]
    fn aggregate_precision_matches_mysql80_formulas() {
        let no_column = |_: &Expr| None;
        let no_placeholder = |_: usize| None;
        let context = TypeContext::context_free(&no_column, &no_placeholder);
        let int = ExprType::new(
            ExprTypeKind::Integer {
                width: IntegerWidth::Int,
                unsigned: false,
            },
            true,
        );
        let decimal = ExprType::new(
            ExprTypeKind::Decimal {
                precision: 6,
                scale: 2,
                unsigned: false,
            },
            true,
        );
        assert_eq!(
            aggregate_result_type("SUM", Some(int), &context).kind,
            ExprTypeKind::Decimal {
                precision: 32,
                scale: 0,
                unsigned: false
            }
        );
        assert_eq!(
            aggregate_result_type("AVG", Some(int), &context).kind,
            ExprTypeKind::Decimal {
                precision: 14,
                scale: 4,
                unsigned: false
            }
        );
        assert_eq!(
            aggregate_result_type("SUM", Some(decimal), &context).kind,
            ExprTypeKind::Decimal {
                precision: 28,
                scale: 2,
                unsigned: false
            }
        );
    }

    #[test]
    fn conditional_nullability_uses_control_flow_not_fold_seed() {
        let no_column = |_: &Expr| None;
        let no_placeholder = |_: usize| None;
        let context = TypeContext::context_free(&no_column, &no_placeholder);
        let statement = crate::parser::parse_statement(
            "SELECT IFNULL(NULL, 'x'), COALESCE(NULL, NULL, 1), IF(1, 1, NULL), CASE WHEN 1 THEN 1 END",
        )
        .unwrap();
        let crate::parser::Statement::Select(select) = statement else {
            panic!("expected SELECT")
        };
        let descriptors = select
            .projection
            .iter()
            .map(|item| match item {
                crate::parser::SelectItem::Expr { expr, .. } => infer_expr_type(expr, &context),
                _ => panic!("expected expression"),
            })
            .collect::<Vec<_>>();
        assert!(!descriptors[0].nullable);
        assert!(!descriptors[1].nullable);
        assert!(descriptors[2].nullable);
        assert!(descriptors[3].nullable);
    }

    #[test]
    fn group_concat_wire_extent_tracks_threshold_and_charset_width() {
        let no_column = |_: &Expr| None;
        let no_placeholder = |_: usize| None;
        let mut context = TypeContext::context_free(&no_column, &no_placeholder);
        context.connection_mbmaxlen = 4;
        context.group_concat_max_len = 400;
        assert_eq!(
            aggregate_result_type("GROUP_CONCAT", None, &context).max_octets,
            Some(1600)
        );
        context.group_concat_max_len = 1024;
        assert_eq!(
            aggregate_result_type("GROUP_CONCAT", None, &context).max_octets,
            Some(65_536)
        );
        context.connection_mbmaxlen = 1;
        assert_eq!(
            aggregate_result_type("GROUP_CONCAT", None, &context).max_octets,
            Some(1024)
        );
    }
}

/// Validate control-flow vector domains without evaluating unselected branches.
pub(crate) fn validate_vector_control_flow(
    expr: &Expr,
    infer: &impl Fn(&Expr) -> ExprType,
) -> Result<(), crate::types::QueryError> {
    let branches: Vec<&Expr> = match expr {
        Expr::Case {
            results,
            else_result,
            ..
        } => results
            .iter()
            .chain(else_result.iter().map(Box::as_ref))
            .collect(),
        Expr::Function { name, args, .. } if name.eq_ignore_ascii_case("IF") => {
            args.iter().skip(1).collect()
        }
        Expr::Function { name, args, .. }
            if name.eq_ignore_ascii_case("IFNULL") || name.eq_ignore_ascii_case("COALESCE") =>
        {
            args.iter().collect()
        }
        _ => return Ok(()),
    };
    let mut vector: Option<ExprType> = None;
    for branch in branches {
        let ty = infer(branch);
        if matches!(ty.kind, ExprTypeKind::Vector { .. }) {
            vector = Some(if let Some(previous) = vector {
                checked_branch_types(previous, ty)?
            } else {
                ty
            });
        }
    }
    Ok(())
}
