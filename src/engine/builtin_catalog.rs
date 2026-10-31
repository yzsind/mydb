//! Scalar-function declarations shared by binding, compilation and costing.
//! Cost class, fixed bind-time volatility and kernel folding eligibility are
//! separate policies: a cheap DATE kernel can still observe session state.
//! Argument-dependent types remain in expr_type's type algebra.

#[cfg(test)]
use crate::engine::cbo::function_cost::function_class as cost_class;
#[cfg(test)]
use crate::engine::expr::retains_ast;
#[cfg(test)]
use crate::engine::expr_type::{builtin_spec, Volatility};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CostClass {
    IntegerArith,
    CheapString,
    RegexOrComplex,
    CryptoHash,
    DateTimeImmutable,
    DateTimeStable,
    SessionDependent,
    Volatile,
    Unknown,
}

macro_rules! fixed_contract {
    (None) => {
        None
    };
    (($id:ident, $min:expr, $max:expr, $volatility:ident, $result:expr)) => {
        Some((BuiltinId::$id, $min, $max, Volatility::$volatility, $result))
    };
}
pub(crate) use fixed_contract;

macro_rules! delegated_kernel {
    (None, $name:literal, $kernel:ident) => {
        None
    };
    (Field, $name:literal, $kernel:ident) => {
        Some((
            $name,
            $kernel::Field(crate::engine::expr::FieldComparisonDomain::Dynamic),
        ))
    };
    ($kind:ident, $name:literal, $kernel:ident) => {
        Some(($name, $kernel::$kind($name)))
    };
}

pub(crate) use delegated_kernel;

// Omit fallback-valued entries at expansion time. Runtime matches contain
// only properties relevant to that consumer, without duplicating declarations.
macro_rules! filtered_lookup {
    (@fixed [$($arms:tt)*];) => {
        macro_rules! declare_builtin_spec {
            () => {
                pub(crate) fn builtin_spec(name: &str) -> Option<BuiltinSpec> {
                    use IntegerWidth::*;
                    use TemporalKind::*;
                    let normalized = crate::engine::sys_schema::NormalizedFuncName::new(name);
                    let (id, min_arity, max_arity, volatility, mut result) = match normalized.as_str() {
                        $($arms)* _ => None
                    }?;
                    if id == BuiltinId::ConnectionId { result.max_octets = Some(21); }
                    Some(BuiltinSpec { id, min_arity, max_arity, volatility, result })
                }
            };
        }
        pub(crate) use declare_builtin_spec;
    };
    (@fixed [$($arms:tt)*]; [$($name:literal),+] => None; $($rest:tt)*) => {
        filtered_lookup!(@fixed [$($arms)*]; $($rest)*);
    };
    (@fixed [$($arms:tt)*]; [$($name:literal),+] => $value:tt; $($rest:tt)*) => {
        filtered_lookup!(@fixed [$($arms)* $($name)|+ => crate::engine::builtin_catalog::fixed_contract!($value),]; $($rest)*);
    };
    (@column [$($arms:tt)*];) => {
        macro_rules! declare_default_result_type {
            () => {
                #[inline]
                pub(crate) fn default_result_type(upper: &str) -> ColumnType {
                    match upper { $($arms)* _ => ColumnType::VarString }
                }
            };
        }
        pub(crate) use declare_default_result_type;
    };
    (@column [$($arms:tt)*]; [$($name:literal),+] => VarString; $($rest:tt)*) => {
        filtered_lookup!(@column [$($arms)*]; $($rest)*);
    };
    (@column [$($arms:tt)*]; [$($name:literal),+] => $value:ident; $($rest:tt)*) => {
        filtered_lookup!(@column [$($arms)* $($name)|+ => ColumnType::$value,]; $($rest)*);
    };
    (@cost [$($arms:tt)*];) => {
        macro_rules! declare_cost_class {
            () => {
                #[inline]
                pub(crate) fn function_class(lower: &str) -> FunctionClass {
                    match lower { $($arms)* _ => FunctionClass::Unknown }
                }
            };
        }
        pub(crate) use declare_cost_class;
    };
    (@cost [$($arms:tt)*]; [$($name:literal),+] => Unknown; $($rest:tt)*) => {
        filtered_lookup!(@cost [$($arms)*]; $($rest)*);
    };
    (@cost [$($arms:tt)*]; [$($name:literal),+] => $value:ident; $($rest:tt)*) => {
        filtered_lookup!(@cost [$($arms)* $($name)|+ => FunctionClass::$value,]; $($rest)*);
    };
    (@retain [$($arms:tt)*];) => {
        macro_rules! declare_retains_ast {
            () => {
                #[inline]
                pub(crate) fn retains_ast(upper: &str) -> bool {
                    match upper { $($arms)* _ => false }
                }
            };
        }
        pub(crate) use declare_retains_ast;
    };
    (@retain [$($arms:tt)*]; [$($name:literal),+] => false; $($rest:tt)*) => {
        filtered_lookup!(@retain [$($arms)*]; $($rest)*);
    };
    (@retain [$($arms:tt)*]; [$($name:literal),+] => true; $($rest:tt)*) => {
        filtered_lookup!(@retain [$($arms)* $($name)|+ => true,]; $($rest)*);
    };
}

macro_rules! scalar_catalog {
    (
        native { $($variant:ident => ($deterministic:literal, $diagnostic:expr) {
            $([$(($name:literal, $lower:literal)),+] => ($cost:ident, $column:ident, $fixed:tt, $retain:tt);)+
        })* }
        delegated { $($kind:ident {
            $([$(($other:literal, $other_lower:literal)),+] => ($other_cost:ident, $other_column:ident, $other_fixed:tt, $other_retain:tt);)+
        })* }
    ) => {
        // Emit the kernel type beside its evaluator, keeping the hot type's
        // original code-generation ownership while declarations stay here.
        macro_rules! declare_function_kernel {
            () => {
        #[derive(Debug, Clone, Copy)]
        enum FunctionKernel {
            Field(crate::engine::expr::FieldComparisonDomain),
            Math(crate::engine::expr::math::MathFunction),
            Network(crate::engine::expr::network::NetworkFunction),
            GeometryConstructor(&'static str, bool),
            Spatial(crate::engine::geo::sql::SpatialFunctionSpec),
            $($variant,)*
            LegacyDeterministic(&'static str),
            LegacySession(&'static str),
        }
        impl FunctionKernel {
            /// Child diagnostics are tracked separately by capability inference.
            fn may_error(self, arity: usize) -> bool {
                match self {
                    $(Self::$variant => ($diagnostic)(arity),)*
                    Self::Field(_) | Self::Math(_) | Self::Network(_) | Self::GeometryConstructor(..) | Self::Spatial(_) | Self::LegacyDeterministic(_) | Self::LegacySession(_) => true,
                }
            }
            fn is_deterministic(self) -> bool {
                match self {
                    $(Self::$variant => $deterministic,)*
                    Self::Field(_) | Self::Math(_) | Self::Network(_) | Self::GeometryConstructor(..) | Self::Spatial(_) | Self::LegacyDeterministic(_) => true,
                    Self::LegacySession(_) => false,
                }
            }
        }
        fn native_kernels() -> &'static [(&'static str, FunctionKernel)] {
            &[$($($(($name, FunctionKernel::$variant),)+)+)*]
        }
        fn delegated_kernels() -> impl Iterator<Item = (&'static str, FunctionKernel)> {
            const ENTRIES: &[Option<(&str, FunctionKernel)>] = &[
                $($($(crate::engine::builtin_catalog::delegated_kernel!($kind, $other, FunctionKernel),)+)+)*
            ];
            ENTRIES.iter().copied().flatten()
        }
            };
        }
        pub(crate) use declare_function_kernel;
        /// Namespace membership is independent of cost or a fixed result type.
        pub(crate) fn is_declared_builtin(upper: &str) -> bool {
            matches!(upper, $($($($name)|+)|+)|* | $($($($other)|+)|+)|*)
        }
        filtered_lookup!(@cost [];
            $($([$($lower),+] => $cost;)+)*
            $($([$($other_lower),+] => $other_cost;)+)*
        );
        filtered_lookup!(@column [];
            $($([$($name),+] => $column;)+)*
            $($([$($other),+] => $other_column;)+)*
        );
        filtered_lookup!(@fixed [];
            $($([$($name),+] => $fixed;)+)*
            $($([$($other),+] => $other_fixed;)+)*
        );
        filtered_lookup!(@retain [];
            $($([$($name),+] => $retain;)+)*
            $($([$($other),+] => $other_retain;)+)*
        );
        #[cfg(test)]
        pub(crate) const NAMES: &[&str] = &[
            $($($($name,)+)+)* $($($($other,)+)+)*
        ];
    };
}

scalar_catalog! {
    native {
        If => (true, |arity| arity != 3) {
            [("IF", "if")] => (Unknown, VarString, None, true);
        }
        IfNull => (true, |arity| arity != 2) {
            [("IFNULL", "ifnull")] => (Unknown, VarString, None, true);
        }
        Coalesce => (true, |arity| arity == 0) {
            [("COALESCE", "coalesce")] => (Unknown, VarString, None, true);
        }
        NullIf => (true, |arity| arity != 2) {
            [("NULLIF", "nullif")] => (Unknown, VarString, None, true);
        }
        Length => (true, |arity| arity != 1) {
            [("LENGTH", "length"), ("OCTET_LENGTH", "octet_length")] => (CheapString, LongLong, None, false);
        }
        CharLength => (true, |arity| arity != 1) {
            [("CHAR_LENGTH", "char_length"), ("CHARACTER_LENGTH", "character_length")] => (CheapString, LongLong, None, false);
        }
        Lower => (true, |arity| arity != 1) {
            [("LOWER", "lower"), ("LCASE", "lcase")] => (CheapString, VarString, None, false);
        }
        Upper => (true, |arity| arity != 1) {
            [("UPPER", "upper"), ("UCASE", "ucase")] => (CheapString, VarString, None, false);
        }
        Abs => (true, |_arity| true) {
            [("ABS", "abs")] => (IntegerArith, Double, None, true);
        }
        Round => (true, |_arity| true) {
            [("ROUND", "round")] => (IntegerArith, Double, None, true);
        }
        Truncate => (true, |_arity| true) {
            [("TRUNCATE", "truncate")] => (Unknown, Double, None, true);
        }
        Ceil => (true, |_arity| true) {
            [("CEIL", "ceil"), ("CEILING", "ceiling")] => (IntegerArith, Double, None, true);
        }
        Floor => (true, |_arity| true) {
            [("FLOOR", "floor")] => (IntegerArith, Double, None, true);
        }
        Sqrt => (true, |_arity| true) {
            [("SQRT", "sqrt")] => (IntegerArith, Double, None, false);
        }
        Sign => (true, |_arity| true) {
            [("SIGN", "sign")] => (IntegerArith, LongLong, None, false);
        }
        Mod => (true, |_arity| true) {
            [("MOD", "mod")] => (IntegerArith, Double, None, false);
        }
        Year => (true, |_arity| true) {
            [("YEAR", "year")] => (DateTimeImmutable, LongLong, None, false);
        }
        Month => (true, |_arity| true) {
            [("MONTH", "month")] => (DateTimeImmutable, LongLong, None, false);
        }
        Day => (true, |_arity| true) {
            [("DAY", "day"), ("DAYOFMONTH", "dayofmonth")] => (DateTimeImmutable, LongLong, None, false);
        }
        Hour => (true, |_arity| true) {
            [("HOUR", "hour")] => (DateTimeImmutable, LongLong, None, false);
        }
        Minute => (true, |_arity| true) {
            [("MINUTE", "minute")] => (DateTimeImmutable, LongLong, None, false);
        }
        Second => (true, |_arity| true) {
            [("SECOND", "second")] => (DateTimeImmutable, LongLong, None, false);
        }
        JsonExtract => (true, |_arity| true) {
            [("JSON_EXTRACT", "json_extract")] => (RegexOrComplex, Json, None, false);
        }
        RegexpLike => (true, |_arity| true) {
            [("REGEXP_LIKE", "regexp_like")] => (RegexOrComplex, LongLong, None, false);
        }
        Char => (true, |_arity| true) {
            [("CHAR", "char")] => (CheapString, VarString, None, false);
        }
        MakeSet => (true, |_arity| true) {
            [("MAKE_SET", "make_set")] => (RegexOrComplex, VarString, None, false);
        }
        IsTrue => (true, |_arity| true) {
            [("__IS_TRUE", "__is_true")] => (Unknown, LongLong, None, false);
        }
        IsFalse => (true, |_arity| true) {
            [("__IS_FALSE", "__is_false")] => (Unknown, LongLong, None, false);
        }
        Benchmark => (false, |_arity| true) {
            [("BENCHMARK", "benchmark")] => (Volatile, LongLong, None, false);
        }
        RegexpReplace => (true, |_arity| true) {
            [("REGEXP_REPLACE", "regexp_replace")] => (RegexOrComplex, VarString, None, false);
        }
        Substring => (true, |_arity| true) {
            [("SUBSTRING", "substring"), ("SUBSTR", "substr"), ("MID", "mid")] => (CheapString, VarString, None, false);
        }
        Left => (true, |_arity| true) {
            [("LEFT", "left")] => (CheapString, VarString, None, false);
        }
        Right => (true, |_arity| true) {
            [("RIGHT", "right")] => (CheapString, VarString, None, false);
        }
        Concat => (true, |_arity| false) {
            [("CONCAT", "concat")] => (CheapString, VarString, None, false);
        }
        ConcatWs => (true, |arity| arity == 0) {
            [("CONCAT_WS", "concat_ws")] => (CheapString, VarString, None, false);
        }
        Crc32 => (true, |arity| arity != 1) {
            [("CRC32", "crc32")] => (CryptoHash, Long, ( Crc32, 1, 1, Immutable, fixed_integer_result(Int, true, true)), false);
        }
        Hex => (true, |_arity| true) {
            [("HEX", "hex")] => (CheapString, VarString, (Hex, 1, 1, Immutable, fixed_string_result(None, true)), true);
        }
        Md5 => (true, |arity| arity != 1) {
            [("MD5", "md5")] => (CryptoHash, VarString, None, false);
        }
        Sha1 => (true, |arity| arity != 1) {
            [("SHA1", "sha1")] => (CryptoHash, VarString, None, false);
        }
        Sha => (true, |arity| arity != 1) {
            [("SHA", "sha")] => (CryptoHash, VarString, None, false);
        }
        Sha2 => (true, |_arity| true) {
            [("SHA2", "sha2")] => (CryptoHash, VarString, None, false);
        }
        AnyValue => (true, |arity| arity != 1) {
            [("ANY_VALUE", "any_value")] => (CheapString, VarString, None, false);
        }
        IsNull => (true, |arity| arity != 1) {
            [("ISNULL", "isnull")] => (Unknown, LongLong, None, false);
        }
        SubstringIndex => (true, |_arity| true) {
            [("SUBSTRING_INDEX", "substring_index")] => (CheapString, VarString, None, false);
        }
        Datediff => (true, |_arity| true) {
            [("DATEDIFF", "datediff")] => (DateTimeImmutable, LongLong, None, false);
        }
        Quarter => (true, |_arity| true) {
            [("QUARTER", "quarter")] => (DateTimeImmutable, LongLong, None, false);
        }
        Dayofweek => (true, |_arity| true) {
            [("DAYOFWEEK", "dayofweek")] => (DateTimeImmutable, LongLong, None, false);
        }
        Weekday => (true, |_arity| true) {
            [("WEEKDAY", "weekday")] => (DateTimeImmutable, LongLong, None, false);
        }
        Dayofyear => (true, |_arity| true) {
            [("DAYOFYEAR", "dayofyear")] => (DateTimeImmutable, LongLong, None, false);
        }
        LastDay => (true, |_arity| true) {
            [("LAST_DAY", "last_day")] => (DateTimeImmutable, Date, (Temporal, 1, 2, Immutable, fixed_temporal_result(Date, true)), false);
        }
        ToDays => (true, |_arity| true) {
            [("TO_DAYS", "to_days")] => (DateTimeImmutable, LongLong, None, false);
        }
        Microsecond => (true, |_arity| true) {
            [("MICROSECOND", "microsecond")] => (Unknown, LongLong, None, false);
        }
        Weekofyear => (true, |_arity| true) {
            [("WEEKOFYEAR", "weekofyear")] => (DateTimeImmutable, LongLong, None, false);
        }
        DateAdd => (true, |_arity| true) {
            [("DATE_ADD", "date_add")] => (DateTimeImmutable, DateTime, None, false);
            [("ADDDATE", "adddate")] => (DateTimeImmutable, Date, None, false);
        }
        DateSub => (true, |_arity| true) {
            [("DATE_SUB", "date_sub")] => (DateTimeImmutable, DateTime, None, false);
            [("SUBDATE", "subdate")] => (DateTimeImmutable, Date, None, false);
        }
        Date => (false, |_arity| true) {
            [("DATE", "date")] => (DateTimeImmutable, Date, (Temporal, 1, 2, Immutable, fixed_temporal_result(Date, true)), false);
        }
        IntervalPair => (true, |_arity| true) {
            [("__INTERVAL", "__interval")] => (Unknown, VarString, None, false);
        }
        DateFormat => (false, |_arity| true) {
            [("DATE_FORMAT", "date_format")] => (DateTimeImmutable, VarString, None, false);
        }
        UnixTimestamp => (false, |_arity| true) {
            [("UNIX_TIMESTAMP", "unix_timestamp")] => (DateTimeStable, LongLong, None, false);
        }
        Week => (false, |_arity| true) {
            [("WEEK", "week")] => (DateTimeImmutable, LongLong, None, false);
        }
        Now => (false, |_arity| true) {
            [("NOW", "now"), ("CURRENT_TIMESTAMP", "current_timestamp"), ("LOCALTIME", "localtime"), ("LOCALTIMESTAMP", "localtimestamp")] => (DateTimeStable, DateTime, ( CurrentTemporal, 0, 1, Stable, fixed_temporal_result(DateTime, false)), false);
        }
        Curdate => (false, |_arity| true) {
            [("CURDATE", "curdate"), ("CURRENT_DATE", "current_date")] => (DateTimeStable, Date, None, false);
        }
        Curtime => (false, |_arity| true) {
            [("CURTIME", "curtime"), ("CURRENT_TIME", "current_time")] => (DateTimeStable, Time, ( CurrentTemporal, 0, 1, Stable, fixed_temporal_result(Time, false)), false);
        }
        UtcTimestamp => (false, |_arity| true) {
            [("UTC_TIMESTAMP", "utc_timestamp")] => (Unknown, DateTime, ( CurrentTemporal, 0, 1, Stable, fixed_temporal_result(DateTime, false)), false);
        }
        UtcDate => (false, |_arity| true) {
            [("UTC_DATE", "utc_date")] => (Unknown, Date, None, false);
        }
        UtcTime => (false, |_arity| true) {
            [("UTC_TIME", "utc_time")] => (Unknown, Time, ( CurrentTemporal, 0, 1, Stable, fixed_temporal_result(Time, false)), false);
        }
        Version => (false, |_arity| true) {
            [("VERSION", "version")] => (SessionDependent, VarString, ( Version, 0, 0, Stable, fixed_string_result(Some(60 * 4), false)), false);
        }
        Database => (false, |_arity| true) {
            [("DATABASE", "database"), ("SCHEMA", "schema")] => (SessionDependent, VarString, ( Database, 0, 0, Stable, fixed_string_result(Some(64 * 4), true)), false);
        }
        User => (false, |_arity| true) {
            [("USER", "user"), ("SESSION_USER", "session_user")] => (SessionDependent, VarString, None, false);
        }
        CurrentUser => (false, |_arity| true) {
            [("CURRENT_USER", "current_user")] => (SessionDependent, VarString, None, false);
        }
        ConnectionId => (false, |_arity| true) {
            [("CONNECTION_ID", "connection_id")] => (SessionDependent, VarString, ( ConnectionId, 0, 0, Stable, fixed_integer_result(Big, true, false)), false);
        }
        FoundRows => (false, |_arity| true) {
            [("FOUND_ROWS", "found_rows")] => (SessionDependent, LongLong, ( StatementCounter, 0, 0, Stable, fixed_integer_result(Big, false, false)), false);
        }
        RowCount => (false, |_arity| true) {
            [("ROW_COUNT", "row_count")] => (SessionDependent, LongLong, ( StatementCounter, 0, 0, Stable, fixed_integer_result(Big, false, false)), false);
        }
        Ltrim => (true, |_arity| true) {
            [("LTRIM", "ltrim")] => (CheapString, VarString, None, false);
        }
        Rtrim => (true, |_arity| true) {
            [("RTRIM", "rtrim")] => (CheapString, VarString, None, false);
        }
        Trim => (true, |arity| !(1..=3).contains(&arity)) {
            [("TRIM", "trim")] => (CheapString, VarString, None, false);
        }
        Replace => (true, |_arity| true) {
            [("REPLACE", "replace")] => (CheapString, VarString, None, false);
        }
        Lpad => (false, |_arity| true) {
            [("LPAD", "lpad")] => (CheapString, VarString, None, false);
        }
        Rpad => (false, |_arity| true) {
            [("RPAD", "rpad")] => (CheapString, VarString, None, false);
        }
        Repeat => (false, |_arity| true) {
            [("REPEAT", "repeat")] => (CheapString, VarString, None, false);
        }
        Space => (false, |_arity| true) {
            [("SPACE", "space")] => (CheapString, VarString, None, false);
        }
        Reverse => (true, |arity| arity != 1) {
            [("REVERSE", "reverse")] => (CheapString, VarString, None, false);
        }
        Ascii => (true, |arity| arity != 1) {
            [("ASCII", "ascii")] => (CheapString, LongLong, None, false);
        }
        Ord => (true, |arity| arity != 1) {
            [("ORD", "ord")] => (CheapString, LongLong, None, false);
        }
        Strcmp => (true, |_arity| true) {
            [("STRCMP", "strcmp")] => (CheapString, LongLong, None, false);
        }
        DistinctCollationKey => (true, |arity| arity != 1) {
            [("__DISTINCT_COLLATION_KEY", "__distinct_collation_key")] => (Unknown, Blob, None, false);
        }
        TemporalComparisonKey => (false, |_arity| true) {
            [("__TEMPORAL_COMPARISON_KEY", "__temporal_comparison_key")] => (Unknown, DateTime, None, false);
        }
    }
    delegated {
        Field {
            [("FIELD", "field")] => (CheapString, LongLong, None, false);
        }
        LegacyDeterministic {
            [("IS_UUID", "is_uuid")] => (Unknown, LongLong, ( IsUuid, 1, 1, Immutable, fixed_integer_result(Big, false, true)), false);
            [("IS_IPV4_COMPAT", "is_ipv4_compat"), ("IS_IPV4_MAPPED", "is_ipv4_mapped")] => (Unknown, LongLong, None, false);
            [("POW", "pow"), ("POWER", "power"), ("LN", "ln"), ("LOG", "log"), ("LOG2", "log2"), ("LOG10", "log10"), ("EXP", "exp")] => (IntegerArith, Double, None, false);
            [("PI", "pi")] => (IntegerArith, Double, None, true);
            [("BIN", "bin")] => (CheapString, VarString, (Bin, 1, 1, Immutable, fixed_string_result(None, true)), true);
            [("OCT", "oct")] => (CheapString, VarString, None, true);
            [("UNHEX", "unhex")] => (CheapString, Blob, None, false);
            [("FIND_IN_SET", "find_in_set"), ("BIT_LENGTH", "bit_length")] => (CheapString, LongLong, None, false);
            [("ELT", "elt"), ("FORMAT", "format"), ("QUOTE", "quote")] => (CheapString, VarString, None, false);
            [("EXTRACT", "extract"), ("TIME_TO_SEC", "time_to_sec")] => (DateTimeImmutable, LongLong, None, false);
            [("STR_TO_DATE", "str_to_date")] => (DateTimeImmutable, Date, (Temporal, 2, 2, Immutable, fixed_temporal_result(Date, true)), true);
            [("FROM_DAYS", "from_days")] => (DateTimeImmutable, Date, (Temporal, 1, 2, Immutable, fixed_temporal_result(Date, true)), false);
            [("SEC_TO_TIME", "sec_to_time")] => (DateTimeImmutable, VarString, (Temporal, 1, 2, Immutable, fixed_temporal_result(Time, true)), true);
            [("JSON_UNQUOTE", "json_unquote"), ("JSON_PRETTY", "json_pretty"), ("SOUNDEX", "soundex"), ("TO_BASE64", "to_base64"), ("EXPORT_SET", "export_set")] => (RegexOrComplex, VarString, None, false);
            [("JSON_VALID", "json_valid"), ("JSON_LENGTH", "json_length"), ("JSON_CONTAINS", "json_contains"), ("JSON_CONTAINS_PATH", "json_contains_path"), ("JSON_DEPTH", "json_depth"), ("JSON_OVERLAPS", "json_overlaps")] => (RegexOrComplex, LongLong, None, false);
            [("JSON_TYPE", "json_type")] => (RegexOrComplex, VarString, ( JsonType, 1, 1, Immutable, fixed_string_result(Some(17 * 4), true)), false);
            [("JSON_KEYS", "json_keys"), ("JSON_OBJECT", "json_object"), ("JSON_ARRAY", "json_array"), ("JSON_MERGE_PATCH", "json_merge_patch"), ("JSON_MERGE_PRESERVE", "json_merge_preserve"), ("JSON_QUOTE", "json_quote")] => (RegexOrComplex, Json, None, false);
            [("JSON_STORAGE_SIZE", "json_storage_size"), ("JSON_STORAGE_FREE", "json_storage_free")] => (RegexOrComplex, VarString, ( JsonStorage, 1, 1, Immutable, fixed_integer_result(Big, false, true)), false);
            [("FROM_BASE64", "from_base64")] => (RegexOrComplex, Blob, None, false);
            [("BIT_COUNT", "bit_count"), ("JSON_SCHEMA_VALID", "json_schema_valid"), ("INET_ATON", "inet_aton"), ("IS_IPV4", "is_ipv4"), ("IS_IPV6", "is_ipv6")] => (Unknown, LongLong, None, false);
            [("DEGREES", "degrees"), ("RADIANS", "radians"), ("SIN", "sin"), ("COS", "cos"), ("TAN", "tan"), ("ASIN", "asin"), ("ACOS", "acos"), ("ATAN", "atan"), ("ATAN2", "atan2"), ("COT", "cot")] => (Unknown, Double, None, false);
            [("__AT_TIME_ZONE", "__at_time_zone")] => (Unknown, DateTime, None, false);
            [("UUID_TO_BIN", "uuid_to_bin"), ("INET6_ATON", "inet6_aton")] => (Unknown, Blob, None, false);
            [("INET_NTOA", "inet_ntoa")] => (Unknown, VarString, ( InetNtoa, 1, 1, Immutable, fixed_string_result(Some(31), true)), false);
            [("CAST", "cast")] => (Unknown, VarString, None, true);
            [("CONV", "conv"), ("GREATEST", "greatest"), ("LEAST", "least"), ("INSERT", "insert"), ("BIN_TO_UUID", "bin_to_uuid"), ("INET6_NTOA", "inet6_ntoa"), ("CONVERT_USING", "convert_using"), ("COLLATE", "collate")] => (Unknown, VarString, None, false);
        }
        LegacySession {
            [("HTTP_GET", "http_get"), ("HTTP_HEAD", "http_head"), ("HTTP_OPTIONS", "http_options"), ("HTTP_DELETE", "http_delete")] => (Volatile, Json, ( HttpRead, 1, 2, Volatile, ExprType::new(ExprTypeKind::Json, true)), false);
            [("HTTP_REQUEST", "http_request"), ("HTTP_POST", "http_post"), ("HTTP_PUT", "http_put"), ("HTTP_PATCH", "http_patch")] => (Volatile, Json, ( HttpWrite, 2, 3, Volatile, ExprType::new(ExprTypeKind::Json, true)), false);
            [("__TIMESTAMP_LITERAL", "__timestamp_literal")] => (Unknown, DateTime, None, true);
        }
        None {
            [("RAND_SEEDED", "rand_seeded")] => (IntegerArith, VarString, None, false);
            [("FORMAT_BYTES", "format_bytes"), ("FORMAT_PICO_TIME", "format_pico_time"), ("VALIDATE_PASSWORD_STRENGTH", "validate_password_strength"), ("STATEMENT_DIGEST_TEXT", "statement_digest_text"), ("INTERVAL", "interval"), ("GROUPING", "grouping"), ("NAME_CONST", "name_const")] => (CheapString, VarString, None, false);
            [("LOCATE", "locate"), ("INSTR", "instr"), ("POSITION", "position")] => (CheapString, LongLong, None, false);
            [("WEIGHT_STRING", "weight_string")] => (CheapString, Blob, None, true);
            [("DAYNAME", "dayname"), ("MONTHNAME", "monthname"), ("TIME_FORMAT", "time_format")] => (DateTimeImmutable, VarString, None, false);
            [("TIME", "time")] => (DateTimeImmutable, VarString, (Temporal, 1, 2, Immutable, fixed_temporal_result(Time, true)), true);
            [("MAKEDATE", "makedate")] => (DateTimeImmutable, Date, (Temporal, 1, 2, Immutable, fixed_temporal_result(Date, true)), false);
            [("MAKETIME", "maketime")] => (DateTimeImmutable, VarString, (Temporal, 3, 3, Immutable, fixed_temporal_result(Time, true)), false);
            [("TIMEDIFF", "timediff")] => (DateTimeImmutable, Time, (Temporal, 2, 2, Immutable, fixed_temporal_result(Time, true)), false);
            [("TIMESTAMPDIFF", "timestampdiff")] => (DateTimeImmutable, LongLong, None, false);
            [("TIMESTAMPADD", "timestampadd"), ("ADDTIME", "addtime"), ("SUBTIME", "subtime")] => (DateTimeImmutable, DateTime, None, false);
            [("PERIOD_ADD", "period_add"), ("PERIOD_DIFF", "period_diff")] => (DateTimeImmutable, VarString, ( Period, 2, 2, Immutable, fixed_integer_result(Big, false, true)), false);
            [("YEARWEEK", "yearweek")] => (DateTimeImmutable, VarString, ( YearWeek, 1, 2, Immutable, fixed_integer_result(Int, false, true)), false);
            [("FROM_UNIXTIME", "from_unixtime")] => (DateTimeStable, DateTime, ( Temporal, 1, 2, Stable, fixed_temporal_result(DateTime, true)), false);
            [("SYSTEM_USER", "system_user"), ("CURRENT_ROLE", "current_role"), ("CHARSET", "charset"), ("COLLATION", "collation"), ("ICU_VERSION", "icu_version")] => (SessionDependent, VarString, None, false);
            [("LAST_INSERT_ID", "last_insert_id")] => (SessionDependent, LongLong, ( StatementCounter, 0, 1, Volatile, fixed_integer_result(Big, false, false)), false);
            [("COERCIBILITY", "coercibility")] => (SessionDependent, LongLong, ( Coercibility, 1, 1, Immutable, fixed_integer_result(Int, false, false)), false);
            [("REGEXP", "regexp"), ("REGEXP_SUBSTR", "regexp_substr"), ("JSON_VALUE", "json_value")] => (RegexOrComplex, VarString, None, false);
            [("REGEXP_INSTR", "regexp_instr")] => (RegexOrComplex, LongLong, None, false);
            [("JSON_SET", "json_set"), ("JSON_INSERT", "json_insert"), ("JSON_REPLACE", "json_replace"), ("JSON_REMOVE", "json_remove"), ("JSON_ARRAY_APPEND", "json_array_append"), ("JSON_ARRAY_INSERT", "json_array_insert"), ("JSON_MERGE", "json_merge"), ("JSON_SEARCH", "json_search")] => (RegexOrComplex, Json, None, false);
            [("STATEMENT_DIGEST", "statement_digest")] => (CryptoHash, VarString, None, true);
            [("AES_ENCRYPT", "aes_encrypt"), ("AES_DECRYPT", "aes_decrypt"), ("COMPRESS", "compress"), ("UNCOMPRESS", "uncompress"), ("RANDOM_BYTES", "random_bytes")] => (CryptoHash, Blob, None, false);
            [("RAND", "rand")] => (Volatile, Double, None, false);
            [("UUID", "uuid")] => (Volatile, VarString, ( Uuid, 0, 0, Volatile, fixed_string_result(Some(36 * 3), false)), false);
            [("UUID_SHORT", "uuid_short")] => (Volatile, VarString, ( UuidShort, 0, 0, Volatile, fixed_integer_result(Big, true, false)), false);
            [("AI_GENERATE", "ai_generate")] => (Volatile, VarString, ( AiGenerate, 2, 3, Volatile, fixed_ai_text_result()), false);
            [("AI_GENERATE_DETAIL", "ai_generate_detail"), ("AI_EMBED_DETAIL", "ai_embed_detail")] => (Volatile, Json, ( AiDetail, 2, 3, Volatile, ExprType::new(ExprTypeKind::Json, true)), false);
            [("AI_EMBED", "ai_embed")] => (Volatile, Vector, ( AiEmbed, 2, 3, Volatile, fixed_ai_vector_result()), false);
            [("SLEEP", "sleep")] => (Volatile, LongLong, None, false);
            [("GET_LOCK", "get_lock"), ("RELEASE_LOCK", "release_lock"), ("IS_FREE_LOCK", "is_free_lock"), ("IS_USED_LOCK", "is_used_lock"), ("RELEASE_ALL_LOCKS", "release_all_locks")] => (Volatile, VarString, None, false);
            [("LOAD_FILE", "load_file")] => (Volatile, Blob, None, false);
            [("__MATCH_AGAINST", "__match_against")] => (Unknown, Double, None, false);
            [("EXTRACT_COMPOUND", "extract_compound"), ("TO_SECONDS", "to_seconds"), ("__MEMBER_OF", "__member_of"), ("KEYRING_KEY_GENERATE", "keyring_key_generate"), ("KEYRING_KEY_STORE", "keyring_key_store"), ("KEYRING_KEY_REMOVE", "keyring_key_remove"), ("KEYRING_KEY_LENGTH", "keyring_key_length"), ("UNCOMPRESSED_LENGTH", "uncompressed_length")] => (Unknown, LongLong, None, false);
            [("SYSDATE", "sysdate")] => (Unknown, DateTime, ( CurrentTemporal, 0, 1, Stable, fixed_temporal_result(DateTime, false)), false);
            [("TIMESTAMP", "timestamp")] => (Unknown, DateTime, ( Temporal, 1, 2, Immutable, fixed_temporal_result(DateTime, true)), false);
            [("CONVERT_TZ", "convert_tz")] => (Unknown, DateTime, ( Temporal, 2, 3, Stable, fixed_temporal_result(DateTime, true)), false);
            [("WEIGHT_STRING_TYPED", "weight_string_typed")] => (Unknown, Blob, None, true);
            [("JSON_SCHEMA_VALIDATION_REPORT", "json_schema_validation_report"), ("JSON_ARRAYAGG", "json_arrayagg"), ("JSON_OBJECTAGG", "json_objectagg")] => (Unknown, Json, None, false);
            [("VECTOR_DIM", "vector_dim")] => (Unknown, VarString, ( VectorDim, 1, 1, Immutable, fixed_integer_result(Int, false, true)), false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_kernel_registrations_have_one_declaration() {
        let names = NAMES
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(names.len(), NAMES.len(), "duplicate scalar name");
        let mut kernels = std::collections::HashSet::new();
        for name in crate::engine::expr_program::catalog_kernel_names() {
            assert!(names.contains(name), "undeclared kernel {name}");
            assert!(kernels.insert(name), "duplicate kernel {name}");
        }
    }

    #[test]
    fn contextual_folding_is_independent_of_type_and_cost() {
        // DATE has a fixed DATE result, but its kernel can observe SQL mode.
        assert!(!crate::engine::expr_program::catalog_kernel_deterministic(
            "DATE"
        ));
        assert_eq!(
            builtin_spec("DATE").unwrap().volatility,
            Volatility::Immutable
        );
        assert_eq!(cost_class("date"), CostClass::DateTimeImmutable);
        assert!(!crate::engine::expr_program::catalog_kernel_deterministic(
            "__TIMESTAMP_LITERAL"
        ));
        // Value folding may lose width and result-family metadata even for
        // deterministic kernels; their typed AST must survive.
        for name in ["ABS", "HEX", "CAST", "IFNULL", "STR_TO_DATE"] {
            assert!(retains_ast(name), "{name}");
        }
        assert!(!retains_ast("CRC32"));
    }
}
