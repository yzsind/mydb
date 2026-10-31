use crate::engine::auth::{
    AccountKey, AuthCatalog, AuthorizationContext, Privilege, PrivilegeScope, RequiredPrivilege,
};
use crate::engine::session_registry::SessionRegistry;
use crate::engine::show::{column_default_value, column_extra, mysql_type_name_with_unsigned};
use crate::storage::{
    error, split_table_name, Catalog, ColumnSchema, DataTypeKind, Database, IndexColumnDirection,
    IndexSchema, RoutineKindSchema, RoutineSchema, TableSchema, TriggerSchema, ViewColumnDef,
    DEFAULT_DATABASE,
};
use crate::types::{Column, ColumnType, QueryError, Value};

#[derive(Clone, Copy)]
pub(crate) struct SystemColumn {
    pub name: &'static str,
    pub result_type: ColumnType,
    pub data_type: &'static str,
    pub column_type_sql: &'static str,
    pub nullable: bool,
    pub char_len: Option<i64>,
    pub octet_len: Option<i64>,
    pub numeric_precision: Option<i64>,
    pub numeric_scale: Option<i64>,
    pub datetime_precision: Option<i64>,
}

impl SystemColumn {
    pub(crate) const fn new(
        name: &'static str,
        result_type: ColumnType,
        data_type: &'static str,
        column_type_sql: &'static str,
        nullable: bool,
    ) -> Self {
        Self {
            name,
            result_type,
            data_type,
            column_type_sql,
            nullable,
            char_len: None,
            octet_len: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
        }
    }
    pub(crate) const fn unsigned_bigint(name: &'static str, nullable: bool) -> Self {
        Self::new(
            name,
            ColumnType::LongLong,
            "bigint",
            "bigint unsigned",
            nullable,
        )
    }
    pub(crate) const fn bigint(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::LongLong, "bigint", "bigint", nullable)
    }
    pub(crate) const fn int(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::Long, "int", "int", nullable)
    }
    pub(crate) const fn unsigned_int(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::Long, "int", "int unsigned", nullable)
    }
    pub(crate) const fn datetime(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::DateTime, "datetime", "datetime", nullable)
    }
    pub(crate) const fn timestamp(name: &'static str, nullable: bool) -> Self {
        Self::new(
            name,
            ColumnType::Timestamp,
            "timestamp",
            "timestamp",
            nullable,
        )
    }
    pub(crate) const fn empty_binary(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::VarString, "binary", "binary(0)", nullable)
    }
    pub(crate) const fn text(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::Blob, "text", "text", nullable)
    }
    pub(crate) const fn longtext(name: &'static str, nullable: bool) -> Self {
        Self::new(name, ColumnType::Blob, "longtext", "longtext", nullable)
    }
    pub(crate) const fn character_lengths(
        mut self,
        chars: Option<i64>,
        octets: Option<i64>,
    ) -> Self {
        self.char_len = chars;
        self.octet_len = octets;
        self
    }
    pub(crate) const fn numeric(mut self, precision: Option<i64>, scale: Option<i64>) -> Self {
        self.numeric_precision = precision;
        self.numeric_scale = scale;
        self
    }
    pub(crate) const fn temporal_precision(mut self, precision: Option<i64>) -> Self {
        self.datetime_precision = precision;
        self
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SystemTableDef {
    pub schema: &'static str,
    pub name: &'static str,
    pub table_type: &'static str,
    pub columns: &'static [SystemColumn],
    /// `ENGINE` reported in `information_schema.tables` / `SHOW CREATE TABLE`.
    /// Empty string means NULL (MySQL reports NULL for information_schema views).
    pub engine: &'static str,
    pub indexes: &'static [SystemTableIndex],
    pub create_sql: Option<&'static str>,
    pub hidden: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct SystemTableIndex {
    pub name: &'static str,
    pub unique: bool,
    pub columns: &'static [&'static str],
    pub index_type: &'static str,
    pub visible: bool,
}

/// Compatibility ownership of an `information_schema` registration.  The
/// MySQL 8.0 Community contract is deliberately kept separate from legacy,
/// optional-plugin and mydb-native extensions so adding useful diagnostics
/// cannot accidentally weaken the supported Community compatibility gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InformationSchemaCompatibility {
    Mysql80Community,
    MysqlLegacy,
    MysqlOptionalPlugin,
    MydbNative,
}

/// Whether a registered view has a real row producer.  An unsupported engine
/// facility must opt in to an honest empty result with a reviewable reason;
/// there is intentionally no implicit "registered means empty" fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InformationSchemaProducer {
    Real,
    HonestEmpty(&'static str),
}

#[derive(Clone, Copy)]
struct InformationSchemaRegistration {
    table: SystemTableDef,
    compatibility: InformationSchemaCompatibility,
    producer: InformationSchemaProducer,
}

/// Safe equality constraints extracted from a system-table predicate.  The
/// ordinary expression evaluator remains the semantic oracle; producers only
/// use these fields to avoid visiting unrelated catalog objects.
#[derive(Clone, Debug, Default)]
pub(crate) struct SystemScanRequest {
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub storage_id: Option<u64>,
    /// Safe equality conjuncts extracted from the original predicate. System
    /// producers may use these for direct lookup; the executor always
    /// re-evaluates the complete predicate as the correctness oracle.
    pub equalities: Vec<(String, Value)>,
}

// Captured MySQL metadata can report independent display, character and octet lengths.
macro_rules! string_column {
    ($name:literal, $kind:literal, $sql:literal, $chars:literal, $octets:literal, $nullable:expr) => {
        SystemColumn::new($name, ColumnType::VarString, $kind, $sql, $nullable)
            .character_lengths(Some($chars), Some($octets))
    };
}

// Length and octet length are independent (utf8mb3/utf8mb4 metadata differ).
macro_rules! varchar_column {
    ($name:literal, $length:literal, $octets:literal, $nullable:expr) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "varchar",
            concat!("varchar(", stringify!($length), ")"),
            $nullable,
        )
        .character_lengths(Some($length), Some($octets))
    };
}

macro_rules! v {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "varchar",
            "varchar(255)",
            false,
        )
        .character_lengths(Some(255), Some(1020))
    };
}

macro_rules! vn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..v!($name)
        }
    };
}

macro_rules! text {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::VarString, "text", "text", true)
            .character_lengths(Some(65535), Some(65535))
    };
}

macro_rules! i32c {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Long, "int", "int", true).numeric(Some(10), Some(0))
    };
}

macro_rules! i64c {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::LongLong, "bigint", "bigint", true)
            .numeric(Some(19), Some(0))
    };
}

macro_rules! f64c {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Double, "double", "double", true)
            .numeric(Some(22), None)
    };
}

macro_rules! dt {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::DateTime, "datetime", "datetime", true)
            .temporal_precision(Some(0))
    };
}

const SCHEMATA_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CATALOG_NAME", 64, 192, true),
    varchar_column!("SCHEMA_NAME", 64, 192, true),
    varchar_column!("DEFAULT_CHARACTER_SET_NAME", 64, 192, false),
    varchar_column!("DEFAULT_COLLATION_NAME", 64, 192, false),
    SystemColumn::empty_binary("SQL_PATH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::new(
        "DEFAULT_ENCRYPTION",
        ColumnType::VarString,
        "enum",
        "enum('NO','YES')",
        false,
    )
    .character_lengths(Some(3), Some(9)),
];

const TABLES_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    SystemColumn::new(
        "TABLE_TYPE",
        ColumnType::VarString,
        "enum",
        "enum('BASE TABLE','VIEW','SYSTEM VIEW')",
        false,
    )
    .character_lengths(Some(11), Some(33)),
    varchar_column!("ENGINE", 64, 192, true),
    SystemColumn::int("VERSION", true).numeric(Some(10), Some(0)),
    SystemColumn::new(
        "ROW_FORMAT",
        ColumnType::VarString,
        "enum",
        "enum('Fixed','Dynamic','Compressed','Redundant','Compact','Paged')",
        true,
    )
    .character_lengths(Some(10), Some(30)),
    SystemColumn::unsigned_bigint("TABLE_ROWS", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("AVG_ROW_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("DATA_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("MAX_DATA_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("INDEX_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("DATA_FREE", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("AUTO_INCREMENT", true).numeric(Some(20), Some(0)),
    SystemColumn::timestamp("CREATE_TIME", false).temporal_precision(Some(0)),
    SystemColumn::datetime("UPDATE_TIME", true).temporal_precision(Some(0)),
    SystemColumn::datetime("CHECK_TIME", true).temporal_precision(Some(0)),
    varchar_column!("TABLE_COLLATION", 64, 192, true),
    SystemColumn::bigint("CHECKSUM", true).numeric(Some(19), Some(0)),
    varchar_column!("CREATE_OPTIONS", 256, 768, true),
    SystemColumn::text("TABLE_COMMENT", true).character_lengths(Some(65535), Some(65535)),
];

const COLUMNS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    varchar_column!("COLUMN_NAME", 64, 192, true),
    SystemColumn::unsigned_int("ORDINAL_POSITION", false).numeric(Some(10), Some(0)),
    SystemColumn::text("COLUMN_DEFAULT", true).character_lengths(Some(65535), Some(65535)),
    varchar_column!("IS_NULLABLE", 3, 9, false),
    SystemColumn::longtext("DATA_TYPE", true).character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::bigint("CHARACTER_MAXIMUM_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("CHARACTER_OCTET_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::unsigned_bigint("NUMERIC_PRECISION", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("NUMERIC_SCALE", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_int("DATETIME_PRECISION", true).numeric(Some(10), Some(0)),
    varchar_column!("CHARACTER_SET_NAME", 64, 192, true),
    varchar_column!("COLLATION_NAME", 64, 192, true),
    SystemColumn::new(
        "COLUMN_TYPE",
        ColumnType::Blob,
        "mediumtext",
        "mediumtext",
        false,
    )
    .character_lengths(Some(16777215), Some(16777215)),
    SystemColumn::new(
        "COLUMN_KEY",
        ColumnType::VarString,
        "enum",
        "enum('','PRI','UNI','MUL')",
        false,
    )
    .character_lengths(Some(3), Some(9)),
    varchar_column!("EXTRA", 256, 768, true),
    varchar_column!("PRIVILEGES", 154, 462, true),
    SystemColumn::text("COLUMN_COMMENT", false).character_lengths(Some(65535), Some(65535)),
    SystemColumn::longtext("GENERATION_EXPRESSION", false)
        .character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::unsigned_int("SRS_ID", true).numeric(Some(10), Some(0)),
];

const STATISTICS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    SystemColumn::int("NON_UNIQUE", false).numeric(Some(10), Some(0)),
    varchar_column!("INDEX_SCHEMA", 64, 192, true),
    varchar_column!("INDEX_NAME", 64, 192, true),
    SystemColumn::unsigned_int("SEQ_IN_INDEX", false).numeric(Some(10), Some(0)),
    varchar_column!("COLUMN_NAME", 64, 192, true),
    varchar_column!("COLLATION", 1, 3, true),
    SystemColumn::bigint("CARDINALITY", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("SUB_PART", true).numeric(Some(19), Some(0)),
    SystemColumn::empty_binary("PACKED", true).character_lengths(Some(0), Some(0)),
    varchar_column!("NULLABLE", 3, 9, false),
    varchar_column!("INDEX_TYPE", 11, 33, false),
    varchar_column!("COMMENT", 8, 24, false),
    varchar_column!("INDEX_COMMENT", 2048, 6144, false),
    varchar_column!("IS_VISIBLE", 3, 9, false),
    SystemColumn::longtext("EXPRESSION", true)
        .character_lengths(Some(4294967295), Some(4294967295)),
];

const TABLE_CONSTRAINTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONSTRAINT_CATALOG", 64, 192, true),
    varchar_column!("CONSTRAINT_SCHEMA", 64, 192, true),
    varchar_column!("CONSTRAINT_NAME", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    varchar_column!("CONSTRAINT_TYPE", 11, 33, false),
    varchar_column!("ENFORCED", 3, 9, false),
];

const KEY_COLUMN_USAGE_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONSTRAINT_CATALOG", 64, 192, true),
    varchar_column!("CONSTRAINT_SCHEMA", 64, 192, true),
    varchar_column!("CONSTRAINT_NAME", 64, 192, true),
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    varchar_column!("COLUMN_NAME", 64, 192, true),
    SystemColumn::unsigned_int("ORDINAL_POSITION", false).numeric(Some(10), Some(0)),
    SystemColumn::unsigned_int("POSITION_IN_UNIQUE_CONSTRAINT", true).numeric(Some(10), Some(0)),
    varchar_column!("REFERENCED_TABLE_SCHEMA", 64, 192, true),
    varchar_column!("REFERENCED_TABLE_NAME", 64, 192, true),
    varchar_column!("REFERENCED_COLUMN_NAME", 64, 192, true),
];

const VIEWS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    SystemColumn::longtext("VIEW_DEFINITION", true)
        .character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::new(
        "CHECK_OPTION",
        ColumnType::VarString,
        "enum",
        "enum('NONE','LOCAL','CASCADED')",
        true,
    )
    .character_lengths(Some(8), Some(24)),
    SystemColumn::new(
        "IS_UPDATABLE",
        ColumnType::VarString,
        "enum",
        "enum('NO','YES')",
        true,
    )
    .character_lengths(Some(3), Some(9)),
    varchar_column!("DEFINER", 288, 864, true),
    varchar_column!("SECURITY_TYPE", 7, 21, true),
    varchar_column!("CHARACTER_SET_CLIENT", 64, 192, false),
    varchar_column!("COLLATION_CONNECTION", 64, 192, false),
];

const CHARACTER_SETS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CHARACTER_SET_NAME", 64, 192, false),
    varchar_column!("DEFAULT_COLLATE_NAME", 64, 192, false),
    varchar_column!("DESCRIPTION", 2048, 6144, false),
    SystemColumn::unsigned_int("MAXLEN", false).numeric(Some(10), Some(0)),
];

const COLLATIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("COLLATION_NAME", 64, 192, false),
    varchar_column!("CHARACTER_SET_NAME", 64, 192, false),
    SystemColumn::unsigned_bigint("ID", false).numeric(Some(20), Some(0)),
    varchar_column!("IS_DEFAULT", 3, 9, false),
    varchar_column!("IS_COMPILED", 3, 9, false),
    SystemColumn::unsigned_int("SORTLEN", false).numeric(Some(10), Some(0)),
    SystemColumn::new(
        "PAD_ATTRIBUTE",
        ColumnType::VarString,
        "enum",
        "enum('PAD SPACE','NO PAD')",
        false,
    )
    .character_lengths(Some(9), Some(27)),
];

const COLLATION_APPLICABILITY_COLUMNS: &[SystemColumn] = &[
    varchar_column!("COLLATION_NAME", 64, 192, false),
    varchar_column!("CHARACTER_SET_NAME", 64, 192, false),
];

const ENGINES_COLUMNS: &[SystemColumn] = &[
    string_column!("ENGINE", "varchar", "varchar(64)", 21, 64, false),
    string_column!("SUPPORT", "varchar", "varchar(8)", 2, 8, false),
    string_column!("COMMENT", "varchar", "varchar(80)", 26, 80, false),
    string_column!("TRANSACTIONS", "varchar", "varchar(3)", 1, 3, true),
    string_column!("XA", "varchar", "varchar(3)", 1, 3, true),
    string_column!("SAVEPOINTS", "varchar", "varchar(3)", 1, 3, true),
];

const PROCESSLIST_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("ID", false),
    string_column!("USER", "varchar", "varchar(32)", 10, 32, false),
    string_column!("HOST", "varchar", "varchar(261)", 87, 261, false),
    string_column!("DB", "varchar", "varchar(64)", 21, 64, true),
    string_column!("COMMAND", "varchar", "varchar(16)", 5, 16, false),
    SystemColumn::int("TIME", false),
    string_column!("STATE", "varchar", "varchar(64)", 21, 64, true),
    string_column!("INFO", "varchar", "varchar(65535)", 21845, 65535, true),
];

const ROUTINES_COLUMNS: &[SystemColumn] = &[
    varchar_column!("SPECIFIC_NAME", 64, 192, false),
    varchar_column!("ROUTINE_CATALOG", 64, 192, true),
    varchar_column!("ROUTINE_SCHEMA", 64, 192, true),
    varchar_column!("ROUTINE_NAME", 64, 192, false),
    SystemColumn::new("ROUTINE_TYPE", ColumnType::VarString, "enum", "enum('FUNCTION','PROCEDURE')", false).character_lengths(Some(9), Some(27)),
    SystemColumn::longtext("DATA_TYPE", true).character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::bigint("CHARACTER_MAXIMUM_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("CHARACTER_OCTET_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::unsigned_int("NUMERIC_PRECISION", true).numeric(Some(10), Some(0)),
    SystemColumn::unsigned_int("NUMERIC_SCALE", true).numeric(Some(10), Some(0)),
    SystemColumn::unsigned_int("DATETIME_PRECISION", true).numeric(Some(10), Some(0)),
    varchar_column!("CHARACTER_SET_NAME", 64, 192, true),
    varchar_column!("COLLATION_NAME", 64, 192, true),
    SystemColumn::longtext("DTD_IDENTIFIER", true).character_lengths(Some(4294967295), Some(4294967295)),
    varchar_column!("ROUTINE_BODY", 3, 9, false),
    SystemColumn::longtext("ROUTINE_DEFINITION", true).character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::empty_binary("EXTERNAL_NAME", true).character_lengths(Some(0), Some(0)),
    varchar_column!("EXTERNAL_LANGUAGE", 64, 192, false),
    varchar_column!("PARAMETER_STYLE", 3, 9, false),
    varchar_column!("IS_DETERMINISTIC", 3, 9, false),
    SystemColumn::new("SQL_DATA_ACCESS", ColumnType::VarString, "enum", "enum('CONTAINS SQL','NO SQL','READS SQL DATA','MODIFIES SQL DATA')", false).character_lengths(Some(17), Some(51)),
    SystemColumn::empty_binary("SQL_PATH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::new("SECURITY_TYPE", ColumnType::VarString, "enum", "enum('DEFAULT','INVOKER','DEFINER')", false).character_lengths(Some(7), Some(21)),
    SystemColumn::timestamp("CREATED", false).temporal_precision(Some(0)),
    SystemColumn::timestamp("LAST_ALTERED", false).temporal_precision(Some(0)),
    SystemColumn::new("SQL_MODE", ColumnType::VarString, "set", "set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", false).character_lengths(Some(520), Some(1560)),
    SystemColumn::text("ROUTINE_COMMENT", false).character_lengths(Some(65535), Some(65535)),
    varchar_column!("DEFINER", 288, 864, false),
    varchar_column!("CHARACTER_SET_CLIENT", 64, 192, false),
    varchar_column!("COLLATION_CONNECTION", 64, 192, false),
    varchar_column!("DATABASE_COLLATION", 64, 192, false),
];

const TRIGGERS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TRIGGER_CATALOG", 64, 192, true),
    varchar_column!("TRIGGER_SCHEMA", 64, 192, true),
    varchar_column!("TRIGGER_NAME", 64, 192, false),
    SystemColumn::new("EVENT_MANIPULATION", ColumnType::VarString, "enum", "enum('INSERT','UPDATE','DELETE')", false).character_lengths(Some(6), Some(18)),
    varchar_column!("EVENT_OBJECT_CATALOG", 64, 192, true),
    varchar_column!("EVENT_OBJECT_SCHEMA", 64, 192, true),
    varchar_column!("EVENT_OBJECT_TABLE", 64, 192, true),
    SystemColumn::unsigned_int("ACTION_ORDER", false).numeric(Some(10), Some(0)),
    SystemColumn::empty_binary("ACTION_CONDITION", true).character_lengths(Some(0), Some(0)),
    SystemColumn::longtext("ACTION_STATEMENT", false).character_lengths(Some(4294967295), Some(4294967295)),
    varchar_column!("ACTION_ORIENTATION", 3, 9, false),
    SystemColumn::new("ACTION_TIMING", ColumnType::VarString, "enum", "enum('BEFORE','AFTER')", false).character_lengths(Some(6), Some(18)),
    SystemColumn::empty_binary("ACTION_REFERENCE_OLD_TABLE", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("ACTION_REFERENCE_NEW_TABLE", true).character_lengths(Some(0), Some(0)),
    varchar_column!("ACTION_REFERENCE_OLD_ROW", 3, 9, false),
    varchar_column!("ACTION_REFERENCE_NEW_ROW", 3, 9, false),
    SystemColumn::new("CREATED", ColumnType::Timestamp, "timestamp", "timestamp(2)", false).temporal_precision(Some(2)),
    SystemColumn::new("SQL_MODE", ColumnType::VarString, "set", "set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", false).character_lengths(Some(520), Some(1560)),
    varchar_column!("DEFINER", 288, 864, false),
    varchar_column!("CHARACTER_SET_CLIENT", 64, 192, false),
    varchar_column!("COLLATION_CONNECTION", 64, 192, false),
    varchar_column!("DATABASE_COLLATION", 64, 192, false),
];

const PARTITIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, false),
    varchar_column!("PARTITION_NAME", 64, 192, true),
    varchar_column!("SUBPARTITION_NAME", 64, 192, true),
    SystemColumn::unsigned_int("PARTITION_ORDINAL_POSITION", true).numeric(Some(10), Some(0)),
    SystemColumn::unsigned_int("SUBPARTITION_ORDINAL_POSITION", true).numeric(Some(10), Some(0)),
    varchar_column!("PARTITION_METHOD", 13, 39, true),
    varchar_column!("SUBPARTITION_METHOD", 13, 39, true),
    varchar_column!("PARTITION_EXPRESSION", 2048, 6144, true),
    varchar_column!("SUBPARTITION_EXPRESSION", 2048, 6144, true),
    SystemColumn::text("PARTITION_DESCRIPTION", true).character_lengths(Some(65535), Some(65535)),
    SystemColumn::unsigned_bigint("TABLE_ROWS", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("AVG_ROW_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("DATA_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("MAX_DATA_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("INDEX_LENGTH", true).numeric(Some(20), Some(0)),
    SystemColumn::unsigned_bigint("DATA_FREE", true).numeric(Some(20), Some(0)),
    SystemColumn::timestamp("CREATE_TIME", false).temporal_precision(Some(0)),
    SystemColumn::datetime("UPDATE_TIME", true).temporal_precision(Some(0)),
    SystemColumn::datetime("CHECK_TIME", true).temporal_precision(Some(0)),
    SystemColumn::bigint("CHECKSUM", true).numeric(Some(19), Some(0)),
    SystemColumn::text("PARTITION_COMMENT", false).character_lengths(Some(65535), Some(65535)),
    varchar_column!("NODEGROUP", 256, 768, true),
    varchar_column!("TABLESPACE_NAME", 268, 804, true),
];

const REFERENTIAL_CONSTRAINTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONSTRAINT_CATALOG", 64, 192, false),
    varchar_column!("CONSTRAINT_SCHEMA", 64, 192, false),
    varchar_column!("CONSTRAINT_NAME", 64, 192, true),
    varchar_column!("UNIQUE_CONSTRAINT_CATALOG", 64, 192, false),
    varchar_column!("UNIQUE_CONSTRAINT_SCHEMA", 64, 192, false),
    varchar_column!("UNIQUE_CONSTRAINT_NAME", 64, 192, true),
    SystemColumn::new(
        "MATCH_OPTION",
        ColumnType::VarString,
        "enum",
        "enum('NONE','PARTIAL','FULL')",
        false,
    )
    .character_lengths(Some(7), Some(21)),
    SystemColumn::new(
        "UPDATE_RULE",
        ColumnType::VarString,
        "enum",
        "enum('NO ACTION','RESTRICT','CASCADE','SET NULL','SET DEFAULT')",
        false,
    )
    .character_lengths(Some(11), Some(33)),
    SystemColumn::new(
        "DELETE_RULE",
        ColumnType::VarString,
        "enum",
        "enum('NO ACTION','RESTRICT','CASCADE','SET NULL','SET DEFAULT')",
        false,
    )
    .character_lengths(Some(11), Some(33)),
    varchar_column!("TABLE_NAME", 64, 192, false),
    varchar_column!("REFERENCED_TABLE_NAME", 64, 192, false),
];

const PARAMETERS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("SPECIFIC_CATALOG", 64, 192, true),
    varchar_column!("SPECIFIC_SCHEMA", 64, 192, true),
    varchar_column!("SPECIFIC_NAME", 64, 192, false),
    SystemColumn::unsigned_bigint("ORDINAL_POSITION", false).numeric(Some(20), Some(0)),
    varchar_column!("PARAMETER_MODE", 5, 15, true),
    varchar_column!("PARAMETER_NAME", 64, 192, true),
    SystemColumn::longtext("DATA_TYPE", true).character_lengths(Some(4294967295), Some(4294967295)),
    SystemColumn::bigint("CHARACTER_MAXIMUM_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("CHARACTER_OCTET_LENGTH", true).numeric(Some(19), Some(0)),
    SystemColumn::unsigned_int("NUMERIC_PRECISION", true).numeric(Some(10), Some(0)),
    SystemColumn::bigint("NUMERIC_SCALE", true).numeric(Some(19), Some(0)),
    SystemColumn::unsigned_int("DATETIME_PRECISION", true).numeric(Some(10), Some(0)),
    varchar_column!("CHARACTER_SET_NAME", 64, 192, true),
    varchar_column!("COLLATION_NAME", 64, 192, true),
    SystemColumn::new(
        "DTD_IDENTIFIER",
        ColumnType::Blob,
        "mediumtext",
        "mediumtext",
        false,
    )
    .character_lengths(Some(16777215), Some(16777215)),
    SystemColumn::new(
        "ROUTINE_TYPE",
        ColumnType::VarString,
        "enum",
        "enum('FUNCTION','PROCEDURE')",
        false,
    )
    .character_lengths(Some(9), Some(27)),
];

const EVENTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("EVENT_CATALOG", 64, 192, true),
    varchar_column!("EVENT_SCHEMA", 64, 192, true),
    varchar_column!("EVENT_NAME", 64, 192, false),
    varchar_column!("DEFINER", 288, 864, false),
    varchar_column!("TIME_ZONE", 64, 192, false),
    varchar_column!("EVENT_BODY", 3, 9, false),
    SystemColumn::longtext("EVENT_DEFINITION", false).character_lengths(Some(4294967295), Some(4294967295)),
    varchar_column!("EVENT_TYPE", 9, 27, false),
    SystemColumn::datetime("EXECUTE_AT", true).temporal_precision(Some(0)),
    varchar_column!("INTERVAL_VALUE", 256, 768, true),
    SystemColumn::new("INTERVAL_FIELD", ColumnType::VarString, "enum", "enum('YEAR','QUARTER','MONTH','DAY','HOUR','MINUTE','WEEK','SECOND','MICROSECOND','YEAR_MONTH','DAY_HOUR','DAY_MINUTE','DAY_SECOND','HOUR_MINUTE','HOUR_SECOND','MINUTE_SECOND','DAY_MICROSECOND','HOUR_MICROSECOND','MINUTE_MICROSECOND','SECOND_MICROSECOND')", true).character_lengths(Some(18), Some(54)),
    SystemColumn::new("SQL_MODE", ColumnType::VarString, "set", "set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','NOT_USED_9','NOT_USED_10','NOT_USED_11','NOT_USED_12','NOT_USED_13','NOT_USED_14','NOT_USED_15','NOT_USED_16','NOT_USED_17','NOT_USED_18','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','ALLOW_INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NOT_USED_29','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH','TIME_TRUNCATE_FRACTIONAL')", false).character_lengths(Some(520), Some(1560)),
    SystemColumn::datetime("STARTS", true).temporal_precision(Some(0)),
    SystemColumn::datetime("ENDS", true).temporal_precision(Some(0)),
    SystemColumn::new("STATUS", ColumnType::VarString, "enum", "enum('ENABLED','DISABLED','SLAVESIDE_DISABLED')", false).character_lengths(Some(18), Some(54)),
    varchar_column!("ON_COMPLETION", 12, 36, false),
    SystemColumn::timestamp("CREATED", false).temporal_precision(Some(0)),
    SystemColumn::timestamp("LAST_ALTERED", false).temporal_precision(Some(0)),
    SystemColumn::datetime("LAST_EXECUTED", true).temporal_precision(Some(0)),
    varchar_column!("EVENT_COMMENT", 2048, 6144, false),
    SystemColumn::unsigned_int("ORIGINATOR", false).numeric(Some(10), Some(0)),
    varchar_column!("CHARACTER_SET_CLIENT", 64, 192, false),
    varchar_column!("COLLATION_CONNECTION", 64, 192, false),
    varchar_column!("DATABASE_COLLATION", 64, 192, false),
];

const PRIVILEGE_COLUMNS: &[SystemColumn] = &[
    string_column!("GRANTEE", "varchar", "varchar(292)", 97, 292, false),
    string_column!("TABLE_CATALOG", "varchar", "varchar(512)", 170, 512, false),
    string_column!("PRIVILEGE_TYPE", "varchar", "varchar(64)", 21, 64, false),
    string_column!("IS_GRANTABLE", "varchar", "varchar(3)", 1, 3, false),
];

const SCHEMA_PRIVILEGES_COLUMNS: &[SystemColumn] = &[
    string_column!("GRANTEE", "varchar", "varchar(292)", 97, 292, false),
    string_column!("TABLE_CATALOG", "varchar", "varchar(512)", 170, 512, false),
    string_column!("TABLE_SCHEMA", "varchar", "varchar(64)", 21, 64, false),
    string_column!("PRIVILEGE_TYPE", "varchar", "varchar(64)", 21, 64, false),
    string_column!("IS_GRANTABLE", "varchar", "varchar(3)", 1, 3, false),
];

const TABLE_PRIVILEGES_COLUMNS: &[SystemColumn] = &[
    string_column!("GRANTEE", "varchar", "varchar(292)", 97, 292, false),
    string_column!("TABLE_CATALOG", "varchar", "varchar(512)", 170, 512, false),
    string_column!("TABLE_SCHEMA", "varchar", "varchar(64)", 21, 64, false),
    string_column!("TABLE_NAME", "varchar", "varchar(64)", 21, 64, false),
    string_column!("PRIVILEGE_TYPE", "varchar", "varchar(64)", 21, 64, false),
    string_column!("IS_GRANTABLE", "varchar", "varchar(3)", 1, 3, false),
];

const COLUMN_PRIVILEGES_COLUMNS: &[SystemColumn] = &[
    string_column!("GRANTEE", "varchar", "varchar(292)", 97, 292, false),
    string_column!("TABLE_CATALOG", "varchar", "varchar(512)", 170, 512, false),
    string_column!("TABLE_SCHEMA", "varchar", "varchar(64)", 21, 64, false),
    string_column!("TABLE_NAME", "varchar", "varchar(64)", 21, 64, false),
    string_column!("COLUMN_NAME", "varchar", "varchar(64)", 21, 64, false),
    string_column!("PRIVILEGE_TYPE", "varchar", "varchar(64)", 21, 64, false),
    string_column!("IS_GRANTABLE", "varchar", "varchar(3)", 1, 3, false),
];

const ROLE_GRANTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("GRANTOR", 97, 291, true),
    varchar_column!("GRANTOR_HOST", 256, 768, true),
    string_column!("GRANTEE", "char", "char(32)", 32, 96, false),
    string_column!("GRANTEE_HOST", "char", "char(255)", 255, 255, false),
    varchar_column!("TABLE_CATALOG", 3, 9, false),
    string_column!("TABLE_SCHEMA", "char", "char(64)", 64, 192, false),
    string_column!("TABLE_NAME", "char", "char(64)", 64, 192, false),
    SystemColumn::new("PRIVILEGE_TYPE", ColumnType::VarString, "set", "set('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter','Create View','Show view','Trigger')", false).character_lengths(Some(98), Some(294)),
    varchar_column!("IS_GRANTABLE", 3, 9, false),
];

const ROLE_COLUMN_GRANTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("GRANTOR", 97, 291, true),
    varchar_column!("GRANTOR_HOST", 256, 768, true),
    string_column!("GRANTEE", "char", "char(32)", 32, 96, false),
    string_column!("GRANTEE_HOST", "char", "char(255)", 255, 255, false),
    varchar_column!("TABLE_CATALOG", 3, 9, false),
    string_column!("TABLE_SCHEMA", "char", "char(64)", 64, 192, false),
    string_column!("TABLE_NAME", "char", "char(64)", 64, 192, false),
    string_column!("COLUMN_NAME", "char", "char(64)", 64, 192, false),
    SystemColumn::new(
        "PRIVILEGE_TYPE",
        ColumnType::VarString,
        "set",
        "set('Select','Insert','Update','References')",
        false,
    )
    .character_lengths(Some(31), Some(93)),
    varchar_column!("IS_GRANTABLE", 3, 9, false),
];

const ROLE_ROUTINE_GRANTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("GRANTOR", 97, 291, true),
    varchar_column!("GRANTOR_HOST", 256, 768, true),
    string_column!("GRANTEE", "char", "char(32)", 32, 96, false),
    string_column!("GRANTEE_HOST", "char", "char(255)", 255, 255, false),
    varchar_column!("SPECIFIC_CATALOG", 3, 9, false),
    string_column!("SPECIFIC_SCHEMA", "char", "char(64)", 64, 192, false),
    string_column!("SPECIFIC_NAME", "char", "char(64)", 64, 192, false),
    varchar_column!("ROUTINE_CATALOG", 3, 9, false),
    string_column!("ROUTINE_SCHEMA", "char", "char(64)", 64, 192, false),
    string_column!("ROUTINE_NAME", "char", "char(64)", 64, 192, false),
    SystemColumn::new(
        "PRIVILEGE_TYPE",
        ColumnType::VarString,
        "set",
        "set('Execute','Alter Routine','Grant')",
        false,
    )
    .character_lengths(Some(27), Some(81)),
    varchar_column!("IS_GRANTABLE", 3, 9, false),
];

const APPLICABLE_ROLES_COLUMNS: &[SystemColumn] = &[
    varchar_column!("USER", 97, 291, true),
    varchar_column!("HOST", 256, 768, true),
    varchar_column!("GRANTEE", 97, 388, true),
    varchar_column!("GRANTEE_HOST", 256, 1024, true),
    varchar_column!("ROLE_NAME", 255, 1020, true),
    varchar_column!("ROLE_HOST", 256, 1024, true),
    varchar_column!("IS_GRANTABLE", 3, 9, false),
    varchar_column!("IS_DEFAULT", 3, 9, true),
    varchar_column!("IS_MANDATORY", 3, 9, false),
];

const ENABLED_ROLES_COLUMNS: &[SystemColumn] = &[
    varchar_column!("ROLE_NAME", 255, 1020, true),
    varchar_column!("ROLE_HOST", 255, 1020, true),
    varchar_column!("IS_DEFAULT", 3, 9, true),
    varchar_column!("IS_MANDATORY", 3, 9, false),
];

const COLUMN_STATISTICS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("SCHEMA_NAME", 64, 192, false),
    varchar_column!("TABLE_NAME", 64, 192, false),
    varchar_column!("COLUMN_NAME", 64, 192, false),
    SystemColumn::new("HISTOGRAM", ColumnType::Json, "json", "json", false),
];

const KEYWORDS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("WORD", 128, 512, true),
    SystemColumn::int("RESERVED", true).numeric(Some(10), Some(0)),
];

const CHECK_CONSTRAINTS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONSTRAINT_CATALOG", 64, 192, true),
    varchar_column!("CONSTRAINT_SCHEMA", 64, 192, true),
    varchar_column!("CONSTRAINT_NAME", 64, 192, false),
    SystemColumn::longtext("CHECK_CLAUSE", false)
        .character_lengths(Some(4294967295), Some(4294967295)),
];

const VIEW_TABLE_USAGE_COLUMNS: &[SystemColumn] = &[
    varchar_column!("VIEW_CATALOG", 64, 192, true),
    varchar_column!("VIEW_SCHEMA", 64, 192, true),
    varchar_column!("VIEW_NAME", 64, 192, true),
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
];

const VIEW_ROUTINE_USAGE_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    varchar_column!("SPECIFIC_CATALOG", 64, 192, true),
    varchar_column!("SPECIFIC_SCHEMA", 64, 192, true),
    varchar_column!("SPECIFIC_NAME", 64, 192, false),
];

const ST_SPATIAL_REFERENCE_SYSTEMS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("SRS_NAME", 80, 240, false),
    SystemColumn::unsigned_int("SRS_ID", false).numeric(Some(10), Some(0)),
    varchar_column!("ORGANIZATION", 256, 768, true),
    SystemColumn::unsigned_int("ORGANIZATION_COORDSYS_ID", true).numeric(Some(10), Some(0)),
    varchar_column!("DEFINITION", 4096, 12288, false),
    varchar_column!("DESCRIPTION", 2048, 6144, true),
];

const ST_GEOMETRY_COLUMNS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, true),
    varchar_column!("TABLE_SCHEMA", 64, 192, true),
    varchar_column!("TABLE_NAME", 64, 192, true),
    varchar_column!("COLUMN_NAME", 64, 192, true),
    varchar_column!("SRS_NAME", 80, 240, true),
    SystemColumn::unsigned_int("SRS_ID", true).numeric(Some(10), Some(0)),
    SystemColumn::longtext("GEOMETRY_TYPE_NAME", true)
        .character_lengths(Some(4294967295), Some(4294967295)),
];

const ST_UNITS_OF_MEASURE_COLUMNS: &[SystemColumn] = &[
    varchar_column!("UNIT_NAME", 255, 1020, true),
    varchar_column!("UNIT_TYPE", 7, 28, true),
    SystemColumn::new(
        "CONVERSION_FACTOR",
        ColumnType::Double,
        "double",
        "double",
        true,
    )
    .numeric(Some(22), None),
    varchar_column!("DESCRIPTION", 255, 1020, true),
];

const EXTENSION_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, false),
    varchar_column!("TABLE_SCHEMA", 64, 192, false),
    varchar_column!("TABLE_NAME", 64, 192, false),
    SystemColumn::new("ENGINE_ATTRIBUTE", ColumnType::Json, "json", "json", true),
    SystemColumn::new(
        "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnType::Json,
        "json",
        "json",
        true,
    ),
];

const SCHEMATA_EXTENSIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CATALOG_NAME", 64, 192, true),
    varchar_column!("SCHEMA_NAME", 64, 192, true),
    varchar_column!("OPTIONS", 256, 768, true),
];

const COLUMNS_EXTENSIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("TABLE_CATALOG", 64, 192, false),
    varchar_column!("TABLE_SCHEMA", 64, 192, false),
    varchar_column!("TABLE_NAME", 64, 192, false),
    varchar_column!("COLUMN_NAME", 64, 192, true),
    SystemColumn::new("ENGINE_ATTRIBUTE", ColumnType::Json, "json", "json", true),
    SystemColumn::new(
        "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnType::Json,
        "json",
        "json",
        true,
    ),
];

const TABLE_CONSTRAINTS_EXTENSIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONSTRAINT_CATALOG", 64, 192, false),
    varchar_column!("CONSTRAINT_SCHEMA", 64, 192, false),
    varchar_column!("CONSTRAINT_NAME", 64, 192, false),
    varchar_column!("TABLE_NAME", 64, 192, false),
    SystemColumn::new("ENGINE_ATTRIBUTE", ColumnType::Json, "json", "json", true),
    SystemColumn::new(
        "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnType::Json,
        "json",
        "json",
        true,
    ),
];

const USER_ATTRIBUTES_COLUMNS: &[SystemColumn] = &[
    string_column!("USER", "char", "char(32)", 32, 96, false),
    string_column!("HOST", "char", "char(255)", 255, 255, false),
    SystemColumn::longtext("ATTRIBUTE", true).character_lengths(Some(4294967295), Some(4294967295)),
];

const PLUGINS_COLUMNS: &[SystemColumn] = &[
    string_column!("PLUGIN_NAME", "varchar", "varchar(64)", 21, 64, false),
    string_column!("PLUGIN_VERSION", "varchar", "varchar(20)", 6, 20, false),
    string_column!("PLUGIN_STATUS", "varchar", "varchar(10)", 3, 10, false),
    string_column!("PLUGIN_TYPE", "varchar", "varchar(80)", 26, 80, false),
    string_column!(
        "PLUGIN_TYPE_VERSION",
        "varchar",
        "varchar(20)",
        6,
        20,
        false
    ),
    string_column!("PLUGIN_LIBRARY", "varchar", "varchar(64)", 21, 64, true),
    string_column!(
        "PLUGIN_LIBRARY_VERSION",
        "varchar",
        "varchar(20)",
        6,
        20,
        true
    ),
    string_column!("PLUGIN_AUTHOR", "varchar", "varchar(64)", 21, 64, true),
    string_column!(
        "PLUGIN_DESCRIPTION",
        "varchar",
        "varchar(65535)",
        21845,
        65535,
        true
    ),
    string_column!("PLUGIN_LICENSE", "varchar", "varchar(80)", 26, 80, true),
    string_column!("LOAD_OPTION", "varchar", "varchar(64)", 21, 64, false),
];

const OPTIMIZER_TRACE_COLUMNS: &[SystemColumn] = &[
    string_column!("QUERY", "varchar", "varchar(65535)", 21845, 65535, false),
    string_column!("TRACE", "varchar", "varchar(65535)", 21845, 65535, false),
    SystemColumn::int("MISSING_BYTES_BEYOND_MAX_MEM_SIZE", false),
    SystemColumn::new(
        "INSUFFICIENT_PRIVILEGES",
        ColumnType::Tiny,
        "tinyint",
        "tinyint(1)",
        false,
    ),
];

const PROFILING_COLUMNS: &[SystemColumn] = &[
    SystemColumn::int("QUERY_ID", false),
    SystemColumn::int("SEQ", false),
    string_column!("STATE", "varchar", "varchar(30)", 10, 30, false),
    SystemColumn::new(
        "DURATION",
        ColumnType::Decimal {
            precision: 65,
            scale: 0,
        },
        "decimal",
        "decimal(905,0)",
        false,
    ),
    SystemColumn::new(
        "CPU_USER",
        ColumnType::Decimal {
            precision: 65,
            scale: 0,
        },
        "decimal",
        "decimal(905,0)",
        true,
    ),
    SystemColumn::new(
        "CPU_SYSTEM",
        ColumnType::Decimal {
            precision: 65,
            scale: 0,
        },
        "decimal",
        "decimal(905,0)",
        true,
    ),
    SystemColumn::int("CONTEXT_VOLUNTARY", true),
    SystemColumn::int("CONTEXT_INVOLUNTARY", true),
    SystemColumn::int("BLOCK_OPS_IN", true),
    SystemColumn::int("BLOCK_OPS_OUT", true),
    SystemColumn::int("MESSAGES_SENT", true),
    SystemColumn::int("MESSAGES_RECEIVED", true),
    SystemColumn::int("PAGE_FAULTS_MAJOR", true),
    SystemColumn::int("PAGE_FAULTS_MINOR", true),
    SystemColumn::int("SWAPS", true),
    string_column!("SOURCE_FUNCTION", "varchar", "varchar(30)", 10, 30, true),
    string_column!("SOURCE_FILE", "varchar", "varchar(20)", 6, 20, true),
    SystemColumn::int("SOURCE_LINE", true),
];

const FILES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::bigint("FILE_ID", true).numeric(Some(19), Some(0)),
    SystemColumn::text("FILE_NAME", true).character_lengths(Some(65535), Some(65535)),
    varchar_column!("FILE_TYPE", 256, 768, true),
    varchar_column!("TABLESPACE_NAME", 268, 804, false),
    string_column!("TABLE_CATALOG", "char", "char(0)", 0, 0, false),
    SystemColumn::empty_binary("TABLE_SCHEMA", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("TABLE_NAME", true).character_lengths(Some(0), Some(0)),
    varchar_column!("LOGFILE_GROUP_NAME", 256, 768, true),
    SystemColumn::bigint("LOGFILE_GROUP_NUMBER", true).numeric(Some(19), Some(0)),
    varchar_column!("ENGINE", 64, 192, false),
    SystemColumn::empty_binary("FULLTEXT_KEYS", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("DELETED_ROWS", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("UPDATE_COUNT", true).character_lengths(Some(0), Some(0)),
    SystemColumn::bigint("FREE_EXTENTS", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("TOTAL_EXTENTS", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("EXTENT_SIZE", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("INITIAL_SIZE", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("MAXIMUM_SIZE", true).numeric(Some(19), Some(0)),
    SystemColumn::bigint("AUTOEXTEND_SIZE", true).numeric(Some(19), Some(0)),
    SystemColumn::empty_binary("CREATION_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("LAST_UPDATE_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("LAST_ACCESS_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("RECOVER_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("TRANSACTION_COUNTER", true).character_lengths(Some(0), Some(0)),
    SystemColumn::bigint("VERSION", true).numeric(Some(19), Some(0)),
    varchar_column!("ROW_FORMAT", 256, 768, true),
    SystemColumn::empty_binary("TABLE_ROWS", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("AVG_ROW_LENGTH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("DATA_LENGTH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("MAX_DATA_LENGTH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("INDEX_LENGTH", true).character_lengths(Some(0), Some(0)),
    SystemColumn::bigint("DATA_FREE", true).numeric(Some(19), Some(0)),
    SystemColumn::empty_binary("CREATE_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("UPDATE_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("CHECK_TIME", true).character_lengths(Some(0), Some(0)),
    SystemColumn::empty_binary("CHECKSUM", true).character_lengths(Some(0), Some(0)),
    varchar_column!("STATUS", 256, 768, true),
    varchar_column!("EXTRA", 256, 768, true),
];

const INNODB_BUFFER_PAGE_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("POOL_ID", false),
    SystemColumn::unsigned_bigint("BLOCK_ID", false),
    SystemColumn::unsigned_bigint("SPACE", false),
    SystemColumn::unsigned_bigint("PAGE_NUMBER", false),
    string_column!("PAGE_TYPE", "varchar", "varchar(64)", 21, 64, true),
    SystemColumn::unsigned_bigint("FLUSH_TYPE", false),
    SystemColumn::unsigned_bigint("FIX_COUNT", false),
    string_column!("IS_HASHED", "varchar", "varchar(3)", 1, 3, true),
    SystemColumn::unsigned_bigint("NEWEST_MODIFICATION", false),
    SystemColumn::unsigned_bigint("OLDEST_MODIFICATION", false),
    SystemColumn::unsigned_bigint("ACCESS_TIME", false),
    string_column!("TABLE_NAME", "varchar", "varchar(1024)", 341, 1024, true),
    string_column!("INDEX_NAME", "varchar", "varchar(1024)", 341, 1024, true),
    SystemColumn::unsigned_bigint("NUMBER_RECORDS", false),
    SystemColumn::unsigned_bigint("DATA_SIZE", false),
    SystemColumn::unsigned_bigint("COMPRESSED_SIZE", false),
    string_column!("PAGE_STATE", "varchar", "varchar(64)", 21, 64, true),
    string_column!("IO_FIX", "varchar", "varchar(64)", 21, 64, true),
    string_column!("IS_OLD", "varchar", "varchar(3)", 1, 3, true),
    SystemColumn::unsigned_bigint("FREE_PAGE_CLOCK", false),
    string_column!("IS_STALE", "varchar", "varchar(3)", 1, 3, true),
];

const INNODB_BUFFER_PAGE_LRU_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("POOL_ID", false),
    SystemColumn::unsigned_bigint("LRU_POSITION", false),
    SystemColumn::unsigned_bigint("SPACE", false),
    SystemColumn::unsigned_bigint("PAGE_NUMBER", false),
    string_column!("PAGE_TYPE", "varchar", "varchar(64)", 21, 64, true),
    SystemColumn::unsigned_bigint("FLUSH_TYPE", false),
    SystemColumn::unsigned_bigint("FIX_COUNT", false),
    string_column!("IS_HASHED", "varchar", "varchar(3)", 1, 3, true),
    SystemColumn::unsigned_bigint("NEWEST_MODIFICATION", false),
    SystemColumn::unsigned_bigint("OLDEST_MODIFICATION", false),
    SystemColumn::unsigned_bigint("ACCESS_TIME", false),
    string_column!("TABLE_NAME", "varchar", "varchar(1024)", 341, 1024, true),
    string_column!("INDEX_NAME", "varchar", "varchar(1024)", 341, 1024, true),
    SystemColumn::unsigned_bigint("NUMBER_RECORDS", false),
    SystemColumn::unsigned_bigint("DATA_SIZE", false),
    SystemColumn::unsigned_bigint("COMPRESSED_SIZE", false),
    string_column!("COMPRESSED", "varchar", "varchar(3)", 1, 3, true),
    string_column!("IO_FIX", "varchar", "varchar(64)", 21, 64, true),
    string_column!("IS_OLD", "varchar", "varchar(3)", 1, 3, true),
    SystemColumn::unsigned_bigint("FREE_PAGE_CLOCK", false),
];

const INNODB_CACHED_INDEXES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_int("SPACE_ID", false),
    SystemColumn::unsigned_bigint("INDEX_ID", false),
    SystemColumn::unsigned_bigint("N_CACHED_PAGES", false),
];

const INNODB_CMP_COLUMNS: &[SystemColumn] = &[
    SystemColumn::int("page_size", false),
    SystemColumn::int("compress_ops", false),
    SystemColumn::int("compress_ops_ok", false),
    SystemColumn::int("compress_time", false),
    SystemColumn::int("uncompress_ops", false),
    SystemColumn::int("uncompress_time", false),
];

const INNODB_CMP_PER_INDEX_COLUMNS: &[SystemColumn] = &[
    string_column!("database_name", "varchar", "varchar(192)", 64, 192, false),
    string_column!("table_name", "varchar", "varchar(192)", 64, 192, false),
    string_column!("index_name", "varchar", "varchar(192)", 64, 192, false),
    SystemColumn::int("compress_ops", false),
    SystemColumn::int("compress_ops_ok", false),
    SystemColumn::int("compress_time", false),
    SystemColumn::int("uncompress_ops", false),
    SystemColumn::int("uncompress_time", false),
];

const INNODB_CMPMEM_COLUMNS: &[SystemColumn] = &[
    SystemColumn::int("page_size", false),
    SystemColumn::int("buffer_pool_instance", false),
    SystemColumn::int("pages_used", false),
    SystemColumn::int("pages_free", false),
    SystemColumn::bigint("relocation_ops", false),
    SystemColumn::int("relocation_time", false),
];

const INNODB_COLUMNS_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    string_column!("NAME", "varchar", "varchar(193)", 64, 193, false),
    SystemColumn::unsigned_bigint("POS", false),
    SystemColumn::int("MTYPE", false),
    SystemColumn::int("PRTYPE", false),
    SystemColumn::int("LEN", false),
    SystemColumn::int("HAS_DEFAULT", false),
    SystemColumn::text("DEFAULT_VALUE", true).character_lengths(Some(65535), Some(65535)),
];

const INNODB_DATAFILES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::new(
        "SPACE",
        ColumnType::VarString,
        "varbinary",
        "varbinary(256)",
        true,
    )
    .character_lengths(Some(256), Some(256)),
    varchar_column!("PATH", 512, 1536, false),
];

const INNODB_FIELDS_COLUMNS: &[SystemColumn] = &[
    SystemColumn::new(
        "INDEX_ID",
        ColumnType::VarString,
        "varbinary",
        "varbinary(256)",
        true,
    )
    .character_lengths(Some(256), Some(256)),
    varchar_column!("NAME", 64, 192, false),
    SystemColumn::unsigned_bigint("POS", false).numeric(Some(20), Some(0)),
];

const INNODB_FOREIGN_COLUMNS: &[SystemColumn] = &[
    varchar_column!("ID", 129, 387, true),
    varchar_column!("FOR_NAME", 129, 387, true),
    varchar_column!("REF_NAME", 129, 387, true),
    SystemColumn::bigint("N_COLS", false).numeric(Some(19), Some(0)),
    SystemColumn::unsigned_bigint("TYPE", false).numeric(Some(20), Some(0)),
];

const INNODB_FOREIGN_COLS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("ID", 129, 387, true),
    varchar_column!("FOR_COL_NAME", 64, 192, false),
    varchar_column!("REF_COL_NAME", 64, 192, false),
    SystemColumn::unsigned_int("POS", false).numeric(Some(10), Some(0)),
];

const INNODB_FT_BEING_DELETED_COLUMNS: &[SystemColumn] =
    &[SystemColumn::unsigned_bigint("DOC_ID", false)];

const INNODB_FT_CONFIG_COLUMNS: &[SystemColumn] = &[
    string_column!("KEY", "varchar", "varchar(193)", 64, 193, false),
    string_column!("VALUE", "varchar", "varchar(193)", 64, 193, false),
];

const INNODB_FT_DEFAULT_STOPWORD_COLUMNS: &[SystemColumn] = &[string_column!(
    "value",
    "varchar",
    "varchar(18)",
    6,
    18,
    false
)];

const INNODB_FT_DELETED_COLUMNS: &[SystemColumn] =
    &[SystemColumn::unsigned_bigint("DOC_ID", false)];

const INNODB_FT_INDEX_CACHE_COLUMNS: &[SystemColumn] = &[
    string_column!("WORD", "varchar", "varchar(337)", 112, 337, false),
    SystemColumn::unsigned_bigint("FIRST_DOC_ID", false),
    SystemColumn::unsigned_bigint("LAST_DOC_ID", false),
    SystemColumn::unsigned_bigint("DOC_COUNT", false),
    SystemColumn::unsigned_bigint("DOC_ID", false),
    SystemColumn::unsigned_bigint("POSITION", false),
];

const INNODB_FT_INDEX_TABLE_COLUMNS: &[SystemColumn] = &[
    string_column!("WORD", "varchar", "varchar(337)", 112, 337, false),
    SystemColumn::unsigned_bigint("FIRST_DOC_ID", false),
    SystemColumn::unsigned_bigint("LAST_DOC_ID", false),
    SystemColumn::unsigned_bigint("DOC_COUNT", false),
    SystemColumn::unsigned_bigint("DOC_ID", false),
    SystemColumn::unsigned_bigint("POSITION", false),
];

const INNODB_INDEXES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("INDEX_ID", false),
    string_column!("NAME", "varchar", "varchar(193)", 64, 193, false),
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    SystemColumn::int("TYPE", false),
    SystemColumn::int("N_FIELDS", false),
    SystemColumn::int("PAGE_NO", false),
    SystemColumn::int("SPACE", false),
    SystemColumn::int("MERGE_THRESHOLD", false),
];

const INNODB_METRICS_COLUMNS: &[SystemColumn] = &[
    string_column!("NAME", "varchar", "varchar(193)", 64, 193, false),
    string_column!("SUBSYSTEM", "varchar", "varchar(193)", 64, 193, false),
    SystemColumn::bigint("COUNT", false),
    SystemColumn::bigint("MAX_COUNT", true),
    SystemColumn::bigint("MIN_COUNT", true),
    SystemColumn::new("AVG_COUNT", ColumnType::Float, "float", "float(12,0)", true),
    SystemColumn::bigint("COUNT_RESET", false),
    SystemColumn::bigint("MAX_COUNT_RESET", true),
    SystemColumn::bigint("MIN_COUNT_RESET", true),
    SystemColumn::new(
        "AVG_COUNT_RESET",
        ColumnType::Float,
        "float",
        "float(12,0)",
        true,
    ),
    SystemColumn::datetime("TIME_ENABLED", true),
    SystemColumn::datetime("TIME_DISABLED", true),
    SystemColumn::bigint("TIME_ELAPSED", true),
    SystemColumn::datetime("TIME_RESET", true),
    string_column!("STATUS", "varchar", "varchar(193)", 64, 193, false),
    string_column!("TYPE", "varchar", "varchar(193)", 64, 193, false),
    string_column!("COMMENT", "varchar", "varchar(193)", 64, 193, false),
];

const INNODB_SESSION_TEMP_TABLESPACES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_int("ID", false),
    SystemColumn::unsigned_int("SPACE", false),
    string_column!("PATH", "varchar", "varchar(4001)", 1333, 4001, false),
    SystemColumn::unsigned_bigint("SIZE", false),
    string_column!("STATE", "varchar", "varchar(192)", 64, 192, false),
    string_column!("PURPOSE", "varchar", "varchar(192)", 64, 192, false),
];

const INNODB_TRX_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("trx_id", false),
    string_column!("trx_state", "varchar", "varchar(13)", 4, 13, false),
    SystemColumn::datetime("trx_started", false),
    string_column!(
        "trx_requested_lock_id",
        "varchar",
        "varchar(126)",
        42,
        126,
        true
    ),
    SystemColumn::datetime("trx_wait_started", true),
    SystemColumn::unsigned_bigint("trx_weight", false),
    SystemColumn::unsigned_bigint("trx_mysql_thread_id", false),
    string_column!("trx_query", "varchar", "varchar(1024)", 341, 1024, true),
    string_column!(
        "trx_operation_state",
        "varchar",
        "varchar(64)",
        21,
        64,
        true
    ),
    SystemColumn::unsigned_bigint("trx_tables_in_use", false),
    SystemColumn::unsigned_bigint("trx_tables_locked", false),
    SystemColumn::unsigned_bigint("trx_lock_structs", false),
    SystemColumn::unsigned_bigint("trx_lock_memory_bytes", false),
    SystemColumn::unsigned_bigint("trx_rows_locked", false),
    SystemColumn::unsigned_bigint("trx_rows_modified", false),
    SystemColumn::unsigned_bigint("trx_concurrency_tickets", false),
    string_column!(
        "trx_isolation_level",
        "varchar",
        "varchar(16)",
        5,
        16,
        false
    ),
    SystemColumn::int("trx_unique_checks", false),
    SystemColumn::int("trx_foreign_key_checks", false),
    string_column!(
        "trx_last_foreign_key_error",
        "varchar",
        "varchar(256)",
        85,
        256,
        true
    ),
    SystemColumn::int("trx_adaptive_hash_latched", false),
    SystemColumn::unsigned_bigint("trx_adaptive_hash_timeout", false),
    SystemColumn::int("trx_is_read_only", false),
    SystemColumn::int("trx_autocommit_non_locking", false),
    SystemColumn::unsigned_bigint("trx_schedule_weight", true),
];

const INNODB_LOCK_WAITS_COLUMNS: &[SystemColumn] = &[
    v!("REQUESTING_TRX_ID"),
    v!("REQUESTED_LOCK_ID"),
    v!("BLOCKING_TRX_ID"),
    v!("BLOCKING_LOCK_ID"),
];

const INNODB_TABLES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    string_column!("NAME", "varchar", "varchar(655)", 218, 655, false),
    SystemColumn::int("FLAG", false),
    SystemColumn::int("N_COLS", false),
    SystemColumn::bigint("SPACE", false),
    string_column!("ROW_FORMAT", "varchar", "varchar(12)", 4, 12, true),
    SystemColumn::unsigned_int("ZIP_PAGE_SIZE", false),
    string_column!("SPACE_TYPE", "varchar", "varchar(10)", 3, 10, true),
    SystemColumn::int("INSTANT_COLS", false),
    SystemColumn::int("TOTAL_ROW_VERSIONS", false),
];

const INNODB_TABLESPACES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_int("SPACE", false),
    string_column!("NAME", "varchar", "varchar(655)", 218, 655, false),
    SystemColumn::unsigned_int("FLAG", false),
    string_column!("ROW_FORMAT", "varchar", "varchar(22)", 7, 22, true),
    SystemColumn::unsigned_int("PAGE_SIZE", false),
    SystemColumn::unsigned_int("ZIP_PAGE_SIZE", false),
    string_column!("SPACE_TYPE", "varchar", "varchar(10)", 3, 10, true),
    SystemColumn::unsigned_int("FS_BLOCK_SIZE", false),
    SystemColumn::unsigned_bigint("FILE_SIZE", false),
    SystemColumn::unsigned_bigint("ALLOCATED_SIZE", false),
    SystemColumn::unsigned_bigint("AUTOEXTEND_SIZE", false),
    string_column!("SERVER_VERSION", "varchar", "varchar(10)", 3, 10, true),
    SystemColumn::unsigned_int("SPACE_VERSION", false),
    string_column!("ENCRYPTION", "varchar", "varchar(1)", 0, 1, true),
    string_column!("STATE", "varchar", "varchar(10)", 3, 10, true),
];

const INNODB_TABLESPACES_BRIEF_COLUMNS: &[SystemColumn] = &[
    SystemColumn::new(
        "SPACE",
        ColumnType::VarString,
        "varbinary",
        "varbinary(256)",
        true,
    )
    .character_lengths(Some(256), Some(256)),
    varchar_column!("NAME", 268, 804, false),
    varchar_column!("PATH", 512, 1536, false),
    SystemColumn::new(
        "FLAG",
        ColumnType::VarString,
        "varbinary",
        "varbinary(256)",
        true,
    )
    .character_lengths(Some(256), Some(256)),
    varchar_column!("SPACE_TYPE", 7, 21, false),
];

const INNODB_BUFFER_POOL_STATS_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("POOL_ID", false),
    SystemColumn::unsigned_bigint("POOL_SIZE", false),
    SystemColumn::unsigned_bigint("FREE_BUFFERS", false),
    SystemColumn::unsigned_bigint("DATABASE_PAGES", false),
    SystemColumn::unsigned_bigint("OLD_DATABASE_PAGES", false),
    SystemColumn::unsigned_bigint("MODIFIED_DATABASE_PAGES", false),
    SystemColumn::unsigned_bigint("PENDING_DECOMPRESS", false),
    SystemColumn::unsigned_bigint("PENDING_READS", false),
    SystemColumn::unsigned_bigint("PENDING_FLUSH_LRU", false),
    SystemColumn::unsigned_bigint("PENDING_FLUSH_LIST", false),
    SystemColumn::unsigned_bigint("PAGES_MADE_YOUNG", false),
    SystemColumn::unsigned_bigint("PAGES_NOT_MADE_YOUNG", false),
    SystemColumn::new(
        "PAGES_MADE_YOUNG_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::new(
        "PAGES_MADE_NOT_YOUNG_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::unsigned_bigint("NUMBER_PAGES_READ", false),
    SystemColumn::unsigned_bigint("NUMBER_PAGES_CREATED", false),
    SystemColumn::unsigned_bigint("NUMBER_PAGES_WRITTEN", false),
    SystemColumn::new(
        "PAGES_READ_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::new(
        "PAGES_CREATE_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::new(
        "PAGES_WRITTEN_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::unsigned_bigint("NUMBER_PAGES_GET", false),
    SystemColumn::unsigned_bigint("HIT_RATE", false),
    SystemColumn::unsigned_bigint("YOUNG_MAKE_PER_THOUSAND_GETS", false),
    SystemColumn::unsigned_bigint("NOT_YOUNG_MAKE_PER_THOUSAND_GETS", false),
    SystemColumn::unsigned_bigint("NUMBER_PAGES_READ_AHEAD", false),
    SystemColumn::unsigned_bigint("NUMBER_READ_AHEAD_EVICTED", false),
    SystemColumn::new(
        "READ_AHEAD_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::new(
        "READ_AHEAD_EVICTED_RATE",
        ColumnType::Float,
        "float",
        "float(12,0)",
        false,
    ),
    SystemColumn::unsigned_bigint("LRU_IO_TOTAL", false),
    SystemColumn::unsigned_bigint("LRU_IO_CURRENT", false),
    SystemColumn::unsigned_bigint("UNCOMPRESS_TOTAL", false),
    SystemColumn::unsigned_bigint("UNCOMPRESS_CURRENT", false),
];

const INNODB_TABLESTATS_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    string_column!("NAME", "varchar", "varchar(193)", 64, 193, false),
    string_column!(
        "STATS_INITIALIZED",
        "varchar",
        "varchar(193)",
        64,
        193,
        false
    ),
    SystemColumn::unsigned_bigint("NUM_ROWS", false),
    SystemColumn::unsigned_bigint("CLUST_INDEX_SIZE", false),
    SystemColumn::unsigned_bigint("OTHER_INDEX_SIZE", false),
    SystemColumn::unsigned_bigint("MODIFIED_COUNTER", false),
    SystemColumn::unsigned_bigint("AUTOINC", false),
    SystemColumn::int("REF_COUNT", false),
];

const INNODB_TEMP_TABLE_INFO_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    string_column!("NAME", "varchar", "varchar(64)", 21, 64, true),
    SystemColumn::unsigned_int("N_COLS", false),
    SystemColumn::unsigned_int("SPACE", false),
];

const INNODB_VIRTUAL_COLUMNS: &[SystemColumn] = &[
    SystemColumn::unsigned_bigint("TABLE_ID", false),
    SystemColumn::unsigned_int("POS", false),
    SystemColumn::unsigned_int("BASE_POS", false),
];

const NDB_TRANSID_MYSQL_CONNECTION_MAP_COLUMNS: &[SystemColumn] = &[
    i64c!("mysql_connection_id"),
    i64c!("node_id"),
    i64c!("ndb_transid"),
];

const TP_THREAD_GROUP_STATE_COLUMNS: &[SystemColumn] = &[
    i64c!("TP_GROUP_ID"),
    i64c!("CONSUMER THREADS"),
    i64c!("RESERVE_THREADS"),
    i64c!("CONNECT_THREAD_COUNT"),
    i64c!("CONNECTION_COUNT"),
    i64c!("QUEUED_QUERIES"),
    i64c!("QUEUED_TRANSACTIONS"),
    i64c!("STALL_LIMIT"),
    i64c!("PRIO_KICKUP_TIMER"),
    i64c!("ALGORITHM"),
    i64c!("THREAD_COUNT"),
    i64c!("ACTIVE_THREAD_COUNT"),
    i64c!("STALLED_THREAD_COUNT"),
    i64c!("WAITING_THREAD_NUMBER"),
    i64c!("OLDEST_QUEUED"),
    i64c!("MAX_THREAD_IDS_IN_GROUP"),
];

const TP_THREAD_GROUP_STATS_COLUMNS: &[SystemColumn] = &[
    i64c!("TP_GROUP_ID"),
    i64c!("CONNECTIONS_STARTED"),
    i64c!("CONNECTIONS_CLOSED"),
    i64c!("QUERIES_EXECUTED"),
    i64c!("QUERIES_QUEUED"),
    i64c!("THREADS_STARTED"),
    i64c!("PRIO_KICKUPS"),
    i64c!("STALLED_QUERIES_EXECUTED"),
    i64c!("BECOME_CONSUMER_THREAD"),
    i64c!("BECOME_RESERVE_THREAD"),
    i64c!("BECOME_WAITING_THREAD"),
    i64c!("WAKE_THREAD_STALL_CHECKER"),
    i64c!("SLEEP_WAITS"),
    i64c!("DISK_IO_WAITS"),
    i64c!("ROW_LOCK_WAITS"),
    i64c!("GLOBAL_LOCK_WAITS"),
    i64c!("META_DATA_LOCK_WAITS"),
    i64c!("TABLE_LOCK_WAITS"),
    i64c!("USER_LOCK_WAITS"),
    i64c!("BINLOG_WAITS"),
    i64c!("GROUP_COMMIT_WAITS"),
    i64c!("FSYNC_WAITS"),
];

const TP_THREAD_STATE_COLUMNS: &[SystemColumn] = &[
    i64c!("TP_GROUP_ID"),
    i64c!("TP_THREAD_NUMBER"),
    i64c!("PROCESS_COUNT"),
    vn!("WAIT_TYPE"),
    v!("TP_THREAD_TYPE"),
    i64c!("THREAD_ID"),
];

const CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_COLUMNS: &[SystemColumn] =
    &[v!("USERHOST"), i64c!("FAILED_ATTEMPTS")];

const MYSQL_FIREWALL_USERS_COLUMNS: &[SystemColumn] = &[v!("USERHOST"), v!("MODE")];

const MYSQL_FIREWALL_WHITELIST_COLUMNS: &[SystemColumn] = &[v!("USERHOST"), text!("RULE")];

/// `VARIABLE_NAME` / `VARIABLE_VALUE` — shared by the `information_schema`
/// global/session variable views (performance_schema keeps its own copy).
const VARIABLES_COLUMNS: &[SystemColumn] = &[v!("VARIABLE_NAME"), v!("VARIABLE_VALUE")];

const SEQUENCES_COLUMNS: &[SystemColumn] = &[
    SystemColumn::text("SEQUENCE_SCHEMA", false),
    SystemColumn::text("SEQUENCE_NAME", false),
    SystemColumn::text("DATA_TYPE", false),
    SystemColumn::bigint("INCREMENT", false),
    SystemColumn::bigint("MIN_VALUE", false),
    SystemColumn::bigint("MAX_VALUE", false),
    SystemColumn::bigint("CACHE_SIZE", false),
    SystemColumn::bigint("CYCLE", false),
    SystemColumn::unsigned_bigint("COUNTER_ID", false),
    SystemColumn::bigint("ALLOCATED_VALUE", true),
    SystemColumn::bigint("RESUME_VALUE", false),
    SystemColumn::bigint("IS_CALLED", false),
    SystemColumn::unsigned_bigint("STATE_LSN", false),
    SystemColumn::unsigned_bigint("CHECKPOINTED_STATE_LSN", false),
    SystemColumn::unsigned_bigint("LOCAL_DURABLE_STATE_LSN", false),
    SystemColumn::text("SYNC_POLICY", false),
    SystemColumn::bigint("PENDING", false),
    SystemColumn::unsigned_bigint("SYNC_CONFIG_EPOCH", false),
];

const MYDB_DDL_JOBS_COLUMNS: &[SystemColumn] = &[
    i64c!("JOB_ID"),
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    v!("OBJECT_KIND"),
    v!("OBJECT_NAME"),
    v!("STATE"),
    i64c!("PROGRESS_ROWS"),
    i64c!("TOTAL_ROWS"),
    i64c!("REPLAY_WATERMARK"),
    text!("ERROR_MESSAGE"),
    i64c!("CANCEL_ALLOWED"),
];

const MYDB_XA_RECOVER_COLUMNS: &[SystemColumn] = &[
    i64c!("FORMAT_ID"),
    i64c!("GTRID_LENGTH"),
    i64c!("BQUAL_LENGTH"),
    v!("DATA"),
    i64c!("TXN_ID"),
    i64c!("PREPARE_SCN"),
    i64c!("STARTED_UNIX_MS"),
];

/// Snapshot of every live server-side cursor — the ones held open by
/// `COM_STMT_EXECUTE` with `CURSOR_TYPE_READ_ONLY` (JDBC's
/// `useCursorFetch=true` path) waiting on `COM_STMT_FETCH`. Useful for
/// spotting leaks (long-idle cursors pinning MVCC snapshots) and for
/// confirming a JDBC client really is consuming rows incrementally
/// instead of pre-buffering the result set on the server.
const MYDB_CURSORS_COLUMNS: &[SystemColumn] = &[
    i64c!("CONNECTION_ID"),
    i64c!("STATEMENT_ID"),
    v!("SQL"),
    i64c!("COLUMNS"),
    i64c!("ROWS_FETCHED"),
    i64c!("CREATED_UNIX_MS"),
    i64c!("LAST_ACTIVE_UNIX_MS"),
    i64c!("IDLE_MS"),
];

/// P2/13 — per-(txn_id, table_id) summary of the in-memory undo
/// mirror. Lets DBAs answer "which transaction is holding 90 % of
/// my undo" without dumping the whole BTreeMap. Sorted by RECORD_COUNT
/// descending so heavy holders surface at the top.
const MYDB_UNDO_HISTORY_COLUMNS: &[SystemColumn] = &[
    i64c!("TXN_ID"),
    i64c!("TABLE_ID"),
    i64c!("RECORD_COUNT"),
    i64c!("BYTES"),
    i64c!("CHAIN_DEPTH"),
    i64c!("OLDEST_CREATED_UNIX_MS"),
];

const MYDB_TEMPORAL_HISTORY_COLUMNS: &[SystemColumn] = &[
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    vn!("INDEX_NAME"),
    i64c!("SYSTEM_TIME_EPOCH_US"),
    i64c!("EARLIEST_AVAILABLE_UNIX_US"),
    i64c!("HISTORY_ENTRIES"),
    i64c!("HISTORY_BYTES"),
    i64c!("LAST_COMPACTION_UNIX_US"),
    i64c!("DEGRADED"),
];

const MYDB_CACHES_COLUMNS: &[SystemColumn] = &[
    v!("CACHE_NAME"),
    v!("CACHE_KIND"),
    v!("SCOPE"),
    i64c!("HITS"),
    i64c!("MISSES"),
    i64c!("HIT_PCT"),
    i64c!("ENTRIES"),
    i64c!("CAPACITY"),
    i64c!("EVICTIONS"),
    i64c!("INVALIDATIONS"),
    i64c!("APPROX_BYTES"),
];

/// One row per resident entry of the cross-session physical-plan cache.
/// Introspection companion to the aggregate `mydb_caches` row named
/// `physical_plan`: it breaks that single line down into the individual
/// cached statements so an operator can see what is hot, what is bloating
/// the cache, and which entries carry the most table dependencies.
const MYDB_PLAN_CACHE_COLUMNS: &[SystemColumn] = &[
    v!("CACHED_SQL"),
    v!("SCHEMA_NAME"),
    i64c!("SQL_MODE_FINGERPRINT"),
    i64c!("HITS"),
    i64c!("TABLE_DEPS"),
    i64c!("APPROX_BYTES"),
    i64c!("CACHE_EPOCH"),
];

const MYDB_MEMORY_ADVISOR_COLUMNS: &[SystemColumn] =
    &[v!("METRIC"), i64c!("VALUE"), v!("UNIT"), v!("DESCRIPTION")];

/// One row per HNSW vector index in the catalog. Static columns (metric, M,
/// ef_construction, declared dim) come from the catalog and are always present;
/// runtime columns (node counts, stale ratio, generation, persisted LSN) are
/// populated only when the index is *warm* (a graph has been built/loaded in
/// `Database::vector_indexes`) — `LOADED = 0` rows report the catalog metadata
/// with `NULL` runtime stats. Powers `SELECT * FROM
/// information_schema.mydb_vector_indexes` and the SHOW ENGINE HNSW section.
const MYDB_VECTOR_INDEXES_COLUMNS: &[SystemColumn] = &[
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    v!("INDEX_NAME"),
    v!("COLUMN_NAME"),
    vn!("BACKEND"),
    v!("METRIC"),
    i64c!("DECLARED_DIM"),
    i64c!("RUNTIME_DIM"),
    i64c!("M"),
    i64c!("EF_CONSTRUCTION"),
    i64c!("DEFAULT_EF_SEARCH"),
    i32c!("LOADED"),
    i64c!("PARTITIONS_LOADED"),
    i64c!("TOTAL_NODES"),
    i64c!("LIVE_NODES"),
    i64c!("STALE_NODES"),
    i64c!("STALE_RATIO_PCT"),
    i64c!("DIRTY_NODES"),
    i64c!("GENERATION"),
    i64c!("LAST_PERSISTED_LSN"),
    // Algorithm discriminator + IVF-specific runtime stats. `ALGO` is
    // always present; `NLIST` / `INSERTS_SINCE_TRAIN` are NULL for HNSW and for
    // cold (never-queried) IVF indexes.
    v!("ALGO"),
    i64c!("REQUESTED_NLIST"),
    i64c!("NLIST"),
    i64c!("LIST_AVG_ENTRIES"),
    i64c!("LIST_MAX_ENTRIES"),
    i64c!("LIST_P95_ENTRIES"),
    vn!("BUILD_STATE"),
    i64c!("INSERTS_SINCE_TRAIN"),
    i64c!("PAGED_FILE_SIZE_BYTES"),
    i64c!("NAV_PAGES"),
    i64c!("SQ8_EXACT_FALLBACK_NODES"),
    i64c!("TOMBSTONES_ON_DISK"),
    i64c!("SQ8_COVERED_NODES"),
    i64c!("DEFAULT_NPROBE"),
    i64c!("KMEANS_ITERS"),
    i64c!("TRAIN_SAMPLE_MULT"),
    v!("QUANTIZATION"),
    vn!("QUANTIZATION_STATE"),
    i64c!("QUANTIZER_BYTES"),
    i64c!("COMPRESSED_BYTES"),
    i64c!("QUANTIZATION_EXCEPTIONS"),
    i64c!("RERANK_MULT"),
    i64c!("CANDIDATES_RETURNED"),
    i64c!("HEAP_RERANKS"),
    i64c!("EXACT_FALLBACKS"),
];

/// Per-table statistics freshness. Lets a DBA see, at a glance, which
/// tables carry stale optimizer statistics (and would be re-sampled by
/// the next background Auto-ANALYZE pass) without dumping the whole
/// catalog. `MODIFIED_ROWS` folds the live sidecar delta into the
/// persisted `modified_rows_since_analyze`, so the figure tracks
/// in-flight DML the way the planner's staleness check does.
const MYDB_STATS_HEALTH_COLUMNS: &[SystemColumn] = &[
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    i64c!("TABLE_ROWS"),
    i64c!("MODIFIED_ROWS"),
    f64c!("STALE_RATIO"),
    i32c!("IS_STALE"),
    dt!("LAST_ANALYZED"),
    i32c!("HAS_HISTOGRAM"),
    i32c!("ANALYZED_COLUMNS"),
];

const ZONE_MAPS_COLUMNS: &[SystemColumn] = &[
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    vn!("PARTITION_NAME"),
    i64c!("STORAGE_ID"),
    i64c!("ZONE_ID"),
    i64c!("PAGE_START"),
    i64c!("PAGE_END"),
    v!("COLUMN_NAME"),
    vn!("MIN_VALUE"),
    vn!("MAX_VALUE"),
    i32c!("HAS_NULL"),
    i32c!("HAS_NON_NULL"),
    i32c!("TRUNCATED"),
    v!("STATE"),
    i64c!("GENERATION"),
    i64c!("REBUILD_SCN"),
    i64c!("STALE_MUTATIONS"),
    v!("MEMBERSHIP_KIND"),
    i64c!("MEMBERSHIP_BYTES"),
    i32c!("MEMBERSHIP_HASHES"),
    i32c!("MEMBERSHIP_HASH_VERSION"),
    i32c!("MEMBERSHIP_LAYOUT_VERSION"),
    i32c!("MEMBERSHIP_SATURATED"),
];

const ZONE_MAP_STORAGE_COLUMNS: &[SystemColumn] = &[
    v!("TABLE_SCHEMA"),
    v!("TABLE_NAME"),
    vn!("PARTITION_NAME"),
    i64c!("STORAGE_ID"),
    i32c!("FORMAT_VERSION"),
    i64c!("GENERATION"),
    i64c!("TRUSTED_LSN"),
    i64c!("FILE_BYTES"),
    i64c!("LIVE_BYTES"),
    i64c!("OBSOLETE_BYTES"),
    i64c!("FRAME_COUNT"),
    i64c!("CACHED_BYTES"),
    i64c!("DIRTY_OVERLAY_BYTES"),
    f64c!("COMPRESSION_RATIO"),
    v!("STATE"),
    vn!("LAST_ERROR"),
    v!("REBUILD_STATE"),
    i64c!("REBUILD_ZONES_TOTAL"),
    i64c!("REBUILD_ZONES_DONE"),
    i64c!("REBUILD_STARTED_MS"),
    i64c!("REBUILD_FINISHED_MS"),
    i64c!("GOVERNOR_CHARGED_BYTES"),
    i64c!("MEMORY_FALLBACKS_GLOBAL"),
];

const AI_MODELS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("MODEL_SCHEMA", 64, 256, false),
    varchar_column!("MODEL_NAME", 64, 256, false),
    SystemColumn::new(
        "MODEL_ID",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    varchar_column!("KIND", 32, 128, false),
    varchar_column!("REMOTE_MODEL", 256, 1024, false),
    varchar_column!("MODEL_REVISION", 128, 512, false),
    SystemColumn::new(
        "GENERATION",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    SystemColumn::new("ENABLED", ColumnType::LongLong, "bigint", "bigint", false),
    SystemColumn::new(
        "DIMENSIONS",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        true,
    ),
];
const AI_CONNECTIONS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("CONNECTION_NAME", 64, 256, false),
    varchar_column!("PROTOCOL", 32, 128, false),
    varchar_column!("BASE_URL", 2048, 8192, false),
    SystemColumn::new(
        "GENERATION",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    SystemColumn::new("ENABLED", ColumnType::LongLong, "bigint", "bigint", false),
    SystemColumn::new(
        "CREDENTIAL_AVAILABLE",
        ColumnType::LongLong,
        "bigint",
        "bigint",
        false,
    ),
];
const AI_SECRETS_COLUMNS: &[SystemColumn] = &[
    varchar_column!("SECRET_NAME", 64, 256, false),
    SystemColumn::new(
        "SECRET_ID",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    SystemColumn::new(
        "GENERATION",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    SystemColumn::new(
        "CRYPTO_GENERATION",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
    varchar_column!("KEY_ID", 64, 256, false),
    SystemColumn::new("AVAILABLE", ColumnType::LongLong, "bigint", "bigint", false),
];
const AI_RUNTIME_COLUMNS: &[SystemColumn] = &[
    varchar_column!("METRIC", 64, 256, false),
    SystemColumn::new(
        "VALUE",
        ColumnType::LongLong,
        "bigint",
        "bigint unsigned",
        false,
    ),
];

const INFORMATION_SCHEMA_REGISTRY: &[InformationSchemaRegistration] = &[
    def("SCHEMATA", SCHEMATA_COLUMNS),
    def("TABLES", TABLES_COLUMNS),
    def("COLUMNS", COLUMNS_COLUMNS),
    def("STATISTICS", STATISTICS_COLUMNS),
    def("TABLE_CONSTRAINTS", TABLE_CONSTRAINTS_COLUMNS),
    def("KEY_COLUMN_USAGE", KEY_COLUMN_USAGE_COLUMNS),
    def("VIEWS", VIEWS_COLUMNS),
    def("VIEW_TABLE_USAGE", VIEW_TABLE_USAGE_COLUMNS),
    def("VIEW_ROUTINE_USAGE", VIEW_ROUTINE_USAGE_COLUMNS),
    def("CHARACTER_SETS", CHARACTER_SETS_COLUMNS),
    def("COLLATIONS", COLLATIONS_COLUMNS),
    def(
        "COLLATION_CHARACTER_SET_APPLICABILITY",
        COLLATION_APPLICABILITY_COLUMNS,
    ),
    def("ENGINES", ENGINES_COLUMNS),
    def("PROCESSLIST", PROCESSLIST_COLUMNS),
    def("ROUTINES", ROUTINES_COLUMNS),
    def("TRIGGERS", TRIGGERS_COLUMNS),
    def("PARTITIONS", PARTITIONS_COLUMNS),
    def("REFERENTIAL_CONSTRAINTS", REFERENTIAL_CONSTRAINTS_COLUMNS),
    def("PARAMETERS", PARAMETERS_COLUMNS),
    def("EVENTS", EVENTS_COLUMNS),
    def("USER_PRIVILEGES", PRIVILEGE_COLUMNS),
    def("SCHEMA_PRIVILEGES", SCHEMA_PRIVILEGES_COLUMNS),
    def("TABLE_PRIVILEGES", TABLE_PRIVILEGES_COLUMNS),
    def("COLUMN_PRIVILEGES", COLUMN_PRIVILEGES_COLUMNS),
    def("APPLICABLE_ROLES", APPLICABLE_ROLES_COLUMNS),
    def(
        "ADMINISTRABLE_ROLE_AUTHORIZATIONS",
        APPLICABLE_ROLES_COLUMNS,
    ),
    def("ENABLED_ROLES", ENABLED_ROLES_COLUMNS),
    def("ROLE_TABLE_GRANTS", ROLE_GRANTS_COLUMNS),
    def("ROLE_COLUMN_GRANTS", ROLE_COLUMN_GRANTS_COLUMNS),
    def("ROLE_ROUTINE_GRANTS", ROLE_ROUTINE_GRANTS_COLUMNS),
    def("COLUMN_STATISTICS", COLUMN_STATISTICS_COLUMNS),
    def("KEYWORDS", KEYWORDS_COLUMNS),
    def("CHECK_CONSTRAINTS", CHECK_CONSTRAINTS_COLUMNS),
    def("PLUGINS", PLUGINS_COLUMNS),
    def("OPTIMIZER_TRACE", OPTIMIZER_TRACE_COLUMNS),
    empty_def(
        "PROFILING",
        PROFILING_COLUMNS,
        "the server reports profiling support as disabled",
    ),
    def("FILES", FILES_COLUMNS),
    def(
        "ST_SPATIAL_REFERENCE_SYSTEMS",
        ST_SPATIAL_REFERENCE_SYSTEMS_COLUMNS,
    ),
    def("ST_GEOMETRY_COLUMNS", ST_GEOMETRY_COLUMNS_COLUMNS),
    def("ST_UNITS_OF_MEASURE", ST_UNITS_OF_MEASURE_COLUMNS),
    def("SCHEMATA_EXTENSIONS", SCHEMATA_EXTENSIONS_COLUMNS),
    def("TABLES_EXTENSIONS", EXTENSION_COLUMNS),
    def("COLUMNS_EXTENSIONS", COLUMNS_EXTENSIONS_COLUMNS),
    def(
        "TABLE_CONSTRAINTS_EXTENSIONS",
        TABLE_CONSTRAINTS_EXTENSIONS_COLUMNS,
    ),
    def("USER_ATTRIBUTES", USER_ATTRIBUTES_COLUMNS),
    legacy_def("global_variables", VARIABLES_COLUMNS),
    legacy_def("session_variables", VARIABLES_COLUMNS),
    mydb_def("ai_models", AI_MODELS_COLUMNS),
    mydb_def("ai_connections", AI_CONNECTIONS_COLUMNS),
    mydb_def("ai_secrets", AI_SECRETS_COLUMNS),
    mydb_def("ai_runtime_status", AI_RUNTIME_COLUMNS),
    mydb_def("mydb_sequences", SEQUENCES_COLUMNS),
    mydb_def("mydb_ddl_jobs", MYDB_DDL_JOBS_COLUMNS),
    mydb_def("mydb_xa_recover", MYDB_XA_RECOVER_COLUMNS),
    mydb_def("mydb_cursors", MYDB_CURSORS_COLUMNS),
    mydb_def("mydb_caches", MYDB_CACHES_COLUMNS),
    mydb_def("mydb_plan_cache", MYDB_PLAN_CACHE_COLUMNS),
    mydb_def("mydb_memory_advisor", MYDB_MEMORY_ADVISOR_COLUMNS),
    mydb_def("mydb_vector_indexes", MYDB_VECTOR_INDEXES_COLUMNS),
    mydb_def("mydb_undo_history", MYDB_UNDO_HISTORY_COLUMNS),
    mydb_def("mydb_temporal_history", MYDB_TEMPORAL_HISTORY_COLUMNS),
    mydb_def("mydb_stats_health", MYDB_STATS_HEALTH_COLUMNS),
    mydb_def("mydb_zone_maps", ZONE_MAPS_COLUMNS),
    mydb_def("mydb_zone_map_storage", ZONE_MAP_STORAGE_COLUMNS),
    plugin_empty_def(
        "ndb_transid_mysql_connection_map",
        NDB_TRANSID_MYSQL_CONNECTION_MAP_COLUMNS,
        "mydb does not implement the NDB engine",
    ),
    def("INNODB_BUFFER_PAGE", INNODB_BUFFER_PAGE_COLUMNS),
    def("INNODB_BUFFER_PAGE_LRU", INNODB_BUFFER_PAGE_LRU_COLUMNS),
    def("INNODB_BUFFER_POOL_STATS", INNODB_BUFFER_POOL_STATS_COLUMNS),
    def("INNODB_CACHED_INDEXES", INNODB_CACHED_INDEXES_COLUMNS),
    empty_def(
        "INNODB_CMP",
        INNODB_CMP_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    empty_def(
        "INNODB_CMP_RESET",
        INNODB_CMP_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    empty_def(
        "INNODB_CMPMEM",
        INNODB_CMPMEM_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    empty_def(
        "INNODB_CMPMEM_RESET",
        INNODB_CMPMEM_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    empty_def(
        "INNODB_CMP_PER_INDEX",
        INNODB_CMP_PER_INDEX_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    empty_def(
        "INNODB_CMP_PER_INDEX_RESET",
        INNODB_CMP_PER_INDEX_COLUMNS,
        "mydb does not implement InnoDB page compression",
    ),
    def("INNODB_COLUMNS", INNODB_COLUMNS_COLUMNS),
    def("INNODB_DATAFILES", INNODB_DATAFILES_COLUMNS),
    def("INNODB_FIELDS", INNODB_FIELDS_COLUMNS),
    def("INNODB_FOREIGN", INNODB_FOREIGN_COLUMNS),
    def("INNODB_FOREIGN_COLS", INNODB_FOREIGN_COLS_COLUMNS),
    empty_def(
        "INNODB_FT_BEING_DELETED",
        INNODB_FT_BEING_DELETED_COLUMNS,
        "full-text deletion is synchronous and has no pending queue",
    ),
    def("INNODB_FT_CONFIG", INNODB_FT_CONFIG_COLUMNS),
    def(
        "INNODB_FT_DEFAULT_STOPWORD",
        INNODB_FT_DEFAULT_STOPWORD_COLUMNS,
    ),
    empty_def(
        "INNODB_FT_DELETED",
        INNODB_FT_DELETED_COLUMNS,
        "full-text deletion is synchronous and has no deleted queue",
    ),
    empty_def(
        "INNODB_FT_INDEX_CACHE",
        INNODB_FT_INDEX_CACHE_COLUMNS,
        "mydb has no separate InnoDB full-text cache",
    ),
    def("INNODB_FT_INDEX_TABLE", INNODB_FT_INDEX_TABLE_COLUMNS),
    def("INNODB_INDEXES", INNODB_INDEXES_COLUMNS),
    def("INNODB_METRICS", INNODB_METRICS_COLUMNS),
    empty_def(
        "INNODB_SESSION_TEMP_TABLESPACES",
        INNODB_SESSION_TEMP_TABLESPACES_COLUMNS,
        "mydb has no physical per-session temporary tablespace",
    ),
    def("INNODB_TABLES", INNODB_TABLES_COLUMNS),
    def("INNODB_TABLESPACES", INNODB_TABLESPACES_COLUMNS),
    def("INNODB_TABLESPACES_BRIEF", INNODB_TABLESPACES_BRIEF_COLUMNS),
    def("INNODB_TABLESTATS", INNODB_TABLESTATS_COLUMNS),
    def("INNODB_TEMP_TABLE_INFO", INNODB_TEMP_TABLE_INFO_COLUMNS),
    def("INNODB_TRX", INNODB_TRX_COLUMNS),
    legacy_def("innodb_lock_waits", INNODB_LOCK_WAITS_COLUMNS),
    def("INNODB_VIRTUAL", INNODB_VIRTUAL_COLUMNS),
    plugin_empty_def(
        "tp_thread_group_state",
        TP_THREAD_GROUP_STATE_COLUMNS,
        "mydb does not implement the MySQL Enterprise Thread Pool plugin",
    ),
    plugin_empty_def(
        "tp_thread_group_stats",
        TP_THREAD_GROUP_STATS_COLUMNS,
        "mydb does not implement the MySQL Enterprise Thread Pool plugin",
    ),
    plugin_empty_def(
        "tp_thread_state",
        TP_THREAD_STATE_COLUMNS,
        "mydb does not implement the MySQL Enterprise Thread Pool plugin",
    ),
    plugin_empty_def(
        "connection_control_failed_login_attempts",
        CONNECTION_CONTROL_FAILED_LOGIN_ATTEMPTS_COLUMNS,
        "mydb does not implement the Connection Control plugin",
    ),
    plugin_empty_def(
        "mysql_firewall_users",
        MYSQL_FIREWALL_USERS_COLUMNS,
        "mydb does not implement the MySQL Enterprise Firewall plugin",
    ),
    plugin_empty_def(
        "mysql_firewall_whitelist",
        MYSQL_FIREWALL_WHITELIST_COLUMNS,
        "mydb does not implement the MySQL Enterprise Firewall plugin",
    ),
];

const fn registration(
    name: &'static str,
    columns: &'static [SystemColumn],
    compatibility: InformationSchemaCompatibility,
    producer: InformationSchemaProducer,
) -> InformationSchemaRegistration {
    InformationSchemaRegistration {
        table: SystemTableDef {
            schema: "information_schema",
            name,
            table_type: "SYSTEM VIEW",
            columns,
            engine: "",
            indexes: &[],
            create_sql: None,
            hidden: false,
        },
        compatibility,
        producer,
    }
}

const fn def(
    name: &'static str,
    columns: &'static [SystemColumn],
) -> InformationSchemaRegistration {
    registration(
        name,
        columns,
        InformationSchemaCompatibility::Mysql80Community,
        InformationSchemaProducer::Real,
    )
}

const fn empty_def(
    name: &'static str,
    columns: &'static [SystemColumn],
    reason: &'static str,
) -> InformationSchemaRegistration {
    registration(
        name,
        columns,
        InformationSchemaCompatibility::Mysql80Community,
        InformationSchemaProducer::HonestEmpty(reason),
    )
}

const fn legacy_def(
    name: &'static str,
    columns: &'static [SystemColumn],
) -> InformationSchemaRegistration {
    registration(
        name,
        columns,
        InformationSchemaCompatibility::MysqlLegacy,
        InformationSchemaProducer::Real,
    )
}

const fn plugin_empty_def(
    name: &'static str,
    columns: &'static [SystemColumn],
    reason: &'static str,
) -> InformationSchemaRegistration {
    registration(
        name,
        columns,
        InformationSchemaCompatibility::MysqlOptionalPlugin,
        InformationSchemaProducer::HonestEmpty(reason),
    )
}

const fn mydb_def(
    name: &'static str,
    columns: &'static [SystemColumn],
) -> InformationSchemaRegistration {
    registration(
        name,
        columns,
        InformationSchemaCompatibility::MydbNative,
        InformationSchemaProducer::Real,
    )
}

pub(crate) fn system_table_definitions() -> impl Iterator<Item = &'static SystemTableDef> {
    INFORMATION_SCHEMA_REGISTRY
        .iter()
        .map(|registration| &registration.table)
        .chain(crate::engine::perf_schema::definitions())
        .chain(crate::engine::mysql_schema::MYSQL_DEFS.iter())
        .chain(crate::engine::sys_schema::definitions().iter())
}

pub(crate) fn find_system_table(schema: &str, table: &str) -> Option<&'static SystemTableDef> {
    if !schema.eq_ignore_ascii_case("information_schema")
        && !schema.eq_ignore_ascii_case("performance_schema")
        && !schema.eq_ignore_ascii_case("mysql")
        && !schema.eq_ignore_ascii_case("sys")
    {
        return None;
    }
    system_table_definitions()
        .find(|def| def.schema.eq_ignore_ascii_case(schema) && def.name.eq_ignore_ascii_case(table))
}

fn system_column_octet_length(def: &SystemTableDef, column: &SystemColumn) -> Option<i64> {
    if def.schema == "mysql" && def.name == "servers" && column.name == "Host" {
        return column.char_len;
    }
    if def.schema == "mysql"
        && matches!(
            column.data_type,
            "char" | "varchar" | "enum" | "set" | "text" | "mediumtext"
        )
    {
        column.char_len.map(|n| n.saturating_mul(3))
    } else {
        column.octet_len
    }
}

/// Exact generated column metadata for the fixed MySQL compatibility
/// catalogs. The schema guard is essential: several system schemas reuse the
/// same table name, and the old table-name-only lookup could apply an
/// information_schema collation/default to a performance_schema column.
pub(crate) fn generated_system_column_metadata(
    def: &SystemTableDef,
    column: &str,
) -> Option<crate::engine::information_schema_mysql80_character_metadata::ColumnMetadata> {
    if def.schema.eq_ignore_ascii_case("performance_schema") {
        crate::engine::performance_schema_mysql80_generated::metadata(def.name, column)
    } else if def.schema.eq_ignore_ascii_case("information_schema") {
        crate::engine::information_schema_mysql80_character_metadata::metadata(def.name, column)
    } else if def.schema.eq_ignore_ascii_case("mysql") {
        let col = def
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))?;
        let character = matches!(
            col.data_type,
            "char" | "varchar" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        );
        let collation = if matches!(
            def.name,
            "general_log"
                | "slow_log"
                | "server_cost"
                | "engine_cost"
                | "innodb_table_stats"
                | "innodb_index_stats"
        ) {
            "utf8mb3_general_ci"
        } else {
            "utf8mb3_bin"
        };
        let ascii_host = def.name == "servers" && col.name == "Host";
        let timestamp_default = (def.name == "general_log" && col.name == "event_time")
            || (def.name == "slow_log" && col.name == "start_time")
            || (def.name == "password_history" && col.name == "Password_timestamp");
        Some(
            crate::engine::information_schema_mysql80_character_metadata::ColumnMetadata {
                default_value: timestamp_default.then_some("CURRENT_TIMESTAMP(6)"),
                describe_default: timestamp_default.then_some("CURRENT_TIMESTAMP(6)"),
                character_set: character.then_some(if ascii_host { "ascii" } else { "utf8mb3" }),
                collation: character.then_some(if ascii_host {
                    "ascii_general_ci"
                } else {
                    collation
                }),
                column_key: crate::engine::mysql_schema::column_key(def, col.name),
                extra: if def.name == "component" && col.name == "component_id" {
                    "auto_increment"
                } else if timestamp_default && def.name != "password_history" {
                    "on update CURRENT_TIMESTAMP(6)"
                } else {
                    ""
                },
                privileges: "select",
                comment: "",
                generation_expression: "",
                srs_id: None,
            },
        )
    } else {
        None
    }
}

fn find_information_schema_registration(
    table: &str,
) -> Option<&'static InformationSchemaRegistration> {
    INFORMATION_SCHEMA_REGISTRY
        .iter()
        .find(|entry| entry.table.name.eq_ignore_ascii_case(table))
}

pub(crate) fn find_system_table_name(table_name: &str) -> Option<&'static SystemTableDef> {
    let (schema, table) = split_table_name(DEFAULT_DATABASE, table_name);
    find_system_table(&schema, &table)
}

/// Explicit product exclusions retain a diagnostic entry point without
/// advertising a fake catalog through SHOW or information_schema metadata.
const UNSUPPORTED_INFORMATION_SCHEMA_TABLES: &[&str] =
    &["RESOURCE_GROUPS", "TABLESPACES", "TABLESPACES_EXTENSIONS"];

pub(crate) fn hidden_system_table_error(table_name: &str) -> Option<QueryError> {
    let (schema, table) = split_table_name(DEFAULT_DATABASE, table_name);
    if schema.eq_ignore_ascii_case("information_schema")
        && UNSUPPORTED_INFORMATION_SCHEMA_TABLES
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&table))
    {
        return Some(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("information_schema.{table} is not supported"),
        ));
    }
    if crate::engine::mysql_schema::is_hidden_dd_table(&schema, &table) {
        Some(crate::engine::mysql_schema::inaccessible_table_error(
            &table,
        ))
    } else {
        None
    }
}

pub(crate) fn columns_for_table_name(table_name: &str) -> Result<Vec<Column>, QueryError> {
    if let Some(err) = hidden_system_table_error(table_name) {
        return Err(err);
    }
    let def = find_system_table_name(table_name).ok_or_else(|| table_not_found(table_name))?;
    Ok(def
        .columns
        .iter()
        .map(|col| {
            use crate::mysql::protocol::{
                BINARY_FLAG, ENUM_FLAG, NOT_NULL_FLAG, SET_FLAG, UNSIGNED_FLAG,
            };
            let mut flags = 0;
            if !col.nullable {
                flags |= NOT_NULL_FLAG;
            }
            if col.column_type_sql.contains("unsigned") {
                flags |= UNSIGNED_FLAG;
            }
            if col.data_type.eq_ignore_ascii_case("enum") {
                flags |= ENUM_FLAG;
            }
            if col.data_type.eq_ignore_ascii_case("set") {
                flags |= SET_FLAG;
            }
            if matches!(col.data_type, "binary" | "varbinary" | "blob") {
                flags |= BINARY_FLAG;
            }
            let length = match col.result_type {
                ColumnType::VarString | ColumnType::Blob => system_column_octet_length(def, col)
                    .and_then(|length| u32::try_from(length).ok()),
                _ => None,
            };
            let decimals = col
                .datetime_precision
                .and_then(|precision| u8::try_from(precision).ok());
            let core_metadata = generated_system_column_metadata(def, col.name);
            let collation_id = core_metadata
                .and_then(|metadata| metadata.collation)
                .map(crate::charset::collation_id)
                .or_else(|| {
                    if matches!(col.data_type, "binary" | "varbinary" | "blob" | "json") {
                        Some(crate::charset::collation_id("binary"))
                    } else if find_information_schema_registration(def.name).is_some_and(|entry| {
                        entry.compatibility == InformationSchemaCompatibility::Mysql80Community
                    }) {
                        None
                    } else if matches!(col.result_type, ColumnType::VarString | ColumnType::Blob) {
                        Some(crate::charset::collation_id(
                            crate::charset::DEFAULT_COLLATION,
                        ))
                    } else {
                        None
                    }
                });
            Column {
                source: crate::types::ColumnSource {
                    vector_type: None,
                    collation_coercibility: Some(2),
                    dynamic_parameter: false,
                    wire_type: None,
                    schema: def.schema.to_string(),
                    table: def.name.to_string(),
                    org_table: def.name.to_string(),
                    org_name: col.name.to_string(),
                    flags,
                    length,
                    numeric_precision: col
                        .numeric_precision
                        .and_then(|precision| u8::try_from(precision).ok()),
                    decimals,
                    collation_id,
                },
                name: col.name.to_string(),
                column_type: col.result_type,
            }
        })
        .collect())
}

pub(crate) fn rows_for_table_name_with_request(
    db: &Database,
    table_name: &str,
    registry: &SessionRegistry,
    request: &SystemScanRequest,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let (schema, table) = split_table_name(DEFAULT_DATABASE, table_name);
    if crate::engine::mysql_schema::is_hidden_dd_table(&schema, &table) {
        return Err(crate::engine::mysql_schema::inaccessible_table_error(
            &table,
        ));
    }
    rows(db, &schema, &table, registry, request)
}

// FULL rows follow the mydb-enhanced result format defined by show::show_tables_columns;
// the extra metadata columns are an intentional product extension, not a MySQL compatibility gap. System objects without matching stats return NULL.
pub(crate) fn show_tables_rows(
    db: &Database,
    database: &str,
    full: bool,
) -> Option<Vec<Vec<Value>>> {
    let schema = database.to_ascii_lowercase();
    if !["information_schema", "mysql", "performance_schema", "sys"].contains(&schema.as_str()) {
        return None;
    }
    let mut rows = system_table_definitions()
        .filter(|def| def.schema == schema)
        .filter(|def| !def.hidden)
        .map(|def| {
            let mut row = vec![Value::String(def.name.to_string())];
            if full {
                row.push(Value::String(def.table_type.to_string()));
                row.extend([
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::String(
                        if def.table_type.to_ascii_uppercase().contains("VIEW") {
                            "VIEW"
                        } else {
                            ""
                        }
                        .to_string(),
                    ),
                ]);
            }
            row
        })
        .collect::<Vec<_>>();
    if schema == "mysql" {
        let catalog = db.catalog();
        for table in catalog.tables.values().filter(|table| {
            !table.system
                && table.database.eq_ignore_ascii_case("mysql")
                && find_system_table("mysql", &table.name).is_none()
                && !crate::engine::mysql_schema::is_hidden_dd_table("mysql", &table.name)
        }) {
            let mut row = vec![Value::String(table.name.clone())];
            if full {
                let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
                row.extend([
                    Value::String("BASE TABLE".to_string()),
                    Value::Int(i64::try_from(stats.rows).unwrap_or(i64::MAX)),
                    Value::Int(i64::try_from(stats.data_length).unwrap_or(i64::MAX)),
                    Value::Int(i64::try_from(stats.index_length).unwrap_or(i64::MAX)),
                    mysql_time_value(table.created_ms),
                    Value::String(table.comment.clone()),
                ]);
            }
            rows.push(row);
        }
        for view in catalog
            .views
            .values()
            .filter(|view| view.database.eq_ignore_ascii_case("mysql"))
        {
            let mut row = vec![Value::String(view.name.clone())];
            if full {
                row.extend([
                    Value::String("VIEW".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::String("VIEW".to_string()),
                ]);
            }
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| a[0].to_display_string().cmp(&b[0].to_display_string()));
    Some(rows)
}

fn rows(
    db: &Database,
    schema: &str,
    table: &str,
    registry: &SessionRegistry,
    request: &SystemScanRequest,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if schema == "performance_schema" {
        return crate::engine::perf_schema::rows(db, table, registry, request);
    }
    if schema == "sys" {
        return crate::engine::sys_schema::rows(db, table, registry);
    }
    if schema.eq_ignore_ascii_case("information_schema")
        && information_schema_requires_process(table)
        && !can_view_process_metadata(db)
    {
        return Err(error(
            crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation",
        ));
    }
    match (schema, table) {
        ("information_schema", "schemata") => Ok(schemata_rows(db)),
        ("information_schema", "tables") => Ok(tables_rows(db, request)),
        ("information_schema", "columns") => Ok(columns_rows(db, request)),
        ("information_schema", "statistics") => Ok(statistics_rows(db, request)),
        ("information_schema", "table_constraints") => Ok(table_constraints_rows(db)),
        ("information_schema", "key_column_usage") => Ok(key_column_usage_rows(db)),
        ("information_schema", "referential_constraints") => Ok(referential_constraints_rows(db)),
        ("information_schema", "check_constraints") => Ok(check_constraints_rows(db)),
        ("information_schema", "views") => Ok(views_rows(db)),
        ("information_schema", "view_table_usage") => Ok(view_table_usage_rows(db)),
        ("information_schema", "view_routine_usage") => Ok(view_routine_usage_rows(db)),
        ("information_schema", "routines") => Ok(routines_rows(db)),
        ("information_schema", "triggers") => Ok(triggers_rows(db)),
        ("information_schema", "partitions") => Ok(partitions_rows(db)),
        ("information_schema", "events") => Ok(events_rows(db)),
        ("information_schema", "parameters") => Ok(parameters_rows(db)),
        ("information_schema", "character_sets") => Ok(character_sets_rows()),
        ("information_schema", "collations") => Ok(collations_rows()),
        ("information_schema", "collation_character_set_applicability") => {
            Ok(collation_applicability_rows())
        }
        ("information_schema", "engines") => Ok(engines_rows()),
        ("information_schema", "processlist") => Ok(processlist_rows(db, registry)),
        ("information_schema", "global_variables") => Ok(system_variables_rows_scoped(
            db,
            crate::engine::sysvar::Scope::Global,
        )),
        ("information_schema", "session_variables") => Ok(system_variables_rows_scoped(
            db,
            crate::engine::sysvar::Scope::Session,
        )),
        ("information_schema", "user_privileges") => Ok(user_privileges_rows(db)),
        ("information_schema", "schema_privileges") => Ok(schema_privileges_rows(db)),
        ("information_schema", "table_privileges") => Ok(table_privileges_rows(db)),
        ("information_schema", "column_privileges") => Ok(column_privileges_rows(db)),
        ("information_schema", "applicable_roles") => Ok(applicable_roles_rows(db, false)),
        ("information_schema", "administrable_role_authorizations") => {
            Ok(applicable_roles_rows(db, true))
        }
        ("information_schema", "enabled_roles") => Ok(enabled_roles_rows(db)),
        ("information_schema", "role_table_grants") => Ok(role_table_grants_rows(db)),
        ("information_schema", "role_column_grants") => Ok(role_column_grants_rows(db)),
        ("information_schema", "role_routine_grants") => Ok(role_routine_grants_rows(db)),
        ("information_schema", "column_statistics") => column_statistics_rows(db),
        ("information_schema", "keywords") => Ok(keywords_rows()),
        ("information_schema", "plugins") => {
            Ok(crate::engine::show::information_schema_plugin_rows())
        }
        ("information_schema", "resource_groups") => Err(crate::storage::error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET, "RESOURCE GROUP is not supported")),
        ("information_schema", "schemata_extensions") => Ok(schemata_extensions_rows(db)),
        ("information_schema", "tables_extensions") => Ok(tables_extensions_rows(db)),
        ("information_schema", "columns_extensions") => Ok(columns_extensions_rows(db)),
        ("information_schema", "table_constraints_extensions") => {
            Ok(table_constraints_extensions_rows(db))
        }
        ("information_schema", "user_attributes") => Ok(user_attributes_rows(db)),
        ("information_schema", "tablespaces" | "tablespaces_extensions") => Err(crate::storage::error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET, "TABLESPACE is not supported")),
        ("information_schema", "optimizer_trace") => Ok(crate::engine::cbo::trace::rows()),
        ("information_schema", "st_spatial_reference_systems") => {
            st_spatial_reference_systems_rows()
        }
        ("information_schema", "st_geometry_columns") => st_geometry_columns_rows(db),
        ("information_schema", "st_units_of_measure") => Ok(st_units_of_measure_rows()),
        ("information_schema", "innodb_trx") => Ok(innodb_trx_rows(db, registry)),
        ("information_schema", "innodb_lock_waits") => Ok(innodb_lock_waits_rows(db)),
        ("information_schema", "innodb_tables") => Ok(innodb_tables_rows(db)),
        ("information_schema", "innodb_columns") => Ok(innodb_columns_rows(db)),
        ("information_schema", "innodb_virtual") => Ok(innodb_virtual_rows(db)),
        ("information_schema", "innodb_indexes") => Ok(innodb_indexes_rows(db)),
        ("information_schema", "innodb_fields") => Ok(innodb_fields_rows(db)),
        ("information_schema", "innodb_foreign") => Ok(innodb_foreign_rows(db)),
        ("information_schema", "innodb_foreign_cols") => Ok(innodb_foreign_cols_rows(db)),
        ("information_schema", "innodb_tablespaces") => Ok(innodb_tablespaces_rows(db)),
        ("information_schema", "innodb_tablespaces_brief") => Ok(innodb_tablespaces_brief_rows(db)),
        ("information_schema", "innodb_datafiles") => Ok(innodb_datafiles_rows(db)),
        ("information_schema", "innodb_tablestats") => Ok(innodb_tablestats_rows(db)),
        ("information_schema", "files") => Ok(files_rows(db)),
        ("information_schema", "innodb_buffer_pool_stats") => Ok(innodb_buffer_pool_stats_rows(db)),
        ("information_schema", "innodb_buffer_page") => Ok(innodb_buffer_page_rows(db, false)),
        ("information_schema", "innodb_buffer_page_lru") => {
            Ok(innodb_buffer_page_rows(db, true))
        }
        ("information_schema", "innodb_cached_indexes") => Ok(innodb_cached_indexes_rows(db)),
        ("information_schema", "innodb_metrics") => Ok(innodb_metrics_rows(db)),
        ("information_schema", "innodb_ft_default_stopword") => {
            Ok(innodb_ft_default_stopword_rows())
        }
        ("information_schema", "innodb_ft_config") => innodb_ft_config_rows(db),
        ("information_schema", "innodb_ft_index_table") => innodb_ft_index_table_rows(db),
        ("information_schema", "innodb_temp_table_info") => {
            Ok(innodb_temp_table_info_rows(db))
        }
        ("information_schema", "ai_models" | "ai_connections" | "ai_secrets" | "ai_runtime_status") => crate::engine::ai::metadata::rows(db, table),
        ("information_schema", "mydb_sequences") => sequence_rows(db),
        ("information_schema", "mydb_ddl_jobs") => Ok(mydb_ddl_jobs_rows(db)),
        ("information_schema", "mydb_xa_recover") => Ok(mydb_xa_recover_rows(db)),
        ("information_schema", "mydb_cursors") => Ok(mydb_cursors_rows(db)),
        ("information_schema", "mydb_caches") => Ok(mydb_caches_rows(db)),
        ("information_schema", "mydb_plan_cache") => Ok(mydb_plan_cache_rows(db)),
        ("information_schema", "mydb_memory_advisor") => Ok(mydb_memory_advisor_rows(db)),
        ("information_schema", "mydb_vector_indexes") => Ok(mydb_vector_indexes_rows(db, request)),
        ("information_schema", "mydb_undo_history") => Ok(mydb_undo_history_rows(db)),
        ("information_schema", "mydb_temporal_history") => {
            Ok(mydb_temporal_history_rows(db))
        }
        ("information_schema", "mydb_stats_health") => Ok(mydb_stats_health_rows(db)),
        ("information_schema", "mydb_zone_maps") => Ok(filter_visible_object_rows(
            db,
            db.zone_map_diagnostic_rows_filtered(
                request.table_schema.as_deref(),
                request.table_name.as_deref(),
                request.storage_id,
                request.column_name.as_deref(),
            ),
            0,
            1,
        )),
        ("information_schema", "mydb_zone_map_storage") => Ok(filter_visible_object_rows(
            db,
            db.zone_map_storage_rows(),
            0,
            1,
        )),
        ("mysql", table) if find_system_table("mysql", table).is_some() => {
            crate::engine::mysql_schema::rows(db, table)
        }
        _ if schema.eq_ignore_ascii_case("information_schema") => {
            match find_information_schema_registration(table) {
                Some(InformationSchemaRegistration {
                    producer: InformationSchemaProducer::HonestEmpty(_),
                    ..
                }) => Ok(Vec::new()),
                Some(registration) => Err(error(
                    crate::error::catalog::ER_INTERNAL_ERROR,
                    format!(
                        "information_schema.{} ({:?}) is registered as a real producer but has no dispatch implementation",
                        registration.table.name, registration.compatibility
                    ),
                )),
                None => Err(table_not_found(&format!("{schema}.{table}"))),
            }
        }
        _ => find_system_table(schema, table)
            .map(|_| Vec::new())
            .ok_or_else(|| table_not_found(&format!("{schema}.{table}"))),
    }
}

fn information_schema_requires_process(table: &str) -> bool {
    let table = table.to_ascii_lowercase();
    (table.starts_with("innodb_") && table != "innodb_ft_default_stopword")
        || matches!(
            table.as_str(),
            "files" | "tablespaces" | "tablespaces_extensions"
        )
}

fn table_not_found(table_name: &str) -> QueryError {
    error(
        crate::error::catalog::ER_NO_SUCH_TABLE,
        format!("Table '{table_name}' doesn't exist"),
    )
}

/// `information_schema` object-metadata filtering is unconditional in
/// production.  Tests may disable it for differential-oracle setup, but a
/// process environment variable can never weaken a release server.
#[cfg(not(test))]
#[inline]
fn is_privilege_filter_enabled() -> bool {
    true
}

#[cfg(test)]
fn is_privilege_filter_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MYDB_IS_PRIVILEGE_FILTER")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("off") && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true)
    })
}

/// MySQL 8.0 exposes these session/status tables without an explicit grant;
/// their producers still apply row visibility and sensitive-variable rules.
pub(crate) fn implicitly_readable_metadata_table(schema: &str, table: &str) -> bool {
    schema.eq_ignore_ascii_case("information_schema")
        || (schema.eq_ignore_ascii_case("performance_schema")
            && matches!(
                table.to_ascii_lowercase().as_str(),
                "global_status"
                    | "global_variables"
                    | "persisted_variables"
                    | "processlist"
                    | "session_account_connect_attrs"
                    | "session_status"
                    | "session_variables"
                    | "variables_info"
            ))
}

fn always_visible_schema(schema: &str, table: Option<&str>) -> bool {
    match table {
        Some(table) => implicitly_readable_metadata_table(schema, table),
        None => {
            schema.eq_ignore_ascii_case("information_schema")
                || schema.eq_ignore_ascii_case("performance_schema")
        }
    }
}

/// Explicit-session variant of the `information_schema` row-visibility rule,
/// used by the `SHOW` surface (`SHOW DATABASES` / `SHOW TABLES` /
/// `SHOW TABLE STATUS`), which holds a `&SessionContext` directly instead of
/// relying on the expression engine's thread-local. Returns `true` when the
/// account may see metadata for `schema` (and, when given, `table`): filter
/// disabled, a system/internal session, an implicitly readable
/// metadata object, or the account holds some privilege covering the object.
/// Mirrors MySQL, where `SHOW DATABASES` / `SHOW TABLES` list only objects the
/// caller has a privilege on.
pub(crate) fn object_visible_to_session(
    db: &Database,
    session: &crate::engine::api::SessionContext,
    schema: &str,
    table: Option<&str>,
) -> bool {
    if !is_privilege_filter_enabled() || session.internal_system_session {
        return true;
    }
    if always_visible_schema(schema, table) {
        return true;
    }
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    auth.account_can_access_schema(&ctx, schema, table)
}

pub(crate) fn routine_visible_to_session(
    db: &Database,
    session: &crate::engine::api::SessionContext,
    routine: &RoutineSchema,
) -> bool {
    if !is_privilege_filter_enabled() || session.internal_system_session {
        return true;
    }
    routine_metadata_visible(
        &db.auth_catalog(),
        &session.authorization_context(),
        routine,
    )
}

pub(crate) fn routine_definition_visible_to_session(
    db: &Database,
    session: &crate::engine::api::SessionContext,
    routine: &RoutineSchema,
) -> bool {
    if !is_privilege_filter_enabled() || session.internal_system_session {
        return true;
    }
    routine_definition_visible(
        &db.auth_catalog(),
        &session.authorization_context(),
        routine,
    )
}

fn routine_metadata_visible(
    auth: &AuthCatalog,
    ctx: &AuthorizationContext,
    routine: &RoutineSchema,
) -> bool {
    if (ctx.authenticated_as.user == routine.definer_user)
        && ctx
            .authenticated_as
            .host
            .eq_ignore_ascii_case(&routine.definer_host)
    {
        return true;
    }
    let scope = PrivilegeScope::Routine {
        database: routine.database.clone(),
        routine: routine.name.clone(),
        kind: routine.kind.lower().to_string(),
    };
    [
        RequiredPrivilege {
            privilege: Privilege::Execute,
            scope: scope.clone(),
        },
        RequiredPrivilege {
            privilege: Privilege::AlterRoutine,
            scope,
        },
        RequiredPrivilege {
            privilege: Privilege::CreateRoutine,
            scope: PrivilegeScope::Database {
                database: routine.database.clone(),
            },
        },
        RequiredPrivilege {
            privilege: Privilege::Dynamic("SHOW_ROUTINE".to_string()),
            scope: PrivilegeScope::Global,
        },
        RequiredPrivilege {
            privilege: Privilege::Select,
            scope: PrivilegeScope::Global,
        },
    ]
    .iter()
    .any(|required| auth.account_has_privilege(ctx, required, false))
}

fn routine_definition_visible(
    auth: &AuthCatalog,
    ctx: &AuthorizationContext,
    routine: &RoutineSchema,
) -> bool {
    ((ctx.authenticated_as.user == routine.definer_user)
        && ctx
            .authenticated_as
            .host
            .eq_ignore_ascii_case(&routine.definer_host))
        || [
            Privilege::Dynamic("SHOW_ROUTINE".to_string()),
            Privilege::Select,
        ]
        .into_iter()
        .any(|privilege| {
            auth.account_has_privilege(
                ctx,
                &RequiredPrivilege {
                    privilege,
                    scope: PrivilegeScope::Global,
                },
                false,
            )
        })
}

/// Visibility decision for one `information_schema` producer.  A missing
/// borrowed session is deliberately fail-closed for user objects; internal
/// metadata schemas remain visible.  This prevents a future parallel/system
/// scan path from turning loss of thread-local context into metadata leakage.
struct Visibility {
    guard: Option<(
        std::sync::Arc<crate::engine::auth::AuthCatalog>,
        crate::engine::auth::AuthorizationContext,
    )>,
    unrestricted: bool,
}

/// One table's owned authorization scope, reused as the producer visits columns.
/// Every column still receives all four independent role/partial-revoke checks.
struct ColumnPrivilegeChecker<'a> {
    visibility: &'a Visibility,
    schema: &'a str,
    table: &'a str,
    required: Option<RequiredPrivilege>,
}

impl ColumnPrivilegeChecker<'_> {
    fn for_column(&mut self, column: &str) -> String {
        if self.visibility.unrestricted {
            return "select,insert,update,references".to_string();
        }
        let Some((auth, ctx)) = &self.visibility.guard else {
            return String::new();
        };
        let required = self
            .required
            .as_mut()
            .expect("guarded column privilege scope");
        let PrivilegeScope::Column { column: name, .. } = &mut required.scope else {
            unreachable!("column privilege checker owns a column scope")
        };
        name.clear();
        name.push_str(column);
        let mut labels = smallvec::SmallVec::<[&str; 4]>::new();
        for (privilege, label) in [
            (Privilege::Select, "select"),
            (Privilege::Insert, "insert"),
            (Privilege::Update, "update"),
            (Privilege::References, "references"),
        ] {
            required.privilege = privilege;
            if auth.account_has_privilege(ctx, required, false) {
                labels.push(label);
            }
        }
        let privileges = labels.join(",");
        if privileges.is_empty() && implicitly_readable_metadata_table(self.schema, self.table) {
            "select".into()
        } else {
            privileges
        }
    }
}

impl Visibility {
    fn compute(db: &Database) -> Self {
        if !is_privilege_filter_enabled() {
            return Self {
                guard: None,
                unrestricted: true,
            };
        }
        match crate::engine::expr::current_authorization_context() {
            Some((_, true)) => Self {
                guard: None,
                unrestricted: true,
            },
            Some((ctx, false)) => Self {
                guard: Some((db.auth_catalog(), ctx)),
                unrestricted: false,
            },
            None => Self {
                guard: None,
                unrestricted: false,
            },
        }
    }

    fn schema_visible(&self, schema: &str) -> bool {
        self.allow(schema, None)
    }

    fn table_visible(&self, schema: &str, table: &str) -> bool {
        self.allow(schema, Some(table))
    }

    fn routine_visible(&self, routine: &RoutineSchema) -> bool {
        if self.unrestricted {
            return true;
        }
        let Some((auth, ctx)) = &self.guard else {
            return false;
        };
        routine_metadata_visible(auth, ctx, routine)
    }

    fn routine_definition_visible(&self, routine: &RoutineSchema) -> bool {
        if self.unrestricted {
            return true;
        }
        let Some((auth, ctx)) = &self.guard else {
            return false;
        };
        routine_definition_visible(auth, ctx, routine)
    }

    fn allow(&self, schema: &str, table: Option<&str>) -> bool {
        if self.unrestricted {
            return true;
        }
        if always_visible_schema(schema, table) {
            return true;
        }
        let Some((auth, ctx)) = &self.guard else {
            return false;
        };
        auth.account_can_access_schema(ctx, schema, table)
    }

    fn column_privileges(&self, schema: &str, table: &str, column: &str) -> String {
        self.column_privilege_checker(schema, table)
            .for_column(column)
    }

    fn column_privilege_checker<'a>(
        &'a self,
        schema: &'a str,
        table: &'a str,
    ) -> ColumnPrivilegeChecker<'a> {
        ColumnPrivilegeChecker {
            visibility: self,
            schema,
            table,
            required: (!self.unrestricted && self.guard.is_some()).then(|| RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Column {
                    database: schema.to_string(),
                    table: table.to_string(),
                    column: String::new(),
                },
            }),
        }
    }

    fn has_privilege(&self, privilege: Privilege, scope: PrivilegeScope) -> bool {
        if self.unrestricted {
            return true;
        }
        let Some((auth, ctx)) = &self.guard else {
            return false;
        };
        auth.account_has_privilege(ctx, &RequiredPrivilege { privilege, scope }, false)
    }

    fn has_table_privilege(&self, privilege: Privilege, schema: &str, table: &str) -> bool {
        self.has_privilege(
            privilege,
            PrivilegeScope::Table {
                database: schema.to_string(),
                table: table.to_string(),
            },
        )
    }

    fn has_database_privilege(&self, privilege: Privilege, schema: &str) -> bool {
        self.has_privilege(
            privilege,
            PrivilegeScope::Database {
                database: schema.to_string(),
            },
        )
    }
}

fn schemata_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    db.catalog()
        .databases
        .values()
        .filter(|schema| vis.schema_visible(&schema.name))
        .map(|schema| {
            vec![
                s("def"),
                s(&schema.name),
                s(&schema.charset),
                s(&schema.collation),
                Value::Null,
                s("NO"),
            ]
        })
        .collect()
}

fn scan_request_matches_table(request: &SystemScanRequest, schema: &str, table: &str) -> bool {
    request
        .table_schema
        .as_deref()
        .is_none_or(|wanted| wanted.eq_ignore_ascii_case(schema))
        && request
            .table_name
            .as_deref()
            .is_none_or(|wanted| wanted.eq_ignore_ascii_case(table))
}

fn exact_requested_relation(request: &SystemScanRequest) -> Option<(&str, &str)> {
    request
        .table_schema
        .as_deref()
        .zip(request.table_name.as_deref())
}

fn visit_requested_tables(
    catalog: &Catalog,
    request: &SystemScanRequest,
    mut visitor: impl FnMut(&TableSchema),
) {
    if let (Some(schema), Some(table)) = (&request.table_schema, &request.table_name) {
        if let Some(table) = catalog
            .tables
            .get(&crate::storage::table_key(schema, table))
        {
            visitor(table);
        }
        return;
    }
    for table in catalog.tables.values() {
        if scan_request_matches_table(request, &table.database, &table.name) {
            visitor(table);
        }
    }
}

fn visit_requested_table_descriptors(
    db: &Database,
    catalog: &Catalog,
    request: &SystemScanRequest,
    mut visitor: impl FnMut(&TableSchema),
) {
    if let (Some(schema), Some(table)) = (&request.table_schema, &request.table_name) {
        if let Ok(table) = db.table(&format!("{schema}.{table}")) {
            visitor(&table);
        }
        return;
    }
    for header in catalog.tables.values() {
        if !scan_request_matches_table(request, &header.database, &header.name) {
            continue;
        }
        if !header.columns.is_empty() {
            visitor(header);
        } else if let Ok(table) = db.table(&header.qualified_name()) {
            visitor(&table);
        }
    }
}

fn visit_requested_views(
    catalog: &Catalog,
    request: &SystemScanRequest,
    mut visitor: impl FnMut(&crate::storage::ViewSchema),
) {
    if let (Some(schema), Some(view)) = (&request.table_schema, &request.table_name) {
        if let Some(view) = catalog.views.get(&crate::storage::table_key(schema, view)) {
            visitor(view);
        }
        return;
    }
    for view in catalog.views.values() {
        if scan_request_matches_table(request, &view.database, &view.name) {
            visitor(view);
        }
    }
}

fn tables_rows(db: &Database, request: &SystemScanRequest) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    if let Some((schema, table_name)) = exact_requested_relation(request) {
        let qualified = format!("{schema}.{table_name}");
        if let Ok(table) = db.table(&qualified) {
            if vis.table_visible(&table.database, &table.name) {
                rows.push(base_table_row(db, &table));
            }
        } else if let Some(view) = db.view_definition(schema, table_name) {
            if vis.table_visible(&view.database, &view.name) {
                rows.push(view_table_row(&view.database, &view.name));
            }
        }
        for def in system_table_definitions()
            .filter(|def| !def.hidden)
            .filter(|def| scan_request_matches_table(request, def.schema, def.name))
            .filter(|def| vis.table_visible(def.schema, def.name))
        {
            rows.push(system_table_row(def));
        }
        return rows;
    }
    let catalog = db.catalog();
    visit_requested_tables(&catalog, request, |table| {
        if vis.table_visible(&table.database, &table.name) {
            rows.push(base_table_row(db, table));
        }
    });
    visit_requested_views(&catalog, request, |view| {
        if vis.table_visible(&view.database, &view.name) {
            rows.push(view_table_row(&view.database, &view.name));
        }
    });
    for def in system_table_definitions()
        .filter(|def| !def.hidden)
        .filter(|def| scan_request_matches_table(request, def.schema, def.name))
        .filter(|def| vis.table_visible(def.schema, def.name))
    {
        rows.push(system_table_row(def));
    }
    rows
}

pub(crate) fn fast_count_rows(
    db: &Database,
    name: &str,
    request: &SystemScanRequest,
) -> Result<Option<u64>, QueryError> {
    let (schema_name, table_name) = split_table_name(DEFAULT_DATABASE, name);
    if !schema_name.eq_ignore_ascii_case("information_schema") {
        return Ok(None);
    }
    let vis = Visibility::compute(db);
    let catalog = db.catalog_read()?;
    match table_name.to_ascii_lowercase().as_str() {
        "tables" => {
            let base_tables = catalog
                .tables
                .values()
                .filter(|table| {
                    scan_request_matches_table(request, &table.database, &table.name)
                        && vis.table_visible(&table.database, &table.name)
                })
                .count() as u64;
            let views = catalog
                .views
                .values()
                .filter(|view| {
                    scan_request_matches_table(request, &view.database, &view.name)
                        && vis.table_visible(&view.database, &view.name)
                })
                .count() as u64;
            let system = system_table_definitions()
                .filter(|def| !def.hidden)
                .filter(|def| scan_request_matches_table(request, def.schema, def.name))
                .filter(|def| vis.table_visible(def.schema, def.name))
                .count() as u64;
            Ok(Some(
                base_tables.saturating_add(views).saturating_add(system),
            ))
        }
        "statistics" => {
            let table_ids = catalog
                .tables
                .values()
                .filter(|table| {
                    scan_request_matches_table(request, &table.database, &table.name)
                        && vis.table_visible(&table.database, &table.name)
                })
                .map(|table| table.id)
                .collect::<HashSet<_>>();
            let user_parts = db.count_catalog_index_parts(&table_ids)?;
            let system_parts = system_table_definitions()
                .filter(|def| !def.hidden)
                .filter(|def| scan_request_matches_table(request, def.schema, def.name))
                .filter(|def| vis.table_visible(def.schema, def.name))
                .flat_map(|def| def.indexes.iter())
                .map(|index| index.columns.len() as u64)
                .sum::<u64>();
            Ok(Some(user_parts.saturating_add(system_parts)))
        }
        _ => Ok(None),
    }
}

fn base_table_row(db: &Database, table: &TableSchema) -> Vec<Value> {
    // Shared with `SHOW TABLE STATUS` via `engine::table_stats_view` —
    // both views now agree on TABLE_ROWS / DATA_LENGTH / INDEX_LENGTH
    // / AVG_ROW_LENGTH / AUTO_INCREMENT for the same table.
    let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
    let create_options = crate::engine::table_stats_view::table_create_options(table);
    let auto_increment = match stats.auto_increment {
        Some(value) => i64::try_from(value)
            .map(Value::Int)
            .unwrap_or(Value::UInt(value)),
        None => Value::Null,
    };
    vec![
        s("def"),
        s(&table.database),
        s(&table.name),
        s(&table.table_type),
        s(&table.engine),
        Value::Int(10),
        s(crate::engine::table_stats_view::table_row_format(table)),
        Value::Int(saturating_i64(stats.rows)),
        Value::Int(saturating_i64(stats.avg_row_length)),
        Value::Int(saturating_i64(stats.data_length)),
        Value::Int(saturating_i64(stats.max_data_length)),
        Value::Int(saturating_i64(stats.index_length)),
        Value::Int(saturating_i64(stats.data_free)),
        auto_increment,
        mysql_time_value(table.created_ms),
        mysql_time_value(table.updated_ms),
        Value::Null,
        s(&table.collation),
        Value::Null,
        s(&create_options),
        s(&table.comment),
    ]
}

fn view_table_row(database: &str, name: &str) -> Vec<Value> {
    vec![
        s("def"),
        s(database),
        s(name),
        s("VIEW"),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        metadata_create_time_value(),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        s("VIEW"),
    ]
}

fn system_table_row(def: &SystemTableDef) -> Vec<Value> {
    let pfs = if def.schema.eq_ignore_ascii_case("performance_schema") {
        crate::engine::performance_schema_mysql80_generated::table_metadata(def.name)
    } else {
        None
    };
    let number = |value: Option<u64>| value.map(Value::UInt).unwrap_or(Value::Null);
    vec![
        s("def"),
        s(def.schema),
        s(def.name),
        s(def.table_type),
        if def.engine.is_empty() {
            Value::Null
        } else {
            s(def.engine)
        },
        number(pfs.and_then(|metadata| metadata.version)),
        pfs.and_then(|metadata| metadata.row_format)
            .map(s)
            .unwrap_or(Value::Null),
        number(pfs.and_then(|metadata| metadata.table_rows)),
        number(pfs.and_then(|metadata| metadata.avg_row_length)),
        number(pfs.and_then(|metadata| metadata.data_length)),
        number(pfs.and_then(|metadata| metadata.max_data_length)),
        number(pfs.and_then(|metadata| metadata.index_length)),
        number(pfs.and_then(|metadata| metadata.data_free)),
        number(pfs.and_then(|metadata| metadata.auto_increment)),
        metadata_create_time_value(),
        Value::Null,
        Value::Null,
        pfs.and_then(|metadata| metadata.table_collation)
            .map(s)
            .unwrap_or_else(|| {
                if def.engine.is_empty() {
                    Value::Null
                } else {
                    s(crate::charset::DEFAULT_COLLATION)
                }
            }),
        number(pfs.and_then(|metadata| metadata.checksum)),
        pfs.map(|metadata| s(metadata.create_options))
            .unwrap_or_else(|| s("")),
        pfs.map(|metadata| s(metadata.table_comment))
            .unwrap_or_else(|| s("")),
    ]
}

fn columns_rows(db: &Database, request: &SystemScanRequest) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for def in system_table_definitions()
        .filter(|def| !def.hidden)
        .filter(|def| scan_request_matches_table(request, def.schema, def.name))
        .filter(|def| vis.table_visible(def.schema, def.name))
    {
        append_system_columns_rows(def, &vis, request, &mut rows);
    }
    if let Some((schema, table_name)) = exact_requested_relation(request) {
        let qualified = format!("{schema}.{table_name}");
        if let Ok(table) = db.table(&qualified) {
            append_table_column_rows(&mut rows, &vis, request, &table);
        } else if let Some(view) = db.view_definition(schema, table_name) {
            append_view_column_rows(&mut rows, &vis, request, &view);
        }
        return rows;
    }
    let catalog = db.catalog();
    visit_requested_table_descriptors(db, &catalog, request, |table| {
        append_table_column_rows(&mut rows, &vis, request, table);
    });
    visit_requested_views(&catalog, request, |view| {
        append_view_column_rows(&mut rows, &vis, request, view);
    });
    rows
}

fn append_table_column_rows(
    rows: &mut Vec<Vec<Value>>,
    vis: &Visibility,
    request: &SystemScanRequest,
    table: &TableSchema,
) {
    if !vis.table_visible(&table.database, &table.name) {
        return;
    }
    let mut privilege_checker = vis.column_privilege_checker(&table.database, &table.name);
    for (ordinal, (_, column)) in table.iter_visible_columns().enumerate() {
        if request
            .column_name
            .as_deref()
            .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(&column.name))
        {
            continue;
        }
        let privileges = privilege_checker.for_column(&column.name);
        if !privileges.is_empty() {
            rows.push(base_column_row(table, column, ordinal + 1, &privileges));
        }
    }
}

fn append_view_column_rows(
    rows: &mut Vec<Vec<Value>>,
    vis: &Visibility,
    request: &SystemScanRequest,
    view: &crate::storage::ViewSchema,
) {
    if !vis.table_visible(&view.database, &view.name) {
        return;
    }
    let mut privilege_checker = vis.column_privilege_checker(&view.database, &view.name);
    for (idx, column) in view.columns.iter().enumerate() {
        if request
            .column_name
            .as_deref()
            .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(&column.name))
        {
            continue;
        }
        let privileges = privilege_checker.for_column(&column.name);
        if !privileges.is_empty() {
            rows.push(view_column_row(
                &view.database,
                &view.name,
                column,
                idx + 1,
                &privileges,
            ));
        }
    }
}

fn append_system_columns_rows(
    def: &SystemTableDef,
    vis: &Visibility,
    request: &SystemScanRequest,
    rows: &mut Vec<Vec<Value>>,
) {
    let mysql80_core = def.schema.eq_ignore_ascii_case("mysql")
        || def.schema.eq_ignore_ascii_case("performance_schema")
        || find_information_schema_registration(def.name).is_some_and(|entry| {
            entry.compatibility == InformationSchemaCompatibility::Mysql80Community
        });
    let mut privilege_checker = vis.column_privilege_checker(def.schema, def.name);
    for (idx, column) in def.columns.iter().enumerate() {
        if request
            .column_name
            .as_deref()
            .is_some_and(|wanted| !wanted.eq_ignore_ascii_case(column.name))
        {
            continue;
        }
        let privileges = privilege_checker.for_column(column.name);
        if privileges.is_empty() {
            continue;
        }

        let core_metadata = generated_system_column_metadata(def, column.name);
        rows.push(vec![
            s("def"),
            s(def.schema),
            s(def.name),
            s(column.name),
            Value::Int((idx + 1) as i64),
            core_metadata
                .and_then(|metadata| metadata.default_value)
                .map(s)
                .unwrap_or(Value::Null),
            s(if column.nullable { "YES" } else { "NO" }),
            s(column.data_type),
            opt_i(column.char_len),
            opt_i(system_column_octet_length(def, column)),
            opt_i(column.numeric_precision),
            opt_i(column.numeric_scale),
            opt_i(column.datetime_precision),
            if let Some(character_set) = core_metadata.and_then(|metadata| metadata.character_set) {
                s(character_set)
            } else if !mysql80_core
                && (column.char_len.is_some()
                    || matches!(column.data_type, "text" | "mediumtext" | "longtext"))
            {
                s(crate::charset::DEFAULT_CHARSET)
            } else {
                Value::Null
            },
            if let Some(collation) = core_metadata.and_then(|metadata| metadata.collation) {
                s(collation)
            } else if !mysql80_core
                && (column.char_len.is_some()
                    || matches!(column.data_type, "text" | "mediumtext" | "longtext"))
            {
                s(crate::charset::DEFAULT_COLLATION)
            } else {
                Value::Null
            },
            s(column.column_type_sql),
            core_metadata
                .map(|metadata| s(metadata.column_key))
                .unwrap_or_else(|| s(crate::engine::mysql_schema::column_key(def, column.name))),
            core_metadata
                .map(|metadata| s(metadata.extra))
                .unwrap_or_else(|| s("")),
            Value::String(privileges),
            core_metadata
                .map(|metadata| s(metadata.comment))
                .unwrap_or_else(|| s("")),
            core_metadata
                .map(|metadata| s(metadata.generation_expression))
                .unwrap_or_else(|| s("")),
            core_metadata
                .and_then(|metadata| metadata.srs_id)
                .map(Value::Int)
                .unwrap_or(Value::Null),
        ])
    }
}

fn base_column_row(
    table: &TableSchema,
    column: &ColumnSchema,
    ordinal: usize,
    privileges: &str,
) -> Vec<Value> {
    vec![
        s("def"),
        s(&table.database),
        s(&table.name),
        s(&column.name),
        Value::Int(ordinal as i64),
        column_default_value(column),
        s(if column.nullable { "YES" } else { "NO" }),
        s(&mysql_data_type_name(&column.data_type)),
        mysql_character_maximum_length(&column.data_type),
        mysql_character_octet_length_for_column(column),
        match (&column.data_type, column.unsigned) {
            (DataTypeKind::BigInt, true) => Value::Int(20),
            (DataTypeKind::MediumInt, true) => Value::Int(8),
            _ => mysql_numeric_precision(&column.data_type),
        },
        mysql_numeric_scale(&column.data_type),
        mysql_datetime_precision(&column.data_type),
        column_charset_value(column),
        column_collation_value(column),
        s(&crate::engine::show::mysql_type_name_with_attrs(
            &column.data_type,
            column.unsigned,
            column.zerofill,
        )),
        s(&column_key(table, &column.name)),
        s(&column_extra(column)),
        s(privileges),
        s(&column.comment),
        column
            .generated_expr
            .clone()
            .map(Value::String)
            .unwrap_or_else(|| s("")),
        match &column.data_type {
            DataTypeKind::Geometry { srid, .. } => srid
                .map(|value| Value::Int(i64::from(value)))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
    ]
}

fn view_column_row(
    database: &str,
    view_name: &str,
    column: &ViewColumnDef,
    ordinal: usize,
    privileges: &str,
) -> Vec<Value> {
    let data_type = column
        .data_type
        .as_ref()
        .unwrap_or(&DataTypeKind::VarChar { len: Some(255) });
    vec![
        s("def"),
        s(database),
        s(view_name),
        s(&column.name),
        Value::Int(ordinal as i64),
        Value::Null,
        s(if column.nullable { "YES" } else { "NO" }),
        s(&mysql_data_type_name(data_type)),
        mysql_character_maximum_length(data_type),
        mysql_view_character_octet_length(column, data_type),
        mysql_numeric_precision(data_type),
        mysql_numeric_scale(data_type),
        mysql_datetime_precision(data_type),
        if matches!(
            data_type,
            DataTypeKind::Char { .. }
                | DataTypeKind::VarChar { .. }
                | DataTypeKind::Text { .. }
                | DataTypeKind::Enum { .. }
                | DataTypeKind::Set { .. }
        ) {
            column
                .charset
                .as_deref()
                .map(s)
                .unwrap_or_else(|| s(crate::charset::DEFAULT_CHARSET))
        } else {
            Value::Null
        },
        if matches!(
            data_type,
            DataTypeKind::Char { .. }
                | DataTypeKind::VarChar { .. }
                | DataTypeKind::Text { .. }
                | DataTypeKind::Enum { .. }
                | DataTypeKind::Set { .. }
        ) {
            column
                .collation
                .as_deref()
                .map(s)
                .unwrap_or_else(|| s(crate::charset::DEFAULT_COLLATION))
        } else {
            Value::Null
        },
        s(&mysql_type_name_with_unsigned(data_type, false)),
        s(""),
        s(""),
        s(privileges),
        s(""),
        s(""),
        Value::Null,
    ]
}

fn mysql_view_character_octet_length(column: &ViewColumnDef, data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => {
            let max_len = column
                .charset
                .as_deref()
                .and_then(|charset| crate::charset::resolve_charset(charset).ok())
                .map_or(4_i64, |charset| i64::from(charset.max_len));
            match mysql_character_maximum_length(data_type) {
                Value::Int(length) => Value::Int(length.saturating_mul(max_len)),
                other => other,
            }
        }
        _ => mysql_character_octet_length(data_type),
    }
}

fn statistics_rows(db: &Database, request: &SystemScanRequest) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for def in system_table_definitions()
        .filter(|def| !def.hidden)
        .filter(|def| scan_request_matches_table(request, def.schema, def.name))
        .filter(|def| vis.table_visible(def.schema, def.name))
    {
        for index in def.indexes {
            for (idx, column) in index.columns.iter().enumerate() {
                let is_performance_schema = def.schema.eq_ignore_ascii_case("performance_schema");
                let nullable_label = def
                    .columns
                    .iter()
                    .find(|col| col.name.eq_ignore_ascii_case(column))
                    .map(|col| if col.nullable { "YES" } else { "" })
                    .unwrap_or("");
                rows.push(vec![
                    s("def"),
                    s(def.schema),
                    s(def.name),
                    Value::Int((!index.unique) as i64),
                    s(def.schema),
                    s(index.name),
                    Value::Int((idx + 1) as i64),
                    s(column),
                    if is_performance_schema {
                        Value::Null
                    } else {
                        s("A")
                    },
                    if is_performance_schema {
                        Value::Null
                    } else {
                        Value::Int(0)
                    },
                    Value::Null,
                    Value::Null,
                    s(nullable_label),
                    s(index.index_type),
                    s(""),
                    s(""),
                    s(if index.visible { "YES" } else { "NO" }),
                    Value::Null,
                ]);
            }
        }
    }
    if let Some((schema, table_name)) = exact_requested_relation(request) {
        let qualified = format!("{schema}.{table_name}");
        if let Ok(table) = db.table(&qualified) {
            append_table_statistics_rows(db, &mut rows, &vis, &table);
        }
        return rows;
    }
    let catalog = db.catalog();
    visit_requested_table_descriptors(db, &catalog, request, |table| {
        append_table_statistics_rows(db, &mut rows, &vis, table);
    });
    rows
}

fn append_table_statistics_rows(
    db: &Database,
    rows: &mut Vec<Vec<Value>>,
    vis: &Visibility,
    table: &TableSchema,
) {
    if !vis.table_visible(&table.database, &table.name) {
        return;
    }
    // Cardinality fallback for never-ANALYZE-d tables: clients
    // (and the planner) interpret `0` as "no data", so a fresh
    // table with a million rows that nobody has ANALYZEd would
    // confuse JDBC metadata and ORMs. We compute a cheap row-count
    // estimate once per table here and reuse it across all index
    // rows for that table.
    let table_stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
    for index in table.iter_public_indexes() {
        let columns_in_index = index.columns.len();
        for (idx, column) in index.columns.iter().enumerate() {
            let (column_value, expression_value, nullable_label) =
                if let Some(expr) = index.key_expression(idx) {
                    (
                        Value::Null,
                        Value::String(expr.expr_sql.clone()),
                        if expr.nullable { "YES" } else { "" },
                    )
                } else {
                    let label = table
                        .columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(column))
                        .map(|col| if col.nullable { "YES" } else { "" })
                        .unwrap_or("");
                    (s(column), Value::Null, label)
                };
            let sub_part = if matches!(column_value, Value::Null) {
                // Expression key parts can't have a prefix length.
                Value::Null
            } else {
                index
                    .column_prefix_len(idx)
                    .map(|len| Value::Int(len as i64))
                    .unwrap_or(Value::Null)
            };
            // Prefer the real ANALYZE-derived cardinality when
            // present; only fall back to a width-based estimate
            // when stats haven't been populated yet (post-CREATE,
            // pre-first-ANALYZE).
            let cardinality = table
                .index_prefix_cardinality(&index.name, idx + 1)
                .filter(|c| *c > 0)
                .unwrap_or_else(|| {
                    crate::engine::table_stats_view::fallback_index_cardinality(
                        table_stats.rows,
                        index.unique,
                        idx + 1,
                        columns_in_index,
                    )
                });
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&table.name),
                Value::Int((!index.unique) as i64),
                s(&table.database),
                s(&index.name),
                Value::Int((idx + 1) as i64),
                column_value,
                if index.index_type.eq_ignore_ascii_case("FULLTEXT")
                    || index.index_type.eq_ignore_ascii_case("SPATIAL")
                {
                    Value::Null
                } else {
                    s(match index.column_direction(idx) {
                        IndexColumnDirection::Asc => "A",
                        IndexColumnDirection::Desc => "D",
                    })
                },
                Value::Int(saturating_i64(cardinality)),
                sub_part,
                Value::Null,
                s(nullable_label),
                s(&index.index_type),
                s(""),
                s(&index.comment),
                s(if index.visible { "YES" } else { "NO" }),
                expression_value,
            ]);
        }
    }
}

fn table_constraints_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !vis.table_visible(&table.database, &table.name) {
            continue;
        }
        for index in table.iter_public_indexes().filter(|index| index.unique) {
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&index.name),
                s(&table.database),
                s(&table.name),
                s(if index.primary {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                }),
                s("YES"),
            ]);
        }
        for check in &table.check_constraints {
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&check.name),
                s(&table.database),
                s(&table.name),
                s("CHECK"),
                s(if check.enforced { "YES" } else { "NO" }),
            ]);
        }
        for fk in &table.foreign_keys {
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&fk.name),
                s(&table.database),
                s(&table.name),
                s("FOREIGN KEY"),
                s("YES"),
            ]);
        }
    }
    rows
}

fn key_column_usage_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !vis.table_visible(&table.database, &table.name) {
            continue;
        }
        for index in table.iter_public_indexes().filter(|index| index.unique) {
            for (idx, column) in index.columns.iter().enumerate() {
                rows.push(vec![
                    s("def"),
                    s(&table.database),
                    s(&index.name),
                    s("def"),
                    s(&table.database),
                    s(&table.name),
                    s(column),
                    Value::Int((idx + 1) as i64),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
            }
        }
        for fk in &table.foreign_keys {
            for (i, column) in fk.columns.iter().enumerate() {
                let refcol = fk.referenced_columns.get(i).cloned().unwrap_or_default();
                rows.push(vec![
                    s("def"),
                    s(&table.database),
                    s(&fk.name),
                    s("def"),
                    s(&table.database),
                    s(&table.name),
                    s(column),
                    Value::Int((i + 1) as i64),
                    Value::Int((i + 1) as i64),
                    s(&fk.referenced_database),
                    s(&fk.referenced_table),
                    s(&refcol),
                ]);
            }
        }
    }
    rows
}

fn referential_constraints_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    let catalog = db.catalog();
    for table in catalog.tables.values() {
        if !vis.table_visible(&table.database, &table.name) {
            continue;
        }
        for fk in &table.foreign_keys {
            // Name of the referenced UNIQUE/PRIMARY index on the parent.
            let unique_name = catalog
                .tables
                .get(&crate::storage::table_key(
                    &fk.referenced_database,
                    &fk.referenced_table,
                ))
                .and_then(|parent| {
                    parent
                        .indexes
                        .iter()
                        .find(|idx| {
                            (idx.unique || idx.primary)
                                && idx.columns.len() >= fk.referenced_columns.len()
                                && fk
                                    .referenced_columns
                                    .iter()
                                    .zip(idx.columns.iter())
                                    .all(|(r, c)| r.eq_ignore_ascii_case(c))
                        })
                        .map(|idx| idx.name.clone())
                });
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&fk.name),
                s("def"),
                s(&fk.referenced_database),
                unique_name.map(|n| s(&n)).unwrap_or(Value::Null),
                s("NONE"),
                s(fk.on_update.as_sql()),
                s(fk.on_delete.as_sql()),
                s(&table.name),
                s(&fk.referenced_table),
            ]);
        }
    }
    rows
}

fn check_constraints_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !vis.table_visible(&table.database, &table.name) {
            continue;
        }
        for check in &table.check_constraints {
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&check.name),
                s(&check.expr),
            ]);
        }
    }
    rows
}

fn views_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows: Vec<Vec<Value>> = db
        .catalog()
        .views
        .values()
        .filter(|view| vis.table_visible(&view.database, &view.name))
        .map(|view| {
            let show_definition =
                vis.has_table_privilege(Privilege::ShowView, &view.database, &view.name);
            vec![
                s("def"),
                s(&view.database),
                s(&view.name),
                // VIEW_DEFINITION echoes MySQL's canonical body; fall back to
                // raw text for pre-normalization / non-renderable views.
                if show_definition {
                    s(if view.definition_normalized.is_empty() {
                        &view.sql
                    } else {
                        &view.definition_normalized
                    })
                } else {
                    Value::Null
                },
                s(view.check_option.as_deref().unwrap_or("NONE")),
                s(if view.is_updatable { "YES" } else { "NO" }),
                s(&format!("{}@{}", view.definer_user, view.definer_host)),
                s(view.security_type.as_str()),
                s(&view.character_set_client),
                s(&view.collation_connection),
            ]
        })
        .collect();
    for mut row in crate::engine::sys_schema::view_information_schema_rows() {
        let name = row[2].to_display_string();
        if !vis.table_visible("sys", &name) {
            continue;
        }
        if !vis.has_table_privilege(Privilege::ShowView, "sys", &name) {
            row[3] = Value::Null;
        }
        rows.push(row);
    }
    rows
}

fn view_routine_usage_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for view in db.catalog().views.values() {
        if !vis.table_visible(&view.database, &view.name)
            || !vis.has_table_privilege(Privilege::ShowView, &view.database, &view.name)
        {
            continue;
        }
        for dep in &view.routine_dependencies {
            rows.push(vec![
                s("def"),
                s(&view.database),
                s(&view.name),
                s("def"),
                s(&dep.database),
                s(&dep.name),
            ]);
        }
    }
    rows
}

fn view_table_usage_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    for view in db.catalog().views.values() {
        if !vis.table_visible(&view.database, &view.name)
            || !vis.has_table_privilege(Privilege::ShowView, &view.database, &view.name)
        {
            continue;
        }
        for dep in &view.dependencies {
            rows.push(vec![
                s("def"),
                s(&view.database),
                s(&view.name),
                s("def"),
                s(&dep.database),
                s(&dep.name),
            ]);
        }
    }
    rows
}

fn routines_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    db.catalog()
        .routines
        .values()
        .map(|routine| routine.as_ref().clone())
        .chain(crate::engine::sys_schema::routine_schemas())
        .filter(|routine| vis.routine_visible(routine))
        .map(|routine| {
            let show_definition = vis.routine_definition_visible(&routine);
            routine_row(&routine, show_definition)
        })
        .collect()
}

fn events_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let events = db.all_events();
    events
        .iter()
        .filter(|event| {
            vis.schema_visible(&event.database)
                && vis.has_database_privilege(Privilege::Event, &event.database)
        })
        .map(event_row)
        .collect()
}

fn event_row(event: &crate::storage::EventSchema) -> Vec<Value> {
    use crate::storage::EventScheduleSchema;
    // Schedule times (EXECUTE_AT / STARTS / ENDS) display in the event's
    // own time zone (G2); the stored ms are UTC.
    let tz = &event.time_zone;
    let tz_time = |ms: u64| -> Value {
        if ms == 0 {
            Value::Null
        } else {
            Value::String(crate::engine::executor::events::format_ms_in_tz(ms, tz))
        }
    };
    let (event_type, execute_at, interval_value, interval_field, starts, ends) =
        match &event.schedule {
            EventScheduleSchema::OneShot { execute_at_ms } => (
                "ONE TIME",
                tz_time(*execute_at_ms),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ),
            EventScheduleSchema::Recurring {
                interval,
                starts_ms,
                ends_ms,
            } => (
                "RECURRING",
                Value::Null,
                s(&interval.value),
                s(&interval.unit),
                tz_time(*starts_ms),
                ends_ms.map(|ms| tz_time(ms)).unwrap_or(Value::Null),
            ),
        };
    vec![
        s("def"),
        s(&event.database),
        s(&event.name),
        s(&event.definer()),
        s(&event.time_zone),
        s("SQL"),
        s(&event.body_sql),
        s(event_type),
        execute_at,
        interval_value,
        interval_field,
        s(&event.sql_mode),
        starts,
        ends,
        s(event.status.as_str()),
        s(event.on_completion.as_str()),
        mysql_time_value(event.created_ms),
        mysql_time_value(event.last_altered_ms),
        tz_time(event.last_executed_ms),
        s(&event.comment),
        Value::Int(event_originator() as i64),
        s(&event.character_set_client),
        s(&event.collation_connection),
        s(&event.database_collation),
    ]
}

/// G9: ORIGINATOR is the server_id of the server that created the event.
/// mydb does not yet vary this per-event, so we report the running
/// server's id (matching MySQL on a single server: ORIGINATOR =
/// @@server_id).
fn event_originator() -> u32 {
    crate::engine::executor::events::current_server_id()
}

fn partitions_rows(db: &Database) -> Vec<Vec<Value>> {
    use crate::storage::partitioning::{PartitionBounds, PartitionLiteral, PartitionMethod};
    let mut out: Vec<Vec<Value>> = Vec::new();
    let catalog = db.catalog();
    let vis = Visibility::compute(db);
    let mut tables: Vec<_> = catalog
        .tables
        .values()
        .filter(|t| vis.table_visible(&t.database, &t.name))
        .cloned()
        .collect();
    tables.sort_by(|a, b| {
        a.database
            .cmp(&b.database)
            .then_with(|| a.name.cmp(&b.name))
    });
    for table in &tables {
        let Some(part) = table.partitioning.as_ref() else {
            // Unpartitioned tables emit a single NULL-partition row
            // matching MySQL's output for non-partitioned tables.
            let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
            out.push(vec![
                s("def"),
                s(&table.database),
                s(&table.name),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(saturating_i64(stats.rows)),
                Value::Int(saturating_i64(stats.avg_row_length)),
                Value::Int(saturating_i64(stats.data_length)),
                Value::Int(saturating_i64(stats.max_data_length)),
                Value::Int(saturating_i64(stats.index_length)),
                Value::Int(saturating_i64(stats.data_free)),
                mysql_time_value(table.created_ms),
                Value::Null,
                Value::Null,
                Value::Null,
                s(""),
                s("default"),
                Value::Null,
            ]);
            continue;
        };
        let method_str = part.method.keyword();
        let expr = if part.method.uses_columns() {
            part.columns
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            canonical_partition_expression(&part.expr_sql)
        };
        let sub_method_str = part
            .subpartitioning
            .as_ref()
            .map(|sub| sub.method.keyword().to_string());
        let sub_expr = part.subpartitioning.as_ref().map(|sub| {
            if sub.method.uses_columns() {
                sub.columns
                    .iter()
                    .map(|c| format!("`{}`", c))
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                canonical_partition_expression(&sub.expr_sql)
            }
        });
        let partition_count = part.partitions.len();
        for (idx, partition) in part.partitions.iter().enumerate() {
            let descr = match &partition.bounds {
                PartitionBounds::RangeLessThan(lits) => {
                    let parts: Vec<String> = lits
                        .iter()
                        .map(|l| match l {
                            PartitionLiteral::MaxValue => "MAXVALUE".to_string(),
                            PartitionLiteral::Value(v) => crate::storage::value_to_string(v),
                        })
                        .collect();
                    s(&parts.join(","))
                }
                PartitionBounds::ListIn(groups) => {
                    let parts: Vec<String> = groups
                        .iter()
                        .map(|t| {
                            let inner: Vec<String> = t
                                .iter()
                                .map(|l| match l {
                                    PartitionLiteral::MaxValue => "MAXVALUE".to_string(),
                                    PartitionLiteral::Value(v) => {
                                        crate::storage::value_to_string(v)
                                    }
                                })
                                .collect();
                            if inner.len() == 1 {
                                inner[0].clone()
                            } else {
                                format!("({})", inner.join(","))
                            }
                        })
                        .collect();
                    s(&parts.join(","))
                }
                PartitionBounds::Hash => Value::Null,
            };
            if !partition.subpartitions.is_empty() {
                for (sub_idx, subpartition) in partition.subpartitions.iter().enumerate() {
                    let pstats = crate::engine::table_stats_view::TableStatsView::for_partition(
                        db,
                        table,
                        partition.subpartitions.len(),
                        sub_idx,
                        subpartition.storage_id,
                    );
                    out.push(vec![
                        s("def"),
                        s(&table.database),
                        s(&table.name),
                        s(&partition.name),
                        s(&subpartition.name),
                        Value::Int((idx + 1) as i64),
                        Value::Int((sub_idx + 1) as i64),
                        s(method_str),
                        s(sub_method_str.as_deref().unwrap_or("")),
                        s(&expr),
                        s(sub_expr.as_deref().unwrap_or("")),
                        descr.clone(),
                        Value::Int(saturating_i64(pstats.rows)),
                        Value::Int(saturating_i64(pstats.avg_row_length)),
                        Value::Int(saturating_i64(pstats.data_length)),
                        Value::Int(saturating_i64(pstats.max_data_length)),
                        Value::Int(saturating_i64(pstats.index_length)),
                        Value::Int(saturating_i64(pstats.data_free)),
                        mysql_time_value(table.created_ms),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        s(&subpartition.comment),
                        partition_nodegroup_value(&subpartition.options),
                        partition_tablespace_value(&subpartition.options),
                    ]);
                }
                continue;
            }
            let pstats = crate::engine::table_stats_view::TableStatsView::for_partition(
                db,
                table,
                partition_count,
                idx,
                partition.storage_id,
            );
            out.push(vec![
                s("def"),
                s(&table.database),
                s(&table.name),
                s(&partition.name),
                Value::Null,
                Value::Int((idx + 1) as i64),
                Value::Null,
                s(method_str),
                Value::Null,
                s(&expr),
                Value::Null,
                descr,
                Value::Int(saturating_i64(pstats.rows)),
                Value::Int(saturating_i64(pstats.avg_row_length)),
                Value::Int(saturating_i64(pstats.data_length)),
                Value::Int(saturating_i64(pstats.max_data_length)),
                Value::Int(saturating_i64(pstats.index_length)),
                Value::Int(saturating_i64(pstats.data_free)),
                mysql_time_value(table.created_ms),
                Value::Null,
                Value::Null,
                Value::Null,
                s(&partition.comment),
                partition_nodegroup_value(&partition.options),
                partition_tablespace_value(&partition.options),
            ]);
        }
    }
    for view in catalog
        .views
        .values()
        .filter(|view| vis.table_visible(&view.database, &view.name))
    {
        let mut row = vec![s("def"), s(&view.database), s(&view.name)];
        row.extend(std::iter::repeat_n(Value::Null, 19));
        row.push(s(""));
        row.push(Value::Null);
        row.push(Value::Null);
        out.push(row);
    }
    out.sort_by(|left, right| {
        left[1]
            .to_display_string()
            .cmp(&right[1].to_display_string())
            .then_with(|| {
                left[2]
                    .to_display_string()
                    .cmp(&right[2].to_display_string())
            })
            .then_with(|| {
                crate::storage::partitioning::compare_value(&left[5], &right[5])
                    .then_with(|| crate::storage::partitioning::compare_value(&left[6], &right[6]))
            })
    });
    let _ = PartitionMethod::Range; // silence unused-variant warning
    out
}

fn canonical_partition_expression(expression: &str) -> String {
    let expression = expression.trim();
    if !expression.is_empty()
        && expression
            .chars()
            .all(|character| character == '_' || character.is_ascii_alphanumeric())
        && expression
            .chars()
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
    {
        format!("`{expression}`")
    } else {
        expression.to_string()
    }
}

/// `information_schema.PARTITIONS.NODEGROUP` — the declared nodegroup, or
/// MySQL's `default` sentinel when none was specified.
fn partition_nodegroup_value(
    options: &crate::storage::partitioning::PartitionPhysicalOptions,
) -> Value {
    match options.nodegroup {
        Some(n) => s(&n.to_string()),
        None => s("default"),
    }
}

/// `information_schema.PARTITIONS.TABLESPACE_NAME` — the declared
/// tablespace, or NULL when none.
fn partition_tablespace_value(
    options: &crate::storage::partitioning::PartitionPhysicalOptions,
) -> Value {
    match &options.tablespace {
        Some(ts) => s(ts),
        None => Value::Null,
    }
}

fn triggers_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut triggers = db
        .catalog()
        .triggers
        .values()
        .filter(|t| {
            vis.table_visible(&t.table_database, &t.table_name)
                && vis.has_table_privilege(Privilege::Trigger, &t.table_database, &t.table_name)
        })
        .cloned()
        .collect::<Vec<_>>();
    triggers.extend(
        crate::engine::sys_schema::trigger_schemas()
            .into_iter()
            .filter(|t| {
                vis.table_visible(&t.table_database, &t.table_name)
                    && vis.has_table_privilege(Privilege::Trigger, &t.table_database, &t.table_name)
            }),
    );
    triggers.sort_by_key(|trigger| {
        (
            trigger.database.clone(),
            trigger.table_name.clone(),
            trigger.event.lower().to_string(),
            trigger.timing.lower().to_string(),
            trigger.action_order,
            trigger.name.clone(),
        )
    });
    triggers.iter().map(trigger_row).collect()
}

fn trigger_row(trigger: &TriggerSchema) -> Vec<Value> {
    vec![
        s("def"),
        s(&trigger.database),
        s(&trigger.name),
        s(trigger.event.as_str()),
        s("def"),
        s(&trigger.table_database),
        s(&trigger.table_name),
        Value::Int(trigger.action_order as i64),
        Value::Null,
        s(&trigger.body_sql),
        s("ROW"),
        s(trigger.timing.as_str()),
        Value::Null,
        Value::Null,
        // MySQL declares both row-reference names NOT NULL and reports the
        // identifiers even when one transition image is not legal for the
        // trigger event (for example OLD on INSERT).
        s("OLD"),
        s("NEW"),
        if trigger.created_ms == 0 {
            metadata_create_time_value()
        } else {
            mysql_time_value(trigger.created_ms)
        },
        s(&trigger.sql_mode),
        s(&trigger.definer()),
        s(&trigger.character_set_client),
        s(&trigger.collation_connection),
        s(&trigger.database_collation),
    ]
}

fn routine_row(routine: &RoutineSchema, show_definition: bool) -> Vec<Value> {
    let return_type = routine
        .return_type
        .as_ref()
        .map(|dt| mysql_data_type_name(dt))
        .unwrap_or_default();
    let dtd = routine
        .return_type
        .as_ref()
        .map(|dt| mysql_type_name_with_unsigned(dt, routine.return_unsigned))
        .or_else(|| routine.return_type_sql.clone())
        .unwrap_or_default();
    vec![
        s(&routine.name),
        s("def"),
        s(&routine.database),
        s(&routine.name),
        s(routine.kind.as_str()),
        s(&return_type),
        routine
            .return_type
            .as_ref()
            .map(mysql_character_maximum_length)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(mysql_character_octet_length)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(mysql_numeric_precision)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(mysql_numeric_scale)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(mysql_datetime_precision)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(character_set_for_data_type)
            .unwrap_or(Value::Null),
        routine
            .return_type
            .as_ref()
            .map(collation_for_data_type)
            .unwrap_or(Value::Null),
        s(&dtd),
        s("SQL"),
        if show_definition {
            s(&routine.body_sql)
        } else {
            Value::Null
        },
        Value::Null,
        s("SQL"),
        s("SQL"),
        s(if routine.deterministic { "YES" } else { "NO" }),
        s(routine.sql_data_access.as_str()),
        Value::Null,
        s(routine.security_type.as_str()),
        if routine.created_ms == 0 {
            metadata_create_time_value()
        } else {
            mysql_time_value(routine.created_ms)
        },
        if routine.updated_ms == 0 {
            metadata_create_time_value()
        } else {
            mysql_time_value(routine.updated_ms)
        },
        s(&routine.sql_mode),
        s(&routine.comment),
        s(&routine.definer()),
        s(&routine.character_set_client),
        s(&routine.collation_connection),
        s(&routine.database_collation),
    ]
}

fn parameters_rows(db: &Database) -> Vec<Vec<Value>> {
    let vis = Visibility::compute(db);
    let mut rows = Vec::new();
    let mut routines = db
        .catalog()
        .routines
        .values()
        .filter(|r| vis.routine_visible(r))
        .map(|routine| routine.as_ref().clone())
        .collect::<Vec<_>>();
    routines.extend(crate::engine::sys_schema::routine_schemas());
    for routine in &routines {
        if routine.kind == RoutineKindSchema::Function {
            if let Some(return_type) = &routine.return_type {
                rows.push(parameter_row(
                    routine,
                    0,
                    None,
                    None,
                    return_type,
                    routine.return_type_sql.as_deref().unwrap_or(""),
                    routine.return_unsigned,
                ));
            }
        }
        for (idx, param) in routine.params.iter().enumerate() {
            rows.push(parameter_row(
                routine,
                idx + 1,
                Some(param.mode.as_str()),
                Some(&param.name),
                &param.data_type,
                &param.data_type_sql,
                param.unsigned,
            ));
        }
    }
    rows
}

fn parameter_row(
    routine: &RoutineSchema,
    ordinal: usize,
    mode: Option<&str>,
    name: Option<&str>,
    data_type: &DataTypeKind,
    dtd_identifier: &str,
    unsigned: bool,
) -> Vec<Value> {
    let dtd = if routine.database.eq_ignore_ascii_case("sys") {
        // Built-in sys routines have exact MySQL 8.0 signatures, including
        // TINYINT(1) parameters that DataTypeKind alone cannot represent.
        dtd_identifier.to_string()
    } else if matches!(data_type, DataTypeKind::Year) {
        // INFORMATION_SCHEMA exposes YEAR's canonical data type, not the
        // deprecated source spelling (`YEAR(4)`) or parser whitespace/case.
        // SHOW CREATE ROUTINE remains the place that preserves source text.
        "year".to_string()
    } else {
        let _ = dtd_identifier;
        mysql_type_name_with_unsigned(data_type, unsigned)
    };
    vec![
        s("def"),
        s(&routine.database),
        s(&routine.name),
        Value::Int(ordinal as i64),
        mode.map(s).unwrap_or(Value::Null),
        name.map(s).unwrap_or(Value::Null),
        s(&mysql_data_type_name(data_type)),
        mysql_character_maximum_length(data_type),
        mysql_character_octet_length(data_type),
        mysql_numeric_precision(data_type),
        mysql_numeric_scale(data_type),
        mysql_datetime_precision(data_type),
        character_set_for_data_type(data_type),
        collation_for_data_type(data_type),
        s(&dtd),
        s(routine.kind.as_str()),
    ]
}

fn character_sets_rows() -> Vec<Vec<Value>> {
    crate::charset::character_sets_rows()
}

fn collations_rows() -> Vec<Vec<Value>> {
    crate::charset::collations_rows()
}

fn collation_applicability_rows() -> Vec<Vec<Value>> {
    crate::charset::collation_applicability_rows()
}

fn engines_rows() -> Vec<Vec<Value>> {
    crate::engine::show::storage_engine_rows()
}

fn processlist_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    let mut processes = registry.snapshot();
    crate::engine::perf_schema::scope_processlist_to_account(db, &mut processes);
    processes.sort_by_key(|p| p.id);
    processes
        .into_iter()
        .map(|p| {
            vec![
                Value::UInt(p.id as u64),
                s(&p.user),
                s(&p.host),
                p.database.map(Value::String).unwrap_or(Value::Null),
                s(&p.command),
                Value::Int(p.time_secs as i64),
                p.state.map(Value::String).unwrap_or(Value::Null),
                p.info.map(Value::String).unwrap_or(Value::Null),
            ]
        })
        .collect()
}

// performance_schema row producers now live in `crate::engine::perf_schema`.

/// Rows for one variable view at the requested scope. SESSION views read
/// from the active session — captured via the expression engine's
/// thread-local pointer so we don't need to thread `&SessionContext`
/// through the row-generator API.
pub(crate) fn system_variables_rows_scoped(
    db: &Database,
    scope: crate::engine::sysvar::Scope,
) -> Vec<Vec<Value>> {
    crate::engine::expr::with_active_session(|session| {
        crate::engine::sysvar::snapshot(scope, session, Some(db))
            .into_iter()
            .map(|row| vec![s(row.name), s(&row.value)])
            .collect()
    })
}

fn user_privileges_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_privilege_grantees(&auth);
    let mut rows: Vec<Vec<Value>> = visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter(|grant| {
            visible.contains(&grant.grantee)
                && matches!(grant.scope, PrivilegeScope::Global)
                && !matches!(grant.privilege, Privilege::GrantOption)
        })
        .map(|grant| {
            vec![
                s(&grant.grantee.display()),
                s("def"),
                s(&grant.privilege.sql_name()),
                yes_no(auth.metadata_grantable(grant)),
            ]
        })
        .collect();
    let static_grantees: std::collections::BTreeSet<_> = visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter(|grant| {
            matches!(grant.scope, PrivilegeScope::Global)
                && !matches!(
                    grant.privilege,
                    Privilege::Dynamic(_) | Privilege::GrantOption
                )
        })
        .map(|grant| &grant.grantee)
        .collect();
    for principal in &visible {
        if !static_grantees.contains(principal) {
            rows.push(vec![
                s(&principal.display()),
                s("def"),
                s("USAGE"),
                yes_no(auth.grants_for(principal).any(|g| {
                    matches!(g.scope, PrivilegeScope::Global)
                        && g.grant_option
                        && !matches!(g.privilege, Privilege::Dynamic(_))
                })),
            ]);
        }
    }
    rows
}

fn schema_privileges_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_privilege_grantees(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter_map(|grant| {
            if !visible.contains(&grant.grantee) {
                return None;
            }
            if matches!(grant.privilege, Privilege::GrantOption)
                && auth.grants_for(&grant.grantee).any(|g| {
                    g.scope == grant.scope && !matches!(g.privilege, Privilege::GrantOption)
                })
            {
                return None;
            }
            let PrivilegeScope::Database { database } = &grant.scope else {
                return None;
            };
            Some(vec![
                s(&grant.grantee.display()),
                s("def"),
                s(database),
                s(&if matches!(grant.privilege, Privilege::GrantOption) {
                    "USAGE".to_string()
                } else {
                    grant.privilege.sql_name()
                }),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn table_privileges_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_privilege_grantees(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter_map(|grant| {
            if !visible.contains(&grant.grantee) {
                return None;
            }
            if matches!(grant.privilege, Privilege::GrantOption)
                && auth.grants_for(&grant.grantee).any(|g| {
                    g.scope == grant.scope && !matches!(g.privilege, Privilege::GrantOption)
                })
            {
                return None;
            }
            let PrivilegeScope::Table { database, table } = &grant.scope else {
                return None;
            };
            Some(vec![
                s(&grant.grantee.display()),
                s("def"),
                s(database),
                s(table),
                s(&if matches!(grant.privilege, Privilege::GrantOption) {
                    "USAGE".to_string()
                } else {
                    grant.privilege.sql_name()
                }),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn column_privileges_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_privilege_grantees(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter_map(|grant| {
            if !visible.contains(&grant.grantee) {
                return None;
            }
            let PrivilegeScope::Column {
                database,
                table,
                column,
            } = &grant.scope
            else {
                return None;
            };
            Some(vec![
                s(&grant.grantee.display()),
                s("def"),
                s(database),
                s(table),
                s(column),
                s(&grant.privilege.sql_name()),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn current_privilege_grantees(auth: &AuthCatalog) -> std::collections::BTreeSet<AccountKey> {
    let Some((ctx, system)) = crate::engine::expr::current_authorization_context() else {
        return std::collections::BTreeSet::new();
    };
    // MySQL exposes all grantees to a reader of the mysql schema. A grant
    // on mysql.user alone does not broaden these I_S privilege projections.
    if system
        || auth.account_has_privilege(
            &ctx,
            &RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Database {
                    database: "mysql".into(),
                },
            },
            false,
        )
    {
        return auth
            .accounts
            .values()
            .map(|account| account.key.clone())
            .collect();
    }
    let mut principals = std::collections::BTreeSet::new();
    principals.extend(auth.effective_roles(&ctx));
    principals.insert(ctx.authenticated_as);
    principals
}

fn applicable_roles_rows(db: &Database, administrable_only: bool) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let Some((ctx, _)) = crate::engine::expr::current_authorization_context() else {
        return Vec::new();
    };
    let mandatory = auth.mandatory_role_keys();
    auth.applicable_roles(&ctx.authenticated_as)
        .into_iter()
        .filter_map(|role| {
            let admin = auth.account_can_administer_role(&ctx, &role);
            if administrable_only && !admin {
                return None;
            }
            let is_default = auth
                .default_roles
                .iter()
                .any(|edge| edge.to_account == ctx.authenticated_as && edge.from_role == role);
            vec![
                s(&ctx.authenticated_as.user),
                s(&ctx.authenticated_as.host),
                s(&ctx.authenticated_as.user),
                s(&ctx.authenticated_as.host),
                s(&role.user),
                s(&role.host),
                yes_no(admin),
                yes_no(is_default),
                yes_no(mandatory.contains(&role)),
            ]
            .into()
        })
        .collect()
}

fn enabled_roles_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let Some((ctx, _)) = crate::engine::expr::current_authorization_context() else {
        return Vec::new();
    };
    let mandatory = auth.mandatory_role_keys();
    auth.role_closure(&ctx.active_roles)
        .into_iter()
        .map(|role| {
            vec![
                s(&role.user),
                s(&role.host),
                yes_no(
                    auth.default_roles.iter().any(|edge| {
                        edge.to_account == ctx.authenticated_as && edge.from_role == role
                    }),
                ),
                yes_no(mandatory.contains(&role)),
            ]
        })
        .collect()
}

fn role_table_grants_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_visible_roles(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter_map(|grant| {
            if matches!(grant.privilege, Privilege::GrantOption) {
                return None;
            }
            let PrivilegeScope::Table { database, table } = &grant.scope else {
                return None;
            };
            if !visible.contains(&grant.grantee) {
                return None;
            }
            Some(vec![
                s(&grant.grantor.user),
                s(&grant.grantor.host),
                s(&grant.grantee.user),
                s(&grant.grantee.host),
                s("def"),
                s(database),
                s(table),
                s(&grant.privilege.sql_name()),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn role_column_grants_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_visible_roles(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter(|grant| !matches!(grant.privilege, Privilege::GrantOption))
        .filter_map(|grant| {
            let PrivilegeScope::Column {
                database,
                table,
                column,
            } = &grant.scope
            else {
                return None;
            };
            if !visible.contains(&grant.grantee) {
                return None;
            }
            Some(vec![
                s(&grant.grantor.user),
                s(&grant.grantor.host),
                s(&grant.grantee.user),
                s(&grant.grantee.host),
                s("def"),
                s(database),
                s(table),
                s(column),
                s(&grant.privilege.sql_name()),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn role_routine_grants_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let visible = current_visible_roles(&auth);
    visible
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter(|grant| !matches!(grant.privilege, Privilege::GrantOption))
        .filter_map(|grant| {
            let PrivilegeScope::Routine {
                database,
                routine,
                kind: _,
            } = &grant.scope
            else {
                return None;
            };
            if !visible.contains(&grant.grantee) {
                return None;
            }
            Some(vec![
                s(&grant.grantor.user),
                s(&grant.grantor.host),
                s(&grant.grantee.user),
                s(&grant.grantee.host),
                s("def"),
                s(database),
                s(routine),
                s("def"),
                s(database),
                s(routine),
                s(&grant.privilege.sql_name()),
                yes_no(auth.metadata_grantable(grant)),
            ])
        })
        .collect()
}

fn column_statistics_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for header in db.catalog().tables.values() {
        if !visibility.table_visible(&header.database, &header.name) {
            continue;
        }
        // Startup headers intentionally omit distributions. Resolve the bounded
        // descriptor before reading user-requested histogram metadata.
        let table = db.table(&header.qualified_name())?;
        for (column, histogram) in &table.stats.explicit_histograms {
            if !table
                .iter_visible_columns()
                .any(|(_, c)| c.name.eq_ignore_ascii_case(column))
            {
                continue;
            }
            if visibility
                .column_privileges(&table.database, &table.name, column)
                .is_empty()
            {
                continue;
            }
            rows.push(vec![
                s(&table.database),
                s(&table.name),
                s(column),
                Value::String(histogram.to_json()),
            ]);
        }
    }
    Ok(rows)
}

fn sequence_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    let visibility = Visibility::compute(db);
    db.list_sequences()?
        .iter()
        .filter(|def| {
            visibility.unrestricted
                || visibility.has_table_privilege(Privilege::Select, &def.database, &def.name)
                || visibility.has_table_privilege(Privilege::Update, &def.database, &def.name)
        })
        .map(|def| db.sequence_diagnostic_row(def))
        .collect()
}

fn mydb_stats_health_rows(db: &Database) -> Vec<Vec<Value>> {
    let ratio = crate::engine::auto_analyze::ratio();
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if table.system || crate::storage::naming::is_system_database(&table.database) {
            continue;
        }
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        let delta = db.table_modification_delta(table.id);
        let row_count = table.stats.row_count;
        let modified = table
            .stats
            .modified_rows_since_analyze
            .saturating_add(delta);
        let stale_ratio = if row_count == 0 {
            // No analyzed rows: any pending modification is "fully" stale,
            // none is 0.
            if modified > 0 {
                1.0
            } else {
                0.0
            }
        } else {
            modified as f64 / row_count as f64
        };
        let is_stale = table.stats_needs_auto_analyze(delta, ratio);
        let has_histogram = table
            .stats
            .columns
            .values()
            .any(|c| !c.histogram.is_empty())
            || !table.stats.explicit_histograms.is_empty();
        let last_analyzed = table
            .stats
            .analyzed_at_unix_ms
            .map(mysql_time_value)
            .unwrap_or(Value::Null);
        rows.push(vec![
            s(&table.database),
            s(&table.name),
            Value::Int(saturating_i64(row_count)),
            Value::Int(saturating_i64(modified)),
            Value::Double(stale_ratio),
            Value::Int(i64::from(is_stale)),
            last_analyzed,
            Value::Int(i64::from(has_histogram)),
            Value::Int(table.stats.columns.len() as i64),
        ]);
    }
    rows
}

fn schemata_extensions_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = db
        .catalog()
        .databases
        .values()
        .filter(|schema| visibility.schema_visible(&schema.name))
        .map(|schema| vec![s("def"), s(&schema.name), Value::Null])
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left[1]
            .to_display_string()
            .cmp(&right[1].to_display_string())
    });
    rows
}

fn tables_extensions_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let catalog = db.catalog();
    let mut rows = catalog
        .tables
        .values()
        .filter(|table| visibility.table_visible(&table.database, &table.name))
        .map(|table| {
            vec![
                s("def"),
                s(&table.database),
                s(&table.name),
                Value::Null,
                Value::Null,
            ]
        })
        .collect::<Vec<_>>();
    rows.extend(
        catalog
            .views
            .values()
            .filter(|view| visibility.table_visible(&view.database, &view.name))
            .map(|view| {
                vec![
                    s("def"),
                    s(&view.database),
                    s(&view.name),
                    Value::Null,
                    Value::Null,
                ]
            }),
    );
    rows
}

fn columns_extensions_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let catalog = db.catalog();
    let mut rows = Vec::new();
    for table in catalog.tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for (_, column) in table.iter_visible_columns() {
            if visibility
                .column_privileges(&table.database, &table.name, &column.name)
                .is_empty()
            {
                continue;
            }
            rows.push(vec![
                s("def"),
                s(&table.database),
                s(&table.name),
                s(&column.name),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    for view in catalog.views.values() {
        if !visibility.table_visible(&view.database, &view.name) {
            continue;
        }
        for column in &view.columns {
            if visibility
                .column_privileges(&view.database, &view.name, &column.name)
                .is_empty()
            {
                continue;
            }
            rows.push(vec![
                s("def"),
                s(&view.database),
                s(&view.name),
                s(&column.name),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    rows
}

fn table_constraints_extensions_rows(db: &Database) -> Vec<Vec<Value>> {
    table_constraints_rows(db)
        .into_iter()
        .filter(|row| row.len() >= 5)
        .map(|row| {
            vec![
                row[0].clone(),
                row[1].clone(),
                row[2].clone(),
                row[4].clone(),
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

fn user_attributes_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let Some((ctx, system)) = crate::engine::expr::current_authorization_context() else {
        return Vec::new();
    };
    let mut rows = auth
        .accounts
        .values()
        .filter(|account| system || account.key == ctx.authenticated_as)
        .map(|account| {
            vec![
                s(&account.key.user),
                s(&account.key.host),
                account
                    .user_attributes_json
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (left[0].to_display_string(), left[1].to_display_string())
            .cmp(&(right[0].to_display_string(), right[1].to_display_string()))
    });
    rows
}

fn keywords_rows() -> Vec<Vec<Value>> {
    crate::engine::information_schema_mysql80_keywords::KEYWORDS
        .iter()
        .map(|(word, reserved)| vec![s(word), Value::Int(i64::from(*reserved))])
        .collect()
}

fn st_spatial_reference_systems_rows() -> Result<Vec<Vec<Value>>, QueryError> {
    Ok(crate::engine::geo::srs::all()?
        .iter()
        .map(|definition| {
            vec![
                s(&definition.name),
                Value::Int(definition.id as i64),
                definition
                    .organization
                    .as_deref()
                    .map(s)
                    .unwrap_or(Value::Null),
                definition
                    .organization_coordsys_id
                    .map(|id| Value::Int(id as i64))
                    .unwrap_or(Value::Null),
                s(&definition.definition),
                definition
                    .description
                    .as_deref()
                    .map(s)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect())
}

fn st_geometry_columns_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for (_, column) in table.iter_visible_columns() {
            if visibility
                .column_privileges(&table.database, &table.name, &column.name)
                .is_empty()
            {
                continue;
            }
            if let DataTypeKind::Geometry { subtype, srid } = &column.data_type {
                let srid = *srid;
                let srs_id = srid.map(|v| v as i64);
                let srs_name = match srid {
                    Some(srid) => crate::engine::geo::srs::find(srid)?
                        .map(|definition| s(&definition.name))
                        .unwrap_or(Value::Null),
                    None => Value::Null,
                };
                rows.push(vec![
                    s("def"),
                    s(&table.database),
                    s(&table.name),
                    s(&column.name),
                    srs_name,
                    srs_id.map(Value::Int).unwrap_or(Value::Null),
                    s(subtype.type_name()),
                ]);
            }
        }
    }
    Ok(rows)
}

fn st_units_of_measure_rows() -> Vec<Vec<Value>> {
    crate::engine::geo::srs::units()
        .iter()
        .map(|unit| {
            vec![
                s(unit.name),
                s(unit.unit_type),
                Value::Double(unit.conversion_factor),
                unit.description.map(s).unwrap_or(Value::Null),
            ]
        })
        .collect()
}

fn innodb_trx_rows(db: &Database, registry: &SessionRegistry) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let processes = registry.snapshot();
    let process_by_txn: std::collections::HashMap<
        u64,
        &crate::engine::session_registry::ProcessInfo,
    > = processes
        .iter()
        .filter_map(|process| process.current_txn.map(|txn| (txn, process)))
        .collect();

    let mut lock_structs: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
    let mut row_locks: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
    let mut table_locks: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
    for lock in db.data_lock_snapshot() {
        *lock_structs.entry(lock.transaction_id).or_default() += 1;
        if lock.lock_type == "RECORD" && lock.lock_status == "GRANTED" {
            *row_locks.entry(lock.transaction_id).or_default() += 1;
        }
        if lock.lock_type == "TABLE" && lock.lock_status == "GRANTED" {
            *table_locks.entry(lock.transaction_id).or_default() += 1;
        }
    }

    let mut rows: Vec<Vec<Value>> = db
        .active_transaction_snapshot()
        .into_iter()
        .map(|trx| {
            let process = process_by_txn.get(&trx.transaction_id);
            let structs = *lock_structs.get(&trx.transaction_id).unwrap_or(&0);
            let row_count = *row_locks.get(&trx.transaction_id).unwrap_or(&0);
            let table_count = *table_locks.get(&trx.transaction_id).unwrap_or(&0);
            let weight = structs + i64::from(trx.is_writer || trx.has_undo);
            let isolation = process.map(|p| p.isolation.replace('-', " "));
            vec![
                s(&trx.transaction_id.to_string()),
                s(trx.state),
                mysql_time_value(trx.started_unix_ms.max(1)),
                trx.requested_lock_id
                    .map(Value::String)
                    .unwrap_or(Value::Null),
                trx.wait_started_unix_ms
                    .map(mysql_time_value)
                    .unwrap_or(Value::Null),
                Value::Int(weight),
                process
                    .map(|p| Value::Int(i64::from(p.id)))
                    .unwrap_or(Value::Int(0)),
                process
                    .and_then(|p| p.info.clone())
                    .map(Value::String)
                    .unwrap_or(Value::Null),
                Value::Null,                             // trx_operation_state
                Value::Int(table_count),                 // trx_tables_in_use
                Value::Int(table_count),                 // trx_tables_locked
                Value::Int(structs),                     // trx_lock_structs
                Value::Int(structs.saturating_mul(128)), // trx_lock_memory_bytes
                Value::Int(row_count),                   // trx_rows_locked
                Value::Int(i64::from(trx.has_undo)),     // trx_rows_modified lower bound
                Value::Int(0),                           // trx_concurrency_tickets
                isolation.map(Value::String).unwrap_or(Value::Null), // trx_isolation_level
                Value::Int(1),                           // trx_unique_checks
                Value::Int(1),                           // trx_foreign_key_checks
                Value::Null,                             // trx_last_foreign_key_error
                Value::Int(0),                           // trx_adaptive_hash_latched
                Value::Int(0),                           // trx_adaptive_hash_timeout
                Value::Int(i64::from(trx.read_only)),    // trx_is_read_only
                Value::Int(0),                           // trx_autocommit_non_locking
                Value::Null,                             // trx_schedule_weight
            ]
        })
        .collect();
    for process in &processes {
        if process.current_txn.is_some() || process.lazy_trx_started_ms == 0 {
            continue;
        }
        rows.push(vec![
            s("0"), // No storage transaction ID has been allocated.
            s("RUNNING"),
            mysql_time_value(process.lazy_trx_started_ms),
            Value::Null,   // trx_requested_lock_id
            Value::Null,   // trx_wait_started
            Value::Int(0), // trx_weight
            Value::Int(i64::from(process.id)),
            process
                .info
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            Value::Null,   // trx_operation_state
            Value::Int(0), // trx_tables_in_use
            Value::Int(0), // trx_tables_locked
            Value::Int(0), // trx_lock_structs
            Value::Int(0), // trx_lock_memory_bytes
            Value::Int(0), // trx_rows_locked
            Value::Int(0), // trx_rows_modified
            Value::Int(0), // trx_concurrency_tickets
            s(&process.isolation.replace('-', " ")),
            Value::Int(1), // trx_unique_checks
            Value::Int(1), // trx_foreign_key_checks
            Value::Null,   // trx_last_foreign_key_error
            Value::Int(0), // trx_adaptive_hash_latched
            Value::Int(0), // trx_adaptive_hash_timeout
            Value::Int(1), // trx_is_read_only (no write or locking read yet)
            Value::Int(0), // trx_autocommit_non_locking
            Value::Null,   // trx_schedule_weight
        ]);
    }
    rows
}

fn innodb_lock_waits_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    db.data_lock_wait_snapshot()
        .into_iter()
        .map(|wait| {
            vec![
                s(&wait.requesting_transaction_id.to_string()),
                s(&wait.requesting_lock_id),
                s(&wait.blocking_transaction_id.to_string()),
                s(&wait.blocking_lock_id),
            ]
        })
        .collect()
}

fn can_view_process_metadata(db: &Database) -> bool {
    let Some((ctx, system)) = crate::engine::expr::current_authorization_context() else {
        return false;
    };
    system
        || db.auth_catalog().account_has_privilege(
            &ctx,
            &RequiredPrivilege {
                privilege: Privilege::Process,
                scope: PrivilegeScope::Global,
            },
            false,
        )
}

fn can_view_dynamic_metadata(db: &Database, privilege_name: &str) -> bool {
    let Some((ctx, system)) = crate::engine::expr::current_authorization_context() else {
        return false;
    };
    system
        || db.auth_catalog().account_has_privilege(
            &ctx,
            &RequiredPrivilege {
                privilege: Privilege::Dynamic(privilege_name.to_ascii_uppercase()),
                scope: PrivilegeScope::Global,
            },
            false,
        )
}

// ---------------------------------------------------------------------------
// InnoDB data-dictionary views (system-metadata plan Phase 2.3–2.7)
//
// mydb has no InnoDB engine; these views map mydb's own physical objects onto
// the InnoDB column shapes ops tools expect, per the plan §5 / E2 convention:
//   - each user table's heap = one file-per-table tablespace, SPACE id = table id
//   - the table's B-tree/FULLTEXT/SPATIAL/HNSW index files = that table's indexes
//   - sizes come from mydb's own page accounting (`TableStatsView`)
// Values with no mydb equivalent (InnoDB internal type codes, root page nos)
// are mapped best-effort or left NULL rather than fabricated.
// ---------------------------------------------------------------------------

/// InnoDB spells a table `db/table` in its dictionary NAME columns.
fn innodb_qualified_name(table: &TableSchema) -> String {
    format!("{}/{}", table.database, table.name)
}

/// The declared ROW_FORMAT (from table options), defaulting to the mydb heap's
/// effective `DYNAMIC` layout — the same string `SHOW TABLE STATUS` reports.
fn innodb_row_format(table: &TableSchema) -> String {
    table
        .mysql_table_options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("ROW_FORMAT"))
        .map(|(_, v)| {
            let mut c = v.chars();
            match c.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &c.as_str().to_lowercase()
                }
                None => "Dynamic".to_string(),
            }
        })
        .unwrap_or_else(|| "Dynamic".to_string())
}

/// A stable synthetic INDEX_ID for `(table, index-ordinal)`. mydb has no
/// per-index numeric id, but `innodb_indexes` and `innodb_fields` must join on
/// it, so we derive it deterministically (MySQL caps indexes at 64/table).
fn synth_index_id(table_id: u64, ordinal: usize) -> i64 {
    saturating_i64(table_id.wrapping_mul(128).wrapping_add(ordinal as u64))
}

fn synth_unmapped_index_id(storage_id: u64, name: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.to_ascii_lowercase().hash(&mut hasher);
    synth_index_id(storage_id, (hasher.finish() as usize) & 127)
}

/// InnoDB index TYPE bitmask: 1=clustered, 2=unique, 32=fulltext, 64=spatial.
fn innodb_index_type_flag(index: &IndexSchema) -> i64 {
    match index.index_type.to_ascii_uppercase().as_str() {
        "FULLTEXT" => 32,
        "SPATIAL" => 64,
        _ => {
            let mut flag = 0;
            if index.primary {
                flag |= 3; // clustered + unique
            } else if index.unique {
                flag |= 2;
            }
            flag
        }
    }
}

/// True for a VIRTUAL generated column (goes into `innodb_virtual`, not the
/// clustered index).
fn is_virtual_column(col: &ColumnSchema) -> bool {
    col.generated_expr.is_some() && !col.generated_stored
}

#[derive(Clone, Debug)]
struct InnodbPhysicalObject {
    storage_id: u64,
    name: String,
    leaf_index: usize,
    leaf_count: usize,
}

fn innodb_physical_objects(table: &TableSchema) -> Vec<InnodbPhysicalObject> {
    let Some(partitioning) = table.partitioning.as_ref() else {
        return vec![InnodbPhysicalObject {
            storage_id: table.id,
            name: innodb_qualified_name(table),
            leaf_index: 0,
            leaf_count: 1,
        }];
    };
    let leaf_count = partitioning
        .partitions
        .iter()
        .map(|partition| partition.subpartitions.len().max(1))
        .sum::<usize>();
    let mut objects = Vec::with_capacity(leaf_count);
    for partition in &partitioning.partitions {
        if partition.subpartitions.is_empty() {
            objects.push(InnodbPhysicalObject {
                storage_id: partition.storage_id,
                name: format!("{}/{}#p#{}", table.database, table.name, partition.name),
                leaf_index: objects.len(),
                leaf_count,
            });
        } else {
            for subpartition in &partition.subpartitions {
                objects.push(InnodbPhysicalObject {
                    storage_id: subpartition.storage_id,
                    name: format!(
                        "{}/{}#p#{}#sp#{}",
                        table.database, table.name, partition.name, subpartition.name
                    ),
                    leaf_index: objects.len(),
                    leaf_count,
                });
            }
        }
    }
    objects
}

fn innodb_physical_stats(
    db: &Database,
    table: &TableSchema,
    object: &InnodbPhysicalObject,
) -> crate::engine::table_stats_view::TableStatsView {
    if table.partitioning.is_some() {
        crate::engine::table_stats_view::TableStatsView::for_partition(
            db,
            table,
            object.leaf_count,
            object.leaf_index,
            object.storage_id,
        )
    } else {
        crate::engine::table_stats_view::TableStatsView::for_table(db, table)
    }
}

fn innodb_tables_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let catalog = db.catalog();
    let mut rows = Vec::new();
    for table in catalog
        .tables
        .values()
        .filter(|table| visibility.table_visible(&table.database, &table.name))
    {
        let n_cols = table.iter_visible_columns().count() as i64;
        for object in innodb_physical_objects(table) {
            rows.push(vec![
                Value::Int(saturating_i64(object.storage_id)),
                s(&object.name),
                Value::Int(0),
                Value::Int(n_cols),
                Value::Int(saturating_i64(object.storage_id)),
                s(&innodb_row_format(table)),
                Value::Int(0),
                s("Single"),
                Value::Int(0),
                Value::Int(0),
            ]);
        }
    }
    rows
}

fn innodb_columns_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for object in innodb_physical_objects(table) {
            for (pos, (_, col)) in table.iter_visible_columns().enumerate() {
                let has_default = col.default.is_some() || col.default_expr.is_some();
                rows.push(vec![
                    Value::Int(saturating_i64(object.storage_id)),
                    s(&col.name),
                    Value::Int(pos as i64),
                    Value::Int(innodb_column_mtype(&col.data_type)),
                    Value::Int(innodb_column_prtype(col)),
                    Value::Int(innodb_column_len(col)),
                    Value::Int(has_default as i64),
                    if has_default {
                        column_default_value(col)
                    } else {
                        Value::Null
                    },
                ]);
            }
        }
    }
    rows
}

fn innodb_column_mtype(data_type: &DataTypeKind) -> i64 {
    match data_type {
        DataTypeKind::VarChar { .. } => 12,
        DataTypeKind::Enum { .. } | DataTypeKind::Set { .. } => 6,
        DataTypeKind::Char { .. } => 13,
        DataTypeKind::Binary { .. } => 3,
        DataTypeKind::VarBinary { .. } => 4,
        DataTypeKind::Text { .. } | DataTypeKind::Blob { .. } | DataTypeKind::Json => 5,
        DataTypeKind::TinyInt
        | DataTypeKind::SmallInt
        | DataTypeKind::MediumInt
        | DataTypeKind::Int
        | DataTypeKind::BigInt
        | DataTypeKind::Bool
        | DataTypeKind::Year
        | DataTypeKind::Bit { .. }
        | DataTypeKind::Date
        | DataTypeKind::Time { .. }
        | DataTypeKind::DateTime { .. }
        | DataTypeKind::Timestamp { .. } => 6,
        DataTypeKind::Float { .. } => 9,
        DataTypeKind::Double { .. } => 10,
        DataTypeKind::Decimal { .. } => 11,
        DataTypeKind::Geometry { .. } | DataTypeKind::Vector { .. } => 14,
    }
}

fn innodb_column_prtype(column: &ColumnSchema) -> i64 {
    const DATA_NOT_NULL: i64 = 256;
    const DATA_UNSIGNED: i64 = 512;
    const DATA_BINARY_TYPE: i64 = 1024;
    let mut flags = 0;
    if !column.nullable {
        flags |= DATA_NOT_NULL;
    }
    if column.unsigned {
        flags |= DATA_UNSIGNED;
    }
    if matches!(
        column.data_type,
        DataTypeKind::Binary { .. } | DataTypeKind::VarBinary { .. } | DataTypeKind::Blob { .. }
    ) {
        flags |= DATA_BINARY_TYPE;
    }
    flags
}

fn innodb_column_len(column: &ColumnSchema) -> i64 {
    match mysql_character_octet_length_for_column(column) {
        Value::Int(length) => length,
        _ => column.data_type.key_byte_length().min(i64::MAX as usize) as i64,
    }
}

fn innodb_virtual_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        let name_to_pos: std::collections::HashMap<String, usize> = table
            .iter_visible_columns()
            .enumerate()
            .map(|(pos, (_, col))| (col.name.to_ascii_lowercase(), pos))
            .collect();
        for object in innodb_physical_objects(table) {
            for (pos, (_, col)) in table.iter_visible_columns().enumerate() {
                if !is_virtual_column(col) {
                    continue;
                }
                for base in &col.generated_column_names {
                    if let Some(base_pos) = name_to_pos.get(&base.to_ascii_lowercase()) {
                        rows.push(vec![
                            Value::Int(saturating_i64(object.storage_id)),
                            Value::Int(pos as i64),
                            Value::Int(*base_pos as i64),
                        ]);
                    }
                }
            }
        }
    }
    rows
}

fn innodb_indexes_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for object in innodb_physical_objects(table) {
            for (ordinal, index) in table.iter_public_indexes().enumerate() {
                rows.push(vec![
                    Value::Int(synth_index_id(object.storage_id, ordinal)),
                    s(&index.name),
                    Value::Int(saturating_i64(object.storage_id)),
                    Value::Int(innodb_index_type_flag(index)),
                    Value::Int(index.columns.len() as i64),
                    Value::Int(0),
                    Value::Int(saturating_i64(object.storage_id)),
                    Value::Int(50),
                ]);
            }
        }
    }
    rows
}

fn innodb_fields_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for object in innodb_physical_objects(table) {
            for (ordinal, index) in table.iter_public_indexes().enumerate() {
                let index_id = synth_index_id(object.storage_id, ordinal);
                for (pos, col) in index.columns.iter().enumerate() {
                    rows.push(vec![Value::Int(index_id), s(col), Value::Int(pos as i64)]);
                }
            }
        }
    }
    rows
}

fn innodb_foreign_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for fk in &table.foreign_keys {
            rows.push(vec![
                s(&format!("{}/{}", table.database, fk.name)),
                s(&innodb_qualified_name(table)),
                s(&format!(
                    "{}/{}",
                    fk.referenced_database, fk.referenced_table
                )),
                Value::Int(fk.columns.len() as i64),
                Value::Int(innodb_fk_type_flag(fk)),
            ]);
        }
    }
    rows
}

/// InnoDB `innodb_foreign.TYPE` bitmask for the referential actions.
fn innodb_fk_type_flag(fk: &crate::storage::schema::ForeignKeySchema) -> i64 {
    use crate::storage::schema::ReferentialAction::*;
    let mut flag = 0;
    match fk.on_delete {
        Cascade => flag |= 1,
        SetNull => flag |= 2,
        _ => {}
    }
    match fk.on_update {
        Cascade => flag |= 4,
        SetNull => flag |= 8,
        _ => {}
    }
    flag
}

fn innodb_foreign_cols_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for fk in &table.foreign_keys {
            let id = format!("{}/{}", table.database, fk.name);
            for (pos, (child, parent)) in fk
                .columns
                .iter()
                .zip(fk.referenced_columns.iter())
                .enumerate()
            {
                rows.push(vec![s(&id), s(child), s(parent), Value::Int(pos as i64)]);
            }
        }
    }
    rows
}

/// Physical byte size of a table's heap (data) and index files, from mydb's own
/// page accounting. Falls back to the width-based `TableStatsView` estimate when
/// pages are not yet resident (e.g. memory durability).
fn innodb_table_sizes(db: &Database, table: &TableSchema) -> (u64, u64) {
    let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
    (stats.data_length, stats.index_length)
}

/// C3: physical byte size of one partition/subpartition (heap + index),
/// using the storage_id-scoped stats view.
fn innodb_partition_sizes(
    db: &Database,
    table: &TableSchema,
    partition_count: usize,
    partition_index: usize,
    storage_id: u64,
) -> (u64, u64) {
    let stats = crate::engine::table_stats_view::TableStatsView::for_partition(
        db,
        table,
        partition_count,
        partition_index,
        storage_id,
    );
    (stats.data_length, stats.index_length)
}

fn innodb_tablespaces_rows(db: &Database) -> Vec<Vec<Value>> {
    let page_size = crate::storage::PAGE_SIZE as i64;
    let visibility = Visibility::compute(db);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        // C3: partitioned tables have no heap at `table.id` (see
        // `naming::table_object_path`); MySQL 8.0 emits one row per
        // partition here so mysqldump / tooling can enumerate the real
        // physical spaces. Non-partitioned tables continue to emit a
        // single row keyed on `table.id`.
        if let Some(part) = table.partitioning.as_ref() {
            let n = part.partitions.len();
            for (idx, partition) in part.partitions.iter().enumerate() {
                for (sub_idx, sub) in partition.subpartitions.iter().enumerate() {
                    let (data_len, index_len) = innodb_partition_sizes(
                        db,
                        table,
                        part.partitions
                            .iter()
                            .map(|p| p.subpartitions.len())
                            .sum::<usize>(),
                        part.partitions
                            .iter()
                            .take(idx)
                            .map(|p| p.subpartitions.len())
                            .sum::<usize>()
                            + sub_idx,
                        sub.storage_id,
                    );
                    let file_size = saturating_i64(data_len + index_len);
                    rows.push(vec![
                        Value::Int(saturating_i64(sub.storage_id)),
                        s(&format!(
                            "{}/{}#p#{}#sp#{}",
                            table.database, table.name, partition.name, sub.name
                        )),
                        Value::Int(0),
                        s(&innodb_row_format(table)),
                        Value::Int(page_size),
                        Value::Int(0),
                        s("Single"),
                        Value::Int(page_size),
                        Value::Int(file_size),
                        Value::Int(file_size),
                        Value::Int(0),
                        s(crate::engine::sysvar::builtin::server::MYDB_VERSION_STRING),
                        Value::Int(1),
                        s("N"),
                        s("normal"),
                    ]);
                }
                if partition.subpartitions.is_empty() {
                    let (data_len, index_len) =
                        innodb_partition_sizes(db, table, n, idx, partition.storage_id);
                    let file_size = saturating_i64(data_len + index_len);
                    rows.push(vec![
                        Value::Int(saturating_i64(partition.storage_id)),
                        s(&format!(
                            "{}/{}#p#{}",
                            table.database, table.name, partition.name
                        )),
                        Value::Int(0),
                        s(&innodb_row_format(table)),
                        Value::Int(page_size),
                        Value::Int(0),
                        s("Single"),
                        Value::Int(page_size),
                        Value::Int(file_size),
                        Value::Int(file_size),
                        Value::Int(0),
                        s(crate::engine::sysvar::builtin::server::MYDB_VERSION_STRING),
                        Value::Int(1),
                        s("N"),
                        s("normal"),
                    ]);
                }
            }
            continue;
        }
        let (data_len, index_len) = innodb_table_sizes(db, table);
        let file_size = saturating_i64(data_len + index_len);
        rows.push(vec![
            Value::Int(saturating_i64(table.id)),
            s(&innodb_qualified_name(table)),
            Value::Int(0),
            s(&innodb_row_format(table)),
            Value::Int(page_size),
            Value::Int(0), // ZIP_PAGE_SIZE
            s("Single"),
            Value::Int(page_size),
            Value::Int(file_size),
            Value::Int(file_size),
            Value::Int(0), // AUTOEXTEND_SIZE
            s(crate::engine::sysvar::builtin::server::MYDB_VERSION_STRING),
            Value::Int(1),
            s("N"),      // ENCRYPTION
            s("normal"), // STATE
        ]);
    }

    rows
}

fn innodb_tablespaces_brief_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        // C3: partitioned tables have no physical file at `table.id` — one
        // row per physical partition/subpartition storage_id.
        if let Some(part) = table.partitioning.as_ref() {
            for partition in &part.partitions {
                for sub in &partition.subpartitions {
                    let path = crate::storage::naming::table_object_generation_path(
                        db.data_dir(),
                        sub.storage_id,
                        table.active_generation.max(1),
                        "heap",
                    );
                    rows.push(vec![
                        Value::Int(saturating_i64(sub.storage_id)),
                        s(&format!(
                            "{}/{}#p#{}#sp#{}",
                            table.database, table.name, partition.name, sub.name
                        )),
                        s(&path.to_string_lossy()),
                        Value::Null,
                        s("Single"),
                    ]);
                }
                if partition.subpartitions.is_empty() {
                    let path = crate::storage::naming::table_object_generation_path(
                        db.data_dir(),
                        partition.storage_id,
                        table.active_generation.max(1),
                        "heap",
                    );
                    rows.push(vec![
                        Value::Int(saturating_i64(partition.storage_id)),
                        s(&format!(
                            "{}/{}#p#{}",
                            table.database, table.name, partition.name
                        )),
                        s(&path.to_string_lossy()),
                        Value::Null,
                        s("Single"),
                    ]);
                }
            }
            continue;
        }
        let path = crate::storage::naming::table_object_generation_path(
            db.data_dir(),
            table.id,
            table.active_generation.max(1),
            "heap",
        );
        rows.push(vec![
            Value::Int(saturating_i64(table.id)),
            s(&innodb_qualified_name(table)),
            s(&path.to_string_lossy()),
            Value::Null,
            s("Single"),
        ]);
    }
    rows
}

fn innodb_datafiles_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        if let Some(part) = table.partitioning.as_ref() {
            // C3: one row per physical partition file for partitioned tables.
            for partition in &part.partitions {
                for sub in &partition.subpartitions {
                    let path = crate::storage::naming::table_object_generation_path(
                        db.data_dir(),
                        sub.storage_id,
                        table.active_generation.max(1),
                        "heap",
                    );
                    rows.push(vec![
                        s(&sub.storage_id.to_string()),
                        s(&path.to_string_lossy()),
                    ]);
                }
                if partition.subpartitions.is_empty() {
                    let path = crate::storage::naming::table_object_generation_path(
                        db.data_dir(),
                        partition.storage_id,
                        table.active_generation.max(1),
                        "heap",
                    );
                    rows.push(vec![
                        s(&partition.storage_id.to_string()),
                        s(&path.to_string_lossy()),
                    ]);
                }
            }
            continue;
        }
        let path = crate::storage::naming::table_object_generation_path(
            db.data_dir(),
            table.id,
            table.active_generation.max(1),
            "heap",
        );
        rows.push(vec![s(&table.id.to_string()), s(&path.to_string_lossy())]);
    }

    rows
}

fn innodb_tablestats_rows(db: &Database) -> Vec<Vec<Value>> {
    let page_size = crate::storage::PAGE_SIZE as u64;
    let visibility = Visibility::compute(db);
    let catalog = db.catalog();
    let mut rows = Vec::new();
    for table in catalog
        .tables
        .values()
        .filter(|table| visibility.table_visible(&table.database, &table.name))
    {
        for object in innodb_physical_objects(table) {
            let stats = innodb_physical_stats(db, table, &object);
            let clust_pages = saturating_i64(stats.data_length / page_size);
            let other_pages = saturating_i64(stats.index_length / page_size);
            let autoinc = stats.auto_increment.map(saturating_i64).unwrap_or(0);
            rows.push(vec![
                Value::Int(saturating_i64(object.storage_id)),
                s(&object.name),
                s("Initialized"),
                Value::Int(saturating_i64(stats.rows)),
                Value::Int(clust_pages),
                Value::Int(other_pages),
                Value::Int(saturating_i64(
                    table
                        .stats
                        .modified_rows_since_analyze
                        .saturating_add(db.table_modification_delta(table.id)),
                )),
                Value::Int(autoinc),
                Value::Int(0),
            ]);
        }
    }
    rows
}

fn files_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let catalog = db.catalog();
    let mut rows = Vec::new();
    for table in catalog
        .tables
        .values()
        .filter(|table| visibility.table_visible(&table.database, &table.name))
    {
        let Some(partitioning) = table.partitioning.as_ref() else {
            let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
            rows.push(files_row(
                db,
                table,
                table.id,
                &innodb_qualified_name(table),
                &stats,
            ));
            continue;
        };
        let leaf_count = partitioning
            .partitions
            .iter()
            .map(|partition| partition.subpartitions.len().max(1))
            .sum::<usize>();
        let mut leaf_index = 0usize;
        for partition in &partitioning.partitions {
            if partition.subpartitions.is_empty() {
                let stats = crate::engine::table_stats_view::TableStatsView::for_partition(
                    db,
                    table,
                    leaf_count,
                    leaf_index,
                    partition.storage_id,
                );
                rows.push(files_row(
                    db,
                    table,
                    partition.storage_id,
                    &format!("{}/{}#p#{}", table.database, table.name, partition.name),
                    &stats,
                ));
                leaf_index += 1;
            } else {
                for subpartition in &partition.subpartitions {
                    let stats = crate::engine::table_stats_view::TableStatsView::for_partition(
                        db,
                        table,
                        leaf_count,
                        leaf_index,
                        subpartition.storage_id,
                    );
                    rows.push(files_row(
                        db,
                        table,
                        subpartition.storage_id,
                        &format!(
                            "{}/{}#p#{}#sp#{}",
                            table.database, table.name, partition.name, subpartition.name
                        ),
                        &stats,
                    ));
                    leaf_index += 1;
                }
            }
        }
    }
    rows
}

fn files_row(
    db: &Database,
    table: &TableSchema,
    storage_id: u64,
    tablespace_name: &str,
    stats: &crate::engine::table_stats_view::TableStatsView,
) -> Vec<Value> {
    let path = crate::storage::naming::table_object_generation_path(
        db.data_dir(),
        storage_id,
        table.active_generation.max(1),
        "heap",
    );
    vec![
        Value::Int(saturating_i64(storage_id)),
        s(&path.to_string_lossy()),
        s("TABLESPACE"),
        s(tablespace_name),
        s("def"),
        s(&table.database),
        s(&table.name),
        Value::Null,
        Value::Null,
        s(&table.engine),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        s(&innodb_row_format(table)),
        Value::Int(saturating_i64(stats.rows)),
        Value::Int(saturating_i64(stats.avg_row_length)),
        Value::Int(saturating_i64(stats.data_length)),
        Value::Int(saturating_i64(stats.max_data_length)),
        Value::Int(saturating_i64(stats.index_length)),
        Value::Int(saturating_i64(stats.data_free)),
        mysql_time_value(table.created_ms),
        mysql_time_value(table.updated_ms),
        Value::Null,
        Value::Null,
        s("NORMAL"),
        Value::Null,
    ]
}

fn innodb_buffer_pool_stats_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let st = db.storage_status();
    let number_get = st.buffer_hits + st.buffer_misses;
    vec![vec![
        Value::Int(0), // POOL_ID
        Value::Int(saturating_i64(st.buffer_pool_pages)),
        Value::Int(saturating_i64(
            st.buffer_pool_pages
                .saturating_sub(st.buffer_pool_used_pages),
        )),
        Value::Int(saturating_i64(st.buffer_pool_used_pages)),
        Value::Int(0), // OLD_DATABASE_PAGES: mydb has no old sublist
        Value::Int(saturating_i64(st.buffer_pool_dirty_pages)),
        Value::Int(0),     // PENDING_DECOMPRESS
        Value::Int(0),     // PENDING_READS
        Value::Int(0),     // PENDING_FLUSH_LRU
        Value::Int(0),     // PENDING_FLUSH_LIST
        Value::Int(0),     // PAGES_MADE_YOUNG
        Value::Int(0),     // PAGES_NOT_MADE_YOUNG
        Value::Float(0.0), // PAGES_MADE_YOUNG_RATE
        Value::Float(0.0), // PAGES_MADE_NOT_YOUNG_RATE
        Value::Int(saturating_i64(st.buffer_pool_read_ios)),
        Value::Int(0), // NUMBER_PAGES_CREATED
        Value::Int(saturating_i64(st.buffer_pool_write_ios)),
        Value::Float(0.0), // PAGES_READ_RATE
        Value::Float(0.0), // PAGES_CREATE_RATE
        Value::Float(0.0), // PAGES_WRITTEN_RATE
        Value::Int(saturating_i64(number_get)),
        Value::Int(saturating_i64(st.buffer_hit_pct * 10)), // HIT_RATE (per 1000)
        Value::Int(0),                                      // YOUNG_MAKE_PER_THOUSAND_GETS
        Value::Int(0),                                      // NOT_YOUNG_MAKE_PER_THOUSAND_GETS
        Value::Int(saturating_i64(st.buffer_pool_prefetches)),
        Value::Int(0),     // NUMBER_READ_AHEAD_EVICTED
        Value::Float(0.0), // READ_AHEAD_RATE
        Value::Float(0.0), // READ_AHEAD_EVICTED_RATE
        Value::Int(saturating_i64(st.buffer_pool_flushes)),
        Value::Int(0), // LRU_IO_CURRENT
        Value::Int(0), // UNCOMPRESS_TOTAL
        Value::Int(0), // UNCOMPRESS_CURRENT
    ]]
}

fn buffer_page_table_names(db: &Database) -> std::collections::HashMap<u64, String> {
    let mut names = std::collections::HashMap::new();
    for table in db.catalog().tables.values() {
        for object in innodb_physical_objects(table) {
            let name = object
                .name
                .split_once('/')
                .map(|(schema, table)| {
                    format!(
                        "`{}`.`{}`",
                        schema.replace('`', "``"),
                        table.replace('`', "``")
                    )
                })
                .unwrap_or(object.name);
            names.insert(object.storage_id, name);
        }
    }
    names
}

fn innodb_buffer_page_rows(db: &Database, lru: bool) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let table_names = buffer_page_table_names(db);
    db.buffer_page_diagnostics()
        .into_iter()
        .map(|page| {
            let table_name = table_names
                .get(&page.storage_id)
                .map(|name| s(name))
                .unwrap_or(Value::Null);
            let index_name = page.index_name.as_deref().map(s).unwrap_or(Value::Null);
            if lru {
                vec![
                    Value::Int(0),
                    Value::Int(saturating_i64(page.replacement_position)),
                    Value::Int(saturating_i64(page.storage_id)),
                    Value::Int(saturating_i64(page.page_number)),
                    s(page.page_type),
                    Value::Int(0),
                    Value::Int(saturating_i64(page.fix_count)),
                    s("NO"),
                    Value::Int(saturating_i64(page.newest_modification)),
                    Value::Int(saturating_i64(page.oldest_modification)),
                    Value::Int(saturating_i64(page.access_rank)),
                    table_name,
                    index_name,
                    Value::Int(0),
                    Value::Int(crate::storage::PAGE_SIZE as i64),
                    Value::Int(0),
                    s("NO"),
                    s(match page.io_fix {
                        "READ" => "IO_READ",
                        "WRITE" => "IO_WRITE",
                        _ => "IO_NONE",
                    }),
                    yes_no(page.is_old),
                    Value::Int(saturating_i64(page.replacement_position)),
                ]
            } else {
                vec![
                    Value::Int(0),
                    Value::Int(saturating_i64(page.block_id)),
                    Value::Int(saturating_i64(page.storage_id)),
                    Value::Int(saturating_i64(page.page_number)),
                    s(page.page_type),
                    Value::Int(0),
                    Value::Int(saturating_i64(page.fix_count)),
                    s("NO"),
                    Value::Int(saturating_i64(page.newest_modification)),
                    Value::Int(saturating_i64(page.oldest_modification)),
                    Value::Int(saturating_i64(page.access_rank)),
                    table_name,
                    index_name,
                    Value::Int(0),
                    Value::Int(crate::storage::PAGE_SIZE as i64),
                    Value::Int(0),
                    s(page.page_state),
                    s(match page.io_fix {
                        "READ" => "IO_READ",
                        "WRITE" => "IO_WRITE",
                        _ => "IO_NONE",
                    }),
                    yes_no(page.is_old),
                    Value::Int(saturating_i64(page.replacement_position)),
                    s("NO"),
                ]
            }
        })
        .collect()
}

fn innodb_cached_indexes_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let catalog = db.catalog();
    let mut index_ids = std::collections::HashMap::<(u64, String), i64>::new();
    for table in catalog.tables.values() {
        for object in innodb_physical_objects(table) {
            for (ordinal, index) in table.iter_public_indexes().enumerate() {
                index_ids.insert(
                    (object.storage_id, index.name.to_ascii_lowercase()),
                    synth_index_id(object.storage_id, ordinal),
                );
            }
        }
    }
    let mut counts = std::collections::BTreeMap::<(u64, i64), u64>::new();
    for page in db.buffer_page_diagnostics() {
        let Some(index_name) = page.index_name else {
            continue;
        };
        let index_id = index_ids
            .get(&(page.storage_id, index_name.to_ascii_lowercase()))
            .copied()
            .unwrap_or_else(|| synth_unmapped_index_id(page.storage_id, &index_name));
        *counts.entry((page.storage_id, index_id)).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|((space, index_id), pages)| {
            vec![
                Value::Int(saturating_i64(space)),
                Value::Int(index_id),
                Value::Int(saturating_i64(pages)),
            ]
        })
        .collect()
}

fn innodb_metrics_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let st = db.storage_status();
    // (name, subsystem, count, type). Only counters mydb genuinely tracks are
    // surfaced; each is reported as an enabled, cumulative metric.
    let metrics: &[(&str, &str, u64, &str, &str)] = &[
        (
            "buffer_pool_reads",
            "buffer",
            st.buffer_pool_read_ios,
            "counter",
            "Pages read from disk into the buffer pool",
        ),
        (
            "buffer_pool_write_requests",
            "buffer",
            st.buffer_pool_write_ios,
            "counter",
            "Pages written from the buffer pool to disk",
        ),
        (
            "buffer_pool_pages_data",
            "buffer",
            st.buffer_pool_used_pages,
            "value",
            "Resident buffer pool pages",
        ),
        (
            "buffer_pool_pages_dirty",
            "buffer",
            st.buffer_pool_dirty_pages,
            "value",
            "Dirty buffer pool pages",
        ),
        (
            "buffer_pool_pages_free",
            "buffer",
            st.buffer_pool_pages
                .saturating_sub(st.buffer_pool_used_pages),
            "value",
            "Unused buffer pool page slots",
        ),
        (
            "buffer_pool_pages_evictable",
            "buffer",
            st.buffer_pool_freeable_pages,
            "value",
            "Resident pages currently eligible for eviction",
        ),
        (
            "buffer_pool_pages_total",
            "buffer",
            st.buffer_pool_pages,
            "value",
            "Total buffer pool page capacity",
        ),
        (
            "buffer_pool_read_requests",
            "buffer",
            st.buffer_hits + st.buffer_misses,
            "counter",
            "Logical page get requests",
        ),
        (
            "os_log_bytes_written",
            "os",
            st.wal_bytes,
            "counter",
            "Bytes written to the WAL",
        ),
        (
            "log_writes",
            "log",
            st.wal_syncs,
            "counter",
            "WAL sync operations",
        ),
        (
            "trx_active_transactions",
            "transaction",
            st.active_transactions as u64,
            "value",
            "Currently active transactions",
        ),
        (
            "lock_deadlocks",
            "lock",
            st.deadlocks,
            "counter",
            "Detected deadlocks",
        ),
        (
            "lock_row_lock_waits",
            "lock",
            st.lock_waits,
            "counter",
            "Row lock wait events",
        ),
        (
            "lock_row_lock_current_waits",
            "lock",
            st.active_lock_waits as u64,
            "value",
            "Row locks currently being waited on",
        ),
        (
            "innodb_checkpoints",
            "recovery",
            st.checkpoints_total,
            "counter",
            "Checkpoints executed",
        ),
        (
            "innodb_row_migrations",
            "dml",
            st.row_migrations,
            "counter",
            "Row migrations (updates that outgrew their page)",
        ),
    ];
    metrics
        .iter()
        .map(|(name, subsystem, count, ty, comment)| {
            let count = saturating_i64(*count);
            vec![
                s(name),
                s(subsystem),
                Value::Int(count),
                Value::Null, // MAX_COUNT
                Value::Null, // MIN_COUNT
                Value::Null, // AVG_COUNT
                Value::Int(count),
                Value::Null, // MAX_COUNT_RESET
                Value::Null, // MIN_COUNT_RESET
                Value::Null, // AVG_COUNT_RESET
                Value::Null, // TIME_ENABLED
                Value::Null, // TIME_DISABLED
                Value::Null, // TIME_ELAPSED
                Value::Null, // TIME_RESET
                s("enabled"),
                s(ty),
                s(comment),
            ]
        })
        .collect()
}

fn innodb_ft_default_stopword_rows() -> Vec<Vec<Value>> {
    crate::storage::fulltext::tokenizer::default_stopwords()
        .iter()
        .map(|word| vec![s(word)])
        .collect()
}

fn selected_fulltext_index(db: &Database) -> Option<(std::sync::Arc<TableSchema>, IndexSchema)> {
    let selected = crate::engine::sysvar::builtin::mydb_storage::innodb_ft_aux_table();
    let (database, table) = selected.split_once('/')?;
    let table = db.table(&format!("{database}.{table}")).ok()?;
    let index = table
        .iter_public_indexes()
        .filter(|index| index.index_type.eq_ignore_ascii_case("FULLTEXT"))
        .min_by(|left, right| left.name.cmp(&right.name))?
        .clone();
    Some((table, index))
}

fn innodb_ft_config_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    if !can_view_process_metadata(db) {
        return Ok(Vec::new());
    }
    let Some((_table, index)) = selected_fulltext_index(db) else {
        return Ok(Vec::new());
    };
    let meta = index.fulltext.unwrap_or_default();
    let defaults = crate::storage::fulltext::tokenizer::global_config();
    Ok(vec![
        vec![s("index_name"), s(&index.name)],
        vec![
            s("parser"),
            s(if meta.parser.is_empty() {
                "default"
            } else {
                &meta.parser
            }),
        ],
        vec![
            s("min_token_size"),
            s(&meta
                .min_token_size
                .map(|value| value as usize)
                .unwrap_or(defaults.min_token_size)
                .to_string()),
        ],
        vec![
            s("max_token_size"),
            s(&meta
                .max_token_size
                .map(|value| value as usize)
                .unwrap_or(defaults.max_token_size)
                .to_string()),
        ],
        vec![
            s("ngram_size"),
            s(&meta
                .ngram_size
                .map(|value| value as usize)
                .unwrap_or(defaults.ngram_size)
                .to_string()),
        ],
        vec![
            s("use_stopword"),
            s(if meta.use_stopwords.unwrap_or(defaults.use_stopwords) {
                "1"
            } else {
                "0"
            }),
        ],
    ])
}

fn innodb_ft_index_table_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    if !can_view_process_metadata(db) {
        return Ok(Vec::new());
    }
    let Some((table, index)) = selected_fulltext_index(db) else {
        return Ok(Vec::new());
    };
    let postings = db.fulltext_diagnostic_postings(&table, &index)?;
    let mut summaries = std::collections::BTreeMap::<String, (u64, u64, u64)>::new();
    for posting in &postings {
        let summary =
            summaries
                .entry(posting.word.clone())
                .or_insert((posting.doc_id, posting.doc_id, 0));
        summary.0 = summary.0.min(posting.doc_id);
        summary.1 = summary.1.max(posting.doc_id);
        summary.2 = summary.2.saturating_add(1);
    }
    let mut rows = Vec::new();
    for posting in postings {
        let (first, last, count) = summaries[&posting.word];
        if posting.positions.is_empty() {
            rows.push(vec![
                s(&posting.word),
                Value::Int(saturating_i64(first)),
                Value::Int(saturating_i64(last)),
                Value::Int(saturating_i64(count)),
                Value::Int(saturating_i64(posting.doc_id)),
                Value::Int(0),
            ]);
        } else {
            for position in posting.positions {
                rows.push(vec![
                    s(&posting.word),
                    Value::Int(saturating_i64(first)),
                    Value::Int(saturating_i64(last)),
                    Value::Int(saturating_i64(count)),
                    Value::Int(saturating_i64(posting.doc_id)),
                    Value::Int(i64::from(position)),
                ]);
            }
        }
    }
    Ok(rows)
}

fn innodb_temp_table_info_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    crate::engine::expr::with_active_session(|session| {
        let Some(session) = session else {
            return Vec::new();
        };
        let mut rows = session
            .temp_tables
            .iter()
            .filter_map(|(visible_name, temp)| {
                let table = db.table(&temp.internal_key).ok()?;
                let (_, name) = split_table_name(DEFAULT_DATABASE, visible_name);
                Some(vec![
                    Value::Int(saturating_i64(table.id)),
                    s(&name),
                    Value::Int(table.iter_visible_columns().count() as i64),
                    Value::Int(table.id.min(u64::from(u32::MAX)) as i64),
                ])
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left[1]
                .to_display_string()
                .cmp(&right[1].to_display_string())
        });
        rows
    })
}

fn mydb_cursors_rows(db: &Database) -> Vec<Vec<Value>> {
    // Unions the two kinds of server-side cursor:
    //   1. Prepared cursors — opened by `COM_STMT_EXECUTE` with
    //      `CURSOR_TYPE_READ_ONLY` (JDBC `useCursorFetch=true`),
    //      addressed by `(connection_id, statement_id)`.
    //   2. Named cursors — opened by `DECLARE <name> CURSOR FOR ...`
    //      at the SQL level, addressed by `(connection_id, name)`.
    // We surface the latter with `STATEMENT_ID = 0` and prefix the
    // identifier into the `SQL` column as `[cursor=<name>] <sql>` so
    // the table stays in a single shape per row.
    let can_view_all = can_view_process_metadata(db);
    let own_connection_id = crate::engine::expr::with_active_session(|session| {
        session.map(|session| session.connection_id)
    });
    let visible = |connection_id: u32| {
        can_view_all || own_connection_id.is_some_and(|own| own == connection_id)
    };
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for snap in crate::engine::cursor_registry::global()
        .snapshot()
        .into_iter()
        .filter(|snap| visible(snap.key.connection_id))
    {
        rows.push(vec![
            Value::Int(snap.key.connection_id as i64),
            Value::Int(snap.key.statement_id as i64),
            Value::String(snap.sql.to_string()),
            Value::Int(snap.columns as i64),
            Value::Int(snap.rows_fetched.min(i64::MAX as u64) as i64),
            Value::Int(snap.created_unix_ms),
            Value::Int(snap.last_active_unix_ms),
            Value::Int(snap.idle_ms.min(i64::MAX as u64) as i64),
        ]);
    }
    for snap in crate::engine::cursor_registry::named_snapshot()
        .into_iter()
        .filter(|snap| visible(snap.connection_id))
    {
        rows.push(vec![
            Value::Int(snap.connection_id as i64),
            Value::Int(0),
            Value::String(format!("[cursor={}] {}", snap.name, snap.sql)),
            Value::Int(snap.columns as i64),
            Value::Int(snap.rows_fetched.min(i64::MAX as u64) as i64),
            Value::Int(snap.created_unix_ms),
            Value::Int(snap.last_active_unix_ms),
            Value::Int(snap.idle_ms.min(i64::MAX as u64) as i64),
        ]);
    }
    rows
}

fn mydb_caches_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    crate::engine::cache_registry::collect()
        .into_iter()
        .map(|row| {
            let hit_pct = row.hit_pct().round() as i64;
            vec![
                Value::String(row.name.to_string()),
                Value::String(row.kind.to_string()),
                Value::String(row.scope.to_string()),
                Value::Int(row.hits.min(i64::MAX as u64) as i64),
                Value::Int(row.misses.min(i64::MAX as u64) as i64),
                Value::Int(hit_pct),
                Value::Int(row.size_entries.min(i64::MAX as u64) as i64),
                Value::Int(row.capacity_entries.min(i64::MAX as u64) as i64),
                Value::Int(row.evictions.min(i64::MAX as u64) as i64),
                Value::Int(row.invalidations.min(i64::MAX as u64) as i64),
                row.bytes
                    .map(|bytes| Value::Int(bytes.min(i64::MAX as u64) as i64))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect()
}

/// One row per resident physical-plan cache entry. Empty when no engine is
/// registered (e.g. a bare test double) or the cache is cold.
fn mydb_plan_cache_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    let Some(cache) = crate::engine::cache::global_plan_cache() else {
        return Vec::new();
    };
    let mut rows: Vec<Vec<Value>> = cache
        .snapshot()
        .into_iter()
        .map(|e| {
            vec![
                Value::String(e.sql.to_string()),
                Value::String(e.database.to_string()),
                Value::Int(saturating_i64(e.sql_mode_fingerprint)),
                Value::Int(e.hits.min(i64::MAX as u64) as i64),
                Value::Int(e.deps as i64),
                Value::Int(e.approx_bytes.min(i64::MAX as u64) as i64),
                Value::Int(e.epoch.min(i64::MAX as u64) as i64),
            ]
        })
        .collect();
    // Stable, useful default ordering: hottest entries first.
    rows.sort_by(|a, b| match (&a[3], &b[3]) {
        (Value::Int(x), Value::Int(y)) => y.cmp(x),
        _ => std::cmp::Ordering::Equal,
    });
    rows
}

/// One row per HNSW vector index in the catalog, joining static catalog
/// metadata against any warm runtime graph. See `MYDB_VECTOR_INDEXES_COLUMNS`.
fn mydb_vector_indexes_rows(db: &Database, request: &SystemScanRequest) -> Vec<Vec<Value>> {
    use crate::storage::naming::normalize_name;
    use crate::storage::schema::DataTypeKind;
    let visibility = Visibility::compute(db);
    let runtime = db.vector_index_runtime_stats();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let catalog = db.catalog();
    visit_requested_table_descriptors(db, &catalog, request, |table| {
        if !visibility.table_visible(&table.database, &table.name) {
            return;
        }
        for index in table.iter_public_indexes() {
            if !index.is_vector() {
                continue;
            }
            let meta = index.vector_meta_for_table(&table);
            let col_name = index.columns.first().cloned().unwrap_or_default();
            let declared_dim = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&col_name))
                .and_then(|c| match &c.data_type {
                    DataTypeKind::Vector { dim, .. } => *dim,
                    _ => None,
                });
            let stats = runtime.get(&(table.id, normalize_name(&index.name)));
            let opt_i64 = |v: Option<i64>| v.map(Value::Int).unwrap_or(Value::Null);
            rows.push(vec![
                s(&table.database),
                s(&table.name),
                s(&index.name),
                s(&col_name),
                stats.map(|st| s(st.backend)).unwrap_or(Value::Null),
                s(&meta.metric),
                opt_i64(declared_dim.map(|d| d as i64)),
                opt_i64(stats.filter(|st| st.dim > 0).map(|st| st.dim as i64)),
                Value::Int(meta.m as i64),
                Value::Int(meta.ef_construction as i64),
                // DEFAULT_EF_SEARCH — per-index override (stage 2); NULL until set.
                meta.default_ef_search
                    .map(|e| Value::Int(e as i64))
                    .unwrap_or(Value::Null),
                Value::Int(stats.is_some() as i64),
                opt_i64(stats.map(|st| st.partitions_loaded as i64)),
                opt_i64(stats.map(|st| st.total_nodes as i64)),
                opt_i64(stats.map(|st| st.live_nodes as i64)),
                opt_i64(stats.map(|st| st.stale_nodes as i64)),
                opt_i64(stats.map(|st| st.stale_ratio_pct())),
                opt_i64(stats.map(|st| st.dirty_nodes as i64)),
                opt_i64(stats.map(|st| st.generation as i64)),
                opt_i64(stats.map(|st| st.last_persisted_lsn as i64)),
                // ALGO from the catalog (available even when cold).
                s(match index.vector_algo() {
                    crate::storage::schema::VectorAlgo::Ivf => "IVF",
                    crate::storage::schema::VectorAlgo::Hnsw => "HNSW",
                }),
                meta.nlist
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                // NLIST / INSERTS_SINCE_TRAIN: IVF-only, NULL for HNSW and cold.
                opt_i64(stats.filter(|st| st.nlist > 0).map(|st| st.nlist as i64)),
                opt_i64(stats.map(|st| st.list_avg_entries as i64)),
                opt_i64(stats.map(|st| st.list_max_entries as i64)),
                opt_i64(stats.map(|st| st.list_p95_entries as i64)),
                match index.vector_algo() {
                    crate::storage::schema::VectorAlgo::Ivf => stats
                        .map(|st| {
                            s(if st.backend != "ivf_paged_native" || st.finalized {
                                "READY"
                            } else {
                                "REBUILD_REQUIRED"
                            })
                        })
                        .unwrap_or(Value::Null),
                    crate::storage::schema::VectorAlgo::Hnsw => Value::Null,
                },
                match index.vector_algo() {
                    crate::storage::schema::VectorAlgo::Ivf => {
                        opt_i64(stats.map(|st| st.inserts_since_train as i64))
                    }
                    crate::storage::schema::VectorAlgo::Hnsw => Value::Null,
                },
                opt_i64(stats.map(|st| st.paged_file_size_bytes as i64)),
                opt_i64(stats.map(|st| st.nav_pages as i64)),
                opt_i64(stats.map(|st| st.sq8_exact_fallback_nodes as i64)),
                opt_i64(stats.map(|st| st.tombstones_on_disk as i64)),
                opt_i64(stats.map(|st| st.sq8_covered_nodes as i64)),
                meta.default_nprobe
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                meta.kmeans_iters
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                meta.train_sample_mult
                    .map(|v| Value::Int(v as i64))
                    .unwrap_or(Value::Null),
                s(index
                    .vector
                    .as_ref()
                    .map_or("none", |v| v.quantization.name())),
                stats
                    .map(|st| {
                        s(if st.quantization_pending {
                            "PENDING_TRAINING"
                        } else {
                            "READY"
                        })
                    })
                    .unwrap_or(Value::Null),
                opt_i64(stats.map(|st| st.quantizer_bytes as i64)),
                opt_i64(stats.map(|st| st.compressed_bytes as i64)),
                opt_i64(stats.map(|st| st.quantization_exceptions as i64)),
                Value::Int(index.vector.as_ref().map_or(4, |v| v.rerank_mult) as i64),
                opt_i64(stats.map(|st| st.candidates_returned as i64)),
                opt_i64(stats.map(|st| st.heap_reranks as i64)),
                opt_i64(stats.map(|st| st.exact_fallbacks as i64)),
            ]);
        }
    });
    rows
}

fn mydb_temporal_history_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let status = db.storage_status();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    let retention_start = now_us.saturating_sub(status.undo_retention_ms.saturating_mul(1_000));
    let ledger_start = status.temporal_earliest_available_unix_us;
    let global_start = if status.undo_retention_ms == 0 {
        now_us
    } else {
        retention_start.max(ledger_start)
    };
    let degraded = i64::from(db.temporal_history_index_degraded());
    let last_compaction = (status.temporal_history_last_compaction_unix_us != 0).then(|| {
        Value::Int(
            status
                .temporal_history_last_compaction_unix_us
                .min(i64::MAX as u64) as i64,
        )
    });
    let mut rows = Vec::new();
    for table in db
        .catalog()
        .tables
        .values()
        .filter(|table| !table.system && visibility.table_visible(&table.database, &table.name))
    {
        let earliest = global_start.max(table.system_time_epoch_us);
        let mut emitted = false;
        for index in table.iter_public_indexes() {
            let (entries, bytes) = db.temporal_history_index_stats(table.id, &index.name);
            rows.push(vec![
                s(&table.database),
                s(&table.name),
                s(&index.name),
                Value::Int(table.system_time_epoch_us.min(i64::MAX as u64) as i64),
                Value::Int(earliest.min(i64::MAX as u64) as i64),
                Value::Int(entries.min(i64::MAX as usize) as i64),
                Value::Int(bytes.min(i64::MAX as usize) as i64),
                last_compaction.clone().unwrap_or(Value::Null),
                Value::Int(degraded),
            ]);
            emitted = true;
        }
        if !emitted {
            rows.push(vec![
                s(&table.database),
                s(&table.name),
                Value::Null,
                Value::Int(table.system_time_epoch_us.min(i64::MAX as u64) as i64),
                Value::Int(earliest.min(i64::MAX as u64) as i64),
                Value::Int(0),
                Value::Int(0),
                last_compaction.clone().unwrap_or(Value::Null),
                Value::Int(degraded),
            ]);
        }
    }
    rows.sort_by(|left, right| {
        left[0]
            .to_display_string()
            .cmp(&right[0].to_display_string())
            .then_with(|| {
                left[1]
                    .to_display_string()
                    .cmp(&right[1].to_display_string())
            })
            .then_with(|| {
                left[2]
                    .to_display_string()
                    .cmp(&right[2].to_display_string())
            })
    });
    rows
}

/// PGA-style work-area arbiter snapshot. One row per metric so a
/// human-friendly description column can travel alongside the value.
/// Used by `SELECT * FROM information_schema.mydb_memory_advisor`.
fn mydb_memory_advisor_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    use crate::engine::executor::mem_budget;
    use crate::engine::executor::work_area;
    let snap = work_area::snapshot();
    let row = |metric: &str, value: i64, unit: &str, desc: &str| -> Vec<Value> {
        vec![
            Value::String(metric.to_string()),
            Value::Int(value),
            Value::String(unit.to_string()),
            Value::String(desc.to_string()),
        ]
    };
    let clamp = |v: u64| v.min(i64::MAX as u64) as i64;
    let target = mem_budget::memory_target_bytes().min(i64::MAX as usize) as i64;
    let exec_target = mem_budget::executor_memory_target_bytes().min(i64::MAX as usize) as i64;
    let bp_pages = crate::storage::resolve_buffer_pool_pages() as i64;
    let alloc = snap.allocated_bytes.min(i64::MAX as usize) as i64;
    let hw = snap.high_water_bytes.min(i64::MAX as usize) as i64;
    let used = snap.used_bytes.min(i64::MAX as usize) as i64;
    let hw_used = snap.high_water_used_bytes.min(i64::MAX as usize) as i64;
    let util_pct = if exec_target > 0 {
        ((alloc as i128).saturating_mul(100) / exec_target as i128) as i64
    } else {
        0
    };
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let (controlled_current, controlled_peak, controlled_categories, controlled_rejections) =
        mem_budget::controlled_memory_snapshot();
    rows.push(row(
        "memory_target_bytes",
        target,
        "bytes",
        "effective advisory working-set target after OS/cgroup clamping",
    ));
    rows.push(row(
        "memory_soft_target_configured_bytes",
        mem_budget::configured_memory_target_bytes()
            .unwrap_or(0)
            .min(i64::MAX as usize) as i64,
        "bytes",
        "explicit MYDB_MEMORY_TARGET, or 0 when automatic",
    ));
    rows.push(row(
        "effective_memory_limit_bytes",
        mem_budget::effective_memory_limit_bytes().min(i64::MAX as usize) as i64,
        "bytes",
        "lower of host RAM and cgroup memory.max",
    ));
    rows.push(row(
        "sql_memory_hard_limit_bytes",
        mem_budget::operational_memory_limit_bytes().min(i64::MAX as usize) as i64,
        "bytes",
        "ordinary governed SQL hard limit (80% of effective memory)",
    ));
    rows.push(row(
        "process_memory_hard_limit_bytes",
        mem_budget::managed_memory_limit_bytes().min(i64::MAX as usize) as i64,
        "bytes",
        "critical rollback/process hard limit (90% of effective memory)",
    ));
    rows.push(row(
        "executor_memory_target_bytes",
        exec_target,
        "bytes",
        "MYDB_EXECUTOR_MEMORY_TARGET — pool the PGA arbiter manages",
    ));
    rows.push(row(
        "controlled_memory_current_bytes",
        controlled_current.min(i64::MAX as usize) as i64,
        "bytes",
        "current managed-resident memory charged to the hard governor",
    ));
    rows.push(row(
        "controlled_memory_peak_bytes",
        controlled_peak.min(i64::MAX as usize) as i64,
        "bytes",
        "peak controlled memory since process start",
    ));
    for (name, value) in [
        ("controlled_buffer_raw_bytes", controlled_categories[0]),
        ("controlled_storage_scratch_bytes", controlled_categories[1]),
        ("controlled_executor_bytes", controlled_categories[2]),
        ("controlled_derived_cache_bytes", controlled_categories[3]),
        (
            "controlled_partition_dml_batch_bytes",
            controlled_categories[4],
        ),
        ("controlled_partition_ddl_bytes", controlled_categories[5]),
        ("controlled_partition_stats_bytes", controlled_categories[6]),
        ("controlled_ddl_copy_bytes", controlled_categories[7]),
        ("controlled_system_metadata_bytes", controlled_categories[8]),
        (
            "controlled_temporal_history_bytes",
            controlled_categories[9],
        ),
        (
            "controlled_protocol_transient_bytes",
            controlled_categories[10],
        ),
        (
            "controlled_binlog_transient_bytes",
            controlled_categories[11],
        ),
        (
            "controlled_parser_transient_bytes",
            controlled_categories[12],
        ),
        (
            "controlled_native_replication_transient_bytes",
            controlled_categories[13],
        ),
        (
            "controlled_performance_schema_bytes",
            controlled_categories[14],
        ),
        ("controlled_ddl_online_log_bytes", controlled_categories[15]),
    ] {
        rows.push(row(
            name,
            value.min(i64::MAX as usize) as i64,
            "bytes",
            "resident bytes in this controlled-memory category",
        ));
    }
    rows.push(row(
        "controlled_memory_rejections",
        controlled_rejections.min(i64::MAX as u64) as i64,
        "count",
        "allocations rejected at MYDB_MEMORY_TARGET",
    ));
    rows.push(row(
        "buffer_pool_pages",
        bp_pages,
        "pages",
        "buffer pool capacity (16 KiB per page)",
    ));
    rows.push(row(
        "work_area_allocated_bytes",
        alloc,
        "bytes",
        "currently reserved across all active work areas",
    ));
    rows.push(row(
        "work_area_high_water_bytes",
        hw,
        "bytes",
        "peak reserved since process start",
    ));
    rows.push(row(
        "work_area_used_bytes",
        used,
        "bytes",
        "in-memory state held by active operators",
    ));
    rows.push(row(
        "work_area_high_water_used_bytes",
        hw_used,
        "bytes",
        "peak in-memory state since process start",
    ));
    rows.push(row(
        "work_area_utilization_pct",
        util_pct,
        "percent",
        "allocated / executor_memory_target × 100",
    ));
    rows.push(row(
        "active_handles",
        clamp(snap.active_handles),
        "count",
        "active work-area handles (operators still running)",
    ));
    rows.push(row(
        "optimal_grants",
        clamp(snap.optimal_grants),
        "count",
        "handles registered at Optimal tier (full in-memory)",
    ));
    rows.push(row(
        "one_pass_grants",
        clamp(snap.one_pass_grants),
        "count",
        "handles registered at OnePass tier (single grace pass)",
    ));
    rows.push(row(
        "min_grants",
        clamp(snap.min_grants),
        "count",
        "handles registered at Min tier (multi-pass spill)",
    ));
    rows.push(row(
        "spill_requests",
        clamp(snap.spill_requests),
        "count",
        "times the arbiter asked an operator to spill",
    ));
    rows.push(row(
        "spill_estimated_bytes",
        clamp(snap.spill_estimated_bytes),
        "bytes",
        "approximate bytes operators released to disk on spill",
    ));
    rows.push(row(
        "force_overflow_bytes",
        clamp(snap.force_overflow_bytes),
        "bytes",
        "bytes charged beyond local grant when pool was full",
    ));
    rows.push(row(
        "force_overflow_events",
        clamp(snap.force_overflow_events),
        "count",
        "times an operator force-charged past its grant",
    ));
    rows.push(row(
        "process_query_mem_used_bytes",
        mem_budget::process_query_mem_used() as i64,
        "bytes",
        "legacy per-query MemBudget total in-use across queries",
    ));
    rows.push(row(
        "process_query_mem_limit_bytes",
        mem_budget::process_mem_limit_bytes() as i64,
        "bytes",
        "process-wide work-memory soft watermark used to request spill",
    ));
    rows.push(row(
        "query_memory_reservation_rejections",
        clamp(mem_budget::query_reservation_rejections()),
        "count",
        "spillable reservations redirected to spill by per-query soft pressure",
    ));
    rows.push(row(
        "process_memory_reservation_rejections",
        clamp(mem_budget::process_reservation_rejections()),
        "count",
        "spillable reservations redirected to spill by process soft pressure",
    ));
    rows.push(row(
        "executor_terminal_oom_cancellations",
        clamp(mem_budget::terminal_oom_cancellations()),
        "count",
        "SQL statements cancelled after a non-recoverable executor OOM",
    ));
    rows.push(row(
        "physical_memory_pressure_rejections",
        clamp(crate::engine::memory_reclaimer::physical_pressure_rejections()),
        "count",
        "resident allocations rejected after allocator reclaim could not restore process headroom",
    ));
    let reclaim = mem_budget::admission_reclaim_snapshot();
    for (metric, value, unit, description) in [
        (
            "memory_reclaim_attempts",
            reclaim.attempts,
            "count",
            "slow-path admission reclaim attempts",
        ),
        (
            "memory_reclaim_retry_successes",
            reclaim.retry_successes,
            "count",
            "reservations admitted after waiting or reclaim",
        ),
        (
            "memory_reclaim_cache_bytes",
            reclaim.cache_bytes,
            "bytes",
            "controlled bytes released by optional caches",
        ),
        (
            "memory_reclaim_buffer_bytes",
            reclaim.buffer_bytes,
            "bytes",
            "controlled bytes released by buffer and zone-map reclaim",
        ),
        (
            "memory_reclaim_allocator_bytes",
            reclaim.allocator_bytes,
            "bytes",
            "private footprint returned by allocator collection",
        ),
        (
            "memory_oom_after_reclaim",
            reclaim.oom_after_retry,
            "count",
            "1037 failures remaining after reclaim and retry",
        ),
    ] {
        rows.push(row(metric, clamp(value), unit, description));
    }
    // P0-1 — the live, concurrency-fair growth ceiling a single work
    // area may currently reach (target / demanding_handles). Registered
    // but idle downstream operators do not dilute the live consumer. This is the
    // estimate-INDEPENDENT cap that replaced the CBO `optimal`.
    rows.push(row(
        "work_area_fair_share_bytes",
        work_area::global_manager()
            .fair_share_bytes()
            .min(i64::MAX as usize) as i64,
        "bytes",
        "current per-operator fair share (target / demanding handles)",
    ));
    // P0-3 — temp-disk quota live state.
    rows.push(row(
        "temp_disk_live_bytes",
        clamp(crate::engine::executor::spill_quota::global_spilled_bytes()),
        "bytes",
        "spill bytes currently on disk across all queries",
    ));
    rows.push(row(
        "temp_disk_high_water_bytes",
        clamp(crate::engine::executor::spill_quota::global_spilled_high_water()),
        "bytes",
        "peak live spill bytes since process start",
    ));
    rows.push(row(
        "temp_disk_quota_rejections",
        clamp(crate::engine::executor::spill_quota::quota_rejections()),
        "count",
        "times a spill write was rejected with 1041 (out of temp disk)",
    ));
    rows.push(row(
        "temp_disk_global_quota_rejections",
        clamp(crate::engine::executor::spill_quota::global_quota_rejections()),
        "count",
        "global spill reservations rejected with 1041",
    ));
    rows.push(row(
        "temp_disk_query_quota_rejections",
        clamp(crate::engine::executor::spill_quota::per_query_quota_rejections()),
        "count",
        "per-query spill reservations rejected with 1041",
    ));
    rows.push(row(
        "active_spill_files",
        clamp(crate::engine::executor::spill_quota::active_spill_files()),
        "count",
        "spill files currently open",
    ));
    rows.push(row(
        "spill_file_opens",
        clamp(crate::engine::executor::spill_quota::spill_file_opens()),
        "count",
        "spill files opened since process start",
    ));
    rows.push(row(
        "orphan_spill_bytes",
        clamp(crate::engine::executor::spill_quota::orphan_spill_bytes()),
        "bytes",
        "spill bytes retained after a failed cleanup",
    ));
    rows.push(row(
        "spill_cleanup_failures",
        clamp(crate::engine::executor::spill_quota::cleanup_failures()),
        "count",
        "failed spill-directory cleanup attempts",
    ));
    for (name, value, description) in [
        (
            "sort_spill_count",
            crate::engine::executor::spill_quota::sort_spill_count(),
            "Sort spill directories created since process start",
        ),
        (
            "hash_join_spill_count",
            crate::engine::executor::spill_quota::hash_join_spill_count(),
            "HashJoin spill directories created since process start",
        ),
        (
            "hash_agg_spill_count",
            crate::engine::executor::spill_quota::hash_agg_spill_count(),
            "HashAgg spill directories created since process start",
        ),
        (
            "distinct_spill_count",
            crate::engine::executor::spill_quota::distinct_spill_count(),
            "Distinct spill directories created since process start",
        ),
        (
            "cte_spill_count",
            crate::engine::executor::spill_quota::cte_spill_count(),
            "CTE spill directories created since process start",
        ),
        (
            "setop_spill_count",
            crate::engine::executor::SETOP_SPILL_COUNT.load(std::sync::atomic::Ordering::Relaxed),
            "Set-operation spill executions since process start",
        ),
    ] {
        rows.push(row(name, clamp(value), "count", description));
    }
    for (name, value, unit, description) in [
        (
            "setop_spill_bytes_written",
            crate::engine::executor::SETOP_SPILL_BYTES_WRITTEN
                .load(std::sync::atomic::Ordering::Relaxed),
            "bytes",
            "Set-operation spill bytes written",
        ),
        (
            "setop_spill_bytes_read",
            crate::engine::executor::SETOP_SPILL_BYTES_READ
                .load(std::sync::atomic::Ordering::Relaxed),
            "bytes",
            "Set-operation spill bytes read",
        ),
        (
            "setop_spill_partitions",
            crate::engine::executor::SETOP_SPILL_PARTITIONS
                .load(std::sync::atomic::Ordering::Relaxed),
            "count",
            "Set-operation input and ordered-output spill partitions",
        ),
        (
            "setop_spill_repartitions",
            crate::engine::executor::SETOP_SPILL_REPARTITIONS
                .load(std::sync::atomic::Ordering::Relaxed),
            "count",
            "Set-operation recursive repartition passes",
        ),
        (
            "setop_spill_max_depth",
            crate::engine::executor::SETOP_SPILL_MAX_DEPTH
                .load(std::sync::atomic::Ordering::Relaxed),
            "count",
            "Maximum set-operation spill recursion depth",
        ),
        (
            "setop_spill_file_opens",
            crate::engine::executor::SETOP_SPILL_FILE_OPENS
                .load(std::sync::atomic::Ordering::Relaxed),
            "count",
            "Set-operation spill reader and writer opens",
        ),
        (
            "setop_spill_merge_passes",
            crate::engine::executor::SETOP_SPILL_MERGE_PASSES
                .load(std::sync::atomic::Ordering::Relaxed),
            "count",
            "Set-operation ordered-output merge passes",
        ),
    ] {
        rows.push(row(name, clamp(value), unit, description));
    }
    rows
}

/// P2/13 — collect the live in-memory undo mirror, group by
/// `(txn_id, table_id)`, sort by record_count descending so heavy
/// holders surface at the top. Each group emits one row with the
/// aggregated bytes / count / max chain depth / oldest creation
/// timestamp.
fn mydb_undo_history_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_process_metadata(db) {
        return Vec::new();
    }
    use std::collections::HashMap;
    let snapshot = match db.collect_undo_history() {
        Some(v) => v,
        None => return Vec::new(),
    };
    #[derive(Default)]
    struct GroupAgg {
        count: u64,
        bytes: u64,
        chain_depth: u32,
        oldest_ms: u64,
    }
    let mut groups: HashMap<(u64, u64), GroupAgg> = HashMap::new();
    for entry in snapshot {
        let g = groups.entry((entry.txn_id, entry.table_id)).or_default();
        g.count = g.count.saturating_add(1);
        g.bytes = g.bytes.saturating_add(entry.bytes);
        g.chain_depth = g.chain_depth.max(entry.chain_depth);
        g.oldest_ms = if g.oldest_ms == 0 {
            entry.created_unix_ms
        } else {
            g.oldest_ms.min(entry.created_unix_ms)
        };
    }
    let mut rows: Vec<((u64, u64), GroupAgg)> = groups.into_iter().collect();
    // Heaviest holders first; tie-break on txn_id for stable ordering.
    rows.sort_by(|a, b| {
        b.1.count
            .cmp(&a.1.count)
            .then_with(|| a.0 .0.cmp(&b.0 .0))
            .then_with(|| a.0 .1.cmp(&b.0 .1))
    });
    rows.into_iter()
        .map(|((txn_id, table_id), g)| {
            vec![
                Value::Int(txn_id.min(i64::MAX as u64) as i64),
                Value::Int(table_id.min(i64::MAX as u64) as i64),
                Value::Int(g.count.min(i64::MAX as u64) as i64),
                Value::Int(g.bytes.min(i64::MAX as u64) as i64),
                Value::Int(i64::from(g.chain_depth)),
                Value::Int(g.oldest_ms.min(i64::MAX as u64) as i64),
            ]
        })
        .collect()
}

fn mydb_xa_recover_rows(db: &Database) -> Vec<Vec<Value>> {
    if !can_view_dynamic_metadata(db, "XA_RECOVER_ADMIN") {
        return Vec::new();
    }
    db.list_prepared_xa()
        .into_iter()
        .map(|p| {
            vec![
                Value::Int(p.xid.format_id),
                Value::Int(p.xid.gtrid_len()),
                Value::Int(p.xid.bqual_len()),
                Value::String(p.xid.render_data(false)),
                Value::Int(saturating_i64(p.txn_id)),
                Value::Int(saturating_i64(p.prepare_scn)),
                Value::Int(saturating_i64(p.started_unix_ms)),
            ]
        })
        .collect()
}

fn mydb_ddl_jobs_rows(db: &Database) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    let mut rows = Vec::new();
    let partition_jobs = db.partition_ddl_job_snapshots();
    let partition_ids = partition_jobs
        .iter()
        .map(|j| j.job_id)
        .collect::<std::collections::HashSet<_>>();
    let copy_jobs = db.alter_copy_job_snapshots();
    for job in copy_jobs.iter().cloned().into_iter().filter(|job| {
        !partition_ids.contains(&job.job_id) && visibility.table_visible(&job.database, &job.table)
    }) {
        rows.push(vec![
            Value::Int(job.job_id.min(i64::MAX as u64) as i64),
            s(&job.database),
            s(&job.table),
            s(match job.operation {
                "INDEX" => "INDEX",
                "CONSTRAINT" => "CONSTRAINT",
                _ => "TABLE",
            }),
            s(&job.object_name),
            s(&job.state),
            Value::Int(job.copied_rows.min(i64::MAX as u64) as i64),
            job.total_rows
                .map(|rows| Value::Int(rows.min(i64::MAX as u64) as i64))
                .unwrap_or(Value::Null),
            job.replay_watermark
                .map(|watermark| Value::Int(watermark.min(i64::MAX as u64) as i64))
                .unwrap_or(Value::Null),
            job.error_message.map(Value::String).unwrap_or(Value::Null),
            Value::Int(i64::from(job.cancel_allowed)),
        ]);
    }
    for mut job in partition_jobs
        .into_iter()
        .filter(|job| visibility.table_visible(&job.database, &job.table))
    {
        if let Some(live) = copy_jobs.iter().find(|live| live.job_id == job.job_id) {
            if !matches!(live.state.as_str(), "COMMITTED" | "FAILED" | "CANCELLED") {
                job.state = live.state.clone();
                job.cancel_allowed = live.cancel_allowed;
            }
            job.copied_rows = live.copied_rows;
            job.total_rows = live.total_rows;
            job.replay_watermark = live.replay_watermark;
            if live.error_message.is_some() {
                job.error_message = live.error_message.clone();
            }
        }
        rows.push(vec![
            Value::Int(job.job_id.min(i64::MAX as u64) as i64),
            s(&job.database),
            s(&job.table),
            s("PARTITION"),
            s(&job.operation),
            s(&job.state),
            Value::Int(job.copied_rows.min(i64::MAX as u64) as i64),
            job.total_rows
                .map(|rows| Value::Int(rows.min(i64::MAX as u64) as i64))
                .unwrap_or(Value::Null),
            job.replay_watermark
                .map(|watermark| Value::Int(watermark.min(i64::MAX as u64) as i64))
                .unwrap_or(Value::Null),
            job.error_message.map(Value::String).unwrap_or(Value::Null),
            Value::Int(i64::from(job.cancel_allowed)),
        ]);
    }
    for table in db.catalog().tables.values() {
        if !visibility.table_visible(&table.database, &table.name) {
            continue;
        }
        for index in &table.indexes {
            if index.state == crate::storage::IndexState::Public {
                continue;
            }
            if copy_jobs.iter().any(|job| {
                job.table_id == table.id
                    && !matches!(job.state.as_str(), "COMMITTED" | "FAILED" | "CANCELLED")
            }) {
                continue;
            }
            let state_label = match index.state {
                crate::storage::IndexState::Building => "BUILDING",
                crate::storage::IndexState::Public => "PUBLIC",
            };
            rows.push(vec![
                Value::Null, // Legacy storage builds without a registered SQL job.
                s(&table.database),
                s(&table.name),
                s("INDEX"),
                s(
                    if table
                        .pending_primary_key
                        .as_ref()
                        .is_some_and(|p| p.capture_name == index.name)
                    {
                        "PRIMARY"
                    } else {
                        &index.name
                    },
                ),
                s(state_label),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(0),
            ]);
        }
    }
    rows
}

fn current_visible_roles(auth: &AuthCatalog) -> std::collections::BTreeSet<AccountKey> {
    let Some((ctx, system)) = crate::engine::expr::current_authorization_context() else {
        return std::collections::BTreeSet::new();
    };
    if system {
        return auth
            .accounts
            .values()
            .filter(|account| account.is_role)
            .map(|account| account.key.clone())
            .collect();
    }
    auth.applicable_roles(&ctx.authenticated_as)
        .into_iter()
        .collect()
}

pub(crate) fn column_key(table: &TableSchema, column: &str) -> String {
    if table
        .primary_key
        .iter()
        .any(|pk| pk.eq_ignore_ascii_case(column))
    {
        return "PRI".to_string();
    }
    if table
        .iter_public_indexes()
        .filter(|index| index.unique && index.columns.len() == 1)
        .any(|index| first_index_column_matches(index, column))
    {
        return "UNI".to_string();
    }
    if table
        .iter_public_indexes()
        .filter(|index| !index.unique || index.columns.len() > 1)
        .any(|index| first_index_column_matches(index, column))
    {
        return "MUL".to_string();
    }
    String::new()
}

fn first_index_column_matches(index: &IndexSchema, column: &str) -> bool {
    index
        .columns
        .first()
        .is_some_and(|first| first.eq_ignore_ascii_case(column))
}

fn mysql_data_type_name(data_type: &DataTypeKind) -> String {
    match data_type {
        DataTypeKind::VarChar { .. } => "varchar".to_string(),
        DataTypeKind::Char { .. } => "char".to_string(),
        other => mysql_type_name_with_unsigned(other, false)
            .split('(')
            .next()
            .unwrap_or("varchar")
            .to_string(),
    }
}

fn mysql_character_maximum_length(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Char { len }
        | DataTypeKind::VarChar { len }
        | DataTypeKind::Binary { len }
        | DataTypeKind::VarBinary { len } => {
            len.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null)
        }
        DataTypeKind::Text { max_len } | DataTypeKind::Blob { max_len } => {
            Value::Int(i64::from(*max_len))
        }
        DataTypeKind::Json => Value::Null,
        // MySQL 8.0: for ENUM this is the character length of the longest
        // member; for SET it's `sum(char_len) + (n-1)` (the CSV output width).
        DataTypeKind::Enum { values } => {
            let n = values
                .iter()
                .map(|v| v.chars().count() as i64)
                .max()
                .unwrap_or(0);
            Value::Int(n)
        }
        DataTypeKind::Set { values } => {
            let sum: i64 = values.iter().map(|v| v.chars().count() as i64).sum();
            let commas = values.len().saturating_sub(1) as i64;
            Value::Int(sum + commas)
        }
        _ => Value::Null,
    }
}

fn mysql_character_octet_length(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Binary { len } | DataTypeKind::VarBinary { len } => {
            len.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null)
        }
        DataTypeKind::Text { max_len } | DataTypeKind::Blob { max_len } => {
            Value::Int(i64::from(*max_len))
        }
        _ => match mysql_character_maximum_length(data_type) {
            Value::Int(len) => Value::Int(len.saturating_mul(4)),
            other => other,
        },
    }
}

fn mysql_character_octet_length_for_column(column: &ColumnSchema) -> Value {
    match &column.data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => {
            let max_len = column
                .charset
                .as_deref()
                .and_then(|charset| crate::charset::resolve_charset(charset).ok())
                .map_or(4_i64, |charset| i64::from(charset.max_len));
            match mysql_character_maximum_length(&column.data_type) {
                Value::Int(length) => Value::Int(length.saturating_mul(max_len)),
                other => other,
            }
        }
        _ => mysql_character_octet_length(&column.data_type),
    }
}

fn mysql_numeric_precision(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Bool | DataTypeKind::TinyInt => Value::Int(3),
        DataTypeKind::SmallInt => Value::Int(5),
        DataTypeKind::MediumInt => Value::Int(7),
        DataTypeKind::Int => Value::Int(10),
        DataTypeKind::BigInt => Value::Int(19),
        // FLOAT(M[,D]) — MySQL reports M when present, otherwise the
        // type's natural maximum (12 binary digits, the IEEE-754
        // single-precision mantissa).
        DataTypeKind::Float { precision, .. } => Value::Int(precision.map_or(12, |p| p as i64)),
        DataTypeKind::Double { precision, .. } => Value::Int(precision.map_or(22, |p| p as i64)),
        DataTypeKind::Decimal { precision, .. } => precision
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        // BIT(M): MySQL reports NUMERIC_PRECISION = M (NUMERIC_SCALE stays
        // NULL), so a client can recover the declared bit width.
        DataTypeKind::Bit { len } => Value::Int(len.unwrap_or(1) as i64),
        _ => Value::Null,
    }
}

fn mysql_numeric_scale(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Decimal { scale, .. } => {
            scale.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null)
        }
        DataTypeKind::Bool
        | DataTypeKind::TinyInt
        | DataTypeKind::SmallInt
        | DataTypeKind::MediumInt
        | DataTypeKind::Int
        | DataTypeKind::BigInt => Value::Int(0),
        // FLOAT(M,D) / DOUBLE(M,D) — MySQL exposes D when present; bare
        // FLOAT / DOUBLE report NULL because the type has no fixed scale.
        DataTypeKind::Float { scale, .. } | DataTypeKind::Double { scale, .. } => {
            scale.map(|s| Value::Int(s as i64)).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn mysql_datetime_precision(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Time { fsp }
        | DataTypeKind::DateTime { fsp }
        | DataTypeKind::Timestamp { fsp } => Value::Int(*fsp as i64),
        _ => Value::Null,
    }
}

fn column_charset_value(column: &ColumnSchema) -> Value {
    match column.data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => Value::String(
            column
                .charset
                .clone()
                .unwrap_or_else(|| "utf8mb4".to_string()),
        ),
        // A JSON column reports CHARACTER_SET_NAME = NULL in MySQL.
        _ => Value::Null,
    }
}

fn column_collation_value(column: &ColumnSchema) -> Value {
    match column.data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => Value::String(
            column
                .collation
                .clone()
                .unwrap_or_else(|| crate::charset::DEFAULT_COLLATION.to_string()),
        ),
        // A JSON column reports COLLATION_NAME = NULL in MySQL.
        _ => Value::Null,
    }
}

fn character_set_for_data_type(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => s(crate::charset::DEFAULT_CHARSET),
        _ => Value::Null,
    }
}

fn collation_for_data_type(data_type: &DataTypeKind) -> Value {
    match data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => s(crate::charset::DEFAULT_COLLATION),
        _ => Value::Null,
    }
}

pub(crate) fn mysql_time_value(unix_ms: u64) -> Value {
    if unix_ms == 0 {
        return Value::Null;
    }
    let secs = (unix_ms / 1000) as i64;
    let offset = crate::engine::expr::session_time_offset_seconds_at(secs);
    chrono::DateTime::from_timestamp(secs.saturating_add(offset), 0)
        .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
        .unwrap_or(Value::Null)
}

/// MySQL exposes a non-NULL CREATE_TIME for dictionary-backed system views.
/// mydb does not persist one timestamp per built-in definition, so freeze a
/// process-lifetime dictionary load time instead of violating the declared
/// `TABLES.CREATE_TIME NOT NULL` contract or returning a value that changes on
/// every scan.
fn metadata_create_time_value() -> Value {
    static CREATE_TIME: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    mysql_time_value(*CREATE_TIME.get_or_init(crate::storage::naming::current_unix_ms))
}

fn opt_i(value: Option<i64>) -> Value {
    value.map(Value::Int).unwrap_or(Value::Null)
}

/// Clamp a `u64` size to fit MySQL's signed BIGINT result column.
/// `information_schema.TABLES.DATA_LENGTH` and friends are declared
/// signed (matching MySQL 8.0), so a multi-terabyte heap would
/// otherwise wrap to a negative number. We saturate at `i64::MAX`
/// instead — clients that watch the column for "too big to fit" will
/// still see a plausibly large value.
fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

fn yes_no(value: bool) -> Value {
    s(if value { "YES" } else { "NO" })
}

fn filter_visible_object_rows(
    db: &Database,
    rows: Vec<Vec<Value>>,
    schema_index: usize,
    table_index: usize,
) -> Vec<Vec<Value>> {
    let visibility = Visibility::compute(db);
    rows.into_iter()
        .filter(|row| {
            let Some(schema) = row.get(schema_index) else {
                return false;
            };
            let Some(table) = row.get(table_index) else {
                return false;
            };
            visibility.table_visible(&schema.to_display_string(), &table.to_display_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql80_information_schema_core_tables_are_registered() {
        for name in [
            "schemata",
            "tables",
            "columns",
            "statistics",
            "table_constraints",
            "key_column_usage",
            "views",
            "routines",
            "triggers",
            "partitions",
            "referential_constraints",
            "column_statistics",
            "innodb_trx",
            "mysql_firewall_users",
        ] {
            assert!(
                find_system_table("information_schema", name).is_some(),
                "{name}"
            );
        }
    }

    #[test]
    fn information_schema_registry_uses_namespaced_native_tables() {
        assert_eq!(INFORMATION_SCHEMA_REGISTRY.len(), 103);
        let mut names = std::collections::BTreeSet::new();
        let mut class_counts = [0usize; 4];
        for registration in INFORMATION_SCHEMA_REGISTRY {
            assert!(
                names.insert(registration.table.name.to_ascii_lowercase()),
                "duplicate information_schema table {}",
                registration.table.name
            );
            match registration.compatibility {
                InformationSchemaCompatibility::Mysql80Community => class_counts[0] += 1,
                InformationSchemaCompatibility::MysqlLegacy => class_counts[1] += 1,
                InformationSchemaCompatibility::MysqlOptionalPlugin => class_counts[2] += 1,
                InformationSchemaCompatibility::MydbNative => {
                    class_counts[3] += 1;
                    assert!(
                        (registration.table.name.starts_with("mydb_")
                            || registration.table.name.starts_with("ai_"))
                    );
                }
            }
            if let InformationSchemaProducer::HonestEmpty(reason) = registration.producer {
                assert!(
                    !reason.trim().is_empty(),
                    "{}.{} needs an honest-empty reason",
                    registration.table.schema,
                    registration.table.name
                );
            }
        }
        assert_eq!(class_counts, [76, 3, 7, 17]);
        assert!(!names.contains("zone_maps"));
        assert!(!names.contains("zone_map_storage"));
        assert!(names.contains("mydb_zone_maps"));
        assert!(names.contains("mydb_zone_map_storage"));
    }

    #[test]
    fn mysql80_46_manifest_covers_all_core_tables_and_column_order() {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../docs/fixtures/mysql80-information-schema-8.0.46.json"
        ))
        .expect("MySQL 8.0.46 information_schema manifest must be valid JSON");
        assert_eq!(manifest["oracle_mysql_version"], "8.0.46");
        assert_eq!(manifest["table_count"], 79);
        assert_eq!(manifest["column_count"], 733);

        let core = INFORMATION_SCHEMA_REGISTRY
            .iter()
            .filter(|entry| entry.compatibility == InformationSchemaCompatibility::Mysql80Community)
            .collect::<Vec<_>>();
        assert_eq!(core.len(), 76);
        for table in manifest["tables"].as_array().expect("tables array") {
            let name = table["name"].as_str().expect("table name");
            if UNSUPPORTED_INFORMATION_SCHEMA_TABLES.contains(&name) {
                assert!(find_system_table("information_schema", name).is_none());
                assert_eq!(
                    hidden_system_table_error(&format!("information_schema.{name}"))
                        .unwrap()
                        .code(),
                    1235
                );
                continue;
            }

            let registration = core
                .iter()
                .find(|entry| entry.table.name.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("missing MySQL 8.0.46 table {name}"));
            let expected_columns = manifest["columns"]
                .as_array()
                .expect("columns array")
                .iter()
                .filter(|column| column["table_name"] == name)
                .map(|column| column["column_name"].as_str().expect("column name"))
                .collect::<Vec<_>>();
            let actual_columns = registration
                .table
                .columns
                .iter()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            assert_eq!(actual_columns, expected_columns, "column order for {name}");

            let manifest_columns = manifest["columns"]
                .as_array()
                .expect("columns array")
                .iter()
                .filter(|column| column["table_name"] == name)
                .collect::<Vec<_>>();
            for (actual, expected) in registration.table.columns.iter().zip(manifest_columns) {
                let expected_i64 = |field: &str| expected[field].as_i64();
                assert_eq!(
                    actual.data_type, expected["data_type"],
                    "{name}.{}",
                    actual.name
                );
                assert_eq!(
                    actual.column_type_sql, expected["column_type"],
                    "{name}.{}",
                    actual.name
                );
                assert_eq!(
                    actual.nullable,
                    expected["is_nullable"] == "YES",
                    "{name}.{}",
                    actual.name
                );
                assert_eq!(
                    actual.char_len,
                    expected_i64("character_maximum_length"),
                    "{name}.{} char length",
                    actual.name
                );
                assert_eq!(
                    actual.octet_len,
                    expected_i64("character_octet_length"),
                    "{name}.{} octet length",
                    actual.name
                );
                assert_eq!(
                    actual.numeric_precision,
                    expected_i64("numeric_precision"),
                    "{name}.{} precision",
                    actual.name
                );
                assert_eq!(
                    actual.numeric_scale,
                    expected_i64("numeric_scale"),
                    "{name}.{} scale",
                    actual.name
                );
                assert_eq!(
                    actual.datetime_precision,
                    expected_i64("datetime_precision"),
                    "{name}.{} datetime precision",
                    actual.name
                );
                let expected_character = expected["character_set_name"]
                    .as_str()
                    .zip(expected["collation_name"].as_str());
                let generated_metadata =
                    crate::engine::information_schema_mysql80_character_metadata::metadata(
                        name,
                        actual.name,
                    )
                    .expect("every core column has generated metadata");
                assert_eq!(
                    generated_metadata
                        .character_set
                        .zip(generated_metadata.collation),
                    expected_character,
                    "{name}.{} character metadata",
                    actual.name
                );
                assert_eq!(
                    generated_metadata.default_value,
                    expected["column_default"].as_str(),
                    "{name}.{} default",
                    actual.name
                );
                assert_eq!(
                    generated_metadata.describe_default,
                    expected["describe_default"].as_str(),
                    "{name}.{} DESCRIBE default",
                    actual.name
                );
                assert_eq!(generated_metadata.column_key, expected["column_key"]);
                assert_eq!(generated_metadata.extra, expected["extra"]);
                assert_eq!(generated_metadata.privileges, expected["privileges"]);
                assert_eq!(generated_metadata.comment, expected["column_comment"]);
                assert_eq!(
                    generated_metadata.generation_expression,
                    expected["generation_expression"]
                );
                assert_eq!(generated_metadata.srs_id, expected_i64("srs_id"));
            }
        }
    }

    #[test]
    fn system_columns_include_catalog_wire_metadata() {
        for name in UNSUPPORTED_INFORMATION_SCHEMA_TABLES {
            assert_eq!(
                columns_for_table_name(&format!("information_schema.{name}"))
                    .unwrap_err()
                    .code(),
                1235
            );
        }
    }

    #[test]
    fn registered_columns_have_mysql_metadata() {
        for def in system_table_definitions() {
            assert!(!def.columns.is_empty(), "{}.{}", def.schema, def.name);
            for (idx, column) in def.columns.iter().enumerate() {
                assert!(
                    !column.name.is_empty(),
                    "{}.{} #{idx}",
                    def.schema,
                    def.name
                );
                assert!(!column.data_type.is_empty(), "{}.{}", def.schema, def.name);
                assert!(
                    !column.column_type_sql.is_empty(),
                    "{}.{}",
                    def.schema,
                    def.name
                );
            }
        }
    }

    #[test]
    fn information_schema_columns_shape_matches_mysql80_tail() {
        let names: Vec<_> = COLUMNS_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(
            names,
            vec![
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "COLUMN_DEFAULT",
                "IS_NULLABLE",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
                "CHARACTER_OCTET_LENGTH",
                "NUMERIC_PRECISION",
                "NUMERIC_SCALE",
                "DATETIME_PRECISION",
                "CHARACTER_SET_NAME",
                "COLLATION_NAME",
                "COLUMN_TYPE",
                "COLUMN_KEY",
                "EXTRA",
                "PRIVILEGES",
                "COLUMN_COMMENT",
                "GENERATION_EXPRESSION",
                "SRS_ID",
            ]
        );
    }

    #[test]
    fn metadata_compat_matrix_is_machine_readable_and_covers_core_surfaces() {
        let matrix: serde_json::Value = serde_json::from_str(include_str!(
            "../../docs/mysql80-metadata-compat-matrix.json"
        ))
        .expect("metadata compatibility matrix must be valid JSON");
        assert_eq!(matrix["version"], 2);
        assert_eq!(matrix["oracle_mysql_baseline"], "8.0.46 Community");
        assert_eq!(matrix["counts"]["mysql80_community"], 76);
        assert_eq!(matrix["counts"]["mysql_legacy_compat"], 3);
        assert_eq!(matrix["counts"]["mysql_optional_plugin_compat"], 7);
        assert_eq!(matrix["counts"]["mydb_native"], 13);
        let surfaces = matrix["surfaces"].as_array().expect("surfaces array");
        assert_eq!(surfaces.len(), 99);
        let mut seen = std::collections::BTreeSet::new();
        for surface in surfaces {
            let schema = surface["schema"].as_str().expect("schema");
            let object = surface["object"].as_str().expect("object");
            assert!(seen.insert((schema, object)), "duplicate {schema}.{object}");
            assert!(matches!(
                surface["producer"].as_str(),
                Some("real_producer" | "honest_empty")
            ));
            assert!(
                surface["data_source"].as_str().is_some(),
                "{schema}.{object}"
            );
            assert!(
                surface["replication_semantics"].as_str().is_some(),
                "{schema}.{object}"
            );
            assert!(
                surface["visibility"].as_str().is_some(),
                "{schema}.{object}"
            );
            assert!(
                surface["pushdown"].as_array().is_some(),
                "{schema}.{object}"
            );
            assert!(
                surface["test_coverage"]
                    .as_array()
                    .is_some_and(|tests| !tests.is_empty()),
                "{schema}.{object}"
            );
            if surface["producer"] == "honest_empty" {
                assert!(
                    surface["reason"]
                        .as_str()
                        .is_some_and(|reason| !reason.is_empty()),
                    "{schema}.{object} needs an honest-empty reason"
                );
            }
        }
        assert!(seen.contains(&("information_schema", "INNODB_TRX")));
        assert!(!seen.contains(&("information_schema", "zone_maps")));
        assert!(!seen.contains(&("information_schema", "zone_map_storage")));
        assert!(seen.contains(&("information_schema", "mydb_zone_maps")));
        assert!(seen.contains(&("information_schema", "mydb_zone_map_storage")));
        assert!(seen.contains(&("information_schema", "mydb_temporal_history")));
    }
}
use std::collections::HashSet;
