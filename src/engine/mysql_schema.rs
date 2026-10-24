//! MySQL 8.0-compatible `mysql` system-schema registry.
//!
//! These definitions are intentionally virtual. The real mydb catalog still
//! lives in the data-dictionary heaps under reserved table ids; this module is
//! the SQL-facing compatibility surface clients expect when introspecting
//! `mysql.*`.

use std::collections::{BTreeMap, BTreeSet};

use crate::engine::auth::{AccountKey, Privilege, PrivilegeScope};
use crate::engine::information_schema::{SystemColumn, SystemTableDef, SystemTableIndex};
use crate::storage::{error, Database, PartitionStats, TableSchema};
use crate::types::{ColumnType, QueryError, Value};

macro_rules! ch {
    ($name:literal, $len:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "char",
            concat!("char(", stringify!($len), ")"),
            false,
        )
        .character_lengths(Some($len), Some($len * 4))
    };
}

macro_rules! chn {
    ($name:literal, $len:literal) => {
        SystemColumn {
            nullable: true,
            ..ch!($name, $len)
        }
    };
}

macro_rules! vc {
    ($name:literal, $len:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "varchar",
            concat!("varchar(", stringify!($len), ")"),
            false,
        )
        .character_lengths(Some($len), Some($len * 4))
    };
}

macro_rules! vcn {
    ($name:literal, $len:literal) => {
        SystemColumn {
            nullable: true,
            ..vc!($name, $len)
        }
    };
}

macro_rules! enum_col {
    ($name:literal, $sql:expr, $nullable:expr) => {
        SystemColumn::new($name, ColumnType::VarString, "enum", $sql, $nullable)
            .character_lengths(Some(1), Some(4))
    };
}

macro_rules! set_col {
    ($name:literal, $sql:expr) => {
        SystemColumn::new($name, ColumnType::VarString, "set", $sql, false)
            .character_lengths(Some(255), Some(1020))
    };
}

macro_rules! yn {
    ($name:literal) => {
        enum_col!($name, "enum('N','Y')", false)
    };
}

macro_rules! textn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..textnn!($name)
        }
    };
}

macro_rules! textnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::VarString, "text", "text", false)
            .character_lengths(Some(65535), Some(262140))
    };
}

macro_rules! mediumtextnn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::VarString,
            "mediumtext",
            "mediumtext",
            false,
        )
        .character_lengths(Some(16777215), Some(67108860))
    };
}

macro_rules! blobnn {
    ($name:literal, $sql:literal) => {
        SystemColumn::new($name, ColumnType::Blob, $sql, $sql, false)
    };
}

macro_rules! intn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..intnn!($name)
        }
    };
}

macro_rules! intnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Long, "int", "int", false).numeric(Some(10), Some(0))
    };
}

macro_rules! uintnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Long, "int", "int unsigned", false)
            .numeric(Some(10), Some(0))
    };
}

macro_rules! uintn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..uintnn!($name)
        }
    };
}

macro_rules! bigintnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::LongLong, "bigint", "bigint", false)
            .numeric(Some(19), Some(0))
    };
}

macro_rules! ubigintnn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::LongLong,
            "bigint",
            "bigint unsigned",
            false,
        )
        .numeric(Some(20), Some(0))
    };
}

macro_rules! ubigintn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..ubigintnn!($name)
        }
    };
}

macro_rules! tiny_uintnn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::Tiny,
            "tinyint",
            "tinyint unsigned",
            false,
        )
        .numeric(Some(3), Some(0))
    };
}

macro_rules! boolnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Tiny, "tinyint", "tinyint(1)", false)
            .numeric(Some(1), Some(0))
    };
}

macro_rules! small_uintn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..small_uintnn!($name)
        }
    };
}

macro_rules! small_uintnn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::Short,
            "smallint",
            "smallint unsigned",
            false,
        )
        .numeric(Some(5), Some(0))
    };
}

macro_rules! floatnn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Float, "float", "float", false).numeric(Some(12), None)
    };
}

macro_rules! floatn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..floatnn!($name)
        }
    };
}

macro_rules! timestampn {
    ($name:literal) => {
        SystemColumn {
            nullable: true,
            ..timestampnn!($name)
        }
    };
}

macro_rules! timestampnn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::Timestamp,
            "timestamp",
            "timestamp",
            false,
        )
        .temporal_precision(Some(0))
    };
}

macro_rules! timestamp6nn {
    ($name:literal) => {
        SystemColumn::new(
            $name,
            ColumnType::Timestamp,
            "timestamp",
            "timestamp(6)",
            false,
        )
        .temporal_precision(Some(6))
    };
}

macro_rules! time6nn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Time, "time", "time(6)", false)
            .temporal_precision(Some(6))
    };
}

macro_rules! jsonn {
    ($name:literal) => {
        SystemColumn::new($name, ColumnType::Json, "json", "json", true)
    };
}

macro_rules! primary {
    ($columns:expr) => {
        SystemTableIndex {
            name: "PRIMARY",
            unique: true,
            columns: $columns,
            index_type: "BTREE",
            visible: true,
        }
    };
}

macro_rules! key {
    ($name:literal, $columns:expr) => {
        SystemTableIndex {
            name: $name,
            unique: false,
            columns: $columns,
            index_type: "BTREE",
            visible: true,
        }
    };
}

macro_rules! unique_key {
    ($name:literal, $columns:expr) => {
        SystemTableIndex {
            name: $name,
            unique: true,
            columns: $columns,
            index_type: "BTREE",
            visible: true,
        }
    };
}

const YN_ENUM_SQL: &str = "enum('N','Y')";

const USER_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("User", 32),
    yn!("Select_priv"),
    yn!("Insert_priv"),
    yn!("Update_priv"),
    yn!("Delete_priv"),
    yn!("Create_priv"),
    yn!("Drop_priv"),
    yn!("Reload_priv"),
    yn!("Shutdown_priv"),
    yn!("Process_priv"),
    yn!("File_priv"),
    yn!("Grant_priv"),
    yn!("References_priv"),
    yn!("Index_priv"),
    yn!("Alter_priv"),
    yn!("Show_db_priv"),
    yn!("Super_priv"),
    yn!("Create_tmp_table_priv"),
    yn!("Lock_tables_priv"),
    yn!("Execute_priv"),
    yn!("Repl_slave_priv"),
    yn!("Repl_client_priv"),
    yn!("Create_view_priv"),
    yn!("Show_view_priv"),
    yn!("Create_routine_priv"),
    yn!("Alter_routine_priv"),
    yn!("Create_user_priv"),
    yn!("Event_priv"),
    yn!("Trigger_priv"),
    yn!("Create_tablespace_priv"),
    enum_col!("ssl_type", "enum('','ANY','X509','SPECIFIED')", false),
    blobnn!("ssl_cipher", "blob"),
    blobnn!("x509_issuer", "blob"),
    blobnn!("x509_subject", "blob"),
    uintnn!("max_questions"),
    uintnn!("max_updates"),
    uintnn!("max_connections"),
    uintnn!("max_user_connections"),
    ch!("plugin", 64),
    textn!("authentication_string"),
    yn!("password_expired"),
    timestampn!("password_last_changed"),
    small_uintn!("password_lifetime"),
    yn!("account_locked"),
    yn!("Create_role_priv"),
    yn!("Drop_role_priv"),
    small_uintn!("Password_reuse_history"),
    small_uintn!("Password_reuse_time"),
    enum_col!("Password_require_current", YN_ENUM_SQL, true),
    jsonn!("User_attributes"),
];
const USER_INDEXES: &[SystemTableIndex] = &[primary!(&["Host", "User"])];

const DB_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("Db", 64),
    ch!("User", 32),
    yn!("Select_priv"),
    yn!("Insert_priv"),
    yn!("Update_priv"),
    yn!("Delete_priv"),
    yn!("Create_priv"),
    yn!("Drop_priv"),
    yn!("Grant_priv"),
    yn!("References_priv"),
    yn!("Index_priv"),
    yn!("Alter_priv"),
    yn!("Create_tmp_table_priv"),
    yn!("Lock_tables_priv"),
    yn!("Create_view_priv"),
    yn!("Show_view_priv"),
    yn!("Create_routine_priv"),
    yn!("Alter_routine_priv"),
    yn!("Execute_priv"),
    yn!("Event_priv"),
    yn!("Trigger_priv"),
];
const DB_INDEXES: &[SystemTableIndex] =
    &[primary!(&["Host", "User", "Db"]), key!("User", &["User"])];

const DEFAULT_ROLES_COLUMNS: &[SystemColumn] = &[
    ch!("HOST", 255),
    ch!("USER", 32),
    ch!("DEFAULT_ROLE_HOST", 255),
    ch!("DEFAULT_ROLE_USER", 32),
];
const DEFAULT_ROLES_INDEXES: &[SystemTableIndex] = &[primary!(&[
    "HOST",
    "USER",
    "DEFAULT_ROLE_HOST",
    "DEFAULT_ROLE_USER"
])];

const ROLE_EDGES_COLUMNS: &[SystemColumn] = &[
    ch!("FROM_HOST", 255),
    ch!("FROM_USER", 32),
    ch!("TO_HOST", 255),
    ch!("TO_USER", 32),
    yn!("WITH_ADMIN_OPTION"),
];
const ROLE_EDGES_INDEXES: &[SystemTableIndex] =
    &[primary!(&["FROM_HOST", "FROM_USER", "TO_HOST", "TO_USER"])];

const GLOBAL_GRANTS_COLUMNS: &[SystemColumn] = &[
    ch!("USER", 32),
    ch!("HOST", 255),
    ch!("PRIV", 32),
    yn!("WITH_GRANT_OPTION"),
];
const GLOBAL_GRANTS_INDEXES: &[SystemTableIndex] = &[primary!(&["USER", "HOST", "PRIV"])];

const PASSWORD_HISTORY_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("User", 32),
    timestamp6nn!("Password_timestamp"),
    textn!("Password"),
];
const PASSWORD_HISTORY_INDEXES: &[SystemTableIndex] =
    &[primary!(&["Host", "User", "Password_timestamp"])];

const FUNC_COLUMNS: &[SystemColumn] = &[
    ch!("name", 64),
    SystemColumn::new("ret", ColumnType::Tiny, "tinyint", "tinyint", false)
        .numeric(Some(3), Some(0)),
    ch!("dl", 128),
    enum_col!("type", "enum('function','aggregate')", false),
];
const FUNC_INDEXES: &[SystemTableIndex] = &[primary!(&["name"])];

const PLUGIN_COLUMNS: &[SystemColumn] = &[vc!("name", 64), vc!("dl", 128)];
const PLUGIN_INDEXES: &[SystemTableIndex] = &[primary!(&["name"])];

const HELP_TOPIC_COLUMNS: &[SystemColumn] = &[
    uintnn!("help_topic_id"),
    ch!("name", 64),
    small_uintnn!("help_category_id"),
    textnn!("description"),
    textnn!("example"),
    textnn!("url"),
];
const HELP_TOPIC_INDEXES: &[SystemTableIndex] =
    &[primary!(&["help_topic_id"]), unique_key!("name", &["name"])];

const HELP_CATEGORY_COLUMNS: &[SystemColumn] = &[
    small_uintnn!("help_category_id"),
    ch!("name", 64),
    small_uintn!("parent_category_id"),
    textnn!("url"),
];
const HELP_CATEGORY_INDEXES: &[SystemTableIndex] = &[
    primary!(&["help_category_id"]),
    unique_key!("name", &["name"]),
];

const HELP_RELATION_COLUMNS: &[SystemColumn] =
    &[uintnn!("help_topic_id"), uintnn!("help_keyword_id")];
const HELP_RELATION_INDEXES: &[SystemTableIndex] =
    &[primary!(&["help_keyword_id", "help_topic_id"])];

const HELP_KEYWORD_COLUMNS: &[SystemColumn] = &[uintnn!("help_keyword_id"), ch!("name", 64)];
const HELP_KEYWORD_INDEXES: &[SystemTableIndex] = &[
    primary!(&["help_keyword_id"]),
    unique_key!("name", &["name"]),
];

const SERVERS_COLUMNS: &[SystemColumn] = &[
    ch!("Server_name", 64),
    ch!("Host", 255),
    ch!("Db", 64),
    ch!("Username", 64),
    ch!("Password", 64),
    intnn!("Port"),
    ch!("Socket", 64),
    ch!("Wrapper", 64),
    ch!("Owner", 64),
];
const SERVERS_INDEXES: &[SystemTableIndex] = &[primary!(&["Server_name"])];

const TABLES_PRIV_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("Db", 64),
    ch!("User", 32),
    ch!("Table_name", 64),
    vc!("Grantor", 288),
    timestampnn!("Timestamp"),
    set_col!(
        "Table_priv",
        "set('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter','Create View','Show view','Trigger')"
    ),
    set_col!("Column_priv", "set('Select','Insert','Update','References')"),
];
const TABLES_PRIV_INDEXES: &[SystemTableIndex] = &[
    primary!(&["Host", "User", "Db", "Table_name"]),
    key!("Grantor", &["Grantor"]),
];

const COLUMNS_PRIV_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("Db", 64),
    ch!("User", 32),
    ch!("Table_name", 64),
    ch!("Column_name", 64),
    timestampnn!("Timestamp"),
    set_col!(
        "Column_priv",
        "set('Select','Insert','Update','References')"
    ),
];
const COLUMNS_PRIV_INDEXES: &[SystemTableIndex] = &[primary!(&[
    "Host",
    "User",
    "Db",
    "Table_name",
    "Column_name"
])];

const TIME_ZONE_NAME_COLUMNS: &[SystemColumn] = &[ch!("Name", 64), uintnn!("Time_zone_id")];
const TIME_ZONE_NAME_INDEXES: &[SystemTableIndex] = &[primary!(&["Name"])];

const TIME_ZONE_COLUMNS: &[SystemColumn] = &[
    uintnn!("Time_zone_id"),
    enum_col!("Use_leap_seconds", "enum('Y','N')", false),
];
const TIME_ZONE_INDEXES: &[SystemTableIndex] = &[primary!(&["Time_zone_id"])];

const TIME_ZONE_TRANSITION_COLUMNS: &[SystemColumn] = &[
    uintnn!("Time_zone_id"),
    bigintnn!("Transition_time"),
    uintnn!("Transition_type_id"),
];
const TIME_ZONE_TRANSITION_INDEXES: &[SystemTableIndex] =
    &[primary!(&["Time_zone_id", "Transition_time"])];

const TIME_ZONE_TRANSITION_TYPE_COLUMNS: &[SystemColumn] = &[
    uintnn!("Time_zone_id"),
    uintnn!("Transition_type_id"),
    intnn!("Offset"),
    tiny_uintnn!("Is_DST"),
    ch!("Abbreviation", 8),
];
const TIME_ZONE_TRANSITION_TYPE_INDEXES: &[SystemTableIndex] =
    &[primary!(&["Time_zone_id", "Transition_type_id"])];

const TIME_ZONE_LEAP_SECOND_COLUMNS: &[SystemColumn] =
    &[bigintnn!("Transition_time"), intnn!("Correction")];
const TIME_ZONE_LEAP_SECOND_INDEXES: &[SystemTableIndex] = &[primary!(&["Transition_time"])];

const PROCS_PRIV_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("Db", 64),
    ch!("User", 32),
    ch!("Routine_name", 64),
    enum_col!("Routine_type", "enum('FUNCTION','PROCEDURE')", false),
    vc!("Grantor", 288),
    set_col!("Proc_priv", "set('Execute','Alter Routine','Grant')"),
    timestampnn!("Timestamp"),
];
const PROCS_PRIV_INDEXES: &[SystemTableIndex] = &[
    primary!(&["Host", "User", "Db", "Routine_name", "Routine_type"]),
    key!("Grantor", &["Grantor"]),
];

const GENERAL_LOG_COLUMNS: &[SystemColumn] = &[
    timestamp6nn!("event_time"),
    mediumtextnn!("user_host"),
    ubigintnn!("thread_id"),
    uintnn!("server_id"),
    vc!("command_type", 64),
    blobnn!("argument", "mediumblob"),
];

const SLOW_LOG_COLUMNS: &[SystemColumn] = &[
    timestamp6nn!("start_time"),
    mediumtextnn!("user_host"),
    time6nn!("query_time"),
    time6nn!("lock_time"),
    intnn!("rows_sent"),
    intnn!("rows_examined"),
    vc!("db", 512),
    intnn!("last_insert_id"),
    intnn!("insert_id"),
    uintnn!("server_id"),
    blobnn!("sql_text", "mediumblob"),
    ubigintnn!("thread_id"),
];

const COMPONENT_COLUMNS: &[SystemColumn] = &[
    uintnn!("component_id"),
    uintnn!("component_group_id"),
    textnn!("component_urn"),
];
const COMPONENT_INDEXES: &[SystemTableIndex] = &[primary!(&["component_id"])];

const SLAVE_RELAY_LOG_INFO_COLUMNS: &[SystemColumn] = &[
    uintnn!("Number_of_lines"),
    textn!("Relay_log_name"),
    ubigintn!("Relay_log_pos"),
    textn!("Master_log_name"),
    ubigintn!("Master_log_pos"),
    intn!("Sql_delay"),
    uintn!("Number_of_workers"),
    uintn!("Id"),
    vc!("Channel_name", 64),
    vcn!("Privilege_checks_username", 32),
    vcn!("Privilege_checks_hostname", 255),
    boolnn!("Require_row_format"),
    enum_col!(
        "Require_table_primary_key_check",
        "enum('STREAM','ON','OFF','GENERATE')",
        false
    ),
    enum_col!(
        "Assign_gtids_to_anonymous_transactions_type",
        "enum('OFF','LOCAL','UUID')",
        false
    ),
    textn!("Assign_gtids_to_anonymous_transactions_value"),
];
const SLAVE_RELAY_LOG_INFO_INDEXES: &[SystemTableIndex] = &[primary!(&["Channel_name"])];

const SLAVE_MASTER_INFO_COLUMNS: &[SystemColumn] = &[
    uintnn!("Number_of_lines"),
    textnn!("Master_log_name"),
    ubigintnn!("Master_log_pos"),
    vcn!("Host", 255),
    textn!("User_name"),
    textn!("User_password"),
    uintnn!("Port"),
    uintnn!("Connect_retry"),
    boolnn!("Enabled_ssl"),
    textn!("Ssl_ca"),
    textn!("Ssl_capath"),
    textn!("Ssl_cert"),
    textn!("Ssl_cipher"),
    textn!("Ssl_key"),
    boolnn!("Ssl_verify_server_cert"),
    floatnn!("Heartbeat"),
    textn!("Bind"),
    textn!("Ignored_server_ids"),
    textn!("Uuid"),
    ubigintnn!("Retry_count"),
    textn!("Ssl_crl"),
    textn!("Ssl_crlpath"),
    boolnn!("Enabled_auto_position"),
    vc!("Channel_name", 64),
    textn!("Tls_version"),
    textn!("Public_key_path"),
    boolnn!("Get_public_key"),
    textn!("Network_namespace"),
    vc!("Master_compression_algorithm", 64),
    uintnn!("Master_zstd_compression_level"),
    textn!("Tls_ciphersuites"),
    boolnn!("Source_connection_auto_failover"),
    boolnn!("Gtid_only"),
];
const SLAVE_MASTER_INFO_INDEXES: &[SystemTableIndex] = &[primary!(&["Channel_name"])];

const SLAVE_WORKER_INFO_COLUMNS: &[SystemColumn] = &[
    uintnn!("Id"),
    textnn!("Relay_log_name"),
    ubigintnn!("Relay_log_pos"),
    textnn!("Master_log_name"),
    ubigintnn!("Master_log_pos"),
    textnn!("Checkpoint_relay_log_name"),
    ubigintnn!("Checkpoint_relay_log_pos"),
    textnn!("Checkpoint_master_log_name"),
    ubigintnn!("Checkpoint_master_log_pos"),
    uintnn!("Checkpoint_seqno"),
    uintnn!("Checkpoint_group_size"),
    blobnn!("Checkpoint_group_bitmap", "blob"),
    vc!("Channel_name", 64),
];
const SLAVE_WORKER_INFO_INDEXES: &[SystemTableIndex] = &[primary!(&["Channel_name", "Id"])];

const GTID_EXECUTED_COLUMNS: &[SystemColumn] = &[
    ch!("source_uuid", 36),
    bigintnn!("interval_start"),
    bigintnn!("interval_end"),
];
const GTID_EXECUTED_INDEXES: &[SystemTableIndex] = &[primary!(&["source_uuid", "interval_start"])];

const REPL_ASYNC_FAILOVER_COLUMNS: &[SystemColumn] = &[
    ch!("Channel_name", 64),
    ch!("Host", 255),
    uintnn!("Port"),
    chn!("Network_namespace", 64),
    tiny_uintnn!("Weight"),
    ch!("Managed_name", 64),
];
const REPL_ASYNC_FAILOVER_INDEXES: &[SystemTableIndex] = &[
    primary!(&[
        "Channel_name",
        "Host",
        "Port",
        "Network_namespace",
        "Managed_name"
    ]),
    key!("Channel_name", &["Channel_name", "Managed_name"]),
];

const REPL_ASYNC_FAILOVER_MANAGED_COLUMNS: &[SystemColumn] = &[
    ch!("Channel_name", 64),
    ch!("Managed_name", 64),
    ch!("Managed_type", 64),
    jsonn!("Configuration"),
];
const REPL_ASYNC_FAILOVER_MANAGED_INDEXES: &[SystemTableIndex] =
    &[primary!(&["Channel_name", "Managed_name"])];

const REPL_GROUP_MEMBER_ACTIONS_COLUMNS: &[SystemColumn] = &[
    ch!("name", 255),
    ch!("event", 64),
    boolnn!("enabled"),
    ch!("type", 64),
    tiny_uintnn!("priority"),
    ch!("error_handling", 64),
];
const REPL_GROUP_MEMBER_ACTIONS_INDEXES: &[SystemTableIndex] =
    &[primary!(&["name", "event"]), key!("event", &["event"])];

const REPL_GROUP_CONFIGURATION_VERSION_COLUMNS: &[SystemColumn] =
    &[ch!("name", 255), ubigintnn!("version")];
const REPL_GROUP_CONFIGURATION_VERSION_INDEXES: &[SystemTableIndex] = &[primary!(&["name"])];

const SERVER_COST_COLUMNS: &[SystemColumn] = &[
    vc!("cost_name", 64),
    floatn!("cost_value"),
    timestampnn!("last_update"),
    vcn!("comment", 1024),
    floatn!("default_value"),
];
const SERVER_COST_INDEXES: &[SystemTableIndex] = &[primary!(&["cost_name"])];

const ENGINE_COST_COLUMNS: &[SystemColumn] = &[
    vc!("engine_name", 64),
    intnn!("device_type"),
    vc!("cost_name", 64),
    floatn!("cost_value"),
    timestampnn!("last_update"),
    vcn!("comment", 1024),
    floatn!("default_value"),
];
const ENGINE_COST_INDEXES: &[SystemTableIndex] =
    &[primary!(&["cost_name", "engine_name", "device_type"])];

const PROXIES_PRIV_COLUMNS: &[SystemColumn] = &[
    ch!("Host", 255),
    ch!("User", 32),
    ch!("Proxied_host", 255),
    ch!("Proxied_user", 32),
    boolnn!("With_grant"),
    vc!("Grantor", 288),
    timestampnn!("Timestamp"),
];
const PROXIES_PRIV_INDEXES: &[SystemTableIndex] = &[
    primary!(&["Host", "User", "Proxied_host", "Proxied_user"]),
    key!("Grantor", &["Grantor"]),
];

const NDB_BINLOG_INDEX_COLUMNS: &[SystemColumn] = &[
    ubigintnn!("Position"),
    vc!("File", 255),
    ubigintnn!("epoch"),
    uintnn!("inserts"),
    uintnn!("updates"),
    uintnn!("deletes"),
    uintnn!("schemaops"),
    uintnn!("orig_server_id"),
    ubigintnn!("orig_epoch"),
    uintnn!("gci"),
    ubigintnn!("next_position"),
    vc!("next_file", 255),
];
const NDB_BINLOG_INDEX_INDEXES: &[SystemTableIndex] =
    &[primary!(&["epoch", "orig_server_id", "orig_epoch"])];

const INNODB_TABLE_STATS_COLUMNS: &[SystemColumn] = &[
    vc!("database_name", 192),
    vc!("table_name", 199),
    timestampnn!("last_update"),
    ubigintnn!("n_rows"),
    ubigintnn!("clustered_index_size"),
    ubigintnn!("sum_of_other_index_sizes"),
];
const INNODB_TABLE_STATS_INDEXES: &[SystemTableIndex] =
    &[primary!(&["database_name", "table_name"])];

const INNODB_INDEX_STATS_COLUMNS: &[SystemColumn] = &[
    vc!("database_name", 192),
    vc!("table_name", 199),
    vc!("index_name", 64),
    timestampnn!("last_update"),
    vc!("stat_name", 64),
    ubigintnn!("stat_value"),
    ubigintn!("sample_size"),
    vc!("stat_description", 1024),
];
const INNODB_INDEX_STATS_INDEXES: &[SystemTableIndex] = &[primary!(&[
    "database_name",
    "table_name",
    "index_name",
    "stat_name"
])];

const USER_CREATE_SQL: &str = "CREATE TABLE `user` (\n  `Host` char(255) NOT NULL,\n  `User` char(32) NOT NULL,\n  `Select_priv` enum('N','Y') NOT NULL,\n  `Insert_priv` enum('N','Y') NOT NULL,\n  `Update_priv` enum('N','Y') NOT NULL,\n  `Delete_priv` enum('N','Y') NOT NULL,\n  `Create_priv` enum('N','Y') NOT NULL,\n  `Drop_priv` enum('N','Y') NOT NULL,\n  `Reload_priv` enum('N','Y') NOT NULL,\n  `Shutdown_priv` enum('N','Y') NOT NULL,\n  `Process_priv` enum('N','Y') NOT NULL,\n  `File_priv` enum('N','Y') NOT NULL,\n  `Grant_priv` enum('N','Y') NOT NULL,\n  `References_priv` enum('N','Y') NOT NULL,\n  `Index_priv` enum('N','Y') NOT NULL,\n  `Alter_priv` enum('N','Y') NOT NULL,\n  `Show_db_priv` enum('N','Y') NOT NULL,\n  `Super_priv` enum('N','Y') NOT NULL,\n  `Create_tmp_table_priv` enum('N','Y') NOT NULL,\n  `Lock_tables_priv` enum('N','Y') NOT NULL,\n  `Execute_priv` enum('N','Y') NOT NULL,\n  `Repl_slave_priv` enum('N','Y') NOT NULL,\n  `Repl_client_priv` enum('N','Y') NOT NULL,\n  `Create_view_priv` enum('N','Y') NOT NULL,\n  `Show_view_priv` enum('N','Y') NOT NULL,\n  `Create_routine_priv` enum('N','Y') NOT NULL,\n  `Alter_routine_priv` enum('N','Y') NOT NULL,\n  `Create_user_priv` enum('N','Y') NOT NULL,\n  `Event_priv` enum('N','Y') NOT NULL,\n  `Trigger_priv` enum('N','Y') NOT NULL,\n  `Create_tablespace_priv` enum('N','Y') NOT NULL,\n  `ssl_type` enum('','ANY','X509','SPECIFIED') NOT NULL,\n  `ssl_cipher` blob NOT NULL,\n  `x509_issuer` blob NOT NULL,\n  `x509_subject` blob NOT NULL,\n  `max_questions` int unsigned NOT NULL,\n  `max_updates` int unsigned NOT NULL,\n  `max_connections` int unsigned NOT NULL,\n  `max_user_connections` int unsigned NOT NULL,\n  `plugin` char(64) NOT NULL,\n  `authentication_string` text,\n  `password_expired` enum('N','Y') NOT NULL,\n  `password_last_changed` timestamp NULL,\n  `password_lifetime` smallint unsigned,\n  `account_locked` enum('N','Y') NOT NULL,\n  `Create_role_priv` enum('N','Y') NOT NULL,\n  `Drop_role_priv` enum('N','Y') NOT NULL,\n  `Password_reuse_history` smallint unsigned,\n  `Password_reuse_time` smallint unsigned,\n  `Password_require_current` enum('N','Y'),\n  `User_attributes` json,\n  PRIMARY KEY (`Host`,`User`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin";

const SERVER_COST_CREATE_SQL: &str = "CREATE TABLE `server_cost` (\n  `cost_name` varchar(64) NOT NULL,\n  `cost_value` float,\n  `last_update` timestamp NOT NULL,\n  `comment` varchar(1024),\n  `default_value` float GENERATED ALWAYS AS (CASE `cost_name` WHEN 'disk_temptable_create_cost' THEN 20.0 WHEN 'disk_temptable_row_cost' THEN 0.5 WHEN 'key_compare_cost' THEN 0.05 WHEN 'memory_temptable_create_cost' THEN 1.0 WHEN 'memory_temptable_row_cost' THEN 0.1 WHEN 'row_evaluate_cost' THEN 0.1 ELSE NULL END) VIRTUAL,\n  PRIMARY KEY (`cost_name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci";

const GENERAL_LOG_CREATE_SQL: &str = "CREATE TABLE `general_log` (\n  `event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n  `user_host` mediumtext NOT NULL,\n  `thread_id` bigint unsigned NOT NULL,\n  `server_id` int unsigned NOT NULL,\n  `command_type` varchar(64) NOT NULL,\n  `argument` mediumblob NOT NULL\n) ENGINE=CSV DEFAULT CHARSET=utf8mb3";

const SLOW_LOG_CREATE_SQL: &str = "CREATE TABLE `slow_log` (\n  `start_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n  `user_host` mediumtext NOT NULL,\n  `query_time` time(6) NOT NULL,\n  `lock_time` time(6) NOT NULL,\n  `rows_sent` int NOT NULL,\n  `rows_examined` int NOT NULL,\n  `db` varchar(512) NOT NULL,\n  `last_insert_id` int NOT NULL,\n  `insert_id` int NOT NULL,\n  `server_id` int unsigned NOT NULL,\n  `sql_text` mediumblob NOT NULL,\n  `thread_id` bigint unsigned NOT NULL\n) ENGINE=CSV DEFAULT CHARSET=utf8mb3";

const COMPONENT_CREATE_SQL: &str = "CREATE TABLE `component` (\n  `component_id` int unsigned NOT NULL AUTO_INCREMENT,\n  `component_group_id` int unsigned NOT NULL,\n  `component_urn` text NOT NULL,\n  PRIMARY KEY (`component_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";

const PASSWORD_HISTORY_CREATE_SQL: &str = "CREATE TABLE `password_history` (\n  `Host` char(255) NOT NULL,\n  `User` char(32) NOT NULL,\n  `Password_timestamp` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n  `Password` text,\n  PRIMARY KEY (`Host`,`User`,`Password_timestamp` DESC)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";

const SERVERS_CREATE_SQL: &str = "CREATE TABLE `servers` (\n  `Server_name` char(64) NOT NULL,\n  `Host` char(255) CHARACTER SET ascii NOT NULL,\n  `Db` char(64) NOT NULL,\n  `Username` char(64) NOT NULL,\n  `Password` char(64) NOT NULL,\n  `Port` int NOT NULL,\n  `Socket` char(64) NOT NULL,\n  `Wrapper` char(64) NOT NULL,\n  `Owner` char(64) NOT NULL,\n  PRIMARY KEY (`Server_name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";

const INNODB_TABLE_STATS_CREATE_SQL: &str = "CREATE TABLE `innodb_table_stats` (\n  `database_name` varchar(192) NOT NULL,\n  `table_name` varchar(199) NOT NULL,\n  `last_update` timestamp NOT NULL,\n  `n_rows` bigint unsigned NOT NULL,\n  `clustered_index_size` bigint unsigned NOT NULL,\n  `sum_of_other_index_sizes` bigint unsigned NOT NULL,\n  PRIMARY KEY (`database_name`,`table_name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";

const fn mysql_table(
    name: &'static str,
    columns: &'static [SystemColumn],
    indexes: &'static [SystemTableIndex],
    engine: &'static str,
    create_sql: Option<&'static str>,
) -> SystemTableDef {
    SystemTableDef {
        schema: "mysql",
        name,
        table_type: "BASE TABLE",
        columns,
        engine,
        indexes,
        create_sql,
        hidden: false,
    }
}

pub(crate) const MYSQL_DEFS: &[SystemTableDef] = &[
    mysql_table(
        "columns_priv",
        COLUMNS_PRIV_COLUMNS,
        COLUMNS_PRIV_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "component",
        COMPONENT_COLUMNS,
        COMPONENT_INDEXES,
        "InnoDB",
        Some(COMPONENT_CREATE_SQL),
    ),
    mysql_table("db", DB_COLUMNS, DB_INDEXES, "InnoDB", None),
    mysql_table(
        "default_roles",
        DEFAULT_ROLES_COLUMNS,
        DEFAULT_ROLES_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "engine_cost",
        ENGINE_COST_COLUMNS,
        ENGINE_COST_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table("func", FUNC_COLUMNS, FUNC_INDEXES, "InnoDB", None),
    mysql_table(
        "general_log",
        GENERAL_LOG_COLUMNS,
        &[],
        "CSV",
        Some(GENERAL_LOG_CREATE_SQL),
    ),
    mysql_table(
        "global_grants",
        GLOBAL_GRANTS_COLUMNS,
        GLOBAL_GRANTS_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "gtid_executed",
        GTID_EXECUTED_COLUMNS,
        GTID_EXECUTED_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "help_category",
        HELP_CATEGORY_COLUMNS,
        HELP_CATEGORY_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "help_keyword",
        HELP_KEYWORD_COLUMNS,
        HELP_KEYWORD_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "help_relation",
        HELP_RELATION_COLUMNS,
        HELP_RELATION_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "help_topic",
        HELP_TOPIC_COLUMNS,
        HELP_TOPIC_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "innodb_index_stats",
        INNODB_INDEX_STATS_COLUMNS,
        INNODB_INDEX_STATS_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "innodb_table_stats",
        INNODB_TABLE_STATS_COLUMNS,
        INNODB_TABLE_STATS_INDEXES,
        "InnoDB",
        Some(INNODB_TABLE_STATS_CREATE_SQL),
    ),
    mysql_table(
        "ndb_binlog_index",
        NDB_BINLOG_INDEX_COLUMNS,
        NDB_BINLOG_INDEX_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "password_history",
        PASSWORD_HISTORY_COLUMNS,
        PASSWORD_HISTORY_INDEXES,
        "InnoDB",
        Some(PASSWORD_HISTORY_CREATE_SQL),
    ),
    mysql_table("plugin", PLUGIN_COLUMNS, PLUGIN_INDEXES, "InnoDB", None),
    mysql_table(
        "procs_priv",
        PROCS_PRIV_COLUMNS,
        PROCS_PRIV_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "proxies_priv",
        PROXIES_PRIV_COLUMNS,
        PROXIES_PRIV_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "replication_asynchronous_connection_failover",
        REPL_ASYNC_FAILOVER_COLUMNS,
        REPL_ASYNC_FAILOVER_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "replication_asynchronous_connection_failover_managed",
        REPL_ASYNC_FAILOVER_MANAGED_COLUMNS,
        REPL_ASYNC_FAILOVER_MANAGED_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "replication_group_configuration_version",
        REPL_GROUP_CONFIGURATION_VERSION_COLUMNS,
        REPL_GROUP_CONFIGURATION_VERSION_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "replication_group_member_actions",
        REPL_GROUP_MEMBER_ACTIONS_COLUMNS,
        REPL_GROUP_MEMBER_ACTIONS_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "role_edges",
        ROLE_EDGES_COLUMNS,
        ROLE_EDGES_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "server_cost",
        SERVER_COST_COLUMNS,
        SERVER_COST_INDEXES,
        "InnoDB",
        Some(SERVER_COST_CREATE_SQL),
    ),
    mysql_table(
        "servers",
        SERVERS_COLUMNS,
        SERVERS_INDEXES,
        "InnoDB",
        Some(SERVERS_CREATE_SQL),
    ),
    mysql_table(
        "slave_master_info",
        SLAVE_MASTER_INFO_COLUMNS,
        SLAVE_MASTER_INFO_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "slave_relay_log_info",
        SLAVE_RELAY_LOG_INFO_COLUMNS,
        SLAVE_RELAY_LOG_INFO_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "slave_worker_info",
        SLAVE_WORKER_INFO_COLUMNS,
        SLAVE_WORKER_INFO_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "slow_log",
        SLOW_LOG_COLUMNS,
        &[],
        "CSV",
        Some(SLOW_LOG_CREATE_SQL),
    ),
    mysql_table(
        "tables_priv",
        TABLES_PRIV_COLUMNS,
        TABLES_PRIV_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "time_zone",
        TIME_ZONE_COLUMNS,
        TIME_ZONE_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "time_zone_leap_second",
        TIME_ZONE_LEAP_SECOND_COLUMNS,
        TIME_ZONE_LEAP_SECOND_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "time_zone_name",
        TIME_ZONE_NAME_COLUMNS,
        TIME_ZONE_NAME_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "time_zone_transition",
        TIME_ZONE_TRANSITION_COLUMNS,
        TIME_ZONE_TRANSITION_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "time_zone_transition_type",
        TIME_ZONE_TRANSITION_TYPE_COLUMNS,
        TIME_ZONE_TRANSITION_TYPE_INDEXES,
        "InnoDB",
        None,
    ),
    mysql_table(
        "user",
        USER_COLUMNS,
        USER_INDEXES,
        "InnoDB",
        Some(USER_CREATE_SQL),
    ),
];

pub(crate) const HIDDEN_DD_TABLES: &[&str] = &[
    "schemata",
    "tables",
    "table_stats",
    "columns",
    "indexes",
    "index_column_usage",
    "check_constraints",
    "views",
    "view_table_usage",
    "routines",
    "parameters",
    "triggers",
    "events",
    "table_partitions",
    "column_type_elements",
    "foreign_keys",
    "foreign_key_column_usage",
    "dd_properties",
    "users",
    "grants",
    "restrictions",
    "sequences",
    "sequence_values",
    "sequence_retirements",
    "innodb_dynamic_metadata",
    "mydb_ddl_retirements",
];

pub(crate) fn is_hidden_dd_table(schema: &str, table: &str) -> bool {
    schema.eq_ignore_ascii_case("mysql")
        && HIDDEN_DD_TABLES
            .iter()
            .any(|name| name.eq_ignore_ascii_case(table))
}

pub(crate) fn inaccessible_table_error(table: &str) -> QueryError {
    error(
        crate::error::catalog::ER_NO_SYSTEM_TABLE_ACCESS,
        format!("Access to data dictionary table 'mysql.{table}' is rejected."),
    )
}

pub(crate) fn rows(db: &Database, table: &str) -> Result<Vec<Vec<Value>>, QueryError> {
    match table {
        "user" => Ok(mysql_user_rows(db)),
        "global_grants" => Ok(mysql_global_grants_rows(db)),
        "role_edges" => Ok(mysql_role_edges_rows(db)),
        "default_roles" => Ok(mysql_default_roles_rows(db)),
        "db" => Ok(mysql_db_rows(db)),
        "tables_priv" => Ok(mysql_tables_priv_rows(db)),
        "columns_priv" => Ok(mysql_columns_priv_rows(db)),
        "procs_priv" => Ok(mysql_procs_priv_rows(db)),
        "password_history" => Ok(mysql_password_history_rows(db)),
        "proxies_priv" => Ok(mysql_proxies_priv_rows(db)),
        "general_log" => crate::engine::logging::general_log_rows(db.data_dir()),
        "slow_log" => crate::engine::logging::slow_log_rows(db.data_dir()),
        "innodb_table_stats" => innodb_table_stats_rows(db),
        "innodb_index_stats" => innodb_index_stats_rows(db),
        "server_cost" => Ok(server_cost_rows()),
        "engine_cost" => Ok(engine_cost_rows()),
        "gtid_executed" => Ok(gtid_executed_rows()),
        "time_zone" => Ok(time_zone_rows()),
        "time_zone_name" => Ok(time_zone_name_rows()),
        "time_zone_transition" => Ok(time_zone_transition_rows()),
        "time_zone_transition_type" => Ok(time_zone_transition_type_rows()),
        // `time_zone_leap_second` stays empty: mydb tracks UTC without leap
        // insertion (Use_leap_seconds = 'N' for every zone), so there are no
        // corrections to report. Honest empty, not a stub.
        _ => Ok(Vec::new()),
    }
}

/// `mysql.gtid_executed` — the set of transaction GTIDs applied on this
/// server, one row per `(source_uuid, interval)`. Sourced from the same
/// `@@global.gtid_executed` string the binlog/replication layer maintains, so
/// the table and the variable never disagree. Empty when GTID mode is off /
/// nothing has been executed (honest empty).
fn gtid_executed_rows() -> Vec<Vec<Value>> {
    let set = crate::binlog::variables_snapshot()
        .into_iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("gtid_executed"))
        .map(|(_, value)| value)
        .unwrap_or_default();
    parse_gtid_set_intervals(&set)
}

/// Parse a GTID set (`uuid:1-5:9,uuid2:1-3`) into `mysql.gtid_executed` rows
/// `(source_uuid, interval_start, interval_end)`, both bounds inclusive.
fn parse_gtid_set_intervals(set: &str) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for group in set.split(',') {
        let group = group.trim();
        if group.is_empty() {
            continue;
        }
        let mut parts = group.split(':');
        let uuid = match parts.next() {
            Some(u) if !u.trim().is_empty() => u.trim().to_string(),
            _ => continue,
        };
        for interval in parts {
            let interval = interval.trim();
            if interval.is_empty() {
                continue;
            }
            let (start, end) = match interval.split_once('-') {
                Some((a, b)) => (a.trim().parse::<i64>().ok(), b.trim().parse::<i64>().ok()),
                None => {
                    let single = interval.parse::<i64>().ok();
                    (single, single)
                }
            };
            if let (Some(start), Some(end)) = (start, end) {
                rows.push(vec![s(&uuid), Value::Int(start), Value::Int(end)]);
            }
        }
    }
    rows
}

/// `mysql.time_zone` — one row per named zone the host zoneinfo database
/// exposes, with a stable id. `Use_leap_seconds` is always `N`: mydb reads
/// the leap-free (non-`right/`) TZif variants and does no leap insertion.
fn time_zone_rows() -> Vec<Vec<Value>> {
    crate::engine::tz::zone_catalog()
        .zones
        .iter()
        .map(|z| vec![Value::UInt(z.id as u64), s("N")])
        .collect()
}

/// `mysql.time_zone_name` — the name ↔ id mapping used to resolve
/// `CONVERT_TZ('…','Europe/Berlin','UTC')` and `SET time_zone='…'`.
fn time_zone_name_rows() -> Vec<Vec<Value>> {
    crate::engine::tz::zone_catalog()
        .zones
        .iter()
        .map(|z| vec![s(&z.name), Value::UInt(z.id as u64)])
        .collect()
}

/// `mysql.time_zone_transition` — the UTC-offset transition history per
/// zone: `(Time_zone_id, Transition_time, Transition_type_id)`, sourced from
/// the parsed TZif transition table.
fn time_zone_transition_rows() -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for zone in &crate::engine::tz::zone_catalog().zones {
        for (utc_secs, type_id) in zone.data.transitions() {
            rows.push(vec![
                Value::UInt(zone.id as u64),
                Value::Int(utc_secs),
                Value::UInt(type_id as u64),
            ]);
        }
    }
    rows
}

/// `mysql.time_zone_transition_type` — the local-time type table
/// (`ttinfo` records) per zone: `(Time_zone_id, Transition_type_id, Offset,
/// Is_DST, Abbreviation)`.
fn time_zone_transition_type_rows() -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for zone in &crate::engine::tz::zone_catalog().zones {
        for (type_id, ty) in zone.data.local_time_types().iter().enumerate() {
            rows.push(vec![
                Value::UInt(zone.id as u64),
                Value::UInt(type_id as u64),
                Value::Int(ty.utoff as i64),
                Value::UInt(u64::from(ty.is_dst)),
                s(&ty.abbreviation),
            ]);
        }
    }
    rows
}

/// Format a unix-ms timestamp as MySQL's `YYYY-MM-DD HH:MM:SS`. A 0 /
/// missing analyzed timestamp degrades to the epoch (the column is
/// declared `NOT NULL`).
fn innodb_stats_timestamp(unix_ms: Option<u64>) -> Value {
    match unix_ms {
        Some(ms) if ms > 0 => crate::engine::information_schema::mysql_time_value(ms),
        _ => timestamp_zero(),
    }
}

fn innodb_stats_targets(table: &TableSchema) -> Vec<(String, u64, Option<&PartitionStats>)> {
    let Some(partitioning) = &table.partitioning else {
        return vec![(table.name.clone(), table.id, None)];
    };
    partitioning
        .partitions
        .iter()
        .flat_map(|partition| {
            if partition.subpartitions.is_empty() {
                vec![(
                    format!("{}#P#{}", table.name, partition.name),
                    partition.storage_id,
                    table.stats.partition_stats.get(&partition.storage_id),
                )]
            } else {
                partition
                    .subpartitions
                    .iter()
                    .map(|sub| {
                        (
                            format!("{}#P#{}#SP#{}", table.name, partition.name, sub.name),
                            sub.storage_id,
                            table.stats.partition_stats.get(&sub.storage_id),
                        )
                    })
                    .collect()
            }
        })
        .collect()
}

/// `mysql.innodb_table_stats` — one row per physical table/partition leaf.
/// For unpartitioned tables, `n_rows` folds the live row delta into
/// the analyzed row count;
/// `clustered_index_size` is the heap page count (analyzed if present,
/// else the live buffered-pages count); `sum_of_other_index_sizes` sums
/// the secondary-index page counts.
fn innodb_table_stats_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut rows = Vec::new();
    let catalog = db.catalog();
    for header in catalog.tables.values() {
        if header.system || crate::storage::naming::is_system_database(&header.database) {
            continue;
        }
        let table = if header.columns.is_empty() {
            db.table(&format!("{}.{}", header.database, header.name))?
        } else {
            std::sync::Arc::clone(header)
        };
        for (name, storage_id, part_stats) in innodb_stats_targets(&table) {
            let n_rows = part_stats.map_or_else(
                || {
                    if table.partitioning.is_some() {
                        0
                    } else {
                        table
                            .stats
                            .row_count
                            .saturating_add_signed(db.table_live_row_delta(table.id))
                    }
                },
                |stats| stats.row_count,
            );
            let analyzed_pages = part_stats.map_or_else(
                || {
                    if table.partitioning.is_some() {
                        0
                    } else {
                        table.stats.heap_page_count
                    }
                },
                |stats| stats.heap_page_count,
            );
            let clustered_pages = if analyzed_pages > 0 {
                analyzed_pages
            } else {
                db.heap_page_count(storage_id)
            };
            let other_index_pages: u64 = table
                .indexes
                .iter()
                .filter(|idx| !idx.primary)
                .map(|idx| db.index_page_count(storage_id, &idx.name))
                .sum();
            rows.push(vec![
                s(&table.database),
                s(&name),
                innodb_stats_timestamp(
                    if table.partitioning.is_some() {
                        part_stats.and_then(|stats| stats.analyzed_at_unix_ms)
                    } else {
                        table.stats.analyzed_at_unix_ms
                    }
                    .or(Some(table.created_ms)),
                ),
                Value::UInt(n_rows),
                Value::UInt(clustered_pages),
                Value::UInt(other_index_pages),
            ]);
        }
    }
    Ok(rows)
}

/// `mysql.innodb_index_stats` — InnoDB emits, per index:
///   * `n_diff_pfx01 .. n_diff_pfx0N` — distinct key count for each
///     leading-column prefix (N = key columns),
///   * `n_leaf_pages` — index leaf page count,
///   * `size` — total index page count.
/// We back the `n_diff_pfx*` values with the per-prefix composite NDV
/// stats (`index_prefix_cardinality`) and the page counts with the live
/// buffered page count.
fn innodb_index_stats_rows(db: &Database) -> Result<Vec<Vec<Value>>, QueryError> {
    let mut rows = Vec::new();
    let catalog = db.catalog();
    for header in catalog.tables.values() {
        if header.system || crate::storage::naming::is_system_database(&header.database) {
            continue;
        }
        let table = if header.columns.is_empty() {
            db.table(&format!("{}.{}", header.database, header.name))?
        } else {
            std::sync::Arc::clone(header)
        };
        for (name, storage_id, part_stats) in innodb_stats_targets(&table) {
            let last_update = innodb_stats_timestamp(
                if table.partitioning.is_some() {
                    part_stats.and_then(|stats| stats.analyzed_at_unix_ms)
                } else {
                    table.stats.analyzed_at_unix_ms
                }
                .or(Some(table.created_ms)),
            );
            for index in &table.indexes {
                // Skip non-BTREE indexes (FULLTEXT / SPATIAL / HNSW): InnoDB's
                // persistent index stats only describe ordinary key indexes,
                // and prefix NDV is meaningless for them.
                if !index.index_type.eq_ignore_ascii_case("BTREE") {
                    continue;
                }
                let page_count = db.index_page_count(storage_id, &index.name);
                let leaf_pages = part_stats
                    .and_then(|stats| {
                        stats
                            .indexes
                            .get(&crate::storage::naming::normalize_name(&index.name))
                    })
                    .or_else(|| {
                        table
                            .stats
                            .indexes
                            .get(&crate::storage::naming::normalize_name(&index.name))
                    })
                    .and_then(|stats| stats.leaf_page_count)
                    .unwrap_or(page_count)
                    .min(page_count);
                // n_diff_pfxNN — one row per leading-column prefix.
                for (pos, _col) in index.columns.iter().enumerate() {
                    let prefix_len = pos + 1;
                    let ndv = part_stats
                        .and_then(|stats| {
                            stats
                                .indexes
                                .get(&crate::storage::naming::normalize_name(&index.name))
                        })
                        .and_then(|stats| stats.prefix_distinct_counts.get(pos))
                        .copied()
                        .or_else(|| {
                            (part_stats.is_none())
                                .then(|| table.index_prefix_cardinality(&index.name, prefix_len))
                                .flatten()
                        })
                        .unwrap_or(0);
                    let cols_desc = index.columns[..prefix_len].join(",");
                    rows.push(vec![
                        s(&table.database),
                        s(&name),
                        s(&index.name),
                        last_update.clone(),
                        s(&format!("n_diff_pfx{prefix_len:02}")),
                        Value::UInt(ndv),
                        Value::Null,
                        s(&cols_desc),
                    ]);
                }
                // n_leaf_pages.
                rows.push(vec![
                    s(&table.database),
                    s(&name),
                    s(&index.name),
                    last_update.clone(),
                    s("n_leaf_pages"),
                    Value::UInt(leaf_pages),
                    Value::Null,
                    s("Number of leaf pages in the index"),
                ]);
                // size — total pages.
                rows.push(vec![
                    s(&table.database),
                    s(&name),
                    s(&index.name),
                    last_update.clone(),
                    s("size"),
                    Value::UInt(page_count),
                    Value::Null,
                    s("Number of pages in the index"),
                ]);
            }
        }
    }
    Ok(rows)
}

pub(crate) fn column_key(def: &SystemTableDef, column: &str) -> &'static str {
    if def.indexes.iter().any(|idx| {
        idx.name == "PRIMARY" && idx.columns.iter().any(|c| c.eq_ignore_ascii_case(column))
    }) {
        return "PRI";
    }
    if def.indexes.iter().any(|idx| {
        idx.unique
            && idx.columns.len() == 1
            && idx.name != "PRIMARY"
            && idx
                .columns
                .first()
                .is_some_and(|c| c.eq_ignore_ascii_case(column))
    }) {
        return "UNI";
    }
    if def.indexes.iter().any(|idx| {
        (!idx.unique || idx.columns.len() > 1)
            && idx
                .columns
                .first()
                .is_some_and(|c| c.eq_ignore_ascii_case(column))
    }) {
        return "MUL";
    }
    ""
}

fn mysql_user_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    auth.accounts
        .values()
        .map(|account| {
            let has_global = |privilege: Privilege| {
                auth.grants.iter().any(|grant| {
                    grant.grantee == account.key
                        && matches!(grant.scope, PrivilegeScope::Global)
                        && (grant.privilege == privilege
                            || matches!(grant.privilege, Privilege::Dynamic(ref name) if name == "ALL"))
                })
            };
            let has_global_grant_option = auth.grants.iter().any(|grant| {
                grant.grantee == account.key
                    && matches!(grant.scope, PrivilegeScope::Global)
                    && grant.grant_option && !matches!(grant.privilege,Privilege::Dynamic(_))
            });
            let (ssl_type, ssl_cipher, x509_issuer, x509_subject) = match &account.ssl_type {
                crate::engine::auth::SslRequirement::None => ("", Vec::new(), Vec::new(), Vec::new()),
                crate::engine::auth::SslRequirement::Ssl => ("ANY", Vec::new(), Vec::new(), Vec::new()),
                crate::engine::auth::SslRequirement::X509 => {
                    ("X509", Vec::new(), Vec::new(), Vec::new())
                }
                crate::engine::auth::SslRequirement::Specified {
                    cipher,
                    issuer,
                    subject,
                } => (
                    "SPECIFIED",
                    cipher.clone().unwrap_or_default().into_bytes(),
                    issuer.clone().unwrap_or_default().into_bytes(),
                    subject.clone().unwrap_or_default().into_bytes(),
                ),
            };
            let mut user_attributes = account
                .user_attributes_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            // Policy metadata comes from the effective account settings, never
            // from a user-supplied ATTRIBUTE with the same reserved key.
            if let Some(object) = user_attributes.as_object_mut() {
                object.remove("Password_locking");
                if account.failed_login_attempts != 0 || account.password_lock_time_days.is_some() {
                    object.insert("Password_locking".into(), serde_json::json!({
                        "failed_login_attempts": account.failed_login_attempts,
                        "password_lock_time_days": account.password_lock_time_days.unwrap_or(0),
                    }));
                }
            }
            let mut restrictions_by_db: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for restriction in auth
                .restrictions
                .iter()
                .filter(|restriction| restriction.grantee == account.key)
            {
                restrictions_by_db
                    .entry(restriction.database.clone())
                    .or_default()
                    .push(restriction.privilege.sql_name());
            }
            if !restrictions_by_db.is_empty() {
                let rows = restrictions_by_db
                    .into_iter()
                    .map(|(database, mut privileges)| {
                        privileges.sort();
                        privileges.dedup();
                        serde_json::json!({
                            "Database": database,
                            "Privileges": privileges,
                        })
                    })
                    .collect::<Vec<_>>();
                if let Some(object) = user_attributes.as_object_mut() {
                    object.insert("Restrictions".to_string(), serde_json::Value::Array(rows));
                }
            }
            vec![
                s(&account.key.host),
                s(&account.key.user),
                yes_y(has_global(Privilege::Select)),
                yes_y(has_global(Privilege::Insert)),
                yes_y(has_global(Privilege::Update)),
                yes_y(has_global(Privilege::Delete)),
                yes_y(has_global(Privilege::Create)),
                yes_y(has_global(Privilege::Drop)),
                yes_y(has_global(Privilege::Reload)),
                yes_y(has_global(Privilege::Shutdown)),
                yes_y(has_global(Privilege::Process)),
                yes_y(has_global(Privilege::File)),
                yes_y(has_global(Privilege::GrantOption) || has_global_grant_option),
                yes_y(has_global(Privilege::References)),
                yes_y(has_global(Privilege::Index)),
                yes_y(has_global(Privilege::Alter)),
                yes_y(has_global(Privilege::ShowDb)),
                yes_y(has_global(Privilege::Super)),
                yes_y(has_global(Privilege::CreateTmpTable)),
                yes_y(has_global(Privilege::LockTables)),
                yes_y(has_global(Privilege::Execute)),
                yes_y(has_global(Privilege::ReplSlave)),
                yes_y(has_global(Privilege::ReplClient)),
                yes_y(has_global(Privilege::CreateView)),
                yes_y(has_global(Privilege::ShowView)),
                yes_y(has_global(Privilege::CreateRoutine)),
                yes_y(has_global(Privilege::AlterRoutine)),
                yes_y(has_global(Privilege::CreateUser)),
                yes_y(has_global(Privilege::Event)),
                yes_y(has_global(Privilege::Trigger)),
                yes_y(has_global(Privilege::CreateTablespace)),
                s(ssl_type),
                Value::Bytes(ssl_cipher),
                Value::Bytes(x509_issuer),
                Value::Bytes(x509_subject),
                Value::UInt(account.max_questions as u64),
                Value::UInt(account.max_updates as u64),
                Value::UInt(account.max_connections as u64),
                Value::UInt(account.max_user_connections as u64),
                s(&account.plugin),
                s(&account.authentication_string),
                yes_y(account.password_expired),
                account
                    .password_last_changed_ms
                    .map(|ms| innodb_stats_timestamp(Some(ms)))
                    .unwrap_or(Value::Null),
                account
                    .password_lifetime_days
                    .map(|days| Value::UInt(days as u64))
                    .unwrap_or(Value::Null),
                yes_y(account.account_locked),
                yes_y(has_global(Privilege::CreateRole)),
                yes_y(has_global(Privilege::DropRole)),
                account
                    .password_reuse_history
                    .map(|days| Value::UInt(days as u64))
                    .unwrap_or(Value::Null),
                account
                    .password_reuse_time_days
                    .map(|days| Value::UInt(days as u64))
                    .unwrap_or(Value::Null),
                account
                    .password_require_current
                    .map(yes_y)
                    .unwrap_or(Value::Null),
                (user_attributes != serde_json::json!({}))
                    .then(|| Value::String(user_attributes.to_string()))
                    .unwrap_or(Value::Null),
            ]
        })
        .collect()
}

fn mysql_global_grants_rows(db: &Database) -> Vec<Vec<Value>> {
    db.auth_catalog()
        .grants
        .iter()
        .filter(|grant| matches!(grant.scope, PrivilegeScope::Global))
        .filter(|grant| grant.privilege.mysql_user_column().is_none())
        .filter(|grant| !matches!(grant.privilege, Privilege::Dynamic(ref name) if name == "ALL"))
        .map(|grant| {
            vec![
                s(&grant.grantee.user),
                s(&grant.grantee.host),
                s(&grant.privilege.sql_name()),
                yes_y(grant.grant_option),
            ]
        })
        .collect()
}

fn mysql_password_history_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let mut rows = auth
        .accounts
        .values()
        .flat_map(|account| {
            account.password_history.iter().map(|entry| {
                let secs = entry.changed_ms as i64 / 1000;
                let offset = crate::engine::expr::session_time_offset_seconds_at(secs);
                let timestamp = chrono::DateTime::from_timestamp_millis(
                    (entry.changed_ms as i64).saturating_add(offset.saturating_mul(1000)),
                )
                .map(|value| value.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                .unwrap_or_else(|| "1970-01-01 00:00:00.000000".to_string());
                vec![
                    s(&account.key.host),
                    s(&account.key.user),
                    s(&timestamp),
                    s(&entry.authentication_string),
                ]
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (
            left[0].to_display_string(),
            left[1].to_display_string(),
            left[2].to_display_string(),
        )
            .cmp(&(
                right[0].to_display_string(),
                right[1].to_display_string(),
                right[2].to_display_string(),
            ))
    });
    rows
}

fn mysql_proxies_priv_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let mut rows = auth
        .grants
        .iter()
        .filter_map(|grant| {
            if !matches!(&grant.privilege, Privilege::Dynamic(name) if name == "PROXY") {
                return None;
            }
            let PrivilegeScope::Proxy { user, host } = &grant.scope else {
                return None;
            };
            Some(vec![
                s(&grant.grantee.host),
                s(&grant.grantee.user),
                s(&host),
                s(&user),
                Value::Int(i64::from(grant.grant_option)),
                s(&grant.grantor.current_user_value()),
                grant_timestamp(grant.updated_ms),
            ])
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (
            left[0].to_display_string(),
            left[1].to_display_string(),
            left[2].to_display_string(),
            left[3].to_display_string(),
        )
            .cmp(&(
                right[0].to_display_string(),
                right[1].to_display_string(),
                right[2].to_display_string(),
                right[3].to_display_string(),
            ))
    });
    rows
}

fn mysql_role_edges_rows(db: &Database) -> Vec<Vec<Value>> {
    db.auth_catalog()
        .role_edges
        .iter()
        .map(|edge| {
            vec![
                s(&edge.from_role.host),
                s(&edge.from_role.user),
                s(&edge.to_account.host),
                s(&edge.to_account.user),
                yes_y(edge.admin_option),
            ]
        })
        .collect()
}

fn mysql_default_roles_rows(db: &Database) -> Vec<Vec<Value>> {
    db.auth_catalog()
        .default_roles
        .iter()
        .map(|edge| {
            vec![
                s(&edge.to_account.host),
                s(&edge.to_account.user),
                s(&edge.from_role.host),
                s(&edge.from_role.user),
            ]
        })
        .collect()
}

fn mysql_db_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let mut grouped: BTreeMap<(AccountKey, String), (BTreeSet<Privilege>, bool)> = BTreeMap::new();
    for grant in &auth.grants {
        if let PrivilegeScope::Database { database } = &grant.scope {
            let entry = grouped
                .entry((grant.grantee.clone(), database.clone()))
                .or_insert_with(|| (BTreeSet::new(), false));
            entry.0.insert(grant.privilege.clone());
            entry.1 |= grant.grant_option;
        }
    }
    grouped
        .into_iter()
        .map(|((account, database), (privileges, grant_option))| {
            let has = |p| {
                privileges.contains(&p)
                    || privileges.contains(&Privilege::Dynamic("ALL".to_string()))
            };
            vec![
                s(&account.host),
                s(&database),
                s(&account.user),
                yes_y(has(Privilege::Select)),
                yes_y(has(Privilege::Insert)),
                yes_y(has(Privilege::Update)),
                yes_y(has(Privilege::Delete)),
                yes_y(has(Privilege::Create)),
                yes_y(has(Privilege::Drop)),
                yes_y(has(Privilege::GrantOption) || grant_option),
                yes_y(has(Privilege::References)),
                yes_y(has(Privilege::Index)),
                yes_y(has(Privilege::Alter)),
                yes_y(has(Privilege::CreateTmpTable)),
                yes_y(has(Privilege::LockTables)),
                yes_y(has(Privilege::CreateView)),
                yes_y(has(Privilege::ShowView)),
                yes_y(has(Privilege::CreateRoutine)),
                yes_y(has(Privilege::AlterRoutine)),
                yes_y(has(Privilege::Execute)),
                yes_y(has(Privilege::Event)),
                yes_y(has(Privilege::Trigger)),
            ]
        })
        .collect()
}

fn mysql_tables_priv_rows(db: &Database) -> Vec<Vec<Value>> {
    let auth = db.auth_catalog();
    let mut grouped: BTreeMap<(AccountKey, String, String), Vec<&crate::engine::auth::GrantEntry>> =
        BTreeMap::new();
    for grant in &auth.grants {
        let (database, table) = match &grant.scope {
            PrivilegeScope::Table { database, table }
            | PrivilegeScope::Column {
                database, table, ..
            } => (database, table),
            _ => continue,
        };
        grouped
            .entry((grant.grantee.clone(), database.clone(), table.clone()))
            .or_default()
            .push(grant);
    }
    grouped
        .into_iter()
        .map(|((account, database, table), grants)| {
            let latest = grants.iter().max_by_key(|g| g.updated_ms).unwrap();
            let mut table_privs = BTreeSet::new();
            let mut column_privs = BTreeSet::new();
            for grant in &grants {
                if matches!(grant.scope, PrivilegeScope::Column { .. }) {
                    if let Some(name) = column_priv_set_name(&grant.privilege) {
                        column_privs.insert(name);
                    }
                } else {
                    if matches!(grant.privilege, Privilege::Dynamic(ref name) if name == "ALL") {
                        table_privs.extend(TABLE_PRIV_SET_VALUES.iter().copied());
                    } else if let Some(name) = table_priv_set_name(&grant.privilege) {
                        table_privs.insert(name);
                    }
                    if grant.grant_option {
                        table_privs.insert("Grant");
                    }
                }
            }
            vec![
                s(&account.host),
                s(&database),
                s(&account.user),
                s(&table),
                s(&latest.grantor.current_user_value()),
                grant_timestamp(latest.updated_ms),
                s(&join_set(table_privs, TABLE_PRIV_SET_VALUES)),
                s(&join_set(column_privs, COLUMN_PRIV_SET_VALUES)),
            ]
        })
        .collect()
}

fn grant_timestamp(unix_ms: u64) -> Value {
    crate::engine::information_schema::mysql_time_value(unix_ms)
}

fn mysql_columns_priv_rows(db: &Database) -> Vec<Vec<Value>> {
    let mut grouped: BTreeMap<
        (AccountKey, String, String, String),
        (
            BTreeSet<&'static str>,
            Option<&crate::engine::auth::GrantEntry>,
        ),
    > = BTreeMap::new();
    let auth = db.auth_catalog();
    for grant in &auth.grants {
        let PrivilegeScope::Column {
            database,
            table,
            column,
        } = &grant.scope
        else {
            continue;
        };
        let entry = grouped
            .entry((
                grant.grantee.clone(),
                database.clone(),
                table.clone(),
                column.to_ascii_lowercase(),
            ))
            .or_default();
        if entry.1.is_none_or(|old| old.updated_ms <= grant.updated_ms) {
            entry.1 = Some(grant);
        }
        if matches!(grant.privilege, Privilege::Dynamic(ref name) if name == "ALL") {
            for name in COLUMN_PRIV_SET_VALUES {
                entry.0.insert(*name);
            }
        } else if let Some(name) = column_priv_set_name(&grant.privilege) {
            entry.0.insert(name);
        }
    }
    grouped
        .into_iter()
        .map(
            |((account, database, table, column), (privileges, latest))| {
                let latest = latest.unwrap();
                vec![
                    s(&account.host),
                    s(&database),
                    s(&account.user),
                    s(&table),
                    s(&column),
                    grant_timestamp(latest.updated_ms),
                    s(&join_set(privileges, COLUMN_PRIV_SET_VALUES)),
                ]
            },
        )
        .collect()
}

fn mysql_procs_priv_rows(db: &Database) -> Vec<Vec<Value>> {
    let mut grouped: BTreeMap<
        (AccountKey, String, String, String),
        (
            BTreeSet<&'static str>,
            Option<&crate::engine::auth::GrantEntry>,
        ),
    > = BTreeMap::new();
    let auth = db.auth_catalog();
    for grant in &auth.grants {
        let PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } = &grant.scope
        else {
            continue;
        };
        let routine_type = if kind.eq_ignore_ascii_case("function") {
            "FUNCTION".to_string()
        } else {
            "PROCEDURE".to_string()
        };
        let entry = grouped
            .entry((
                grant.grantee.clone(),
                database.clone(),
                routine.clone(),
                routine_type,
            ))
            .or_default();
        if entry.1.is_none_or(|old| old.updated_ms <= grant.updated_ms) {
            entry.1 = Some(grant);
        }
        if matches!(grant.privilege, Privilege::Dynamic(ref name) if name == "ALL") {
            for name in PROC_PRIV_SET_VALUES {
                entry.0.insert(*name);
            }
        } else if let Some(name) = proc_priv_set_name(&grant.privilege) {
            entry.0.insert(name);
        }
        if grant.grant_option {
            entry.0.insert("Grant");
        }
    }
    grouped
        .into_iter()
        .map(
            |((account, database, routine, kind), (privileges, latest))| {
                let latest = latest.unwrap();
                vec![
                    s(&account.host),
                    s(&database),
                    s(&account.user),
                    s(&routine),
                    s(&kind),
                    s(&latest.grantor.current_user_value()),
                    s(&join_set(privileges, PROC_PRIV_SET_VALUES)),
                    grant_timestamp(latest.updated_ms),
                ]
            },
        )
        .collect()
}

const TABLE_PRIV_SET_VALUES: &[&str] = &[
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Create",
    "Drop",
    "Grant",
    "References",
    "Index",
    "Alter",
    "Create View",
    "Show view",
    "Trigger",
];

const COLUMN_PRIV_SET_VALUES: &[&str] = &["Select", "Insert", "Update", "References"];
const PROC_PRIV_SET_VALUES: &[&str] = &["Execute", "Alter Routine", "Grant"];

fn table_priv_set_name(privilege: &Privilege) -> Option<&'static str> {
    match privilege {
        Privilege::Select => Some("Select"),
        Privilege::Insert => Some("Insert"),
        Privilege::Update => Some("Update"),
        Privilege::Delete => Some("Delete"),
        Privilege::Create => Some("Create"),
        Privilege::Drop => Some("Drop"),
        Privilege::GrantOption => Some("Grant"),
        Privilege::References => Some("References"),
        Privilege::Index => Some("Index"),
        Privilege::Alter => Some("Alter"),
        Privilege::CreateView => Some("Create View"),
        Privilege::ShowView => Some("Show view"),
        Privilege::Trigger => Some("Trigger"),
        _ => None,
    }
}

fn column_priv_set_name(privilege: &Privilege) -> Option<&'static str> {
    match privilege {
        Privilege::Select => Some("Select"),
        Privilege::Insert => Some("Insert"),
        Privilege::Update => Some("Update"),
        Privilege::References => Some("References"),
        _ => None,
    }
}

fn proc_priv_set_name(privilege: &Privilege) -> Option<&'static str> {
    match privilege {
        Privilege::Execute => Some("Execute"),
        Privilege::AlterRoutine => Some("Alter Routine"),
        Privilege::GrantOption => Some("Grant"),
        _ => None,
    }
}

fn join_set(values: BTreeSet<&'static str>, order: &[&'static str]) -> String {
    order
        .iter()
        .copied()
        .filter(|value| values.contains(value))
        .collect::<Vec<_>>()
        .join(",")
}

fn timestamp_zero() -> Value {
    s("1970-01-01 00:00:00")
}

/// `mysql.server_cost` — the optimizer's tunable per-row/temptable cost
/// constants. The `cost_value` column is NULL unless a DBA has overridden
/// the compiled-in default (mydb does not persist overrides here, so it is
/// always NULL); `default_value` carries the MySQL 8.0 compiled default so
/// optimizer-tuning tools see the same baseline. mydb's own CBO uses its
/// internal cost model (`engine/cbo/`); these rows are the MySQL-compatible
/// surface, not a live mirror of that model.
fn server_cost_rows() -> Vec<Vec<Value>> {
    const COSTS: &[(&str, f64)] = &[
        ("disk_temptable_create_cost", 20.0),
        ("disk_temptable_row_cost", 0.5),
        ("key_compare_cost", 0.05),
        ("memory_temptable_create_cost", 1.0),
        ("memory_temptable_row_cost", 0.1),
        ("row_evaluate_cost", 0.1),
    ];
    COSTS
        .iter()
        .map(|(name, default)| {
            vec![
                s(name),
                Value::Null,
                timestamp_zero(),
                Value::Null,
                Value::Double(*default),
            ]
        })
        .collect()
}

/// `mysql.engine_cost` — per-engine I/O cost constants. Same NULL-override /
/// default-baseline contract as [`server_cost_rows`]. MySQL ships the
/// `default` engine (device_type 0) rows; mydb reports its single engine the
/// same way.
fn engine_cost_rows() -> Vec<Vec<Value>> {
    const COSTS: &[(&str, f64)] = &[
        ("io_block_read_cost", 1.0),
        ("memory_block_read_cost", 0.25),
    ];
    COSTS
        .iter()
        .map(|(name, default)| {
            vec![
                s("default"),
                Value::Int(0),
                s(name),
                Value::Null,
                timestamp_zero(),
                Value::Null,
                Value::Double(*default),
            ]
        })
        .collect()
}

fn yes_y(value: bool) -> Value {
    s(if value { "Y" } else { "N" })
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtid_set_parses_into_interval_rows() {
        // Multiple sources, multi-interval, and single-transaction forms.
        let rows = parse_gtid_set_intervals("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:9,aaaa:1-3");
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            vec![
                s("3E11FA47-71CA-11E1-9E33-C80AA9429562"),
                Value::Int(1),
                Value::Int(5)
            ]
        );
        assert_eq!(rows[1][1], Value::Int(9), "single txn → start");
        assert_eq!(rows[1][2], Value::Int(9), "single txn → end == start");
        assert_eq!(rows[2][0], s("aaaa"));
        // Empty / whitespace GTID set → no rows (honest empty).
        assert!(parse_gtid_set_intervals("").is_empty());
        assert!(parse_gtid_set_intervals("   ").is_empty());
    }

    #[test]
    fn mysql80_visible_table_list_is_registered() {
        let names: Vec<_> = MYSQL_DEFS.iter().map(|def| def.name).collect();
        assert_eq!(
            names,
            vec![
                "columns_priv",
                "component",
                "db",
                "default_roles",
                "engine_cost",
                "func",
                "general_log",
                "global_grants",
                "gtid_executed",
                "help_category",
                "help_keyword",
                "help_relation",
                "help_topic",
                "innodb_index_stats",
                "innodb_table_stats",
                "ndb_binlog_index",
                "password_history",
                "plugin",
                "procs_priv",
                "proxies_priv",
                "replication_asynchronous_connection_failover",
                "replication_asynchronous_connection_failover_managed",
                "replication_group_configuration_version",
                "replication_group_member_actions",
                "role_edges",
                "server_cost",
                "servers",
                "slave_master_info",
                "slave_relay_log_info",
                "slave_worker_info",
                "slow_log",
                "tables_priv",
                "time_zone",
                "time_zone_leap_second",
                "time_zone_name",
                "time_zone_transition",
                "time_zone_transition_type",
                "user",
            ]
        );
    }

    #[test]
    fn mysql80_representative_shapes_match_expected_order() {
        let user = MYSQL_DEFS.iter().find(|def| def.name == "user").unwrap();
        assert_eq!(user.columns.len(), 51);
        assert_eq!(user.columns[0].name, "Host");
        assert_eq!(user.columns[50].name, "User_attributes");
        assert_eq!(user.indexes[0].columns, &["Host", "User"]);

        let db = MYSQL_DEFS.iter().find(|def| def.name == "db").unwrap();
        assert_eq!(db.columns.len(), 22);
        assert_eq!(db.indexes.len(), 2);

        let innodb = MYSQL_DEFS
            .iter()
            .find(|def| def.name == "innodb_table_stats")
            .unwrap();
        assert_eq!(innodb.columns[0].name, "database_name");
        assert_eq!(innodb.indexes[0].columns, &["database_name", "table_name"]);
    }
}
