#![recursion_limit = "256"]

// Library tests have their own executable, so main.rs's allocator does not
// apply to them. Use the server allocator for representative allocation and
// memory-reclamation behavior in the default test suite as well.
#[cfg(all(test, feature = "fast-alloc"))]
#[global_allocator]
static TEST_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(test, target_os = "linux", not(feature = "fast-alloc")))]
#[global_allocator]
static TEST_ALLOCATOR: crate::util::mapped_alloc::MappedSystem =
    crate::util::mapped_alloc::MappedSystem;

// Reuse nextest's already-built library test executable for fixture setup.
#[cfg(test)]
extern crate self as mydb;
#[cfg(test)]
#[path = "bin/test_seed.rs"]
mod test_seed_setup;

pub mod backup;
pub mod binlog;
#[cfg(test)]
mod build_metadata;
pub mod charset;
pub mod engine;
pub mod error;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
mod gtid;
pub mod mysql;
pub mod parser;
pub mod parser_partition;
pub mod planner;
pub mod planner_decorrelate;
pub(crate) mod platform;
pub mod repl;
#[cfg(feature = "mysql-replica")]
pub mod replication;
#[cfg(not(feature = "mysql-replica"))]
#[path = "replication_stub.rs"]
pub mod replication;
pub mod replication_columns;
mod replication_sql;
mod replication_types;
pub(crate) mod server_version;
pub(crate) mod sql_lex;
pub mod startup_config;
pub(crate) mod statement_effects;
pub mod storage;
pub mod types;
pub mod util;
pub mod vector;
pub mod wal;
pub mod xa;
