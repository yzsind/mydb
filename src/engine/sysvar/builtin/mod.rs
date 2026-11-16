//! Built-in system variable registrations, split by topic.
//!
//! Each submodule's `register` function pushes its variables into the
//! shared registry. Order of registration only matters for `SHOW VARIABLES`
//! iteration if we relied on insertion order — we don't, the registry
//! sorts by name at finalize time.

use super::registry::Registry;

pub mod binlog;
pub mod cbo;
pub mod charset;
pub mod connection;
pub mod logging;
pub mod mydb_storage;
pub mod orm_stubs;
pub mod performance_schema;
pub mod query;
pub mod replication;
pub mod server;
pub mod transaction;
pub mod tz;

pub(crate) fn register_all(reg: &mut Registry) {
    server::register(reg);
    performance_schema::register(reg);
    connection::register(reg);
    transaction::register(reg);
    charset::register(reg);
    query::register(reg);
    tz::register(reg);
    replication::register(reg);
    logging::register(reg);
    binlog::register(reg);
    cbo::register(reg);
    mydb_storage::register(reg);
    orm_stubs::register(reg);
}
