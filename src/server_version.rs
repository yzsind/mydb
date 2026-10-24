//! Single source of truth for the MySQL compatibility version exposed by all
//! protocol, SQL, binlog and executable-comment surfaces.

pub const MYSQL_COMPAT_VERSION: &str = concat!(
    "8.0.46-mydb-",
    env!("CARGO_PKG_VERSION"),
    "-",
    env!("MYDB_BUILD_DATE")
);
pub const MYSQL_COMPAT_VERSION_ID: u32 = 80_046;
