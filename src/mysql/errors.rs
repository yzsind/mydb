//! MySQL protocol error facade.
//!
//! The canonical types and generated MySQL 8.0.46 metadata live in
//! [`crate::error`].  This module remains as the protocol-facing import path.

pub use crate::error::catalog::*;
pub use crate::error::{ErrorSpec as MysqlErrorSpec, QueryError};

#[inline]
pub fn error(spec: MysqlErrorSpec, message: impl Into<String>) -> QueryError {
    spec.error(message)
}

pub fn unsupported(feature: impl AsRef<str>) -> QueryError {
    ER_NOT_SUPPORTED_YET.error(format!(
        "This version of mydb doesn't yet support {}",
        feature.as_ref()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql80_reference_pins_core_error_metadata() {
        let parse = lookup_by_code(1064).unwrap();
        assert_eq!(parse.symbol(), "ER_PARSE_ERROR");
        assert_eq!(parse.sql_state(), "42000");

        let no_table = lookup_by_code(1146).unwrap();
        assert_eq!(no_table.symbol(), "ER_NO_SUCH_TABLE");
        assert_eq!(no_table.sql_state(), "42S02");

        let check = lookup_by_code(3819).unwrap();
        assert_eq!(check.symbol(), "ER_CHECK_CONSTRAINT_VIOLATED");
        assert_eq!(check.sql_state(), "HY000");
    }
}
