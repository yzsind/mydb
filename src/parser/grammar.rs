//! Typed hand-written grammar families sharing one lexical cursor.
mod cursor;
mod explain;
mod into;
mod roles;
mod transaction;
mod view;

pub(crate) use explain::strip_comments_in_explain_header;
pub(super) use explain::{normalize_explain, parse_explain_connection, parse_explain_extension};
pub(super) use roles::parse_role_statement;
pub(super) use transaction::{parse_set_session_statement, parse_transaction_statement};

pub(super) use view::parse_view_statement;

pub(super) use into::parse_select_into_statement;

mod sequence;
pub(super) use sequence::{
    set_owner as set_sequence_owner, try_parse_alter_sequence, try_parse_create_sequence,
    try_parse_show_sequence,
};

mod accounts;
pub(super) use accounts::parse_account_statement;

mod users;
pub(super) use users::parse_user_statement;

mod privileges;
pub(super) use privileges::{parse_definer, parse_privilege_statement};
