//! Reciprocal-rank fusion over independently planned finite inputs.
use super::*;
use crate::parser::{QueryExpr, RrfFusion};
use crate::types::{Column, ColumnType};

mod metadata;
pub(in crate::engine::executor) use metadata::statement_has_fusion;
pub(crate) use metadata::{bind_options, describe_columns, plan_metadata};
use metadata::{coerce_identity, invalid, ordered_json, ranker};
mod sources;
pub(in crate::engine::executor) use sources::execute_response;
use sources::with_branch_scope;
pub(super) use sources::{plan_for_explain, with_analyzed_sources};
mod fusion;
pub(super) use fusion::open_plan;
mod window;
pub(super) use window::window_for_select;
pub use window::OutputWindow;
pub(crate) use window::{current_output_window, window_json};
#[cfg(test)]
mod hooks;
#[cfg(test)]
pub(in crate::engine::executor) use hooks::{branch_hook, enter_branch_hook, BranchHook};
