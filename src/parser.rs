use std::fmt;

use sqlparser::ast::{
    Analyze, AnalyzeFormatKind, AssignmentTarget as SqlAssignmentTarget,
    BinaryOperator as SqlBinaryOperator, CeilFloorKind, CharacterLength, ColumnOption,
    CreateView as SqlCreateView, DataType as SqlDataType, DescribeAlias, DuplicateTreatment,
    ExactNumberInfo, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArgumentClause,
    FunctionArguments, GeneratedAs, GeneratedExpressionMode, Ident, IndexOption, IndexType,
    Interval, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart, ObjectType,
    OrderByKind, Query, Select as SqlSelect, SelectItem as SqlSelectItem,
    SelectItemQualifiedWildcardKind, SequenceOptions as SqlSequenceOptions,
    SetAssignment as SqlSetAssignment, SetExpr, SetOperator, SetQuantifier, ShowCreateObject,
    ShowStatementFilter as SqlShowStatementFilter, ShowStatementFilterPosition,
    ShowStatementInParentType, ShowStatementOptions, Spanned, Statement as SqlStatement,
    TableAlias, TableConstraint, TableFactor, TableFunctionArgs, TableIndexHintForClause,
    TableIndexHintType, TableIndexHints, TableObject, TableWithJoins, TrimWhereField,
    UnaryOperator as SqlUnaryOperator, Use, Value as SqlValue,
};
use sqlparser::dialect::{Dialect, MySqlDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser as SqlParser, ParserError};
use sqlparser::tokenizer::Token;

mod adapter;
#[cfg(test)]
pub(crate) use adapter::max_set_operation_branches;
mod ast;
pub(crate) mod boolean;
mod budget;
pub(crate) use budget::MAX_SELECT_NESTING;
mod bulk_string;
mod convert;
mod entry;
mod extensions;
mod grammar;
mod intrinsic;
mod manual;
mod names;
pub use names::{BoundName, QualifiedName};
mod print;
mod query_ref;
pub(crate) mod query_shape;
mod rewrite;
pub(crate) mod rrf;
mod source;
pub(crate) mod visit;

pub use ast::*;
use budget::*;
pub(crate) use convert::parse_catalog_expr;
use convert::*;
pub use convert::{is_aggregate_function, parse_expr_fragment, render_table_option};
#[cfg(test)]
pub(crate) use entry::parse_statement_calls_for_test;
pub(crate) use entry::MydbDialect;
use entry::*;
pub(crate) use entry::{
    current_parser_sql_mode_bits, extract_first_keyword, split_query_statements,
    take_large_literal_digest_sql, ParserSqlModeOverrideGuard,
};
pub use entry::{parse_statement, parse_statements, ParserSqlModeGuard};
pub(crate) use grammar::strip_comments_in_explain_header;
use grammar::*;
pub(crate) use intrinsic::IntrinsicFunction;
pub(crate) use manual::decode_bit_string;
pub(crate) use manual::parse_xa_statement as parse_replication_xa_statement;
use manual::*;
pub use manual::{parse_routine_body_sql, sql_is_effectively_empty};
pub use print::{binary_op_sql_pub, literal_to_sql_pub};
pub(crate) use print::{expr_to_catalog_sql, index_expression_to_catalog_sql};
pub use print::{expr_to_sql, try_expr_to_sql, SqlEncodeError};
pub(crate) use query_ref::QueryMut;
pub use query_ref::{QueryRef, WithRef};
pub(crate) use rewrite::preserve_decoded_projection_labels;
use rewrite::*;
use source::*;
pub(crate) use visit::{
    bind_unqualified_identifiers_to_placeholders, canonicalize_expr_for_plan_cache,
    canonicalize_select_for_plan_cache, collect_routine_expression_function_names,
    collect_select_function_names, collect_statement_function_names, collect_system_time_bindings,
    collect_unqualified_identifier_names, number_statement_placeholders,
    system_time_expr_is_statement_constant,
};

pub use crate::xa::Xid;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod quality_tests;

#[cfg(test)]
mod audit_fixes;

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_fast_path_differential(sql: &str, bits: u16) {
    let _mode = ParserSqlModeGuard::activate(bits);
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(bits);
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, MYSQL_VERSION_ID) else {
        return;
    };
    let sql = prepared.effective();
    let _extensions = extensions::Guard::enter(sql);
    let _source = ParserRawSqlGuard::activate(sql);
    if let Some(fast) = try_parse_bulk_insert(sql) {
        let slow = parse_statement_via_sqlparser_legacy(sql)
            .expect("bulk parser accepted SQL rejected by canonical parser");
        assert_eq!(fast, slow);
    }
    for route in [try_parse_plain_query_or_delete, try_parse_plain_update_fast] {
        if let Ok(Some(fast)) = route(sql) {
            let slow = parse_statement_via_sqlparser_legacy(sql)
                .expect("direct parser accepted SQL rejected by canonical parser");
            assert_eq!(fast, slow);
        }
    }
}

mod vector_options;
use vector_options::{apply_vector_options, strip_vector_options};

#[cfg(test)]
mod rrf_tests;
