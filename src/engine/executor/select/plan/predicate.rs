//! predicate execution and preparation.

use super::*;

/// If `expr` is a simple column reference (Identifier or
/// CompoundIdentifier whose last part names a column of `schema`),
/// returns the column's positional index. Used by the fused
/// HashAgg-over-SeqScan fast-path to recognise GROUP BY columns
/// without going through full expression evaluation.
pub(in crate::engine::executor::select) fn simple_column_index_of(
    expr: &Expr,
    schema: &TableSchema,
) -> Option<usize> {
    match expr {
        Expr::Identifier(name) => schema.column_index(name).ok(),
        Expr::CompoundIdentifier(parts) => {
            parts.last().and_then(|name| schema.column_index(name).ok())
        }
        _ => None,
    }
}

/// Walk `expr` and add the index of every base-table column referenced.
/// Returns `false` (and may leave `set` partially populated) if the
/// expression contains a feature the fused fast-path can't handle:
/// subqueries, EXISTS, window functions, or aggregate calls. The
/// caller checks the bool before relying on the collected set.
pub(in crate::engine::executor::select) fn collect_table_column_refs(
    expr: &Expr,
    schema: &TableSchema,
    set: &mut std::collections::BTreeSet<usize>,
) -> bool {
    crate::parser::visit::try_inspect_value_expr::<()>(expr, |expr| {
        match expr {
            Expr::Identifier(name) => {
                set.insert(schema.column_index(name).map_err(|_| ())?);
            }
            Expr::CompoundIdentifier(parts) => {
                let name = parts.last().ok_or(())?;
                set.insert(schema.column_index(name).map_err(|_| ())?);
            }
            Expr::Function { name, .. } => {
                if crate::planner::is_aggregate_function(name) {
                    return Err(());
                }
            }
            Expr::Tuple(_)
            | Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
            | Expr::Exists { .. }
            | Expr::WindowFunction { .. } => return Err(()),
            _ => {}
        }
        Ok(true)
    })
    .is_ok()
}

/// Build a `TableSchema` view containing only `columns[*]` from the
/// original schema, in the same order as `columns`. The result is used
/// as the schema argument to `eval_expr` against the projected
/// `&[Value]` returned by `scan_rows_columns_streaming`.
pub(in crate::engine::executor::select) fn projected_schema_view(
    schema: &TableSchema,
    columns: &[usize],
) -> TableSchema {
    let mut projected_columns = Vec::with_capacity(columns.len());
    let mut name_map: std::collections::HashMap<String, usize, ahash::RandomState> =
        Default::default();
    let mut id_map = std::collections::HashMap::new();
    for (out_idx, src_idx) in columns.iter().enumerate() {
        // Rebase physical/logical positions onto the projected slice:
        // downstream readers index the resulting `&[Value]` by these
        // positions, and the slice is laid out in projection order
        // rather than original heap order. column_id is preserved so
        // any column_id-based plan caching keys still match.
        let mut col = schema.columns[*src_idx].clone();
        col.physical_pos = out_idx as u32;
        col.logical_pos = out_idx as u32;
        name_map.insert(col.name.to_ascii_lowercase(), out_idx);
        id_map.insert(col.column_id, out_idx);
        projected_columns.push(col);
    }
    TableSchema {
        id: schema.id,
        object_id: schema.object_id,
        active_generation: 1,
        database: schema.database.clone(),
        name: schema.name.clone(),
        engine: schema.engine.clone(),
        charset: schema.charset.clone(),
        collation: schema.collation.clone(),
        comment: schema.comment.clone(),
        created_ms: schema.created_ms,
        updated_ms: schema.updated_ms,
        table_type: schema.table_type.clone(),
        columns: projected_columns,
        primary_key: Vec::new(),
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        fk_referenced_by: Vec::new(),
        // This is an expression input layout, never a planning schema.
        stats: crate::storage::TableStats::empty(),
        system: schema.system,
        schema_version: schema.schema_version,
        system_time_epoch_us: schema.system_time_epoch_us,
        row_generation_txn_id: schema.row_generation_txn_id,
        next_column_id: schema.next_column_id,
        active_layout_id: schema.active_layout_id,
        row_layouts: schema.row_layouts.clone(),
        pending_primary_key: schema.pending_primary_key.clone(),
        primary_definition_scn: schema.primary_definition_scn,
        next_index_id: schema.next_index_id,
        primary_publication_job_id: schema.primary_publication_job_id,
        column_name_map: name_map,
        column_id_map: id_map,
        row_codec: Default::default(),
        partitioning: None,
        pctfree: schema.pctfree,
        pctused: schema.pctused,
        mysql_table_options: Vec::new(),
    }
}

#[derive(Clone)]
pub(crate) struct CompiledPredicate {
    pub(super) root: PredicateNode,
    pub(super) typed_conjunction: Option<Vec<PredicateNode>>,
}

#[derive(Clone)]
pub(crate) struct DecodedPredicateProgram {
    pub(super) root: DecodedPredicateNode,
    pub(super) leaves: Vec<DecodedPredicateLeaf>,
}

#[derive(Clone)]
pub(super) enum DecodedPredicateNode {
    And(Box<DecodedPredicateNode>, Box<DecodedPredicateNode>),
    Or(Box<DecodedPredicateNode>, Box<DecodedPredicateNode>),
    Not(Box<DecodedPredicateNode>),
    Leaf(usize),
    Const(DecodedTruth),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodedTruth {
    False,
    True,
    Unknown,
}

#[derive(Clone)]
pub(super) enum DecodedPredicateLeaf {
    StringCmp {
        column: usize,
        op: BinaryOp,
        literal: String,
        collation: Option<&'static crate::charset::CollationInfo>,
        flipped: bool,
    },
    StringLike {
        column: usize,
        plan: crate::engine::expr_program::LikePlan,
        negated: bool,
    },
    IsNull {
        column: usize,
        negated: bool,
    },
}

#[derive(Clone)]
pub(super) enum PredicateNode {
    And(Box<PredicateNode>, Box<PredicateNode>),
    Or(Box<PredicateNode>, Box<PredicateNode>),
    Cmp {
        col: usize,
        op: BinaryOp,
        value: FastConst,
        flipped: bool,
    },
    StringCmp {
        col: usize,
        op: BinaryOp,
        literal: String,
        collation: Option<&'static crate::charset::CollationInfo>,
        flipped: bool,
    },
    StringIn {
        col: usize,
        literals: Vec<String>,
        collation: Option<&'static crate::charset::CollationInfo>,
        negated: bool,
        has_null: bool,
    },
    ColumnCmp {
        left: usize,
        op: BinaryOp,
        right: usize,
    },
    IsNull {
        col: usize,
        negated: bool,
    },
    TimestampDateIntCmp {
        timestamp: usize,
        duration: usize,
        arithmetic: BinaryOp,
        op: BinaryOp,
        cutoff: i64,
        zero_date_numeric: bool,
        converter: crate::engine::expr::SessionTimestampDateNumber,
        fallback: crate::engine::expr_program::ExprProgram,
    },
    Const(bool),
    Program(crate::engine::expr_program::ExprProgram),
}

#[derive(Clone)]
pub(super) struct FastConst {
    pub(super) value: Value,
    pub(super) numeric: Option<f64>,
    pub(super) date_days: Option<i32>,
    pub(super) timestamp_micros: Option<i64>,
    pub(super) decimal_parts: Option<(i128, u8)>,
}

impl CompiledPredicate {
    /// Storage callbacks borrow pinned page bytes. Only closed, pure
    /// expression programs may run there: an opaque/session-dependent
    /// program can wait for a transaction which needs to write that page.
    /// Inspect the entire boolean tree, not just its typed prefix.
    pub(in crate::engine::executor::select) fn page_latch_safe(&self) -> bool {
        fn safe(node: &PredicateNode) -> bool {
            match node {
                PredicateNode::And(left, right) | PredicateNode::Or(left, right) => {
                    safe(left) && safe(right)
                }
                PredicateNode::Program(program) => {
                    let capability = program.capability();
                    capability.deterministic
                        && !capability.depends_on_session
                        && !capability.legacy_escape
                }
                // These leaves only compare values or use the captured
                // timestamp converter; none invokes arbitrary SQL functions.
                PredicateNode::Cmp { .. }
                | PredicateNode::StringCmp { .. }
                | PredicateNode::StringIn { .. }
                | PredicateNode::ColumnCmp { .. }
                | PredicateNode::IsNull { .. }
                | PredicateNode::TimestampDateIntCmp { .. }
                | PredicateNode::Const(_) => true,
            }
        }
        safe(&self.root)
    }

    pub(in crate::engine::executor::select) fn compile_for_storage(
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<Self> {
        Self::compile(expr, schema).filter(Self::page_latch_safe)
    }

    pub(crate) fn compile(expr: &Expr, schema: &TableSchema) -> Option<Self> {
        compile_predicate_node(expr, schema).map(|root| {
            let mut terms = Vec::new();
            let typed_conjunction = collect_typed_conjunction(&root, &mut terms).then_some(terms);
            Self {
                root,
                typed_conjunction,
            }
        })
    }

    #[inline]
    pub(crate) fn eval(&self, values: &[Value]) -> Result<bool, QueryError> {
        eval_predicate_node(&self.root, values)
    }

    /// Evaluate fixed-width predicate inputs without materialising `Value`s.
    /// `None` asks the scan bridge to invoke the legacy evaluator for this
    /// row, preserving semantics for programs and uncommon SQL types.
    #[inline]
    pub(in crate::engine::executor::select) fn eval_typed(
        &self,
        values: &[crate::engine::executor::vec::scan_bridge::PredicateValue],
    ) -> Result<Option<bool>, QueryError> {
        if let Some(terms) = &self.typed_conjunction {
            for term in terms {
                match eval_typed_predicate_leaf(term, values)? {
                    Some(true) => {}
                    result => return Ok(result),
                }
            }
            return Ok(Some(true));
        }
        eval_typed_predicate_node(&self.root, values)
    }

    #[inline]
    pub(in crate::engine::executor::select) fn supports_typed_eval(&self) -> bool {
        self.typed_conjunction.is_some()
    }

    /// Exact single-column inclusive TIMESTAMP range suitable for direct
    /// evaluation by the row-decode sink.  Returning `None` preserves the
    /// complete typed/ExprProgram oracle for every other predicate shape.
    pub(in crate::engine::executor::select) fn inclusive_timestamp_range(
        &self,
    ) -> Option<(usize, i64, i64)> {
        let terms = self.typed_conjunction.as_deref()?;
        let [first, second] = terms else { return None };
        let bound = |node: &PredicateNode| {
            let PredicateNode::Cmp {
                col,
                op,
                value,
                flipped: false,
            } = node
            else {
                return None;
            };
            Some((*col, *op, value.timestamp_micros?))
        };
        let (first_col, first_op, first_value) = bound(first)?;
        let (second_col, second_op, second_value) = bound(second)?;
        if first_col != second_col {
            return None;
        }
        match (first_op, second_op) {
            (BinaryOp::GtEq, BinaryOp::LtEq) => Some((first_col, first_value, second_value)),
            (BinaryOp::LtEq, BinaryOp::GtEq) => Some((first_col, second_value, first_value)),
            _ => None,
        }
    }
}

impl DecodedPredicateProgram {
    pub(crate) fn compile(expr: &Expr, schema: &TableSchema) -> Option<Self> {
        let mut leaves = Vec::new();
        let root = compile_decoded_predicate_node(expr, schema, &mut leaves)?;
        Some(Self { root, leaves })
    }

    #[inline]
    pub(crate) fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Whether evaluating this program while the row payload is borrowed
    /// avoids constructing an owned string for a LIKE pattern.  This is a
    /// materially different cost shape from fixed-width/date comparisons:
    /// the latter are usually cheaper in the common batch filter when most
    /// rows survive, while LIKE benefits from matching the borrowed bytes
    /// even at a high selectivity.
    pub(in crate::engine::executor::select) fn has_string_like(&self) -> bool {
        self.leaves
            .iter()
            .any(|leaf| matches!(leaf, DecodedPredicateLeaf::StringLike { .. }))
    }

    /// Consume a UTF-8 value while the heap page is pinned. Every predicate
    /// leaf that references `column` is evaluated from the borrowed slice;
    /// no `String` or `Value` is constructed. `false` means that the decoded
    /// type/layout did not satisfy this program and the caller must use the
    /// row evaluator oracle.
    #[inline]
    pub(crate) fn put_string(
        &self,
        column: usize,
        bytes: &[u8],
        results: &mut [Option<DecodedTruth>],
    ) -> bool {
        let ascii = bytes.is_ascii();
        let value = if ascii {
            // ASCII is a strict UTF-8 subset. Sysbench-style identifiers and
            // most protocol text predicates take this branch, avoiding a
            // second full UTF-8 validation pass after row decoding.
            // SAFETY: every byte was proven ASCII above.
            unsafe { std::str::from_utf8_unchecked(bytes) }
        } else {
            let Ok(value) = simdutf8::basic::from_utf8(bytes) else {
                return false;
            };
            value
        };
        let mut consumed = false;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let truth = match leaf {
                DecodedPredicateLeaf::StringCmp {
                    column: leaf_column,
                    op,
                    literal,
                    collation,
                    flipped,
                } if *leaf_column == column => {
                    // Equality against the empty string does not need an
                    // ordering key.  In particular, building a Unicode
                    // collation key for every row turns a very selective
                    // `text <> ''` scan into a normalization workload even
                    // though almost all values can be decided by the first
                    // non-ignorable character.
                    if literal.is_empty()
                        && matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq | BinaryOp::NotEq)
                    {
                        let equal = crate::charset::string_equals_empty_for_info(
                            collation.unwrap_or_else(crate::charset::default_collation_info),
                            value,
                        );
                        let matches = if matches!(op, BinaryOp::NotEq) {
                            !equal
                        } else {
                            equal
                        };
                        results[index] = Some(DecodedTruth::from_bool(matches));
                        consumed = true;
                        continue;
                    }
                    let mut ordering = match collation {
                        Some(info) => {
                            crate::charset::compare_strings_for_info(info, value, literal)
                        }
                        None => crate::charset::default_compare_strings(value, literal),
                    };
                    if *flipped {
                        ordering = ordering.reverse();
                    }
                    DecodedTruth::from_bool(compare_ordering(*op, ordering))
                }
                DecodedPredicateLeaf::StringLike {
                    column: leaf_column,
                    plan,
                    negated,
                } if *leaf_column == column => {
                    let matches = if ascii {
                        plan.matches_ascii_bytes(bytes)
                    } else {
                        plan.matches_str(value)
                    };
                    DecodedTruth::from_bool(matches != *negated)
                }
                DecodedPredicateLeaf::IsNull {
                    column: leaf_column,
                    negated,
                } if *leaf_column == column => DecodedTruth::from_bool(*negated),
                _ => continue,
            };
            results[index] = Some(truth);
            consumed = true;
        }
        consumed
    }

    #[inline]
    pub(crate) fn put_null(&self, column: usize, results: &mut [Option<DecodedTruth>]) -> bool {
        let mut consumed = false;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let truth = match leaf {
                DecodedPredicateLeaf::StringCmp {
                    column: leaf_column,
                    op,
                    ..
                } if *leaf_column == column => {
                    if matches!(op, BinaryOp::NullSafeEq) {
                        DecodedTruth::False
                    } else {
                        DecodedTruth::Unknown
                    }
                }
                DecodedPredicateLeaf::StringLike {
                    column: leaf_column,
                    ..
                } if *leaf_column == column => DecodedTruth::Unknown,
                DecodedPredicateLeaf::IsNull {
                    column: leaf_column,
                    negated,
                } if *leaf_column == column => DecodedTruth::from_bool(!*negated),
                _ => continue,
            };
            results[index] = Some(truth);
            consumed = true;
        }
        consumed
    }

    #[inline]
    pub(crate) fn finish(&self, results: &[Option<DecodedTruth>]) -> Option<bool> {
        eval_decoded_predicate_node(&self.root, results)
            .map(|truth| matches!(truth, DecodedTruth::True))
    }
}

impl DecodedTruth {
    #[inline]
    pub(super) fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }

    #[inline]
    pub(super) fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    #[inline]
    pub(super) fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    #[inline]
    pub(super) fn not(self) -> Self {
        match self {
            Self::False => Self::True,
            Self::True => Self::False,
            Self::Unknown => Self::Unknown,
        }
    }
}

pub(super) fn eval_decoded_predicate_node(
    node: &DecodedPredicateNode,
    results: &[Option<DecodedTruth>],
) -> Option<DecodedTruth> {
    match node {
        DecodedPredicateNode::And(left, right) => Some(
            eval_decoded_predicate_node(left, results)?
                .and(eval_decoded_predicate_node(right, results)?),
        ),
        DecodedPredicateNode::Or(left, right) => Some(
            eval_decoded_predicate_node(left, results)?
                .or(eval_decoded_predicate_node(right, results)?),
        ),
        DecodedPredicateNode::Not(expr) => Some(eval_decoded_predicate_node(expr, results)?.not()),
        DecodedPredicateNode::Leaf(index) => results.get(*index).copied().flatten(),
        DecodedPredicateNode::Const(value) => Some(*value),
    }
}

pub(super) fn compile_decoded_predicate_node(
    expr: &Expr,
    schema: &TableSchema,
    leaves: &mut Vec<DecodedPredicateLeaf>,
) -> Option<DecodedPredicateNode> {
    let binary_node = |left, right, is_and| {
        if is_and {
            DecodedPredicateNode::And(Box::new(left), Box::new(right))
        } else {
            DecodedPredicateNode::Or(Box::new(left), Box::new(right))
        }
    };
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Some(binary_node(
            compile_decoded_predicate_node(left, schema, leaves)?,
            compile_decoded_predicate_node(right, schema, leaves)?,
            true,
        )),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Some(binary_node(
            compile_decoded_predicate_node(left, schema, leaves)?,
            compile_decoded_predicate_node(right, schema, leaves)?,
            false,
        )),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Some(DecodedPredicateNode::Not(Box::new(
            compile_decoded_predicate_node(expr, schema, leaves)?,
        ))),
        Expr::Binary { left, op, right } if crate::parser::boolean::is_comparison!(*op) => {
            compile_decoded_string_comparison(left, *op, right, schema, leaves)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let low = compile_decoded_string_comparison(expr, BinaryOp::GtEq, low, schema, leaves)?;
            let high =
                compile_decoded_string_comparison(expr, BinaryOp::LtEq, high, schema, leaves)?;
            let between = DecodedPredicateNode::And(Box::new(low), Box::new(high));
            Some(if *negated {
                DecodedPredicateNode::Not(Box::new(between))
            } else {
                between
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                items.push(if matches!(const_expr_value(item), Some(Value::Null)) {
                    DecodedPredicateNode::Const(DecodedTruth::Unknown)
                } else {
                    compile_decoded_string_comparison(expr, BinaryOp::Eq, item, schema, leaves)?
                });
            }
            // OR is associative under three-valued logic and both sides are
            // always evaluated, so a balanced tree keeps per-row evaluation
            // and destruction logarithmic in depth for long IN lists.
            while items.len() > 1 {
                let mut level = Vec::with_capacity(items.len().div_ceil(2));
                let mut pairs = items.into_iter();
                while let Some(left) = pairs.next() {
                    level.push(match pairs.next() {
                        Some(right) => DecodedPredicateNode::Or(Box::new(left), Box::new(right)),
                        None => left,
                    });
                }
                items = level;
            }
            let result = items
                .pop()
                .unwrap_or(DecodedPredicateNode::Const(DecodedTruth::False));
            Some(if *negated {
                DecodedPredicateNode::Not(Box::new(result))
            } else {
                result
            })
        }
        Expr::Like { expr: value, .. } => {
            let input = InputSchema::from_table(schema);
            let program = ExprCompiler::compile(expr, &input).ok()?;
            let (column, plan, negated) = program.as_column_like_const()?;
            if simple_string_predicate_column(schema, column).is_none() {
                return None;
            }
            let index = leaves.len();
            leaves.push(DecodedPredicateLeaf::StringLike {
                column,
                plan: plan.clone(),
                negated,
            });
            let _ = value;
            Some(DecodedPredicateNode::Leaf(index))
        }
        Expr::IsNull(value) | Expr::IsNotNull(value) => {
            let column = simple_column_index_of(value, schema)?;
            simple_string_predicate_column(schema, column)?;
            let index = leaves.len();
            leaves.push(DecodedPredicateLeaf::IsNull {
                column,
                negated: matches!(expr, Expr::IsNotNull(_)),
            });
            Some(DecodedPredicateNode::Leaf(index))
        }
        Expr::Literal(_) | Expr::BoundValue(_) => {
            let value = const_expr_value(expr)?;
            Some(DecodedPredicateNode::Const(
                if matches!(value, Value::Null) {
                    DecodedTruth::Unknown
                } else {
                    DecodedTruth::from_bool(crate::engine::expr::truthy(&value))
                },
            ))
        }
        _ => None,
    }
}

/// Compile `DATE(timestamp_col) +/- signed_integer_col <op> DATE-constant`
/// into fixed-width lanes. MySQL treats the `+/-` as numeric temporal
/// arithmetic (`YYYYMMDD +/- n`), not interval arithmetic. The fallback
/// program remains attached for zero sentinels, unexpected runtime lanes or
/// overflow, so this fast path cannot weaken diagnostics or coercion rules.
pub(super) fn compile_timestamp_date_int_cmp(
    expr: &Expr,
    schema: &TableSchema,
) -> Option<PredicateNode> {
    let Expr::Binary { left, op, right } = expr else {
        return None;
    };
    if !crate::parser::boolean::is_comparison!(*op) {
        return None;
    }
    let Expr::Binary {
        left: date_expr,
        op: arithmetic @ (BinaryOp::Add | BinaryOp::Sub),
        right: duration_expr,
    } = left.as_ref()
    else {
        return None;
    };
    let Expr::Function {
        name,
        args,
        distinct: _,
    } = date_expr.as_ref()
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("DATE") || args.len() != 1 {
        return None;
    }
    let timestamp = simple_column_index_of(&args[0], schema)?;
    let duration = simple_column_index_of(duration_expr, schema)?;
    let timestamp_column = schema.columns.get(timestamp)?;
    let duration_column = schema.columns.get(duration)?;
    if !matches!(
        timestamp_column.data_type,
        DataTypeKind::Timestamp { .. } | DataTypeKind::DateTime { .. }
    ) || duration_column.unsigned
        || !matches!(
            duration_column.data_type,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
        )
    {
        return None;
    }
    let cutoff_value = crate::engine::expr::eval_const(right).ok()?;
    if !matches!(cutoff_value, Value::Date(_)) {
        return None;
    }
    let cutoff = crate::types::temporal_to_mysql_number(&cutoff_value)?.to_i64();
    let input = InputSchema::from_table(schema);
    let fallback = ExprCompiler::compile(expr, &input).ok()?;
    let converter = crate::engine::expr::SessionTimestampDateNumber::capture().ok()?;
    let zero_date_numeric = crate::engine::expr::session_temporal_sql_mode_bits()
        & crate::engine::api::TEMPORAL_MODE_NO_ZERO_DATE
        == 0;
    Some(PredicateNode::TimestampDateIntCmp {
        timestamp,
        duration,
        arithmetic: *arithmetic,
        op: *op,
        cutoff,
        zero_date_numeric,
        converter,
        fallback,
    })
}

pub(super) fn compile_decoded_string_comparison(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    schema: &TableSchema,
    leaves: &mut Vec<DecodedPredicateLeaf>,
) -> Option<DecodedPredicateNode> {
    let (column, constant, flipped) = if let Some(column) = simple_column_index_of(left, schema) {
        (column, right, false)
    } else {
        (simple_column_index_of(right, schema)?, left, true)
    };
    let column_schema = simple_string_predicate_column(schema, column)?;
    let value = coerce_filter_constant(const_expr_value(constant)?, &column_schema.data_type);
    if matches!(value, Value::Null) {
        return Some(if matches!(op, BinaryOp::NullSafeEq) {
            let index = leaves.len();
            leaves.push(DecodedPredicateLeaf::IsNull {
                column,
                negated: false,
            });
            DecodedPredicateNode::Leaf(index)
        } else {
            DecodedPredicateNode::Const(DecodedTruth::Unknown)
        });
    }
    let literal = crate::types::value_str(&value)?.to_owned();
    let collation = crate::engine::expr_program::resolve_column_collation_id(schema, column_schema)
        .and_then(crate::charset::resolve_collation_id);
    let index = leaves.len();
    leaves.push(DecodedPredicateLeaf::StringCmp {
        column,
        op,
        literal,
        collation,
        flipped,
    });
    Some(DecodedPredicateNode::Leaf(index))
}

pub(super) fn simple_string_predicate_column(
    schema: &TableSchema,
    column: usize,
) -> Option<&crate::storage::ColumnSchema> {
    let column = schema.columns.get(column)?;
    matches!(
        column.data_type,
        DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. }
    )
    .then_some(column)
}

pub(super) fn collect_typed_conjunction(
    node: &PredicateNode,
    out: &mut Vec<PredicateNode>,
) -> bool {
    match node {
        PredicateNode::And(left, right) => {
            collect_typed_conjunction(left, out) && collect_typed_conjunction(right, out)
        }
        PredicateNode::Cmp { .. }
        | PredicateNode::StringCmp { .. }
        | PredicateNode::StringIn { .. }
        | PredicateNode::ColumnCmp { .. }
        | PredicateNode::IsNull { .. }
        | PredicateNode::TimestampDateIntCmp { .. }
        | PredicateNode::Const(_) => {
            out.push(node.clone());
            true
        }
        PredicateNode::Or(_, _) | PredicateNode::Program(_) => false,
    }
}

impl CompiledNumericExpr {
    #[cfg(test)]
    pub(in crate::engine::executor::select) fn compile(
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<Self> {
        Self::compile_with_column_resolver(expr, &|expr| simple_column_index_of(expr, schema))
    }

    pub(in crate::engine::executor::select) fn compile_with_column_resolver(
        expr: &Expr,
        resolve_column: &impl Fn(&Expr) -> Option<usize>,
    ) -> Option<Self> {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                resolve_column(expr).map(Self::Column)
            }
            Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => {
                let value = const_expr_value(expr)?;
                value_to_f64_fast(&value).map(|_| Self::Const(value))
            }
            Expr::Binary { left, op, right }
                if matches!(
                    op,
                    BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
                ) =>
            {
                Some(Self::Binary {
                    left: Box::new(Self::compile_with_column_resolver(left, resolve_column)?),
                    op: *op,
                    right: Box::new(Self::compile_with_column_resolver(right, resolve_column)?),
                })
            }
            _ => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(in crate::engine::executor::select) fn eval(
        &self,
        values: &[Value],
    ) -> Result<Option<f64>, QueryError> {
        match self {
            Self::Column(idx) => Ok(value_to_f64_fast(&values[*idx])),
            Self::Const(value) => Ok(value_to_f64_fast(value)),
            Self::Binary { left, op, right } => {
                let Some(l) = left.eval(values)? else {
                    return Ok(None);
                };
                let Some(r) = right.eval(values)? else {
                    return Ok(None);
                };
                if matches!(op, BinaryOp::Div | BinaryOp::Mod) && r == 0.0 {
                    return Ok(None);
                }
                let value = match op {
                    BinaryOp::Add => l + r,
                    BinaryOp::Sub => l - r,
                    BinaryOp::Mul => l * r,
                    BinaryOp::Div => l / r,
                    BinaryOp::Mod => l % r,
                    _ => return Ok(None),
                };
                Ok(Some(value))
            }
        }
    }

    /// Best-effort batch evaluation that stays in Decimal lanes
    /// end-to-end. Powers the TPC-H Q1 `extprice * (1 - disc) * (1 + tax)`
    /// family of expressions: for these the entire tree compiles to a
    /// sequence of `expr_batch` SIMD kernels feeding into a single
    /// `Decimal128` output column, which `update_state_from_column` can
    /// then fold straight into the SUM accumulator without ever
    /// materialising a `Value`.
    ///
    /// Returns `None` if any leaf or intermediate value can't be
    /// represented as a sign-magnitude scaled integer (Float, String,
    /// Division/Mod) — the fused-agg worker then falls back to
    /// per-row [`eval_value`].
    pub(in crate::engine::executor::select) fn try_eval_batch_decimal(
        &self,
        columns: &[crate::engine::executor::vec::batch::ColumnVector],
        n_rows: usize,
    ) -> Option<crate::engine::executor::vec::batch::ColumnVector> {
        let lane = numeric_batch::try_eval_lane(self, columns, n_rows)?;
        Some(numeric_batch::lane_into_column(lane, n_rows))
    }

    /// Evaluate exact signed BIGINT addition/subtraction/multiplication over
    /// typed lanes. Any unsupported signedness or overflow declines only the
    /// current batch and leaves the ExprProgram oracle in control.
    pub(crate) fn try_eval_batch_integer(
        &self,
        columns: &[crate::engine::executor::vec::batch::ColumnVector],
        n_rows: usize,
    ) -> Option<crate::engine::executor::vec::batch::ColumnVector> {
        numeric_batch::try_eval_integer_column(self, columns, n_rows)
    }
}

#[path = "../numeric_batch.rs"]
pub(crate) mod numeric_batch;

pub(super) fn compile_predicate_node(expr: &Expr, schema: &TableSchema) -> Option<PredicateNode> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Some(PredicateNode::And(
            Box::new(compile_predicate_node(left, schema)?),
            Box::new(compile_predicate_node(right, schema)?),
        )),
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Some(PredicateNode::Or(
            Box::new(compile_predicate_node(left, schema)?),
            Box::new(compile_predicate_node(right, schema)?),
        )),
        Expr::Binary { left, op, right } if crate::parser::boolean::is_comparison!(*op) => {
            if let Some(node) = compile_timestamp_date_int_cmp(expr, schema) {
                return Some(node);
            }
            // Same-type fixed-width column comparisons stay in typed lanes.
            // This covers predicates such as `commit_date < receipt_date`
            // without converting either operand to `Value` for every row.
            // Different declared types retain ExprProgram as the coercion
            // oracle.
            if let (Some(left_col), Some(right_col)) = (
                simple_column_index_of(left, schema),
                simple_column_index_of(right, schema),
            ) {
                if fast_scalar_predicate_column(schema, left_col)
                    && fast_scalar_predicate_column(schema, right_col)
                    && schema.columns[left_col].data_type == schema.columns[right_col].data_type
                {
                    return Some(PredicateNode::ColumnCmp {
                        left: left_col,
                        op: *op,
                        right: right_col,
                    });
                }
            }
            // Fast Cmp path: column ⊕ literal (either side). Only fires
            // when one operand is a plain column ref AND the other is a
            // FastConst-eligible constant; column-to-column comparisons
            // (e.g. `l_commitdate < l_receiptdate` in TPC-H Q12) and
            // column-to-expression cases fall through to the generic
            // `PredicateNode::Program` arm below — that's the only
            // change vs the old code, which previously returned `None`
            // for those shapes and forced the caller (CompiledPredicate)
            // to surrender the whole AND tree to row-mode eval.
            if let Some(col) = simple_column_index_of(left, schema) {
                if fast_scalar_predicate_column(schema, col) {
                    if let Some(value) = fast_const_for_column(right, schema, col) {
                        return Some(PredicateNode::Cmp {
                            col,
                            op: *op,
                            value,
                            flipped: false,
                        });
                    }
                }
            }
            if let Some(col) = simple_column_index_of(right, schema) {
                if fast_scalar_predicate_column(schema, col) {
                    if let Some(value) = fast_const_for_column(left, schema, col) {
                        return Some(PredicateNode::Cmp {
                            col,
                            op: *op,
                            value,
                            flipped: true,
                        });
                    }
                }
            }
            if let Some((col, literal, collation, flipped)) =
                compile_fast_string_comparison(left, right, schema)
            {
                return Some(PredicateNode::StringCmp {
                    col,
                    op: *op,
                    literal,
                    collation,
                    flipped,
                });
            }
            let input = InputSchema::from_table(schema);
            ExprCompiler::compile(expr, &input)
                .ok()
                .map(PredicateNode::Program)
        }
        Expr::Between {
            expr: value_expr,
            low,
            high,
            negated,
        } => {
            // BETWEEN over a fixed-width column and two constants is exactly
            // two ordered comparisons in WHERE's truthy domain. Lower it
            // here instead of handing every row to ExprProgram. Besides
            // removing the interpreter dispatch, keeping the whole
            // conjunction in Cmp leaves lets the predicate-first scan decode
            // directly into PredicateValue lanes.
            if let Some(col) = simple_column_index_of(value_expr, schema) {
                if fast_scalar_predicate_column(schema, col) {
                    if let (Some(low), Some(high)) = (
                        fast_const_for_column(low, schema, col),
                        fast_const_for_column(high, schema, col),
                    ) {
                        let (low_op, high_op, combine) = if *negated {
                            (BinaryOp::Lt, BinaryOp::Gt, false)
                        } else {
                            (BinaryOp::GtEq, BinaryOp::LtEq, true)
                        };
                        let low = PredicateNode::Cmp {
                            col,
                            op: low_op,
                            value: low,
                            flipped: false,
                        };
                        let high = PredicateNode::Cmp {
                            col,
                            op: high_op,
                            value: high,
                            flipped: false,
                        };
                        return Some(if combine {
                            PredicateNode::And(Box::new(low), Box::new(high))
                        } else {
                            PredicateNode::Or(Box::new(low), Box::new(high))
                        });
                    }
                }
            }
            let input = InputSchema::from_table(schema);
            ExprCompiler::compile(expr, &input)
                .ok()
                .map(PredicateNode::Program)
        }
        Expr::InList {
            expr: tested,
            list,
            negated,
        } => {
            if let Some(node) = compile_fast_string_in(tested, list, *negated, schema) {
                return Some(node);
            }
            let input = InputSchema::from_table(schema);
            ExprCompiler::compile(expr, &input)
                .ok()
                .map(PredicateNode::Program)
        }
        Expr::IsNull(value) | Expr::IsNotNull(value) => {
            if let Some(col) = simple_column_index_of(value, schema) {
                return Some(PredicateNode::IsNull {
                    col,
                    negated: matches!(expr, Expr::IsNotNull(_)),
                });
            }
            let input = InputSchema::from_table(schema);
            ExprCompiler::compile(expr, &input)
                .ok()
                .map(PredicateNode::Program)
        }
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_) => {
            let value = const_expr_value(expr)?;
            Some(PredicateNode::Const(crate::engine::expr::truthy(&value)))
        }
        _ => {
            let input = InputSchema::from_table(schema);
            ExprCompiler::compile(expr, &input)
                .ok()
                .map(PredicateNode::Program)
        }
    }
}

pub(super) fn compile_fast_string_in(
    tested: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &TableSchema,
) -> Option<PredicateNode> {
    let col = simple_column_index_of(tested, schema)?;
    let column = simple_string_predicate_column(schema, col)?;
    let collation = crate::engine::expr_program::resolve_column_collation_id(schema, column)
        .and_then(crate::charset::resolve_collation_id);
    let mut literals = Vec::with_capacity(list.len());
    let mut has_null = false;
    for item in list {
        let value = coerce_filter_constant(const_expr_value(item)?, &column.data_type);
        if matches!(value, Value::Null) {
            has_null = true;
            continue;
        }
        literals.push(crate::types::value_str(&value)?.to_owned());
    }
    Some(PredicateNode::StringIn {
        col,
        literals,
        collation,
        negated,
        has_null,
    })
}

pub(super) fn compile_fast_string_comparison(
    left: &Expr,
    right: &Expr,
    schema: &TableSchema,
) -> Option<(
    usize,
    String,
    Option<&'static crate::charset::CollationInfo>,
    bool,
)> {
    let (column, constant, flipped) = if let Some(column) = simple_column_index_of(left, schema) {
        (column, right, false)
    } else {
        (simple_column_index_of(right, schema)?, left, true)
    };
    let column_schema = simple_string_predicate_column(schema, column)?;
    let value = const_expr_value(constant)?;
    if matches!(value, Value::Null) {
        return None;
    }
    let literal = crate::types::value_str(&value)?.to_owned();
    let collation = crate::engine::expr_program::resolve_column_collation_id(schema, column_schema)
        .and_then(crate::charset::resolve_collation_id);
    Some((column, literal, collation, flipped))
}

/// The scalar comparison kernel intentionally accepts only types whose SQL
/// ordering is fully represented by its numeric coercion, plus DATE after its
/// constant has been pre-coerced to the storage day representation. Character
/// collations, BIT/hex coercion and the remaining temporal-vs-number rules are
/// compiled by `ExprProgram`; keeping them out of this leaf is the correctness
/// contract that makes the program/legacy fallback an actual oracle.
pub(super) fn fast_scalar_predicate_column(schema: &TableSchema, col: usize) -> bool {
    matches!(
        schema.columns.get(col).map(|column| &column.data_type),
        Some(
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Bool
                | DataTypeKind::Float { .. }
                | DataTypeKind::Double { .. }
                | DataTypeKind::Decimal { .. }
                | DataTypeKind::Date
                | DataTypeKind::Timestamp { .. }
        )
    )
}

pub(super) fn eval_predicate_node(
    node: &PredicateNode,
    values: &[Value],
) -> Result<bool, QueryError> {
    match node {
        PredicateNode::And(left, right) => {
            Ok(eval_predicate_node(left, values)? && eval_predicate_node(right, values)?)
        }
        PredicateNode::Or(left, right) => {
            Ok(eval_predicate_node(left, values)? || eval_predicate_node(right, values)?)
        }
        PredicateNode::Cmp {
            col,
            op,
            value,
            flipped,
        } => {
            let Some(ord) = fast_compare_value_const(&values[*col], value)? else {
                return Ok(matches!(op, BinaryOp::NullSafeEq)
                    && matches!((&values[*col], &value.value), (Value::Null, Value::Null)));
            };
            let ord = if *flipped { ord.reverse() } else { ord };
            Ok(match op {
                BinaryOp::Eq | BinaryOp::NullSafeEq => ord == std::cmp::Ordering::Equal,
                BinaryOp::NotEq => ord != std::cmp::Ordering::Equal,
                BinaryOp::Lt => ord == std::cmp::Ordering::Less,
                BinaryOp::LtEq => ord != std::cmp::Ordering::Greater,
                BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
                BinaryOp::GtEq => ord != std::cmp::Ordering::Less,
                _ => false,
            })
        }
        PredicateNode::StringCmp {
            col,
            op,
            literal,
            collation,
            flipped,
        } => Ok(
            compare_string_predicate_value(&values[*col], *op, literal, *collation, *flipped)
                .unwrap_or(false),
        ),
        PredicateNode::StringIn {
            col,
            literals,
            collation,
            negated,
            has_null,
        } => Ok(
            eval_string_in_predicate(&values[*col], literals, *collation, *negated, *has_null)
                .unwrap_or(false),
        ),
        PredicateNode::ColumnCmp { left, op, right } => {
            if matches!(values[*left], Value::Null) || matches!(values[*right], Value::Null) {
                return Ok(matches!(op, BinaryOp::NullSafeEq)
                    && matches!(values[*left], Value::Null)
                    && matches!(values[*right], Value::Null));
            }
            Ok(compare_ordering(
                *op,
                crate::engine::expr::compare_values(&values[*left], &values[*right])?,
            ))
        }
        PredicateNode::IsNull { col, negated } => {
            Ok(matches!(values[*col], Value::Null) != *negated)
        }
        PredicateNode::TimestampDateIntCmp {
            timestamp,
            duration,
            arithmetic,
            op,
            cutoff,
            zero_date_numeric,
            converter,
            fallback,
        } => {
            let fast = match (values.get(*timestamp), values.get(*duration)) {
                (Some(Value::Timestamp(micros)), Some(Value::Int(duration))) => {
                    let date = if *micros == crate::types::ZERO_DATETIME && *zero_date_numeric {
                        Some(0)
                    } else {
                        converter.convert(*micros)?
                    };
                    date.and_then(|date| match arithmetic {
                        BinaryOp::Add => date.checked_add(*duration),
                        BinaryOp::Sub => date.checked_sub(*duration),
                        _ => None,
                    })
                }
                (Some(Value::DateTime(micros)), Some(Value::Int(duration))) => {
                    let date = if *micros == crate::types::ZERO_DATETIME {
                        (*zero_date_numeric).then_some(0)
                    } else {
                        let (year, month, day, ..) = crate::types::datetime_components(*micros);
                        Some(i64::from(year) * 10_000 + i64::from(month) * 100 + i64::from(day))
                    };
                    date.and_then(|date| match arithmetic {
                        BinaryOp::Add => date.checked_add(*duration),
                        BinaryOp::Sub => date.checked_sub(*duration),
                        _ => None,
                    })
                }
                _ => None,
            };
            if let Some(value) = fast {
                return Ok(compare_ordering(*op, value.cmp(cutoff)));
            }
            let mut scratch = crate::engine::expr_program::EvalScratch::default();
            fallback
                .eval_source(&values, &mut scratch)
                .map(|value| crate::engine::expr::truthy(&value))
        }
        PredicateNode::Const(value) => Ok(*value),
        PredicateNode::Program(program) => {
            // EvalScratch is intentionally allocation-free (function caches
            // live in their own bounded shared cache). Construct it on the
            // stack instead of entering a RefCell-backed TLS slot for every
            // scanned row; the TLS borrow alone dominated short compiled
            // predicates under parallel scans.
            let mut scratch = crate::engine::expr_program::EvalScratch::default();
            program
                .eval_source(&values, &mut scratch)
                .map(|value| crate::engine::expr::truthy(&value))
        }
    }
}

#[inline(always)]
pub(super) fn eval_typed_predicate_node(
    node: &PredicateNode,
    values: &[crate::engine::executor::vec::scan_bridge::PredicateValue],
) -> Result<Option<bool>, QueryError> {
    match node {
        PredicateNode::And(left, right) => match eval_typed_predicate_node(left, values)? {
            Some(false) => Ok(Some(false)),
            Some(true) => eval_typed_predicate_node(right, values),
            None => Ok(None),
        },
        PredicateNode::Or(left, right) => match eval_typed_predicate_node(left, values)? {
            Some(true) => Ok(Some(true)),
            Some(false) => eval_typed_predicate_node(right, values),
            None => Ok(None),
        },
        _ => eval_typed_predicate_leaf(node, values),
    }
}

#[inline(always)]
pub(super) fn eval_typed_predicate_leaf(
    node: &PredicateNode,
    values: &[crate::engine::executor::vec::scan_bridge::PredicateValue],
) -> Result<Option<bool>, QueryError> {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    match node {
        PredicateNode::Cmp {
            col,
            op,
            value,
            flipped,
        } => {
            let left = &values[*col];
            if matches!(left, PredicateValue::Null) || matches!(value.value, Value::Null) {
                return Ok(Some(
                    matches!(op, BinaryOp::NullSafeEq)
                        && matches!(left, PredicateValue::Null)
                        && matches!(value.value, Value::Null),
                ));
            }
            let Some(mut ordering) = fast_compare_typed_const(left, value)? else {
                return Ok(None);
            };
            if *flipped {
                ordering = ordering.reverse();
            }
            Ok(Some(compare_ordering(*op, ordering)))
        }
        PredicateNode::StringCmp {
            col,
            op,
            literal,
            collation,
            flipped,
        } => match &values[*col] {
            PredicateValue::Null => Ok(Some(false)),
            PredicateValue::Value(value) => Ok(compare_string_predicate_value(
                value, *op, literal, *collation, *flipped,
            )),
            _ => Ok(None),
        },
        PredicateNode::StringIn {
            col,
            literals,
            collation,
            negated,
            has_null,
        } => match &values[*col] {
            PredicateValue::Null => Ok(Some(false)),
            PredicateValue::Value(value) => Ok(eval_string_in_predicate(
                value, literals, *collation, *negated, *has_null,
            )),
            _ => Ok(None),
        },
        PredicateNode::ColumnCmp { left, op, right } => {
            let left = &values[*left];
            let right = &values[*right];
            if matches!(left, PredicateValue::Null) || matches!(right, PredicateValue::Null) {
                return Ok(Some(
                    matches!(op, BinaryOp::NullSafeEq)
                        && matches!(left, PredicateValue::Null)
                        && matches!(right, PredicateValue::Null),
                ));
            }
            Ok(compare_typed_predicate_values(left, right)?
                .map(|ordering| compare_ordering(*op, ordering)))
        }
        PredicateNode::IsNull { col, negated } => Ok(Some(
            matches!(values[*col], PredicateValue::Null) != *negated,
        )),
        PredicateNode::TimestampDateIntCmp {
            timestamp,
            duration,
            arithmetic,
            op,
            cutoff,
            zero_date_numeric,
            converter,
            ..
        } => {
            if matches!(values[*timestamp], PredicateValue::Null)
                || matches!(values[*duration], PredicateValue::Null)
            {
                return Ok(Some(false));
            }
            let date = match &values[*timestamp] {
                PredicateValue::Timestamp(micros)
                | PredicateValue::Value(Value::Timestamp(micros)) => {
                    if *micros == crate::types::ZERO_DATETIME && *zero_date_numeric {
                        Some(0)
                    } else {
                        converter.convert(*micros)?
                    }
                }
                PredicateValue::DateTime(micros)
                | PredicateValue::Value(Value::DateTime(micros)) => {
                    if *micros == crate::types::ZERO_DATETIME {
                        (*zero_date_numeric).then_some(0)
                    } else {
                        let (year, month, day, ..) = crate::types::datetime_components(*micros);
                        Some(i64::from(year) * 10_000 + i64::from(month) * 100 + i64::from(day))
                    }
                }
                value => {
                    let date = crate::engine::expr::eval_date_function_value(value.to_value())?;
                    if matches!(date, Value::Null) {
                        None
                    } else {
                        crate::types::temporal_to_mysql_number(&date).map(|value| value.to_i64())
                    }
                }
            };
            let duration = match &values[*duration] {
                PredicateValue::I64(duration) | PredicateValue::Value(Value::Int(duration)) => {
                    *duration
                }
                PredicateValue::U64(duration) | PredicateValue::Value(Value::UInt(duration))
                    if *duration <= i64::MAX as u64 =>
                {
                    *duration as i64
                }
                value => crate::engine::expr::int_value(&value.to_value())?,
            };
            let fast = date.and_then(|date| match arithmetic {
                BinaryOp::Add => date.checked_add(duration),
                BinaryOp::Sub => date.checked_sub(duration),
                _ => None,
            });
            Ok(fast.map(|value| compare_ordering(*op, value.cmp(cutoff))))
        }
        PredicateNode::Const(value) => Ok(Some(*value)),
        PredicateNode::Program(_) | PredicateNode::And(_, _) | PredicateNode::Or(_, _) => Ok(None),
    }
}

#[inline]
pub(super) fn eval_string_in_predicate(
    value: &Value,
    literals: &[String],
    collation: Option<&'static crate::charset::CollationInfo>,
    negated: bool,
    has_null: bool,
) -> Option<bool> {
    let value = crate::types::value_str(value)?;
    let matched = literals.iter().any(|literal| match collation {
        Some(info) => crate::charset::strings_equal_for_info(info, value, literal),
        None => crate::charset::strings_equal_for_info(
            crate::charset::default_collation_info(),
            value,
            literal,
        ),
    });
    if matched {
        Some(!negated)
    } else if has_null {
        // `x IN (..., NULL)` and `x NOT IN (..., NULL)` are UNKNOWN when no
        // non-NULL member matched; UNKNOWN is rejected by WHERE.
        Some(false)
    } else {
        Some(negated)
    }
}

#[inline]
pub(super) fn compare_typed_predicate_values(
    left: &crate::engine::executor::vec::scan_bridge::PredicateValue,
    right: &crate::engine::executor::vec::scan_bridge::PredicateValue,
) -> Result<Option<std::cmp::Ordering>, QueryError> {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;
    let ordering = match (left, right) {
        (PredicateValue::I64(left), PredicateValue::I64(right)) => left.cmp(right),
        (PredicateValue::U64(left), PredicateValue::U64(right)) => left.cmp(right),
        (PredicateValue::I64(left), PredicateValue::U64(right)) => {
            (*left as i128).cmp(&(*right as i128))
        }
        (PredicateValue::U64(left), PredicateValue::I64(right)) => {
            (*left as i128).cmp(&(*right as i128))
        }
        (PredicateValue::Date(left), PredicateValue::Date(right)) => left.cmp(right),
        (PredicateValue::Year(left), PredicateValue::Year(right)) => left.cmp(right),
        (
            PredicateValue::Decimal(left, left_scale),
            PredicateValue::Decimal(right, right_scale),
        ) => {
            let Some(ordering) = compare_decimal_parts(*left, *left_scale, *right, *right_scale)
            else {
                return Ok(None);
            };
            ordering
        }
        (PredicateValue::F64(left), PredicateValue::F64(right)) => {
            left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PredicateValue::Value(left), PredicateValue::Value(right)) => {
            crate::engine::expr::compare_values(left, right)?
        }
        _ => return Ok(None),
    };
    Ok(Some(ordering))
}

#[inline]
pub(super) fn compare_string_predicate_value(
    value: &Value,
    op: BinaryOp,
    literal: &str,
    collation: Option<&'static crate::charset::CollationInfo>,
    flipped: bool,
) -> Option<bool> {
    let value = crate::types::value_str(value)?;
    if literal.is_empty() && matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq | BinaryOp::NotEq) {
        let equal = crate::charset::string_equals_empty_for_info(
            collation.unwrap_or_else(crate::charset::default_collation_info),
            value,
        );
        return Some(if matches!(op, BinaryOp::NotEq) {
            !equal
        } else {
            equal
        });
    }
    let mut ordering = match collation {
        Some(info) => crate::charset::compare_strings_for_info(info, value, literal),
        None => crate::charset::default_compare_strings(value, literal),
    };
    if flipped {
        ordering = ordering.reverse();
    }
    Some(compare_ordering(op, ordering))
}

#[inline]
pub(super) fn compare_ordering(op: BinaryOp, ordering: std::cmp::Ordering) -> bool {
    match op {
        BinaryOp::Eq | BinaryOp::NullSafeEq => ordering == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
        _ => false,
    }
}

#[inline(always)]
pub(super) fn fast_compare_typed_const(
    left: &crate::engine::executor::vec::scan_bridge::PredicateValue,
    right: &FastConst,
) -> Result<Option<std::cmp::Ordering>, QueryError> {
    use crate::engine::executor::vec::scan_bridge::PredicateValue;

    if let (PredicateValue::Date(left), Some(right)) = (left, right.date_days) {
        return Ok(Some(left.cmp(&right)));
    }
    if let (PredicateValue::Timestamp(left), Some(right)) = (left, right.timestamp_micros) {
        return Ok(Some(left.cmp(&right)));
    }
    if let Some((right_magnitude, right_scale)) = right.decimal_parts {
        let left_parts = match left {
            PredicateValue::I64(value) => Some((*value as i128, 0)),
            PredicateValue::U64(value) => Some((*value as i128, 0)),
            PredicateValue::Decimal(magnitude, scale) => Some((*magnitude, *scale)),
            _ => None,
        };
        if let Some((left_magnitude, left_scale)) = left_parts {
            return Ok(compare_decimal_parts(
                left_magnitude,
                left_scale,
                right_magnitude,
                right_scale,
            ));
        }
    }
    if let (PredicateValue::F64(left), Some(right)) = (left, right.numeric) {
        return Ok(Some(
            left.partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
        ));
    }
    if let PredicateValue::Value(left) = left {
        return fast_compare_value_const(left, right);
    }
    Ok(None)
}

pub(super) fn fast_const_decimal_parts(value: &Value) -> Option<(i128, u8)> {
    match value {
        Value::Int(value) => Some((*value as i128, 0)),
        Value::UInt(value) => Some((*value as i128, 0)),
        Value::DecimalCompact(magnitude, scale) => Some((*magnitude as i128, *scale)),
        Value::Decimal(value) | Value::String(value) => {
            crate::util::decimal::parse_exact_decimal_str(value)
        }
        Value::SmallString(value) => crate::util::decimal::parse_exact_decimal_str(unsafe {
            std::str::from_utf8_unchecked(value)
        }),
        Value::Bytes(value) => std::str::from_utf8(value)
            .ok()
            .and_then(crate::util::decimal::parse_exact_decimal_str),
        _ => None,
    }
}

#[inline]
pub(super) fn compare_decimal_parts(
    left_magnitude: i128,
    left_scale: u8,
    right_magnitude: i128,
    right_scale: u8,
) -> Option<std::cmp::Ordering> {
    if left_scale == right_scale {
        return Some(left_magnitude.cmp(&right_magnitude));
    }
    let target_scale = left_scale.max(right_scale);
    let left = left_magnitude.checked_mul(crate::util::decimal::pow10_i128(
        (target_scale - left_scale) as u32,
    )?)?;
    let right = right_magnitude.checked_mul(crate::util::decimal::pow10_i128(
        (target_scale - right_scale) as u32,
    )?)?;
    Some(left.cmp(&right))
}

pub(super) fn const_expr_value(expr: &Expr) -> Option<Value> {
    crate::engine::expr::expr_literal_value(expr)
}

pub(super) fn fast_const_for_column(
    expr: &Expr,
    schema: &TableSchema,
    col: usize,
) -> Option<FastConst> {
    let value = const_expr_value(expr)?;
    let data_type = schema.columns.get(col).map(|column| &column.data_type)?;
    let value = crate::engine::expr::contextual_numeric_literal(value, expr, data_type);
    let value = coerce_filter_constant(value, data_type);
    // DATE's typed lane compares epoch days. If coercion failed, retain the
    // ExprProgram fallback: treating an arbitrary string's first ten bytes as
    // a date would diverge from MySQL temporal coercion/error semantics.
    if matches!(data_type, DataTypeKind::Date) && !matches!(value, Value::Date(_) | Value::Null) {
        return None;
    }
    if matches!(data_type, DataTypeKind::Timestamp { .. })
        && !matches!(value, Value::Timestamp(_) | Value::Null)
    {
        return None;
    }
    Some(FastConst {
        numeric: value_to_f64_fast(&value),
        date_days: value_to_date_days_fast(&value),
        timestamp_micros: match &value {
            Value::Timestamp(micros) => Some(*micros),
            _ => None,
        },
        decimal_parts: fast_const_decimal_parts(&value).or_else(|| match &value {
            Value::Float(value) => {
                crate::util::decimal::parse_exact_decimal_str(&value.to_string())
            }
            Value::Double(value) => {
                crate::util::decimal::parse_exact_decimal_str(&value.to_string())
            }
            _ => None,
        }),
        value,
    })
}

#[inline]
pub(super) fn fast_compare_value_const(
    left: &Value,
    right: &FastConst,
) -> Result<Option<std::cmp::Ordering>, QueryError> {
    if matches!(left, Value::Null) || matches!(right.value, Value::Null) {
        return Ok(None);
    }
    if let (Some(l), Some(r)) = (value_to_date_days_fast(left), right.date_days) {
        return Ok(Some(l.cmp(&r)));
    }
    if let (Value::Timestamp(left), Some(right)) = (left, right.timestamp_micros) {
        return Ok(Some(left.cmp(&right)));
    }
    let left_decimal = match left {
        Value::Int(value) => Some((*value as i128, 0)),
        Value::UInt(value) => Some((*value as i128, 0)),
        Value::DecimalCompact(magnitude, scale) => Some((*magnitude as i128, *scale)),
        _ => None,
    };
    if let (Some((left_magnitude, left_scale)), Some((right_magnitude, right_scale))) =
        (left_decimal, right.decimal_parts)
    {
        if let Some(ordering) =
            compare_decimal_parts(left_magnitude, left_scale, right_magnitude, right_scale)
        {
            return Ok(Some(ordering));
        }
    }
    if let (Some(l), Some(r)) = (value_to_f64_fast(left), right.numeric) {
        return Ok(Some(l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal)));
    }
    crate::engine::expr::compare_nullable_values(left, &right.value)
}

#[inline]
pub(in crate::engine::executor::select) fn value_to_f64_fast(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Int(v) => Some(*v as f64),
        Value::UInt(v) => Some(*v as f64),
        Value::Float(v) => Some(*v as f64),
        Value::Double(v) => Some(*v),
        Value::DecimalCompact(mag, scale) => {
            const POW10: [f64; 19] = [
                1.0,
                10.0,
                100.0,
                1_000.0,
                10_000.0,
                100_000.0,
                1_000_000.0,
                10_000_000.0,
                100_000_000.0,
                1_000_000_000.0,
                10_000_000_000.0,
                100_000_000_000.0,
                1_000_000_000_000.0,
                10_000_000_000_000.0,
                100_000_000_000_000.0,
                1_000_000_000_000_000.0,
                10_000_000_000_000_000.0,
                100_000_000_000_000_000.0,
                1_000_000_000_000_000_000.0,
            ];
            POW10.get(*scale as usize).map(|denom| *mag as f64 / *denom)
        }
        Value::Decimal128(mag, scale) => Some(*mag as f64 / 10f64.powi(*scale as i32)),
        Value::DecimalWide(value) => {
            crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale)
                .parse::<f64>()
                .ok()
        }
        Value::Decimal(s) | Value::String(s) => s.parse::<f64>().ok(),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .parse::<f64>()
            .ok(),
        Value::Bytes(b) => std::str::from_utf8(b).ok()?.parse::<f64>().ok(),
        Value::Date(d) => Some(*d as f64),
        Value::Time(t) | Value::DateTime(t) => Some(*t as f64),
        // TIMESTAMP numeric semantics depend on the session-local calendar;
        // let the row oracle handle it instead of treating UTC micros as a
        // SQL number in this raw fast path.
        Value::Timestamp(_) => None,
        Value::Year(y) => Some(if *y == 0 { 0.0 } else { 1900.0 + *y as f64 }),
        Value::Json(b) => Some(crate::engine::json::coerce_f64(b)),
        Value::Geometry(_) => None,
        Value::Vector(_) => None,
    }
}

pub(super) fn value_to_date_days_fast(value: &Value) -> Option<i32> {
    match value {
        Value::Date(days) => Some(*days),
        Value::String(s) | Value::Decimal(s) => parse_yyyy_mm_dd_days(s),
        Value::SmallString(b) => parse_yyyy_mm_dd_days(unsafe { std::str::from_utf8_unchecked(b) }),
        _ => None,
    }
}

pub(super) fn parse_yyyy_mm_dd_days(s: &str) -> Option<i32> {
    if s.len() < 10 {
        return None;
    }
    let bytes = s.as_bytes();
    if bytes.get(4) != Some(&b'-') || bytes.get(7) != Some(&b'-') {
        return None;
    }
    let year = parse_fixed_i32(&bytes[0..4])?;
    let month = parse_fixed_u32(&bytes[5..7])?;
    let day = parse_fixed_u32(&bytes[8..10])?;
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(date.signed_duration_since(epoch).num_days() as i32)
}

pub(super) fn parse_fixed_i32(bytes: &[u8]) -> Option<i32> {
    let mut out = 0i32;
    for b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        out = out * 10 + (b - b'0') as i32;
    }
    Some(out)
}

pub(super) fn parse_fixed_u32(bytes: &[u8]) -> Option<u32> {
    parse_fixed_i32(bytes).and_then(|v| u32::try_from(v).ok())
}

pub(in crate::engine::executor::select) fn should_use_predicate_first_projected_scan(
    estimated_rows: f64,
    access_rows: f64,
    predicate_columns: &[usize],
    output_columns: &[usize],
) -> bool {
    if predicate_columns.is_empty() || output_columns.is_empty() {
        return false;
    }
    if !estimated_rows.is_finite()
        || !access_rows.is_finite()
        || estimated_rows < 0.0
        || access_rows <= 0.0
    {
        return false;
    }
    if estimated_rows / access_rows > 0.15 {
        return false;
    }

    let mut union = std::collections::BTreeSet::new();
    union.extend(predicate_columns.iter().copied());
    union.extend(output_columns.iter().copied());
    if predicate_columns.len() >= union.len() {
        return false;
    }
    output_columns
        .iter()
        .any(|col| !predicate_columns.contains(col))
}
