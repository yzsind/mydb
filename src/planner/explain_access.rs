//! Format-independent index access facts used by EXPLAIN renderers.
use crate::storage::{IndexBound, IndexSchema};

pub(crate) fn lookup_type(index: &IndexSchema, exact: bool) -> &'static str {
    if exact && index.unique {
        "const"
    } else {
        "ref"
    }
}

pub(crate) fn range_prefix_len(
    index: &IndexSchema,
    low: &Option<IndexBound>,
    high: &Option<IndexBound>,
) -> usize {
    low.as_ref()
        .or(high.as_ref())
        .map(|bound| bound.key.0.len())
        .unwrap_or(index.columns.len())
}

use crate::{parser::Expr, planner};

// Borrowed access facts shared by tabular, TREE and JSON renderers.
// Extract no strings or plan copies until a renderer requests them.

pub(crate) struct IndexAccess<'a> {
    pub table: &'a planner::PlanTable,
    pub index: &'a crate::storage::IndexSchema,
    pub filter: &'a Option<Expr>,
    pub hint: Option<&'a str>,
    pub access_rows: f64,
    pub estimated_rows: f64,
    pub covering: bool,
    pub reverse: bool,
    pub possible_keys: &'a [String],
    pub kind: IndexAccessKind<'a>,
}

pub(crate) enum IndexAccessKind<'a> {
    Lookup {
        parts: usize,
        exact: bool,
    },
    MultiLookup {
        parts: usize,
        probes: usize,
    },
    Range {
        low: &'a Option<crate::storage::IndexBound>,
        high: &'a Option<crate::storage::IndexBound>,
        covering: bool,
    },
    Fast {
        low: &'a Option<crate::storage::IndexBound>,
        high: &'a Option<crate::storage::IndexBound>,
    },
}

impl<'a> IndexAccess<'a> {
    pub fn from_plan(plan: &'a planner::PhysicalPlan) -> Option<Self> {
        use planner::PhysicalPlan as P;
        let (table, index, filter, hint_note, access_rows, possible_keys) = match plan {
            P::IndexLookup {
                table,
                index,
                filter,
                hint_note,
                access_rows,
                possible_keys,
                ..
            }
            | P::IndexMultiLookup {
                table,
                index,
                filter,
                hint_note,
                access_rows,
                possible_keys,
                ..
            }
            | P::IndexRangeScan {
                table,
                index,
                filter,
                hint_note,
                access_rows,
                possible_keys,
                ..
            }
            | P::IndexFastScan {
                table,
                index,
                filter,
                hint_note,
                access_rows,
                possible_keys,
                ..
            } => (table, index, filter, hint_note, access_rows, possible_keys),
            _ => return None,
        };
        let kind = match plan {
            P::IndexLookup { key, exact, .. } => IndexAccessKind::Lookup {
                parts: key.0.len(),
                exact: *exact,
            },
            P::IndexMultiLookup { keys, .. } => IndexAccessKind::MultiLookup {
                parts: keys.first().map_or(0, |key| key.0.len()),
                probes: keys.len(),
            },
            P::IndexRangeScan {
                low,
                high,
                covering,
                ..
            } => IndexAccessKind::Range {
                low,
                high,
                covering: *covering,
            },
            P::IndexFastScan { low, high, .. } => IndexAccessKind::Fast { low, high },
            _ => unreachable!("index access matched above"),
        };
        let (covering, reverse) = match plan {
            P::IndexLookup { covering, .. } | P::IndexMultiLookup { covering, .. } => {
                (*covering, false)
            }
            P::IndexRangeScan {
                covering, reverse, ..
            } => (*covering, *reverse),
            P::IndexFastScan { reverse, .. } => (true, *reverse),
            _ => unreachable!("index access matched above"),
        };
        Some(Self {
            table,
            index,
            filter,
            hint: hint_note.as_deref(),
            access_rows: *access_rows,
            estimated_rows: planner::estimated_rows(plan),
            covering,
            reverse,
            possible_keys,
            kind,
        })
    }

    pub fn access_type(&self) -> &'static str {
        match self.kind {
            IndexAccessKind::Lookup { exact, .. } => {
                crate::planner::explain_access::lookup_type(self.index, exact)
            }
            IndexAccessKind::MultiLookup { .. } | IndexAccessKind::Range { .. } => "range",
            IndexAccessKind::Fast { .. } => "index",
        }
    }

    pub fn key_length(&self) -> usize {
        let parts = match self.kind {
            IndexAccessKind::Lookup { parts, .. } | IndexAccessKind::MultiLookup { parts, .. } => {
                parts
            }
            IndexAccessKind::Range { low, high, .. } | IndexAccessKind::Fast { low, high } => {
                crate::planner::explain_access::range_prefix_len(self.index, low, high)
            }
        };
        self.table.schema.index_key_len(self.index, parts)
    }
}
