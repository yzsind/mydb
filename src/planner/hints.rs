//! SQL and optimizer index-hint policy.

use super::*;

pub(super) fn index_allowed_by_hints(
    index_name: &str,
    hints: &[IndexHint],
    scope: IndexHintScope,
) -> bool {
    let applicable: Vec<&IndexHint> = hints
        .iter()
        .filter(|h| h.scope == IndexHintScope::All || h.scope == scope)
        .collect();
    if applicable.is_empty() {
        return true;
    }
    for hint in applicable
        .iter()
        .filter(|h| matches!(h.hint_type, IndexHintType::Ignore))
    {
        if hint
            .index_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case(index_name))
        {
            return false;
        }
    }
    let allowed: Vec<&str> = applicable
        .iter()
        .filter(|h| matches!(h.hint_type, IndexHintType::Use | IndexHintType::Force))
        .flat_map(|h| h.index_names.iter().map(|n| n.as_str()))
        .collect();
    if !allowed.is_empty() {
        return allowed.iter().any(|n| n.eq_ignore_ascii_case(index_name));
    }
    true
}

pub(super) fn filter_index_candidates(
    candidates: &mut Vec<IndexAccess>,
    hints: &[IndexHint],
    scope: IndexHintScope,
) {
    let applicable: Vec<&IndexHint> = hints
        .iter()
        .filter(|h| h.scope == IndexHintScope::All || h.scope == scope)
        .collect();
    if applicable.is_empty() {
        return;
    }
    for hint in applicable
        .iter()
        .filter(|h| matches!(h.hint_type, IndexHintType::Ignore))
    {
        candidates.retain(|c| {
            !hint
                .index_names
                .iter()
                .any(|n| c.index.name.eq_ignore_ascii_case(n))
        });
    }
    let allowed: Vec<&str> = applicable
        .iter()
        .filter(|h| matches!(h.hint_type, IndexHintType::Use | IndexHintType::Force))
        .flat_map(|h| h.index_names.iter().map(|n| n.as_str()))
        .collect();
    if !allowed.is_empty() {
        candidates.retain(|c| allowed.iter().any(|n| c.index.name.eq_ignore_ascii_case(n)));
    }
}

pub(super) fn query_hint_full_scan(
    hints: &[QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> bool {
    hints.iter().any(|h| {
        if let QueryHint::FullScan { table } = h {
            query_hint_table_matches(table, hint_table_name, schema_table_name)
        } else {
            false
        }
    })
}

pub(super) fn query_hint_zone_map(
    hints: &[QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Option<bool> {
    hints.iter().rev().find_map(|h| match h {
        QueryHint::ZoneMap { table }
            if query_hint_table_matches(table, hint_table_name, schema_table_name) =>
        {
            Some(true)
        }
        QueryHint::NoZoneMap { table }
            if query_hint_table_matches(table, hint_table_name, schema_table_name) =>
        {
            Some(false)
        }
        _ => None,
    })
}

pub(super) fn query_hint_table_matches(
    table: &str,
    hint_table_name: &str,
    schema_table_name: &str,
) -> bool {
    let matches_name = |name: &str| {
        table.eq_ignore_ascii_case(name)
            || name
                .rsplit('.')
                .next()
                .is_some_and(|suffix| table.eq_ignore_ascii_case(suffix))
    };
    matches_name(hint_table_name)
        || (!schema_table_name.is_empty() && matches_name(schema_table_name))
}

/// Returns the optionally-named index for an `INDEX_FFS(table [idx])`
/// hint that targets `hint_table_name` / `schema_table_name`. Outer
/// `Option` is `None` when no INDEX_FFS hint applies to this table;
/// inner `Option<&str>` is `None` when the hint did not name a
/// specific index (planner picks the narrowest covering one).
pub(super) fn query_hint_index_ffs<'a>(
    hints: &'a [QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Option<Option<&'a str>> {
    hints.iter().find_map(|h| {
        if let QueryHint::IndexFfs { table, index } = h {
            if query_hint_table_matches(table, hint_table_name, schema_table_name) {
                Some(index.as_deref())
            } else {
                None
            }
        } else {
            None
        }
    })
}

pub(super) fn query_hint_use_index<'a>(
    hints: &'a [QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Vec<&'a str> {
    hints
        .iter()
        .filter_map(|h| {
            if let QueryHint::UseIndex { table, indexes } = h {
                if query_hint_table_matches(table, hint_table_name, schema_table_name) {
                    Some(indexes.iter().map(|s| s.as_str()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .flatten()
        .collect()
}

pub(super) fn query_hint_no_index<'a>(
    hints: &'a [QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Vec<&'a str> {
    hints
        .iter()
        .filter_map(|h| {
            if let QueryHint::NoIndex { table, index } = h {
                if query_hint_table_matches(table, hint_table_name, schema_table_name) {
                    Some(index.as_str())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn query_hint_index_merge<'a>(
    hints: &'a [QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Option<Vec<&'a str>> {
    hints.iter().find_map(|hint| match hint {
        QueryHint::IndexMerge { table, indexes }
            if query_hint_table_matches(table, hint_table_name, schema_table_name) =>
        {
            Some(indexes.iter().map(String::as_str).collect())
        }
        _ => None,
    })
}

pub(super) fn query_hint_no_index_merge<'a>(
    hints: &'a [QueryHint],
    hint_table_name: &str,
    schema_table_name: &str,
) -> Option<Vec<&'a str>> {
    hints.iter().find_map(|hint| match hint {
        QueryHint::NoIndexMerge { table, indexes }
            if query_hint_table_matches(table, hint_table_name, schema_table_name) =>
        {
            Some(indexes.iter().map(String::as_str).collect())
        }
        _ => None,
    })
}
