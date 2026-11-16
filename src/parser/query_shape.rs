//! Shared structural guards; callers keep their own expression-scope policy.
use super::Select;

pub(crate) fn simple_single_source_body(select: &Select) -> bool {
    select.from.len() == 1
        && select.joins.is_empty()
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && !select.distinct
        && !select.for_update
        && select.derived.is_none()
        && select.named_windows.is_empty()
}
