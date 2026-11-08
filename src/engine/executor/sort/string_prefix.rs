//! Bounded collation prefixes for sorts with already-evaluated text keys.

use super::{apply_sorted_perm_in_place_raw, SortField};
use crate::engine::executor::mem_budget::ScopedCharge;
use crate::types::Value;
use std::cmp::Ordering;

#[derive(Clone, Copy)]
struct PrefixIndex {
    prefix: u64,
    index: u32,
    null_rank: u8,
}

#[inline]
fn prefix_index(
    index: usize,
    value: &Value,
    field: SortField,
    collation: u16,
    scratch: &mut Vec<u8>,
) -> Option<PrefixIndex> {
    let text = match value {
        Value::String(text) => Some(text.as_str()),
        Value::SmallString(text) => {
            Some(std::str::from_utf8(text.as_ref()).expect("valid SmallString"))
        }
        Value::Null => None,
        _ => return None,
    };
    let mut bytes = [0; 8];
    if let Some(text) = text {
        scratch.clear();
        crate::charset::collation_key_prefix_by_id_into(collation, text, 8, scratch);
        bytes[..scratch.len()].copy_from_slice(&scratch);
    }
    let prefix = u64::from_be_bytes(bytes);
    Some(PrefixIndex {
        prefix: if field.ascending { prefix } else { !prefix },
        index: index as u32,
        null_rank: if text.is_some() {
            1
        } else if field.nulls_first {
            0
        } else {
            2
        },
    })
}

/// Prefix inequality proves the first field's order; equality always uses the
/// complete comparator, including remaining fields. No row moves until every
/// key has proved to be text or NULL. The bounded reservation is optional.
pub(crate) fn try_sort_by_string_prefix<T>(
    items: &mut [T],
    field: SortField,
    key: impl Fn(&T) -> &Value,
    compare: impl Fn(&T, &T) -> Ordering,
) -> bool {
    let Some(collation) = field.collation_id else {
        return false;
    };
    let Some(info) = crate::charset::resolve_collation_id(collation) else {
        return false;
    };
    // Accent-sensitive normalization can need a complete temporary key.
    // Keep this optional path bounded even for very long strings.
    if info.accent_sensitive && !info.binary {
        return false;
    }
    if items.len() < 32 || items.len() > u32::MAX as usize {
        return false;
    }
    let mut charge = ScopedCharge::new(0);
    if charge
        .add(
            items
                .len()
                .saturating_mul(std::mem::size_of::<PrefixIndex>() + 4)
                .saturating_add(8),
        )
        .is_err()
    {
        return false;
    }
    let mut scratch = Vec::with_capacity(8);
    // A prefix cannot accelerate duplicate-heavy or shared-prefix keys. Probe
    // a bounded sample before allocating the full index, and reuse that work
    // when the sample proves discriminating. Refusal leaves input untouched.
    const SAMPLE_ROWS: usize = 8;
    let mut sample = [PrefixIndex {
        prefix: 0,
        index: 0,
        null_rank: 0,
    }; SAMPLE_ROWS];
    for (index, item) in items.iter().take(SAMPLE_ROWS).enumerate() {
        let Some(prefix) = prefix_index(index, key(item), field, collation, &mut scratch) else {
            return false;
        };
        sample[index] = prefix;
    }
    if sample
        .iter()
        .all(|entry| entry.null_rank == sample[0].null_rank && entry.prefix == sample[0].prefix)
    {
        return false;
    }
    let mut prefixes = Vec::with_capacity(items.len());
    prefixes.extend_from_slice(&sample);
    for (index, item) in items.iter().enumerate().skip(SAMPLE_ROWS) {
        let Some(prefix) = prefix_index(index, key(item), field, collation, &mut scratch) else {
            return false;
        };
        prefixes.push(prefix);
    }
    prefixes.sort_unstable_by(|left, right| {
        left.null_rank
            .cmp(&right.null_rank)
            .then_with(|| left.prefix.cmp(&right.prefix))
            .then_with(|| compare(&items[left.index as usize], &items[right.index as usize]))
    });
    apply_sorted_perm_in_place_raw(items, prefixes.into_iter().map(|entry| entry.index));
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::sort::compare_value_for_sort;

    #[test]
    fn prefix_sort_matches_full_comparison_across_collations_and_directions() {
        let texts = [
            "",
            "a",
            "A",
            "a ",
            "a\0",
            "\0",
            "\0\0x",
            "abcdefgh1",
            "abcdefgh2",
            "abcdefghijklmnopZ",
            "abcdefghijklmnopA",
            "é",
            "e\u{301}",
            "ß",
            "ss",
            "Æ",
            "ae",
            "汉字",
            "中文",
            "Ａ",
            "a\u{301}\u{327}",
            "abcdEFGH\0尾",
            "abcdEFGH尾",
        ];
        for info in crate::charset::collations() {
            if info.accent_sensitive && !info.binary {
                continue;
            }
            for ascending in [false, true] {
                for nulls_first in [false, true] {
                    let field = SortField {
                        ascending,
                        nulls_first,
                        collation_id: Some(info.id),
                    };
                    let mut rows: Vec<_> = (0..128)
                        .map(|i| {
                            let text = texts[(i * 17) % texts.len()];
                            let value = if i % 11 == 0 {
                                Value::Null
                            } else if text.len() <= crate::types::SMALL_STRING_INLINE_CAP
                                && i % 2 == 0
                            {
                                Value::SmallString(crate::types::SmallStringBuf::from_slice(
                                    text.as_bytes(),
                                ))
                            } else {
                                Value::String(text.into())
                            };
                            (value, i)
                        })
                        .collect();
                    let compare = |a: &(Value, usize), b: &(Value, usize)| {
                        compare_value_for_sort(&a.0, &b.0, field).then_with(|| b.1.cmp(&a.1))
                    };
                    let mut expected = rows.clone();
                    expected.sort_unstable_by(compare);
                    assert!(try_sort_by_string_prefix(
                        &mut rows,
                        field,
                        |row| &row.0,
                        compare
                    ));
                    assert_eq!(
                        rows, expected,
                        "{} asc={ascending} nulls={nulls_first}",
                        info.name
                    );
                }
            }
        }
    }

    #[test]
    fn unsupported_key_domains_leave_rows_unchanged() {
        let field = SortField {
            ascending: true,
            nulls_first: true,
            collation_id: Some(255),
        };
        for unsupported in [Value::Int(7), Value::Bytes(b"text".to_vec())] {
            let mut rows: Vec<_> = (0..64).map(|i| Value::String(format!("text{i}"))).collect();
            rows[63] = unsupported;
            let before = rows.clone();
            assert!(!try_sort_by_string_prefix(
                &mut rows,
                field,
                |row| row,
                |a, b| compare_value_for_sort(a, b, field)
            ));
            assert_eq!(rows, before);
        }
    }

    #[test]
    fn identical_prefixes_decline_without_consuming_the_full_input() {
        let field = SortField {
            ascending: false,
            nulls_first: false,
            collation_id: Some(255),
        };
        let mut rows: Vec<_> = (0..128)
            .map(|i| Value::String(format!("abcdefgh汉字{:03}", 127 - i)))
            .collect();
        let mut expected = rows.clone();
        expected.sort_unstable_by(|a, b| compare_value_for_sort(a, b, field));
        let before = rows.clone();
        let inspected = std::cell::Cell::new(0);
        assert!(!try_sort_by_string_prefix(
            &mut rows,
            field,
            |row| {
                inspected.set(inspected.get() + 1);
                row
            },
            |_, _| unreachable!("non-discriminating sample must decline before sorting")
        ));
        assert_eq!(rows, before);
        assert!(inspected.get() < rows.len());
        rows.sort_unstable_by(|a, b| compare_value_for_sort(a, b, field));
        assert_eq!(rows, expected);
    }
}

#[cfg(test)]
#[test]
fn optional_prefix_reservation_preserves_rows_and_budget_on_refusal() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let budget = std::sync::Arc::new(MemBudget::new(16));
    let mut rows = vec![Value::String("text".into()); 64];
    let before = rows.clone();
    with_query_budget(budget.clone(), || {
        let field = SortField {
            ascending: true,
            nulls_first: true,
            collation_id: Some(255),
        };
        assert!(!try_sort_by_string_prefix(
            &mut rows,
            field,
            |row| row,
            |_, _| unreachable!()
        ));
        assert_eq!(rows, before);
        assert_eq!(budget.used(), 0);
        assert!(budget.terminal_oom().is_none());
    });
}
