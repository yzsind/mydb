//! Object-scoped optimizer statistics in the transactional data dictionary.
use super::data_dictionary::DdRowSet;
use super::schema::TableSchema;
use super::stats::*;
use super::stats_codec::{corrupt, decode, encode};
use crate::types::{QueryError, Value};
use std::collections::{BTreeMap, BTreeSet};

fn u(v: u64) -> Value {
    Value::UInt(v)
}
fn opt(v: Option<u64>) -> Value {
    v.map_or(Value::Null, u)
}
fn real(v: Option<f64>) -> Value {
    v.map_or(Value::Null, Value::Double)
}
fn uint(row: &[Value], p: usize) -> Result<u64, QueryError> {
    match row.get(p) {
        Some(Value::UInt(v)) => Ok(*v),
        Some(Value::Int(v)) if *v >= 0 => Ok(*v as u64),
        _ => Err(corrupt()),
    }
}
fn optional(row: &[Value], p: usize) -> Result<Option<u64>, QueryError> {
    if matches!(row.get(p), Some(Value::Null)) {
        Ok(None)
    } else {
        uint(row, p).map(Some)
    }
}
fn float(row: &[Value], p: usize) -> Result<Option<f64>, QueryError> {
    match row.get(p) {
        Some(Value::Null) => Ok(None),
        Some(Value::Double(v)) if v.is_finite() && *v >= 0.0 => Ok(Some(*v)),
        _ => Err(corrupt()),
    }
}
fn bytes(row: &[Value], p: usize) -> Result<&[u8], QueryError> {
    match row.get(p) {
        Some(Value::Bytes(v)) => Ok(v),
        _ => Err(corrupt()),
    }
}
fn col_id(table: &TableSchema, name: &str) -> Result<u32, QueryError> {
    table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .map(|c| c.column_id)
        .ok_or_else(corrupt)
}
fn col_name(table: &TableSchema, id: u32) -> Result<String, QueryError> {
    table
        .columns
        .iter()
        .find(|c| c.column_id == id)
        .map(|c| super::normalize_name(&c.name))
        .ok_or_else(corrupt)
}
fn index_id(table: &TableSchema, name: &str) -> Result<u64, QueryError> {
    table
        .indexes
        .iter()
        .find(|i| i.name.eq_ignore_ascii_case(name))
        .map(|i| i.index_id)
        .ok_or_else(corrupt)
}
fn ids(table: &TableSchema, names: &[String]) -> Result<Vec<u32>, QueryError> {
    names.iter().map(|n| col_id(table, n)).collect()
}
fn names(table: &TableSchema, ids: &[u32]) -> Result<Vec<String>, QueryError> {
    ids.iter().map(|id| col_name(table, *id)).collect()
}

fn base(
    table: &TableSchema,
    scope: u64,
    rows: u64,
    pages: u64,
    modified: u64,
    analyzed: Option<u64>,
    check: Option<bool>,
) -> Vec<Value> {
    vec![
        u(table.id),
        u(scope),
        u(table.schema_version),
        u(rows),
        u(pages),
        u(modified),
        opt(analyzed),
        real(if scope == 0 {
            table.stats.sample_fraction
        } else {
            None
        }),
        real(if scope == 0 {
            table.stats.confidence
        } else {
            None
        }),
        u(table.stats.analyze_revision),
        Value::Int(if scope == 0 {
            table.stats.last_analyze_live_baseline
        } else {
            0
        }),
        u(if scope == 0 {
            table.stats.last_analyze_modification_baseline
        } else {
            0
        }),
        check.map_or(Value::Null, |v| Value::Int(v as i64)),
        u(0),
        u(0),
    ]
}
fn objects(
    set: &mut DdRowSet,
    table: &TableSchema,
    scope: u64,
    columns: &BTreeMap<String, ColumnStats>,
    indexes: &BTreeMap<String, IndexStats>,
) -> Result<(), QueryError> {
    for (name, stat) in columns {
        // DDL may have retired an identity; it must not be reattached to a new column.
        let Ok(id) = col_id(table, name) else {
            continue;
        };
        set.column_stats.push(vec![
            u(table.id),
            u(scope),
            u(id as u64),
            u(table.schema_version),
            u(stat.null_count),
            u(stat.distinct_count),
            real(stat.average_width_bytes),
            Value::Bytes(encode(1, stat)?),
        ]);
    }
    for (name, stat) in indexes {
        let Ok(id) = index_id(table, name) else {
            continue;
        };
        set.index_stats.push(vec![
            u(table.id),
            u(scope),
            u(id),
            u(table.schema_version),
            Value::Bytes(encode(2, stat)?),
        ]);
    }
    Ok(())
}

pub(super) fn append(set: &mut DdRowSet, table: &TableSchema) -> Result<(), QueryError> {
    let s = &table.stats;
    set.table_stats.push(base(
        table,
        0,
        s.row_count,
        s.heap_page_count,
        s.modified_rows_since_analyze,
        s.analyzed_at_unix_ms,
        None,
    ));
    objects(set, table, 0, &s.columns, &s.indexes)?;
    for (&scope, p) in &s.partition_stats {
        if !table.storage_ids().contains(&scope) {
            continue;
        }
        set.table_stats.push(base(
            table,
            scope,
            p.row_count,
            p.heap_page_count,
            p.modified_rows_since_analyze,
            p.analyzed_at_unix_ms,
            p.check_ok,
        ));
        objects(set, table, scope, &p.columns, &p.indexes)?;
    }
    let mut groups = Vec::new();
    for g in s.multi_column_stats.values() {
        let Ok(columns) = ids(table, &g.columns) else {
            continue;
        };
        groups.push((columns, g.distinct_count));
    }
    groups.sort_by(|a, b| a.0.cmp(&b.0));
    for (ordinal, g) in groups.iter().enumerate() {
        set.extended_stats.push(vec![
            u(table.id),
            u(0),
            u(1),
            u(ordinal as u64),
            u(table.schema_version),
            Value::Bytes(encode(3, g)?),
        ]);
    }
    let mut domains = Vec::new();
    for g in &s.conditional_key_domains {
        let (Ok(filters), Ok(keys)) = (ids(table, &g.filter_columns), ids(table, &g.key_columns))
        else {
            continue;
        };
        domains.push((filters, keys, g.entries.clone()));
    }
    domains.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    for (ordinal, g) in domains.iter().enumerate() {
        set.extended_stats.push(vec![
            u(table.id),
            u(0),
            u(2),
            u(ordinal as u64),
            u(table.schema_version),
            Value::Bytes(encode(4, g)?),
        ]);
    }
    for (name, histogram) in &s.explicit_histograms {
        let Ok(id) = col_id(table, name) else {
            continue;
        };
        set.column_histograms.push(vec![
            u(table.id),
            u(id as u64),
            u(table.schema_version),
            Value::Bytes(encode(5, histogram)?),
        ]);
    }
    Ok(())
}

pub(super) fn validate_rows(set: &DdRowSet) -> Result<(), QueryError> {
    let owners: BTreeSet<_> = set
        .tables
        .iter()
        .map(|r| uint(r, 0))
        .collect::<Result<_, _>>()?;
    for (id, rows) in set.per_table() {
        if !super::data_dictionary::DD_STATS_TABLES.contains(&id) {
            continue;
        }
        let schema = super::data_dictionary::dd_table_schema(id).ok_or_else(corrupt)?;
        for row in rows {
            if row.len() != schema.columns.len() || !owners.contains(&uint(row, 0)?) {
                return Err(corrupt());
            }
            for n in 0..schema.primary_key.len() {
                uint(row, n)?;
            }
        }
    }
    Ok(())
}

/// Every family is read at one MVCC snapshot by the caller.
pub(super) fn load(set: &DdRowSet, table: &TableSchema) -> Result<TableStats, QueryError> {
    let mut stats = TableStats::empty();
    stats.analyzed_at_unix_ms = None;
    let mut seen = BTreeSet::new();
    let roots: Vec<_> = set
        .table_stats
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id) && uint(r, 1).ok() == Some(0))
        .collect();
    if roots.len() != 1 {
        return Err(corrupt());
    }
    let version = uint(roots[0], 2)?;
    if version > table.schema_version {
        return Err(corrupt());
    }
    for row in set
        .table_stats
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id))
    {
        if row.len() != 15 {
            return Err(corrupt());
        }
        let scope = uint(row, 1)?;
        if !seen.insert(scope) {
            return Err(corrupt());
        }
        let object_version = uint(row, 2)?;
        if object_version > version {
            return Err(corrupt());
        }
        let (count, pages, modified, analyzed) = (
            uint(row, 3)?,
            uint(row, 4)?,
            uint(row, 5)?,
            optional(row, 6)?,
        );
        if scope == 0 {
            stats.row_count = count;
            stats.heap_page_count = pages;
            stats.modified_rows_since_analyze = modified;
            stats.analyzed_at_unix_ms = analyzed;
            stats.sample_fraction = float(row, 7)?;
            stats.confidence = float(row, 8)?;
            if stats.sample_fraction.is_some_and(|v| v > 1.0)
                || stats.confidence.is_some_and(|v| v > 1.0)
            {
                return Err(corrupt());
            }
            stats.analyze_revision = uint(row, 9)?;
            stats.last_analyze_live_baseline = match row.get(10) {
                Some(Value::Int(v)) => *v,
                _ => return Err(corrupt()),
            };
            stats.last_analyze_modification_baseline = uint(row, 11)?;
        } else {
            if !table.is_partitioned() || !table.storage_ids().contains(&scope) {
                return Err(corrupt());
            }
            let check_ok = optional(row, 12)?.map(|v| v != 0);
            stats.partition_stats.insert(
                scope,
                PartitionStats {
                    row_count: count,
                    heap_page_count: pages,
                    columns: BTreeMap::new(),
                    indexes: BTreeMap::new(),
                    modified_rows_since_analyze: modified,
                    analyzed_at_unix_ms: analyzed,
                    check_ok,
                },
            );
        }
    }
    if !seen.contains(&0) {
        return Err(corrupt());
    }
    for row in set
        .column_stats
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id))
    {
        if row.len() != 8 || uint(row, 3)? > version {
            return Err(corrupt());
        }
        let scope = uint(row, 1)?;
        let name = col_name(table, u32::try_from(uint(row, 2)?).map_err(|_| corrupt())?)?;
        let s: ColumnStats = decode(1, bytes(row, 7)?)?;
        if s.null_count != uint(row, 4)?
            || s.distinct_count != uint(row, 5)?
            || s.average_width_bytes != float(row, 6)?
            || s.histogram
                .windows(2)
                .any(|w| w[0].cumulative_count > w[1].cumulative_count)
        {
            return Err(corrupt());
        }
        let cols = if scope == 0 {
            &mut stats.columns
        } else {
            &mut stats
                .partition_stats
                .get_mut(&scope)
                .ok_or_else(corrupt)?
                .columns
        };
        if cols.insert(name, s).is_some() {
            return Err(corrupt());
        }
    }
    for row in set
        .index_stats
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id))
    {
        if row.len() != 5 || uint(row, 3)? > version {
            return Err(corrupt());
        }
        let scope = uint(row, 1)?;
        let id = uint(row, 2)?;
        let index = table
            .indexes
            .iter()
            .find(|i| i.index_id == id)
            .ok_or_else(corrupt)?;
        let s: IndexStats = decode(2, bytes(row, 4)?)?;
        if s.prefix_distinct_counts.len() > index.columns.len() {
            return Err(corrupt());
        }
        let indexes = if scope == 0 {
            &mut stats.indexes
        } else {
            &mut stats
                .partition_stats
                .get_mut(&scope)
                .ok_or_else(corrupt)?
                .indexes
        };
        if indexes
            .insert(super::normalize_name(&index.name), s)
            .is_some()
        {
            return Err(corrupt());
        }
    }
    let mut seen = BTreeSet::new();
    for row in set
        .extended_stats
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id))
    {
        if row.len() != 6 || uint(row, 1)? != 0 || uint(row, 4)? > version {
            return Err(corrupt());
        }
        let kind = uint(row, 2)?;
        if !seen.insert((kind, uint(row, 3)?)) {
            return Err(corrupt());
        }
        match kind {
            1 => {
                let (cols, distinct_count): (Vec<u32>, u64) = decode(3, bytes(row, 5)?)?;
                let columns = names(table, &cols)?;
                let key = multi_column_group_key(&columns);
                if columns.len() < 2
                    || stats
                        .multi_column_stats
                        .insert(
                            key,
                            MultiColumnStats {
                                columns,
                                distinct_count,
                            },
                        )
                        .is_some()
                {
                    return Err(corrupt());
                }
            }
            2 => {
                let (filters, keys, entries): (Vec<u32>, Vec<u32>, Vec<ConditionalKeyDomainEntry>) =
                    decode(4, bytes(row, 5)?)?;
                if filters.is_empty()
                    || keys.is_empty()
                    || entries
                        .iter()
                        .any(|e| e.filter_values.len() != filters.len())
                {
                    return Err(corrupt());
                }
                stats
                    .conditional_key_domains
                    .push(ConditionalKeyDomainStats {
                        filter_columns: names(table, &filters)?,
                        key_columns: names(table, &keys)?,
                        entries,
                    });
            }
            _ => return Err(corrupt()),
        }
    }
    for row in set
        .column_histograms
        .iter()
        .filter(|r| uint(r, 0).ok() == Some(table.id))
    {
        if row.len() != 4 || uint(row, 2)? > version {
            return Err(corrupt());
        }
        let name = col_name(table, u32::try_from(uint(row, 1)?).map_err(|_| corrupt())?)?;
        let h: SqlHistogram = decode(5, bytes(row, 3)?)?;
        if h.requested_buckets == 0
            || h.requested_buckets > 1024
            || h.buckets.len() > h.requested_buckets as usize
            || !h.null_fraction.is_finite()
            || !(0.0..=1.0).contains(&h.null_fraction)
            || !h.sample_fraction.is_finite()
            || !(0.0..=1.0).contains(&h.sample_fraction)
            || h.buckets.iter().any(|b| {
                !b.cumulative_frequency.is_finite()
                    || !(0.0..=1.0).contains(&b.cumulative_frequency)
            })
            || h.buckets
                .windows(2)
                .any(|w| w[0].cumulative_frequency > w[1].cumulative_frequency)
            || stats.explicit_histograms.insert(name, h).is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(stats)
}
