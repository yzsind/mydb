//! `QueryResponse` / `QueryError` → mysql `--batch` tab-separated text.
//!
//! This is the single source of truth for turning a statement's outcome into
//! the textual shape MySQL's `mysqltest` records into a `.result` golden file:
//!
//! * a result set → a tab-separated header row (column names) followed by one
//!   tab-separated row per record (`Value::to_display_string`, SQL `NULL` →
//!   the literal token `NULL`);
//! * a bare `OK` / `OK` with affected-row count → **no output** (MTR only
//!   records affected rows under `--enable_info`, which M0 does not implement);
//! * an error → `ERROR <sqlstate>: <message>`.
//!
//! Keep this aligned with the MySQL client's text encoding so imported
//! `.result` files diff meaningfully against mydb's own recorded goldens.

use crate::engine::api::{QueryResponse, ResultSet};
use crate::types::{Column, ColumnType, QueryError, Value};

/// Per-statement output modifiers gathered from `--sorted_result` /
/// `--replace_column`.
#[derive(Clone, Default)]
pub(super) struct RenderMods {
    pub sorted: bool,
    /// Emit mysqltest's protocol column metadata table before the ordinary
    /// header/rows (`--enable_metadata`).
    pub metadata: bool,
    /// Active `character_set_results` for the connection boundary.
    pub result_charset: Option<String>,
    /// Render each result cell as `column<TAB>value`, matching mysqltest's
    /// `query_vertical` / `--vertical_results` mode.
    pub vertical: bool,
    /// Number of leading columns whose existing order must be preserved.
    /// Rows are sorted only within contiguous groups sharing this prefix.
    pub partially_sorted_from: Option<usize>,
    /// `(1-based column, replacement value)` from `--replace_column`.
    pub replace_column: Vec<(usize, String)>,
}

/// Render a successful statement outcome into `out`, appending a trailing
/// newline per emitted line. A `MultiResult` renders each part in order.
pub(super) fn render_response(resp: &QueryResponse, mods: &RenderMods, out: &mut String) {
    match resp {
        QueryResponse::Ok | QueryResponse::OkAffected { .. } => {}
        QueryResponse::ResultSet(rs) => render_result_set(rs, mods, out),
        QueryResponse::SharedResultSet(rs) => render_result_set(
            &crate::engine::api::ResultSet {
                columns: rs.columns.to_vec(),
                rows: rs.rows.clone(),
            },
            mods,
            out,
        ),
        QueryResponse::StreamingResultSet(_) => {
            // The test harness executes through `execute`, which materialises
            // streaming results into `ResultSet`, so this arm is unreachable
            // in practice. Emit a stable marker rather than silently dropping.
            out.push_str("<streaming result not materialised by mtr-lite>\n");
        }
        QueryResponse::MultiResult(parts) => {
            for part in parts {
                render_response(part, mods, out);
            }
        }
    }
}

/// Render a result set as `col1\tcol2\n` header + one `v1\tv2\n` line per row.
/// With `mods.sorted` the rendered data rows are sorted lexicographically,
/// which is how `--sorted_result` removes row-order nondeterminism (parallel
/// scans in mydb can otherwise emit rows in an arbitrary order).
/// `mods.replace_column` overwrites the given 1-based columns with a fixed
/// value (`--replace_column`) to mask nondeterministic cells.
fn render_result_set(rs: &ResultSet, mods: &RenderMods, out: &mut String) {
    if mods.metadata {
        render_metadata(rs, mods.result_charset.as_deref(), out);
    }
    if mods.vertical {
        let zerofill = crate::mysql::protocol::zerofill_pad_spec(&rs.columns);
        for row in &rs.rows {
            let padded;
            let row = if let Some(spec) = zerofill.as_deref() {
                padded = crate::mysql::protocol::zerofill_padded_row(row, spec);
                padded.as_slice()
            } else {
                row.as_slice()
            };
            for (idx, column) in rs.columns.iter().enumerate() {
                out.push_str(&column.name);
                out.push('\t');
                let value = row
                    .get(idx)
                    .map(|value| {
                        render_cell(value, mods.result_charset.as_deref(), rs.columns.get(idx))
                    })
                    .unwrap_or_else(|| "NULL".to_string());
                out.push_str(&value);
                out.push('\n');
            }
        }
        return;
    }

    let header = rs
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join("\t");
    out.push_str(&header);
    out.push('\n');

    let zerofill = crate::mysql::protocol::zerofill_pad_spec(&rs.columns);
    let mut rows = rs
        .rows
        .iter()
        .map(|row| {
            let padded;
            let row = if let Some(spec) = zerofill.as_deref() {
                padded = crate::mysql::protocol::zerofill_padded_row(row, spec);
                padded.as_slice()
            } else {
                row.as_slice()
            };
            row.iter()
                .enumerate()
                .map(
                    |(idx, v)| match mods.replace_column.iter().find(|(c, _)| *c == idx + 1) {
                        Some((_, val)) => val.clone(),
                        None => render_cell(v, mods.result_charset.as_deref(), rs.columns.get(idx)),
                    },
                )
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if mods.sorted {
        rows.sort();
    } else if let Some(start_column) = mods.partially_sorted_from {
        partially_sort_rows(&mut rows, start_column);
    }
    for row in rows {
        out.push_str(&row.join("\t"));
        out.push('\n');
    }
}

fn read_lenenc_string(packet: &[u8], offset: &mut usize) -> Vec<u8> {
    let Some(&length) = packet.get(*offset) else {
        return Vec::new();
    };
    *offset += 1;
    // Column-definition identifiers in this harness are far below 251 bytes.
    let length = usize::from(length);
    let end = (*offset).saturating_add(length).min(packet.len());
    let value = packet[*offset..end].to_vec();
    *offset = end;
    value
}

fn render_metadata(rs: &ResultSet, result_charset: Option<&str>, out: &mut String) {
    out.push_str(
        "Catalog\tDatabase\tTable\tTable_alias\tColumn\tColumn_alias\tType\tLength\tMax length\tIs_null\tFlags\tDecimals\tCharsetnr\n",
    );
    for (column_index, column) in rs.columns.iter().enumerate() {
        let mut source = column.source.clone();
        if matches!(
            column.column_type,
            crate::types::ColumnType::VarString | crate::types::ColumnType::Vector
        ) {
            if let Some(charset) = crate::charset::resolve_result_charset(result_charset) {
                let source_max_len = source
                    .collation_id
                    .and_then(crate::charset::resolve_collation_id)
                    .and_then(|collation| crate::charset::resolve_charset(collation.charset).ok())
                    .map(|charset| u32::from(charset.max_len))
                    .unwrap_or(1);
                if let Some(length) = source.length.as_mut() {
                    let source_length = *length;
                    *length = source_length
                        .checked_div(source_max_len)
                        .unwrap_or(source_length)
                        .saturating_mul(u32::from(charset.max_len));
                }
                source.collation_id = crate::charset::default_collation_for_charset(charset.name)
                    .ok()
                    .map(crate::charset::collation_id);
            }
        }
        let packet = crate::mysql::protocol::column_definition_packet_full(
            &column.name,
            column.column_type,
            &source,
        );
        let mut offset = 0usize;
        let catalog = read_lenenc_string(&packet, &mut offset);
        let schema = read_lenenc_string(&packet, &mut offset);
        let table_alias = read_lenenc_string(&packet, &mut offset);
        let table = read_lenenc_string(&packet, &mut offset);
        let column_alias = read_lenenc_string(&packet, &mut offset);
        let org_column = read_lenenc_string(&packet, &mut offset);
        offset = offset.saturating_add(1); // fixed-fields length (0x0c)
        if offset + 10 > packet.len() {
            continue;
        }
        let charset = u16::from_le_bytes([packet[offset], packet[offset + 1]]);
        offset += 2;
        let length = u32::from_le_bytes([
            packet[offset],
            packet[offset + 1],
            packet[offset + 2],
            packet[offset + 3],
        ]);
        offset += 4;
        let column_type = packet[offset];
        offset += 1;
        let flags = u16::from_le_bytes([packet[offset], packet[offset + 1]]);
        offset += 2;
        let decimals = packet[offset];
        let max_length = rs
            .rows
            .iter()
            .filter_map(|row| row.get(column_index))
            .filter(|value| !matches!(value, Value::Null))
            .map(|value| match value {
                Value::Bytes(bytes) => bytes.len(),
                _ => render_cell(value, None, rs.columns.get(column_index))
                    .as_bytes()
                    .len(),
            })
            .max()
            .unwrap_or(0);
        let is_null = if flags & 1 == 0 { "Y" } else { "N" };
        let fields = [
            super::lossless_report_text(&catalog),
            super::lossless_report_text(&schema),
            super::lossless_report_text(&table),
            super::lossless_report_text(&table_alias),
            super::lossless_report_text(&org_column),
            super::lossless_report_text(&column_alias),
            column_type.to_string(),
            length.to_string(),
            max_length.to_string(),
            is_null.to_string(),
            flags.to_string(),
            decimals.to_string(),
            charset.to_string(),
        ];
        out.push_str(&fields.join("\t"));
        out.push('\n');
    }
}

fn partially_sort_rows(rows: &mut [Vec<String>], start_column: usize) {
    if start_column == 0 {
        rows.sort();
        return;
    }
    let mut group_start = 0;
    while group_start < rows.len() {
        let prefix_len = start_column.min(rows[group_start].len());
        let mut group_end = group_start + 1;
        while group_end < rows.len()
            && rows[group_end].get(..prefix_len) == rows[group_start].get(..prefix_len)
        {
            group_end += 1;
        }
        rows[group_start..group_end]
            .sort_by(|left, right| left[prefix_len..].cmp(&right[prefix_len..]));
        group_start = group_end;
    }
}

/// One cell in mysql `--batch` form. `Value::to_display_string` already
/// renders SQL `NULL` as the literal token `NULL`, which matches the client.
fn render_cell(v: &Value, result_charset: Option<&str>, column: Option<&Column>) -> String {
    if let (
        Value::UInt(_) | Value::Int(_),
        Some(Column {
            column_type: ColumnType::Bit { bits },
            ..
        }),
    ) = (v, column)
    {
        let value = match v {
            Value::UInt(value) => *value,
            Value::Int(value) => *value as u64,
            _ => unreachable!(),
        };
        let width = usize::from((*bits).max(1).div_ceil(8));
        let encoded = value.to_be_bytes();
        return super::lossless_report_text(&encoded[encoded.len() - width..]);
    }
    match v {
        // `mysqltest` writes binary cells byte-for-byte. The harness output is
        // a Rust `String`, so use the comparator's reversible private-use
        // projection for invalid octets instead of U+FFFD (which loses both
        // value identity and length).
        Value::Bytes(bytes) => super::lossless_report_text(bytes),
        Value::String(text) | Value::Decimal(text) => result_charset
            .and_then(|charset| crate::charset::encode_string(charset, text).ok())
            .map(|bytes| super::lossless_report_text(&bytes))
            .unwrap_or_else(|| text.clone()),
        Value::Time(micros) => {
            let mut buf = [0u8; 32];
            String::from_utf8_lossy(crate::types::write_time_canonical_with_fsp(
                *micros,
                column.and_then(|column| column.source.decimals),
                &mut buf,
            ))
            .into_owned()
        }
        Value::DateTime(micros) | Value::Timestamp(micros) => {
            let mut buf = [0u8; 32];
            String::from_utf8_lossy(crate::types::write_datetime_canonical_with_fsp(
                *micros,
                column.and_then(|column| column.source.decimals),
                &mut buf,
            ))
            .into_owned()
        }
        Value::Float(value) => match column.and_then(|column| column.source.decimals) {
            Some(decimals) if decimals != 0x1f => {
                format!("{value:.decimals$}", decimals = usize::from(decimals))
            }
            _ => v.to_display_string(),
        },
        Value::Double(value) => match column.and_then(|column| column.source.decimals) {
            Some(decimals) if decimals != 0x1f => {
                format!("{value:.decimals$}", decimals = usize::from(decimals))
            }
            _ => v.to_display_string(),
        },
        _ => v.to_display_string(),
    }
}

/// Render an error the way MTR records it: `ERROR <sqlstate>: <message>`.
pub(super) fn render_error(err: &QueryError, out: &mut String) {
    out.push_str("ERROR ");
    out.push_str(&err.sql_state());
    out.push_str(": ");
    out.push_str(&err.message());
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::{partially_sort_rows, render_result_set, RenderMods};
    use crate::engine::api::ResultSet;
    use crate::types::{Column, ColumnType, Value};

    #[test]
    fn partially_sorted_result_preserves_prefix_order() {
        let mut rows = vec![
            vec!["2".into(), "b".into()],
            vec!["2".into(), "a".into()],
            vec!["1".into(), "d".into()],
            vec!["1".into(), "c".into()],
        ];
        partially_sort_rows(&mut rows, 1);
        assert_eq!(
            rows,
            vec![
                vec!["2", "a"],
                vec!["2", "b"],
                vec!["1", "c"],
                vec!["1", "d"],
            ]
        );
    }

    #[test]
    fn vertical_result_renders_one_column_value_pair_per_line() {
        let rs = ResultSet {
            columns: vec![
                Column {
                    name: "a".into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                },
                Column {
                    name: "b".into(),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                },
            ],
            rows: vec![vec![Value::Int(1), Value::String("x".into())]],
        };
        let mut out = String::new();
        render_result_set(
            &rs,
            &RenderMods {
                vertical: true,
                ..Default::default()
            },
            &mut out,
        );
        assert_eq!(out, "a\t1\nb\tx\n");
    }

    #[test]
    fn fixed_decimal_float_metadata_controls_text_rendering() {
        let mut source = crate::types::ColumnSource::default();
        source.decimals = Some(6);
        let rs = ResultSet {
            columns: vec![Column {
                name: "PI()".into(),
                column_type: ColumnType::Double,
                source,
            }],
            rows: vec![vec![Value::Double(std::f64::consts::PI)]],
        };
        let mut out = String::new();
        render_result_set(&rs, &RenderMods::default(), &mut out);
        assert_eq!(out, "PI()\n3.141593\n");
    }
}
