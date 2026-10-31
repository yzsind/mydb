//! Stable entry points for out-of-process fuzz targets.
//!
//! This module is feature-gated so decoder internals do not become part of
//! the normal public API merely to make `cargo-fuzz` a separate crate.

/// Exercise MySQL classic-protocol packet framing with arbitrary bytes.
pub fn mysql_packet(bytes: &[u8]) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    else {
        return;
    };
    let mut input = bytes;
    let _ = runtime.block_on(crate::mysql::packet::read_packet(&mut input));
    let _ = crate::mysql::protocol::parse_ssl_request(bytes);
    let _ = crate::mysql::protocol::parse_handshake_response(bytes);
    let _ = crate::mysql::protocol::parse_change_user_request(bytes, u32::MAX);
    let _ = crate::mysql::protocol::parse_query_attributes(bytes);
    let param_count = bytes.first().copied().unwrap_or(0).min(32) as usize;
    let _ = crate::mysql::protocol::parse_stmt_execute_params(bytes, param_count, None, None, true);
    let _ = crate::binlog::parse_register_slave_payload(bytes);
    let _ = crate::binlog::parse_binlog_dump_request(bytes);
    let _ = crate::binlog::parse_binlog_dump_gtid_request(bytes);
    let _ = crate::binlog::parse_semi_sync_ack(bytes);
}

/// Exercise the self-describing legacy row decoder.
pub fn row_codec(bytes: &[u8]) {
    crate::storage::fuzz_decode_row(bytes);
}

/// Exercise JSONB validation, traversal and text materialization.
pub fn jsonb(bytes: &[u8]) {
    crate::engine::json::fuzz_decode_jsonb(bytes);
}

/// Exercise WAL frame header, checksum and payload decoding.
pub fn wal_frame(bytes: &[u8]) {
    let _ = crate::wal::decode_frame_for_replication(bytes);
}

/// Exercise native-replication envelope/message decoding, including manifest
/// lengths and the protocol-v2 metadata tails.
pub fn native_repl_protocol(bytes: &[u8]) {
    let mut cursor = std::io::Cursor::new(bytes);
    let _ = crate::repl::protocol::read_frame(&mut cursor);
}

/// Differential oracle for the compiled expression program. Input bytes build
/// a bounded scalar AST and one typed row; results, errors and diagnostics must
/// match the legacy row evaluator exactly.
pub fn expression_differential(bytes: &[u8]) {
    use crate::engine::expr_program::{with_eval_scratch, ExprCompiler, InputSchema};
    use crate::parser::{BinaryOp, Expr, Literal, UnaryOp};
    use crate::types::{Column, ColumnType, Value};

    struct Bytes<'a> {
        input: &'a [u8],
        at: usize,
    }
    impl Bytes<'_> {
        fn next(&mut self) -> u8 {
            if self.input.is_empty() {
                return 0;
            }
            let value = self.input[self.at % self.input.len()];
            self.at += 1;
            value
        }
    }
    fn build(input: &mut Bytes<'_>, depth: usize) -> Expr {
        let leaf = |tag: u8| match tag % 7 {
            0 => Expr::Identifier("n".into()),
            1 => Expr::Identifier("s".into()),
            2 => Expr::Literal(Literal::Int((tag as i8) as i64)),
            3 => Expr::Literal(Literal::UInt(tag as u64)),
            4 => Expr::Literal(Literal::String(format!(" {} ", tag % 10))),
            5 => Expr::Literal(Literal::Decimal(format!("{}.{}", tag % 10, tag % 7))),
            _ => Expr::Literal(Literal::Null),
        };
        let tag = input.next();
        if depth == 0 || tag % 5 == 0 {
            return leaf(tag);
        }
        match tag % 12 {
            0 => Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(build(input, depth - 1)),
            },
            1 => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(build(input, depth - 1)),
            },
            2 => Expr::IsNull(Box::new(build(input, depth - 1))),
            3..=9 => {
                let op = match tag % 7 {
                    0 => BinaryOp::Add,
                    1 => BinaryOp::Sub,
                    2 => BinaryOp::Mul,
                    3 => BinaryOp::Div,
                    4 => BinaryOp::Eq,
                    5 => BinaryOp::Lt,
                    _ => BinaryOp::And,
                };
                Expr::Binary {
                    left: Box::new(build(input, depth - 1)),
                    op,
                    right: Box::new(build(input, depth - 1)),
                }
            }
            10 => Expr::Function {
                name: "COALESCE".into(),
                args: vec![build(input, depth - 1), build(input, depth - 1)],
                distinct: false,
            },
            _ => Expr::Function {
                name: if tag & 0x80 == 0 { "ABS" } else { "LENGTH" }.into(),
                args: vec![build(input, depth - 1)],
                distinct: false,
            },
        }
    }

    if bytes.is_empty() {
        return;
    }
    let mut input = Bytes {
        input: bytes,
        at: 0,
    };
    let expr = build(&mut input, 5);
    let number = (input.next() as i8) as i64;
    let string = format!("Ab {} ", input.next() % 10);
    let row = vec![Value::Int(number), Value::String(string)];
    let columns = vec![
        Column {
            name: "n".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        Column {
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
    ];
    let mut table = crate::storage::TableSchema::synthetic_for_planning("fuzz_expr", &columns);
    table.build_column_index();
    // Both evaluators must see the same catalog type and conversion provenance.
    let schema = InputSchema::from_table(&table);
    let Ok(program) = ExprCompiler::compile(&expr, &schema) else {
        return;
    };
    crate::engine::api::clear_statement_warnings();
    let row_view = crate::engine::expr_program::RowView::from_values(&row);
    let compiled = with_eval_scratch(|scratch| program.eval_row(&row_view, scratch));
    let compiled_warnings = crate::engine::api::statement_warnings_snapshot();
    crate::engine::api::clear_statement_warnings();
    let legacy = crate::engine::expr::eval_expr_legacy(&expr, &table, &row);
    let legacy_warnings = crate::engine::api::statement_warnings_snapshot();
    assert_eq!(
        compiled, legacy,
        "compiled/legacy value mismatch for {expr:?}"
    );
    assert_eq!(
        compiled_warnings, legacy_warnings,
        "compiled/legacy warning mismatch for {expr:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzz_round1_string_column_warning_parity() {
        expression_differential(b"$");
    }

    #[test]
    fn fuzz_round1_comparison_preserves_division_warnings() {
        expression_differential(&[0x04, 0x05, 0x88]);
    }

    #[test]
    fn fuzz_round1_nested_conditional_numeric_domain() {
        expression_differential(&[
            177, 82, 56, 74, 116, 88, 50, 64, 214, 133, 204, 87, 145, 222, 213, 230, 125, 79, 181,
            218, 1, 87, 153, 12, 84, 176, 191, 38, 54, 6, 98, 142, 81, 19,
        ]);
    }

    #[test]
    fn fuzz_round1_abs_bound_numeric_result() {
        expression_differential(&[
            119, 63, 81, 189, 238, 31, 77, 95, 7, 171, 70, 66, 44, 102, 58, 22, 218, 207, 108, 242,
            235, 18, 18, 10, 202,
        ]);
    }

    #[test]
    fn fuzz_round1_coalesce_unsigned_signed_result() {
        expression_differential(&[0x22, 0x0a, 0x25, 0x0a]);
    }

    #[test]
    fn decoder_fuzz_entry_points_reject_truncated_inputs_without_panicking() {
        for bytes in [&[][..], &[0][..], &[0xff, 0xff, 0xff, 0xff][..]] {
            mysql_packet(bytes);
            row_codec(bytes);
            jsonb(bytes);
            wal_frame(bytes);
            expression_differential(bytes);
        }
        mysql_packet(&[1, 0, 0, 0, b'x']);
    }
}

/// Bounded UTF-8 SQL corpus with a two-byte parser mode prefix.
fn parser_input(bytes: &[u8]) -> Option<(u16, &str)> {
    if bytes.len() < 2 || bytes.len() > 65_538 {
        return None;
    }
    Some((
        u16::from_le_bytes([bytes[0], bytes[1]]),
        std::str::from_utf8(&bytes[2..]).ok()?,
    ))
}

pub fn sql_parser(bytes: &[u8]) {
    let Some((bits, sql)) = parser_input(bytes) else {
        return;
    };
    let _mode = crate::parser::ParserSqlModeGuard::activate(bits);
    let _ = crate::parser::parse_statement(sql);
    let _ = crate::parser::parse_statements(sql);
    let _ = crate::parser::parse_expr_fragment(sql);
    assert!(crate::parser::parse_statement("SELECT 1").is_ok());
}

pub fn sql_lex(bytes: &[u8]) {
    let Some((bits, sql)) = parser_input(bytes) else {
        return;
    };
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(bits);
    let version = crate::server_version::MYSQL_COMPAT_VERSION_ID;
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, version) else {
        return;
    };
    let effective = prepared.effective();
    let mut cursor = crate::sql_lex::SqlTokenCursor::new(effective, mode, version);
    let mut end = 0;
    while let Ok(Some(token)) = cursor.next() {
        assert!(token.span.start >= end && token.span.end > token.span.start);
        assert!(effective.get(token.span.clone()).is_some());
        end = token.span.end;
    }
    if let Ok(parts) =
        crate::sql_lex::StructuralCursor::split_top_level(effective, b',', mode, version)
    {
        assert!(parts
            .iter()
            .all(|range| effective.get(range.clone()).is_some()));
    }
}

pub fn parser_roundtrip(bytes: &[u8]) {
    let Some((bits, sql)) = parser_input(bytes) else {
        return;
    };
    let _mode = crate::parser::ParserSqlModeGuard::activate(bits);
    let Ok(expr) = crate::parser::parse_expr_fragment(sql) else {
        return;
    };
    let Ok(encoded) = crate::parser::try_expr_to_sql(&expr) else {
        return;
    };
    let decoded =
        crate::parser::parse_expr_fragment(&encoded).expect("encoded scalar SQL must parse");
    assert_eq!(expr, decoded);
}

pub fn parser_fast_path_differential(bytes: &[u8]) {
    let Some((bits, sql)) = parser_input(bytes) else {
        return;
    };
    crate::parser::fuzz_fast_path_differential(sql, bits);
}

#[cfg(test)]
mod parser_smoke {
    use super::*;

    #[test]
    fn parser_corpus_modes_and_recovery() {
        let seeds = [
            "SELECT 1",
            "INSERT INTO t VALUES(1,'a'),(2,'b')",
            "SELECT 'a\\f'",
            "a + (b*3)",
            "CASE WHEN a IS NULL THEN 1 ELSE 2 END",
            "CAST(a AS SIGNED)",
            "TRIM('x' FROM 'xabcx')",
            "1e0",
            "a LIKE 'x%' ESCAPE ''",
            "SELECT /*!1 + */ 2",
            "CREATE VIEW a视图 AS SELECT 1",
            "SELECT (",
            "SELECT 1 FOR JSON AUTO",
            "INSERT INTO t VALUES(__MYDB_EMPTY_ROW__)",
        ];
        for bits in [
            0u16,
            1 << 9,
            1 << 10,
            (1 << 9) | (1 << 10),
            1 << 13,
            1 << 14,
            1 << 15,
        ] {
            for seed in seeds {
                let mut input = bits.to_le_bytes().to_vec();
                input.extend_from_slice(seed.as_bytes());
                sql_parser(&input);
                sql_lex(&input);
                parser_roundtrip(&input);
                parser_fast_path_differential(&input);
            }
        }
        let alphabet = b" abcdef0123(),'`\";/*+-?#!";
        let mut state = 20261014u64;
        for _ in 0..1000 {
            let mut input = vec![0, 0];
            for _ in 0..64 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                input.push(alphabet[state as usize % alphabet.len()]);
            }
            sql_parser(&input);
            sql_lex(&input);
            parser_roundtrip(&input);
            parser_fast_path_differential(&input);
        }
    }
}

/// Exercise persistence control, row-codec metadata, undo and page decoders.
pub fn storage_formats(bytes: &[u8]) {
    crate::storage::fuzz_decode_storage_formats(bytes);
}
