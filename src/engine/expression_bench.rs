//! Tools-only microbenchmarks for the real expression compiler and batch lanes.
//! No copies of expression semantics or kernels are used here.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::executor::vec::batch::{Batch, ColumnVector};
use super::executor::vec::expr_batch::{
    batch_schema_to_input, eval_value_per_row, BatchExprProgram,
};
use super::executor::vec::schema::{BatchField, BatchSchema};
use super::expr_program::ExprCompiler;
use crate::parser::{Expr, SelectItem, Statement};
use crate::types::{ColumnType, Value};

#[derive(serde::Serialize)]
pub struct ExpressionSample {
    pub name: &'static str,
    pub rows_per_call: usize,
    pub repeats: usize,
    pub samples_ns: Vec<f64>,
}

fn measure(name: &'static str, rows_per_call: usize, mut run: impl FnMut()) -> ExpressionSample {
    for _ in 0..8 {
        run();
    }
    let mut repeats = 1usize;
    loop {
        let start = Instant::now();
        for _ in 0..repeats {
            run();
        }
        if start.elapsed() >= Duration::from_millis(50) || repeats >= 1 << 20 {
            break;
        }
        repeats *= 2;
    }
    let samples_ns = (0..11)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..repeats {
                run();
            }
            start.elapsed().as_nanos() as f64 / repeats as f64
        })
        .collect();
    ExpressionSample {
        name,
        rows_per_call,
        repeats,
        samples_ns,
    }
}

fn expression(sql: &str) -> Expr {
    let Statement::Select(select) =
        crate::parser::parse_statement(&format!("SELECT {sql}")).unwrap()
    else {
        unreachable!()
    };
    let SelectItem::Expr { expr, .. } = select.projection.into_iter().next().unwrap() else {
        unreachable!()
    };
    expr
}

/// Run in a separate process for each MYDB_VECTORIZED mode. Report per-call
/// samples; the caller writes JSON alongside end-to-end executor measurements.
pub fn run() -> Vec<ExpressionSample> {
    let schema = Arc::new(BatchSchema::new(
        (0..32)
            .map(|i| BatchField {
                table: "t".into(),
                alias: None,
                name: format!("c{i}"),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            })
            .collect(),
    ));
    let input = batch_schema_to_input(&schema);
    let arithmetic = expression("(c0 + c31 * 3) % 17 > 4");
    let program = ExprCompiler::compile(&arithmetic, &input).unwrap();
    let rows = 513;
    let batch = Batch::new(
        schema.clone(),
        (0..32)
            .map(|col| {
                ColumnVector::from_values(
                    (0..rows)
                        .map(|row| {
                            if row % 19 == 0 {
                                Value::Null
                            } else {
                                Value::Int((row + col) as i64)
                            }
                        })
                        .collect(),
                )
            })
            .collect(),
    );
    let mut results = vec![
        measure("compile_uncached", 0, || {
            black_box(ExprCompiler::compile(black_box(&arithmetic), black_box(&input)).unwrap());
        }),
        measure("compile_cached", 0, || {
            black_box(
                ExprCompiler::compile_cached(black_box(&arithmetic), black_box(&input)).unwrap(),
            );
        }),
        measure("referenced_columns", 0, || {
            black_box(black_box(&program).referenced_columns());
        }),
        measure("row_projection_wide", rows, || {
            black_box(eval_value_per_row(black_box(&program), black_box(&batch)).unwrap());
        }),
    ];
    for (name, sql) in [
        ("typed_predicate", "c0 > 127"),
        ("fallback_predicate", "(c0 + c31 * 3) % 17 > 4"),
    ] {
        let predicate = BatchExprProgram::compile(&expression(sql), &schema)
            .unwrap()
            .unwrap();
        results.push(measure(name, rows, || {
            black_box(predicate.eval_predicate_bitmap(black_box(&batch)).unwrap());
        }));
    }
    results
}
