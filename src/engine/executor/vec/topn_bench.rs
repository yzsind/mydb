//! Manual release benchmarks for serial and streaming parallel Top-N.
use super::*;
use crate::engine::executor::vec::{
    batch::{Batch, ColumnVector},
    scheduler::topn_driver::{run_topn_parallel, SortKey},
    schema::{BatchField, BatchSchema},
    stream::MaterializedBatchOperator,
};
use crate::types::ColumnType;
use std::time::Instant;

#[test]
#[ignore = "manual release performance comparison"]
fn topn_refactor_benchmark() {
    let n = 100_000usize;
    for (label, k, width, shape) in [
        ("random-small", 32, 8, 0),
        ("random-large", 4096, 8, 0),
        ("reverse-wide", 1024, 1024, 1),
        ("ties-wide", 32, 1024, 2),
    ] {
        for workers in [0, 4] {
            let mut samples = Vec::new();
            for round in 0..7 {
                let key = |i: usize| match shape {
                    1 => (n - i) as i64,
                    2 => 1,
                    _ => ((i * 7919) % n) as i64,
                };
                let start;
                let out;
                if workers == 0 {
                    start = Instant::now();
                    let mut acc = TopNAccumulator::new(
                        vec![SortColumnSpec {
                            column_index: 0,
                            ascending: true,
                            nulls_first: true,
                        }],
                        k,
                    )
                    .unwrap();
                    for i in 0..n {
                        acc.push_lazy(vec![Value::Int(key(i))], || {
                            vec![Value::Int(i as i64), Value::String("x".repeat(width))]
                        })
                        .unwrap();
                    }
                    out = acc.finish().unwrap();
                } else {
                    let schema = Arc::new(BatchSchema::new(vec![
                        BatchField {
                            table: "t".into(),
                            alias: None,
                            name: "k".into(),
                            column_type: ColumnType::LongLong,
                            source: Default::default(),
                        },
                        BatchField {
                            table: "t".into(),
                            alias: None,
                            name: "v".into(),
                            column_type: ColumnType::VarString,
                            source: Default::default(),
                        },
                    ]));
                    let batches = (0..n)
                        .step_by(1024)
                        .map(|base| {
                            Batch::new(
                                schema.clone(),
                                vec![
                                    ColumnVector::from_values(
                                        (base..(base + 1024).min(n))
                                            .map(|i| Value::Int(key(i)))
                                            .collect(),
                                    ),
                                    ColumnVector::from_values(
                                        (base..(base + 1024).min(n))
                                            .map(|_| Value::String("x".repeat(width)))
                                            .collect(),
                                    ),
                                ],
                            )
                        })
                        .collect();
                    let mut stream = MaterializedBatchOperator::new(schema, batches);
                    start = Instant::now();
                    out = run_topn_parallel(
                        &mut stream,
                        vec![SortKey {
                            column: 0,
                            ascending: true,
                            nulls_first: true,
                            collation_id: None,
                        }],
                        k,
                        workers,
                    )
                    .unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(out.len(), k);
                std::hint::black_box(out);
                if round > 0 {
                    samples.push(elapsed);
                }
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "TOPK_BENCH {label} workers={workers} median_ms={:.3} samples={samples:?}",
                (samples[2] + samples[3]) / 2.0
            );
        }
    }
}
