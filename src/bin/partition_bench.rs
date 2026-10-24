//! Deterministic in-process partition router benchmark.
//!
//! The benchmark excludes protocol and heap I/O so it can detect routing
//! regressions at the 8192-leaf limit. It emits JSON suitable for checked-in
//! or CI baselines.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mydb::engine::partition_bench_api::route_row;
use mydb::engine::StorageQueryEngine;
use mydb::storage::partitioning::{
    Partition, PartitionBounds, PartitionLiteral, PartitionPhysicalOptions,
};
use mydb::types::Value;
use serde::Serialize;

const SEED: u64 = 0x4d59_4442_5041_5254;

#[derive(Serialize)]
struct BenchReport {
    seed: u64,
    rows_per_sample: usize,
    warmup: usize,
    iterations: usize,
    build_optimization: &'static str,
    cases: Vec<CaseReport>,
}

#[derive(Serialize)]
struct CaseReport {
    name: &'static str,
    leaves: usize,
    ddl_ns: u128,
    samples_ns: Vec<u128>,
    median_ns: u128,
    coefficient_of_variation: f64,
    routes_per_second: f64,
    checksum: u64,
}

struct Case {
    name: &'static str,
    base_table: &'static str,
    leaves: usize,
    kind: CaseKind,
}

#[derive(Clone, Copy)]
enum CaseKind {
    Range,
    List,
    Hash,
    Key,
}

fn main() {
    let rows = arg_value("--rows").unwrap_or(1_000_000usize).max(1);
    let warmup = arg_value("--warmup").unwrap_or(2usize);
    let iterations = arg_value("--iterations").unwrap_or(7usize).max(1);
    let json_path = arg_value::<PathBuf>("--json");

    let data_dir = env::temp_dir().join(format!(
        "mydb-partition-bench-{}-{SEED:016x}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&data_dir);
    fs::create_dir_all(&data_dir).expect("create benchmark data directory");
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(data_dir.clone());
    let engine = StorageQueryEngine::open(&data_dir).expect("open benchmark engine");
    engine
        .execute(
            "CREATE TABLE base_range (k INT) PARTITION BY RANGE(k) (\
               PARTITION p0 VALUES LESS THAN MAXVALUE)",
        )
        .expect("create RANGE template");
    engine
        .execute("CREATE TABLE base_list (k INT) PARTITION BY LIST(k) (PARTITION p0 VALUES IN (0))")
        .expect("create LIST template");
    engine
        .execute("CREATE TABLE base_hash (k INT) PARTITION BY HASH(k) PARTITIONS 1")
        .expect("create HASH template");
    engine
        .execute("CREATE TABLE base_key (k INT) PARTITION BY KEY(k) PARTITIONS 1")
        .expect("create KEY template");

    let cases = vec![
        Case {
            name: "range_128",
            base_table: "base_range",
            leaves: 128,
            kind: CaseKind::Range,
        },
        Case {
            name: "range_8192",
            base_table: "base_range",
            leaves: 8192,
            kind: CaseKind::Range,
        },
        Case {
            name: "list_8192",
            base_table: "base_list",
            leaves: 8192,
            kind: CaseKind::List,
        },
        Case {
            name: "hash_8192",
            base_table: "base_hash",
            leaves: 8192,
            kind: CaseKind::Hash,
        },
        Case {
            name: "key_8192",
            base_table: "base_key",
            leaves: 8192,
            kind: CaseKind::Key,
        },
    ];

    let mut reports = Vec::with_capacity(cases.len());
    for case in cases {
        let ddl_started = Instant::now();
        let template = engine
            .database_for_replication()
            .table(&format!("test.{}", case.base_table))
            .expect("benchmark template in catalog");
        let mut table = template.as_ref().clone();
        table
            .partitioning
            .as_mut()
            .expect("partition metadata")
            .partitions = benchmark_partitions(case.kind, case.leaves);
        let ddl_ns = ddl_started.elapsed().as_nanos();
        let partitioning = table.partitioning.as_ref().expect("partition metadata");

        for pass in 0..warmup {
            std::hint::black_box(route_sample(
                &table,
                partitioning,
                rows,
                SEED.wrapping_add(pass as u64),
                case.leaves,
            ));
        }
        let mut samples_ns = Vec::with_capacity(iterations);
        let mut checksum = 0u64;
        for pass in 0..iterations {
            let started = Instant::now();
            checksum ^= route_sample(
                &table,
                partitioning,
                rows,
                SEED.wrapping_add(pass as u64),
                case.leaves,
            );
            samples_ns.push(started.elapsed().as_nanos());
        }
        let mut sorted = samples_ns.clone();
        sorted.sort_unstable();
        let median_ns = sorted[sorted.len() / 2];
        let routes_per_second = rows as f64 * 1_000_000_000.0 / median_ns.max(1) as f64;
        reports.push(CaseReport {
            name: case.name,
            leaves: case.leaves,
            ddl_ns,
            coefficient_of_variation: coefficient_of_variation(&samples_ns),
            samples_ns,
            median_ns,
            routes_per_second,
            checksum,
        });
    }

    let report = BenchReport {
        seed: SEED,
        rows_per_sample: rows,
        warmup,
        iterations,
        build_optimization: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        cases: reports,
    };
    let json = serde_json::to_string_pretty(&report).expect("serialize benchmark report");
    if let Some(path) = json_path {
        fs::write(path, format!("{json}\n")).expect("write benchmark JSON");
    }
    println!("{json}");
}

fn benchmark_partitions(kind: CaseKind, leaves: usize) -> Vec<Partition> {
    let mut partitions = Vec::with_capacity(leaves);
    for idx in 0..leaves {
        let bounds = match kind {
            CaseKind::Range => PartitionBounds::RangeLessThan(vec![if idx + 1 == leaves {
                PartitionLiteral::MaxValue
            } else {
                PartitionLiteral::Value(Value::Int((idx + 1) as i64))
            }]),
            CaseKind::List => {
                PartitionBounds::ListIn(vec![vec![PartitionLiteral::Value(Value::Int(idx as i64))]])
            }
            CaseKind::Hash | CaseKind::Key => PartitionBounds::Hash,
        };
        partitions.push(Partition {
            name: format!("p{idx}"),
            storage_id: idx as u64 + 1,
            bounds,
            comment: String::new(),
            options: PartitionPhysicalOptions::default(),
            subpartitions: Vec::new(),
        });
    }
    partitions
}

fn route_sample(
    table: &mydb::storage::TableSchema,
    partitioning: &mydb::storage::partitioning::PartitionInfo,
    rows: usize,
    seed: u64,
    leaves: usize,
) -> u64 {
    let mut state = seed;
    let mut checksum = 0u64;
    for _ in 0..rows {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = ((state >> 32) as usize % leaves) as i64;
        let routed = route_row(table, partitioning, &[Value::Int(key)]).expect("route row");
        checksum = checksum.rotate_left(7) ^ routed.storage_id;
    }
    std::hint::black_box(checksum)
}

fn coefficient_of_variation(samples: &[u128]) -> f64 {
    let mean = samples.iter().map(|sample| *sample as f64).sum::<f64>() / samples.len() as f64;
    if mean == 0.0 {
        return 0.0;
    }
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = *sample as f64 - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    variance.sqrt() / mean
}

fn arg_value<T: std::str::FromStr>(name: &str) -> Option<T> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next().and_then(|value| value.parse().ok());
        }
        if let Some(value) = arg.strip_prefix(&format!("{name}=")) {
            return value.parse().ok();
        }
    }
    None
}
