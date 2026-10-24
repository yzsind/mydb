use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;

struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    sql: String,
    threads: usize,
    iterations: usize,
    warmup: usize,
}

struct ThreadResult {
    latencies: Vec<Duration>,
    errors: u64,
}

fn main() {
    let config = match parse_args() {
        Some(c) => Arc::new(c),
        None => std::process::exit(2),
    };

    eprintln!(
        "sql_bench: {}:{} db={} threads={} iterations={} warmup={}",
        config.host, config.port, config.database, config.threads, config.iterations, config.warmup,
    );
    eprintln!("sql_bench: sql = {}", config.sql);

    let barrier = Arc::new(Barrier::new(config.threads));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..config.threads)
        .map(|id| {
            let cfg = Arc::clone(&config);
            let bar = Arc::clone(&barrier);
            thread::spawn(move || run_worker(id, &cfg, &bar))
        })
        .collect();

    let mut all_latencies: Vec<Duration> = Vec::new();
    let mut total_errors: u64 = 0;
    for h in handles {
        let result = h.join().unwrap_or_else(|_| {
            eprintln!("sql_bench: worker thread panicked");
            ThreadResult {
                latencies: Vec::new(),
                errors: config.iterations as u64,
            }
        });
        all_latencies.extend(result.latencies);
        total_errors += result.errors;
    }

    let wall_elapsed = wall_start.elapsed();
    print_results(&config, wall_elapsed, &mut all_latencies, total_errors);
}

fn run_worker(id: usize, config: &Config, barrier: &Barrier) -> ThreadResult {
    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false);

    let mut conn = mysql::Conn::new(opts).unwrap_or_else(|e| {
        eprintln!("sql_bench: thread {id} connect failed: {e}");
        std::process::exit(1);
    });

    for _ in 0..config.warmup {
        let _ = drain_query(&mut conn, &config.sql);
    }

    barrier.wait();

    let mut latencies = Vec::with_capacity(config.iterations);
    let mut errors: u64 = 0;
    let mut first_error_shown = false;

    for _ in 0..config.iterations {
        let start = Instant::now();
        match drain_query(&mut conn, &config.sql) {
            Ok(_) => {
                latencies.push(start.elapsed());
            }
            Err(e) => {
                if !first_error_shown {
                    eprintln!("sql_bench: thread {id} query error: {e}");
                    first_error_shown = true;
                }
                errors += 1;
            }
        }
    }

    ThreadResult { latencies, errors }
}

fn drain_query(conn: &mut mysql::Conn, sql: &str) -> Result<u64, mysql::Error> {
    let result = conn.query_iter(sql)?;
    let mut rows = 0u64;
    for row in result {
        let _ = row?;
        rows += 1;
    }
    Ok(rows)
}

fn print_results(config: &Config, wall: Duration, latencies: &mut Vec<Duration>, errors: u64) {
    let total_queries = config.threads * config.iterations;
    let qps = total_queries as f64 / wall.as_secs_f64();

    println!(
        "\nsql_bench: {} threads x {} iterations\n",
        config.threads, config.iterations,
    );
    println!("  total time:     {:.3} s", wall.as_secs_f64());
    println!("  total queries:  {}", total_queries);
    println!("  errors:         {}", errors);
    println!("  QPS:            {:.2}", qps);

    if latencies.is_empty() {
        println!("\n  (no successful queries to report latency)");
        return;
    }

    latencies.sort_unstable();
    let n = latencies.len();
    let total: Duration = latencies.iter().sum();
    let avg = total / n as u32;

    println!("\n  latency (us):");
    println!("    min:          {}", latencies[0].as_micros());
    println!("    avg:          {}", avg.as_micros());
    println!(
        "    p50:          {}",
        latencies[pct_idx(n, 50)].as_micros()
    );
    println!(
        "    p95:          {}",
        latencies[pct_idx(n, 95)].as_micros()
    );
    println!(
        "    p99:          {}",
        latencies[pct_idx(n, 99)].as_micros()
    );
    println!("    max:          {}", latencies[n - 1].as_micros());
}

fn pct_idx(n: usize, p: usize) -> usize {
    let idx = (p as f64 / 100.0 * n as f64).ceil() as usize;
    idx.saturating_sub(1).min(n - 1)
}

fn parse_args() -> Option<Config> {
    let args: Vec<String> = std::env::args().collect();
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 3307;
    let mut user = "root".to_string();
    let mut password = "root".to_string();
    let mut database = "test".to_string();
    let mut sql: Option<String> = None;
    let mut threads: usize = 1;
    let mut iterations: usize = 1000;
    let mut warmup: usize = 0;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                i += 1;
                host = args[i].clone();
            }
            "--port" => {
                i += 1;
                port = args[i].parse().unwrap_or(3307);
            }
            "--user" => {
                i += 1;
                user = args[i].clone();
            }
            "--password" => {
                i += 1;
                password = args[i].clone();
            }
            "--database" | "--db" => {
                i += 1;
                database = args[i].clone();
            }
            "--sql" => {
                i += 1;
                sql = Some(args[i].clone());
            }
            "--threads" | "-t" => {
                i += 1;
                threads = args[i].parse().unwrap_or(1);
            }
            "--iterations" | "-n" => {
                i += 1;
                iterations = args[i].parse().unwrap_or(1000);
            }
            "--warmup" => {
                i += 1;
                warmup = args[i].parse().unwrap_or(0);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            unknown => {
                eprintln!("unknown argument: {unknown}");
                print_usage();
                return None;
            }
        }
        i += 1;
    }

    if sql.is_none() {
        eprintln!("error: --sql is required");
        print_usage();
        return None;
    }

    if threads == 0 || iterations == 0 {
        eprintln!("error: --threads and --iterations must be > 0");
        return None;
    }

    Some(Config {
        host,
        port,
        user,
        password,
        database,
        sql: sql.unwrap(),
        threads,
        iterations,
        warmup,
    })
}

fn print_usage() {
    eprintln!(
        "\
Usage: sql_bench [OPTIONS] --sql <SQL>

Options:
  --host <HOST>            Server host [default: 127.0.0.1]
  --port <PORT>            Server port [default: 3307]
  --user <USER>            Username [default: root]
  --password <PASS>        Password [default: root]
  --database, --db <DB>    Database name [default: test]
  --sql <SQL>              SQL statement to benchmark (required)
  --threads, -t <N>        Concurrent threads [default: 1]
  --iterations, -n <N>     Iterations per thread [default: 1000]
  --warmup <N>             Warmup iterations per thread [default: 0]
  -h, --help               Print this help"
    );
}
