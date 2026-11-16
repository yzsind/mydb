use std::time::{Duration, Instant};

use mysql::prelude::Queryable;

struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    iterations: usize,
    warmup: usize,
    select_one: bool,
}

fn main() {
    let config = match parse_args() {
        Some(config) => config,
        None => std::process::exit(2),
    };

    eprintln!(
        "connect_bench: {}:{} db={} iterations={} warmup={} select_one={}",
        config.host,
        config.port,
        config.database,
        config.iterations,
        config.warmup,
        config.select_one
    );

    for _ in 0..config.warmup {
        let _ = connect_once(&config);
    }

    let mut latencies = Vec::with_capacity(config.iterations);
    let mut errors = 0_u64;
    let mut first_error_shown = false;
    let started = Instant::now();
    for _ in 0..config.iterations {
        let connect_started = Instant::now();
        match connect_once(&config) {
            Ok(()) => latencies.push(connect_started.elapsed()),
            Err(err) => {
                errors += 1;
                if !first_error_shown {
                    eprintln!("connect_bench: first error: {err}");
                    first_error_shown = true;
                }
            }
        }
    }
    let elapsed = started.elapsed();
    print_results(&config, elapsed, &mut latencies, errors);
}

fn connect_once(config: &Config) -> Result<(), mysql::Error> {
    let opts = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false);
    let mut conn = mysql::Conn::new(opts)?;
    if config.select_one {
        let _: Option<u8> = conn.query_first("SELECT 1")?;
    }
    Ok(())
}

fn print_results(config: &Config, elapsed: Duration, latencies: &mut [Duration], errors: u64) {
    let successes = latencies.len();
    let attempted = config.iterations;
    let connects_per_sec = successes as f64 / elapsed.as_secs_f64();

    println!();
    println!("connect_bench:");
    println!("  attempted:       {attempted}");
    println!("  successes:       {successes}");
    println!("  errors:          {errors}");
    println!("  total time:      {:.3} s", elapsed.as_secs_f64());
    println!("  connects/sec:    {:.2}", connects_per_sec);

    if latencies.is_empty() {
        println!();
        println!("  (no successful connects to report latency)");
        return;
    }

    latencies.sort_unstable();
    let n = latencies.len();
    let total: Duration = latencies.iter().copied().sum();
    let avg = total / n as u32;
    println!();
    println!("  latency (us):");
    println!("    min:           {}", latencies[0].as_micros());
    println!("    avg:           {}", avg.as_micros());
    println!(
        "    p50:           {}",
        latencies[pct_idx(n, 50)].as_micros()
    );
    println!(
        "    p95:           {}",
        latencies[pct_idx(n, 95)].as_micros()
    );
    println!(
        "    p99:           {}",
        latencies[pct_idx(n, 99)].as_micros()
    );
    println!("    max:           {}", latencies[n - 1].as_micros());
}

fn pct_idx(n: usize, p: usize) -> usize {
    let idx = (p as f64 / 100.0 * n as f64).ceil() as usize;
    idx.saturating_sub(1).min(n - 1)
}

fn parse_args() -> Option<Config> {
    let args: Vec<String> = std::env::args().collect();
    let mut host = "127.0.0.1".to_string();
    let mut port = 3307_u16;
    let mut user = "root".to_string();
    let mut password = "root".to_string();
    let mut database = "test".to_string();
    let mut iterations = 5000_usize;
    let mut warmup = 200_usize;
    let mut select_one = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                i += 1;
                host = args.get(i)?.clone();
            }
            "--port" => {
                i += 1;
                port = args.get(i)?.parse().ok()?;
            }
            "--user" => {
                i += 1;
                user = args.get(i)?.clone();
            }
            "--password" => {
                i += 1;
                password = args.get(i)?.clone();
            }
            "--database" => {
                i += 1;
                database = args.get(i)?.clone();
            }
            "--iterations" => {
                i += 1;
                iterations = args.get(i)?.parse().ok()?;
            }
            "--warmup" => {
                i += 1;
                warmup = args.get(i)?.parse().ok()?;
            }
            "--select-one" => select_one = true,
            "--help" | "-h" => {
                print_help();
                return None;
            }
            other => {
                eprintln!("connect_bench: unknown argument: {other}");
                print_help();
                return None;
            }
        }
        i += 1;
    }

    Some(Config {
        host,
        port,
        user,
        password,
        database,
        iterations,
        warmup,
        select_one,
    })
}

fn print_help() {
    eprintln!(
        "Usage: connect_bench [--host 127.0.0.1] [--port 3307] [--user root] [--password root] [--database test] [--iterations 5000] [--warmup 200] [--select-one]"
    );
}
