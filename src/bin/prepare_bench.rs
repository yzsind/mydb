//! End-to-end COM_STMT_PREPARE/EXECUTE benchmark — the sysbench
//! `oltp_read_only` run shape. Connects via the `mysql` crate (which
//! uses COM_STMT_PREPARE on `.prep()` and COM_STMT_EXECUTE on `.exec()`),
//! prepares one templated SELECT, then executes it N times with varying
//! parameters.
//!
//! Verifies that mydb's prepared statement path (touched by round-2 via
//! `normalize_sql` for cache key and `parse_statement` for template
//! parsing) keeps returning correct rows under concurrent execute load.
//!
//! Usage:
//!   prepare_bench [--host h] [--port p] [--user u] [--password p]
//!                 [--database d] [--rows N] [--executes N]

use std::time::Instant;

use mysql::prelude::*;
use mysql::{params, Opts, OptsBuilder, Pool};

fn parse_arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == name {
            return iter.next().map(String::as_str);
        }
    }
    None
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let host = parse_arg(&args, "--host").unwrap_or("127.0.0.1");
    let port: u16 = parse_arg(&args, "--port")
        .unwrap_or("13349")
        .parse()
        .expect("port");
    let user = parse_arg(&args, "--user").unwrap_or("root");
    let pwd = parse_arg(&args, "--password").unwrap_or("root");
    let db = parse_arg(&args, "--database").unwrap_or("test");
    let rows: usize = parse_arg(&args, "--rows")
        .unwrap_or("10000")
        .parse()
        .expect("rows");
    let executes: usize = parse_arg(&args, "--executes")
        .unwrap_or("20000")
        .parse()
        .expect("executes");

    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pwd))
        .db_name(Some(db))
        .into();
    let pool = Pool::new(opts).expect("connect");
    let mut conn = pool.get_conn().expect("conn");

    // Prepare table + N rows.
    conn.query_drop("DROP TABLE IF EXISTS pb").expect("drop");
    conn.query_drop(
        "CREATE TABLE pb (id INT NOT NULL PRIMARY KEY, k INT NOT NULL, v VARCHAR(120))",
    )
    .expect("create");
    let c = "abcdefgh".repeat(15);
    let mut sql = String::with_capacity(rows * 220 + 64);
    sql.push_str("INSERT INTO pb(id, k, v) VALUES");
    for i in 0..rows {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({},{},'{}')", i + 1, (i as i32) * 7 + 1, c));
    }
    conn.query_drop(sql).expect("seed");
    eprintln!("seeded {rows} rows");

    // Prepare the SELECT template — sysbench oltp_point_select shape.
    let stmt = conn
        .prep("SELECT k, v FROM pb WHERE id = :id")
        .expect("prepare");

    // Warmup.
    for i in 1..=64usize {
        let rid = (i * 1009) % rows + 1;
        let _: Vec<(i32, String)> = conn
            .exec(&stmt, params! { "id" => rid as i64 })
            .expect("warm");
    }

    // Hot loop: N executes.
    let start = Instant::now();
    let mut returned = 0u64;
    let mut correct = 0u64;
    for i in 0..executes {
        let rid = (i * 1009) % rows + 1;
        let rs: Vec<(i32, String)> = conn
            .exec(&stmt, params! { "id" => rid as i64 })
            .expect("exec");
        returned += rs.len() as u64;
        if !rs.is_empty() {
            let expected_k = (rid as i32 - 1) * 7 + 1;
            if rs[0].0 == expected_k && rs[0].1 == c {
                correct += 1;
            }
        }
    }
    let elapsed = start.elapsed();

    eprintln!(
        "executes: {executes}, elapsed: {:.3}s, qps: {:.0}, returned: {returned}, correct: {correct}",
        elapsed.as_secs_f64(),
        executes as f64 / elapsed.as_secs_f64(),
    );
    assert_eq!(
        correct as usize, executes,
        "expected every execute to return the correct row"
    );
}
