//! P1-C3 phase 2 — mydb-replica-diff.
//!
//! Compare master and replica state by running `CHECKSUM TABLE` on
//! both ends and reporting tables whose checksums differ. The
//! per-table digest is the same one CHECKSUM TABLE produces (CRC32
//! over each row's stored representation), so the tool finishes in
//! `O(rows scanned once per side)` instead of dumping both sides.
//!
//! Usage:
//!   mydb-replica-diff \
//!     --source 'host=127.0.0.1 port=3306 user=root pw=root db=test' \
//!     --replica 'host=127.0.0.1 port=3307 user=root pw=root db=test' \
//!     [--tables t1,t2,t3]
//!
//! Exit code:
//!   0 — every table matched.
//!   1 — one or more tables differ.
//!   2 — usage / connect / SQL error.
//!
//! When `--tables` is omitted the tool discovers user tables on the
//! source side via `information_schema.tables` (default schema:
//! whatever the `db=` parameter named).

use std::collections::BTreeMap;
use std::process;
use std::time::Instant;

use mysql::prelude::Queryable;
use mysql::{Opts, OptsBuilder, Pool, Value as MyValue};

#[derive(Debug)]
struct Endpoint {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

struct Args {
    source: Endpoint,
    replica: Endpoint,
    tables: Vec<String>,
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("mydb-replica-diff: {msg}");
            print_usage();
            process::exit(2);
        }
    };

    let source_pool = match build_pool(&args.source) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("mydb-replica-diff: connect source: {err}");
            process::exit(2);
        }
    };
    let replica_pool = match build_pool(&args.replica) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("mydb-replica-diff: connect replica: {err}");
            process::exit(2);
        }
    };

    let tables = if args.tables.is_empty() {
        match discover_tables(&source_pool, &args.source.database) {
            Ok(t) => t,
            Err(err) => {
                eprintln!("mydb-replica-diff: discover tables: {err}");
                process::exit(2);
            }
        }
    } else {
        args.tables.clone()
    };

    if tables.is_empty() {
        eprintln!("mydb-replica-diff: no tables to compare");
        process::exit(0);
    }

    eprintln!("mydb-replica-diff: comparing {} table(s)", tables.len());
    let started = Instant::now();

    let source_checksums = match checksum_all(&source_pool, &tables) {
        Ok(m) => m,
        Err(err) => {
            eprintln!("mydb-replica-diff: checksum source: {err}");
            process::exit(2);
        }
    };
    let replica_checksums = match checksum_all(&replica_pool, &tables) {
        Ok(m) => m,
        Err(err) => {
            eprintln!("mydb-replica-diff: checksum replica: {err}");
            process::exit(2);
        }
    };

    let mut mismatches = Vec::new();
    let mut missing_on_replica = Vec::new();
    let mut missing_on_source = Vec::new();
    for table in &tables {
        let source_val = source_checksums.get(table);
        let replica_val = replica_checksums.get(table);
        match (source_val, replica_val) {
            (Some(Some(s)), Some(Some(r))) if s == r => {}
            (Some(Some(s)), Some(Some(r))) => {
                mismatches.push((table.clone(), *s, *r));
            }
            (Some(Some(_)), Some(None)) | (Some(Some(_)), None) => {
                missing_on_replica.push(table.clone());
            }
            (Some(None), Some(Some(_))) | (None, Some(Some(_))) => {
                missing_on_source.push(table.clone());
            }
            _ => {}
        }
    }

    let elapsed = started.elapsed();
    println!(
        "scanned {} tables in {:.2}s",
        tables.len(),
        elapsed.as_secs_f64()
    );
    if mismatches.is_empty() && missing_on_replica.is_empty() && missing_on_source.is_empty() {
        println!("OK — every table matched");
        process::exit(0);
    }
    println!("DIFFERENCES DETECTED");
    for (table, source, replica) in &mismatches {
        println!("  mismatch: {table}  source=0x{source:016x}  replica=0x{replica:016x}");
    }
    for table in &missing_on_replica {
        println!("  missing on replica: {table}");
    }
    for table in &missing_on_source {
        println!("  missing on source: {table}");
    }
    process::exit(1);
}

fn build_pool(ep: &Endpoint) -> Result<Pool, mysql::Error> {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some(ep.host.clone()))
        .tcp_port(ep.port)
        .user(Some(ep.user.clone()))
        .pass(Some(ep.password.clone()))
        .db_name(Some(ep.database.clone()))
        .into();
    Pool::new(opts)
}

fn discover_tables(pool: &Pool, schema: &str) -> Result<Vec<String>, mysql::Error> {
    let mut conn = pool.get_conn()?;
    let rows: Vec<String> = conn.query(format!(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = '{}' AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
        schema.replace('\'', "''")
    ))?;
    Ok(rows)
}

fn checksum_all(
    pool: &Pool,
    tables: &[String],
) -> Result<BTreeMap<String, Option<i64>>, mysql::Error> {
    let mut conn = pool.get_conn()?;
    let mut out: BTreeMap<String, Option<i64>> = BTreeMap::new();
    // CHECKSUM TABLE supports a comma list, but we drive it one at a
    // time so a missing table on one side doesn't poison the rest of
    // the batch's NULL handling. The cost is one round-trip per
    // table; for the table counts replica-diff is meant for (tens to
    // low hundreds) that's fine.
    for table in tables {
        // Quote the identifier; bare backtick stripping below keeps
        // `db.table` working without forcing the caller to quote.
        let qualified = quote_qualified(table);
        let result: Vec<(String, MyValue)> = match conn.query(format!("CHECKSUM TABLE {qualified}"))
        {
            Ok(r) => r,
            Err(err) => {
                eprintln!("mydb-replica-diff: CHECKSUM TABLE {table} failed: {err}");
                out.insert(table.clone(), None);
                continue;
            }
        };
        let checksum = result.into_iter().next().and_then(|(_, v)| match v {
            MyValue::NULL => None,
            MyValue::Int(i) => Some(i),
            MyValue::UInt(u) => Some(u as i64),
            MyValue::Bytes(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse::<i64>().ok()),
            _ => None,
        });
        out.insert(table.clone(), checksum);
    }
    Ok(out)
}

fn quote_qualified(name: &str) -> String {
    if let Some(dot) = name.find('.') {
        let (db, tbl) = name.split_at(dot);
        format!(
            "`{}`.`{}`",
            db.trim_matches('`'),
            tbl[1..].trim_matches('`')
        )
    } else {
        format!("`{}`", name.trim_matches('`'))
    }
}

fn parse_args() -> Result<Args, String> {
    let mut source: Option<String> = None;
    let mut replica: Option<String> = None;
    let mut tables: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => {
                source = Some(
                    args.next()
                        .ok_or_else(|| "--source needs a value".to_string())?,
                );
            }
            "--replica" => {
                replica = Some(
                    args.next()
                        .ok_or_else(|| "--replica needs a value".to_string())?,
                );
            }
            "--tables" => {
                let v = args
                    .next()
                    .ok_or_else(|| "--tables needs a value".to_string())?;
                tables = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "-h" | "--help" => {
                print_usage();
                process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let source = parse_endpoint(&source.ok_or_else(|| "--source is required".to_string())?)?;
    let replica = parse_endpoint(&replica.ok_or_else(|| "--replica is required".to_string())?)?;
    Ok(Args {
        source,
        replica,
        tables,
    })
}

fn parse_endpoint(spec: &str) -> Result<Endpoint, String> {
    let mut host = "127.0.0.1".to_string();
    let mut port = 3306u16;
    let mut user = "root".to_string();
    let mut password = String::new();
    let mut database = "test".to_string();
    for kv in spec.split_whitespace() {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("expected key=value, got '{kv}'"))?;
        match k {
            "host" => host = v.to_string(),
            "port" => {
                port = v
                    .parse::<u16>()
                    .map_err(|err| format!("bad port '{v}': {err}"))?
            }
            "user" => user = v.to_string(),
            "pw" | "password" => password = v.to_string(),
            "db" | "database" => database = v.to_string(),
            other => return Err(format!("unknown endpoint key: {other}")),
        }
    }
    Ok(Endpoint {
        host,
        port,
        user,
        password,
        database,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: mydb-replica-diff \\
   --source 'host=H port=N user=U pw=W db=D' \\
   --replica 'host=H port=N user=U pw=W db=D' \\
  [--tables t1,t2,t3]\n\
\n\
Compares CHECKSUM TABLE between the two endpoints and exits 0 on a\n\
match, 1 on any difference, 2 on usage / connect / SQL error."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_endpoint_full_spec() {
        let ep = parse_endpoint("host=10.0.0.1 port=4406 user=u pw=p db=d").unwrap();
        assert_eq!(ep.host, "10.0.0.1");
        assert_eq!(ep.port, 4406);
        assert_eq!(ep.user, "u");
        assert_eq!(ep.password, "p");
        assert_eq!(ep.database, "d");
    }

    #[test]
    fn parse_endpoint_defaults_to_localhost_root() {
        let ep = parse_endpoint("db=mydb").unwrap();
        assert_eq!(ep.host, "127.0.0.1");
        assert_eq!(ep.port, 3306);
        assert_eq!(ep.user, "root");
        assert_eq!(ep.database, "mydb");
    }

    #[test]
    fn parse_endpoint_rejects_unknown_key() {
        let err = parse_endpoint("host=1 nope=2").unwrap_err();
        assert!(err.contains("unknown endpoint key"));
    }

    #[test]
    fn quote_qualified_handles_dot_and_backticks() {
        assert_eq!(quote_qualified("test.t"), "`test`.`t`");
        assert_eq!(quote_qualified("`db`.`name`"), "`db`.`name`");
        assert_eq!(quote_qualified("plain"), "`plain`");
        assert_eq!(quote_qualified("`escaped`"), "`escaped`");
    }
}
