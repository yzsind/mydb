//! Local classic-protocol ACL stress gate: persistent connections, prepared
//! and text reads, metadata, and acknowledged concurrent role-grant changes.
use mysql::{prelude::Queryable, Conn, OptsBuilder, Row, Statement};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
use std::time::{Duration, Instant};
#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
const USERS: usize = 1000;
const SQL: &str = "SELECT id FROM auth_stress.acl49 WHERE id=1";
fn connect(port: u16, user: &str, password: &str) -> Conn {
    Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some(user))
            .pass(Some(password))
            .prefer_socket(false)
            .read_timeout(Some(Duration::from_secs(30)))
            .write_timeout(Some(Duration::from_secs(30))),
    )
    .unwrap()
}
fn setup(root: &mut Conn) {
    root.query_drop("SET GLOBAL max_connections=1200").unwrap();
    root.query_drop("CREATE DATABASE auth_stress").unwrap();
    let users: Vec<_> = (0..USERS)
        .map(|i| {
            format!("'acl_user_{i}'@'%' IDENTIFIED WITH mysql_native_password BY 'stress-local'")
        })
        .collect();
    let names: Vec<_> = (0..USERS).map(|i| format!("'acl_user_{i}'@'%'")).collect();
    let roles: Vec<_> = (0..USERS).map(|i| format!("'acl_role_{i}'@'%'")).collect();
    root.query_drop(format!("CREATE USER {}", users.join(",")))
        .unwrap();
    root.query_drop(format!("CREATE ROLE {}", roles.join(",")))
        .unwrap();
    for i in 0..USERS {
        root.query_drop(format!("GRANT {} TO {}", roles[i], names[i]))
            .unwrap();
    }
    let all = names
        .iter()
        .chain(roles.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    for i in 0..51 {
        root.query_drop(format!(
            "CREATE TABLE auth_stress.acl{i} (id INT PRIMARY KEY)"
        ))
        .unwrap();
        root.query_drop(format!("INSERT INTO auth_stress.acl{i} VALUES (1)"))
            .unwrap();
        let targets = if i == 49 {
            roles.join(",")
        } else if i == 50 {
            names.join(",")
        } else {
            all.clone()
        };
        root.query_drop(format!("GRANT SELECT ON auth_stress.acl{i} TO {targets}"))
            .unwrap();
    }
    println!("setup users=1000 roles=1000 role_edges=1000 object_grants=100000 tables=51");
}
struct Client {
    id: usize,
    conn: Conn,
    prepared: Statement,
}
#[derive(Default)]
struct Counts {
    queries: u64,
    denied: u64,
    verified: u64,
    metadata: u64,
    errors: u64,
    max_us: u64,
    histogram: Vec<u64>,
}
impl Counts {
    fn new() -> Self {
        Self {
            histogram: vec![0; 50_001],
            ..Self::default()
        }
    }
    fn latency(&mut self, us: u64) {
        self.max_us = self.max_us.max(us);
        self.histogram[(us / 20).min(50_000) as usize] += 1;
    }
    fn percentile(&self, pct: u64) -> u64 {
        let target = (self.queries * pct).div_ceil(100).max(1);
        let mut count = 0;
        for (i, n) in self.histogram.iter().enumerate() {
            count += n;
            if count >= target {
                return (i as u64 + 1) * 20;
            }
        }
        0
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let port = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(13319);
    let seconds = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(600u64);
    let workers = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(32usize);
    assert!(
        (1..=USERS).contains(&workers),
        "workers must be in 1..=1000"
    );
    let mut root = connect(port, "root", "root");
    setup(&mut root);
    let mut groups: Vec<Vec<Client>> = (0..workers).map(|_| Vec::new()).collect();
    for id in 0..USERS {
        let mut conn = connect(port, &format!("acl_user_{id}"), "stress-local");
        conn.query_drop("SET ROLE ALL").unwrap();
        let prepared = conn.prep(SQL).unwrap();
        groups[id % workers].push(Client { id, conn, prepared });
    }
    println!("connected=1000 workers={workers} duration_seconds={seconds} latency_bucket_us=20 latency_cap_us=1000000");
    let states = Arc::new((0..USERS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::new();
    for mut clients in groups {
        let states = states.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut counts=Counts::new();
            barrier.wait(); let until=Instant::now()+Duration::from_secs(seconds);
            let mut tick=0usize;
            while Instant::now()<until {
                for client in &mut clients {
                    let before=states[client.id].load(Ordering::Acquire);
                    let start=Instant::now();
                    let metadata=tick%32==0;
                    let reply: Result<Vec<Row>,mysql::Error> = if metadata {
                        client.conn.query("SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA='auth_stress' AND TABLE_NAME='acl49'")
                    } else if tick%32==1 {
                        client.conn.query("SHOW GRANTS")
                    } else if tick%2==0 { client.conn.exec(&client.prepared,()) }
                    else { client.conn.query(SQL) };
                    let after=states[client.id].load(Ordering::Acquire);
                    counts.queries+=1; counts.latency(start.elapsed().as_micros() as u64);
                    let stable=before==after && before%2==0;
                    let allowed=before%4==0;
                    let ok=match reply {
                        Ok(rows) if metadata => {
                            counts.metadata+=1;
                            !stable || rows.first().and_then(|r|r.get::<u64,_>(0))==Some(u64::from(allowed))
                        },
                        Ok(_) if tick%32==1 => { counts.metadata+=1; true },
                        Ok(rows) => !stable || (allowed && rows.first().and_then(|r|r.get::<i64,_>(0))==Some(1)),
                        Err(mysql::Error::MySqlError(e)) if e.code==1142 && !metadata && tick%32!=1 => {
                            counts.denied+=1; !stable || !allowed
                        },
                        Err(e) => { if counts.errors<3 { eprintln!("unexpected client {}: {e}",client.id); } false },
                    };
                    if stable { counts.verified+=1; }
                    if !ok { counts.errors+=1; }
                    tick+=1;
                }
            }
            counts
        }));
    }
    barrier.wait();
    let start = Instant::now();
    let mut mutations = 0u64;
    while start.elapsed() < Duration::from_secs(seconds) {
        let id = (mutations as usize / 2) % USERS;
        let v = states[id].load(Ordering::Acquire);
        states[id].store(v + 1, Ordering::Release);
        root.query_drop(format!(
            "REVOKE SELECT ON auth_stress.acl49 FROM 'acl_role_{id}'@'%'"
        ))
        .unwrap();
        states[id].store(v + 2, Ordering::Release);
        mutations += 1;
        std::thread::sleep(Duration::from_millis(10));
        states[id].store(v + 3, Ordering::Release);
        root.query_drop(format!(
            "GRANT SELECT ON auth_stress.acl49 TO 'acl_role_{id}'@'%'"
        ))
        .unwrap();
        states[id].store(v + 4, Ordering::Release);
        mutations += 1;
        if mutations % 1000 == 0 {
            println!(
                "progress seconds={:.1} mutations={mutations}",
                start.elapsed().as_secs_f64()
            );
        }
    }
    let mut total = Counts::new();
    for handle in handles {
        let n = handle.join().expect("worker panic");
        total.queries += n.queries;
        total.denied += n.denied;
        total.verified += n.verified;
        total.metadata += n.metadata;
        total.errors += n.errors;
        total.max_us = total.max_us.max(n.max_us);
        for (a, b) in total.histogram.iter_mut().zip(n.histogram) {
            *a += b;
        }
    }
    println!("queries={} verified={} denied={} metadata={} mutations={} errors={} elapsed_seconds={:.3} qps={:.0} p50_us={} p95_us={} p99_us={} max_us={}",total.queries,total.verified,total.denied,total.metadata,mutations,total.errors,start.elapsed().as_secs_f64(),total.queries as f64/start.elapsed().as_secs_f64(),total.percentile(50),total.percentile(95),total.percentile(99),total.max_us);
    assert_eq!(total.errors, 0, "authorization or protocol errors");
    assert!(total.verified > 0 && total.denied > 0 && total.metadata > 0 && mutations > 0);
}
