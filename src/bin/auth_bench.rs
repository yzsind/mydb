//! In-memory ACL benchmark. Optional --sql-dir runs durable SQL DCL separately.
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mydb::engine::auth::{
    account_id, AccountKey, AuthCatalog, AuthorizationContext, GrantEntry, GrantOrigin, Privilege,
    PrivilegeScope, RequiredPrivilege, RoleEdge, UserAccount,
};

fn benchmark_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

// Match the server binary's allocator; role closure allocations otherwise
// measure the platform allocator's cross-thread contention rather than the
// deployment configuration selected by the existing Cargo features.
#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(target_os = "linux", not(feature = "fast-alloc")))]
#[global_allocator]
static GLOBAL: mydb::util::mapped_alloc::MappedSystem = mydb::util::mapped_alloc::MappedSystem;

fn measure(name: &str, iterations: usize, mut check: impl FnMut() -> bool, expected: bool) {
    let start = Instant::now();
    for _ in 0..iterations {
        assert_eq!(black_box(check()), expected);
    }
    println!(
        "{name}_ns_per_check={:.2}",
        start.elapsed().as_nanos() as f64 / iterations as f64
    );
}

fn main() {
    if let Some(dir) = std::env::args().skip_while(|a| a != "--scale-dir").nth(1) {
        scale_bench(&dir);
        return;
    }
    println!(
        "allocator={}",
        if cfg!(feature = "fast-alloc") {
            "mimalloc"
        } else {
            "platform"
        }
    );
    let mut auth = AuthCatalog::default();
    let grantor = AccountKey::new("root", "localhost");
    let start = Instant::now();
    for n in 0..1000 {
        let user = AccountKey::new(format!("user_{n}"), "%");
        let role = AccountKey::new(format!("role_{n}"), "%");
        for (key, is_role) in [(user.clone(), false), (role.clone(), true)] {
            let mut account =
                UserAccount::new(key.clone(), "mysql_native_password".into(), String::new());
            account.is_role = is_role;
            account.account_locked = is_role;
            auth.accounts.insert(account_id(&key), account);
            for object in 0..50 {
                auth.grants.push(GrantEntry {
                    updated_ms: benchmark_timestamp_ms(),
                    grantor: grantor.clone(),
                    grantee: key.clone(),
                    privilege: Privilege::Select,
                    scope: PrivilegeScope::Table {
                        database: "bench".into(),
                        table: format!("{}_t{object}", key.user),
                    },
                    grant_option: false,
                    origin: GrantOrigin::Explicit,
                });
            }
        }
        auth.role_edges.push(RoleEdge {
            from_role: role,
            to_account: user,
            admin_option: false,
        });
    }
    println!("users=1000 roles=1000 object_grants=100000 role_edges=1000");
    println!(
        "in_memory_fixture_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    auth.rebuild_runtime_indexes();
    println!(
        "index_build_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let mut ctx = AuthorizationContext {
        authenticated_as: AccountKey::new("user_999", "%"),
        active_roles: Default::default(),
        epoch: auth.epoch,
    };
    let mut req = RequiredPrivilege {
        privilege: Privilege::Select,
        scope: PrivilegeScope::Table {
            database: "bench".into(),
            table: "user_999_t49".into(),
        },
    };
    measure(
        "direct_allow",
        500_000,
        || auth.account_has_privilege(black_box(&ctx), black_box(&req), false),
        true,
    );
    req.scope = PrivilegeScope::Table {
        database: "bench".into(),
        table: "role_999_t49".into(),
    };
    measure(
        "direct_deny",
        500_000,
        || auth.account_has_privilege(black_box(&ctx), black_box(&req), false),
        false,
    );
    ctx.active_roles.insert(AccountKey::new("role_999", "%"));
    measure(
        "role_allow",
        500_000,
        || auth.account_has_privilege(black_box(&ctx), black_box(&req), false),
        true,
    );
    measure(
        "metadata_role",
        100_000,
        || auth.account_can_access_schema(black_box(&ctx), "bench", Some("role_999_t49")),
        true,
    );
    let auth = Arc::new(auth);
    let barrier = Arc::new(Barrier::new(9));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let (auth, ctx, req, barrier) = (auth.clone(), ctx.clone(), req.clone(), barrier.clone());
        readers.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..100_000 {
                assert!(auth.account_has_privilege(black_box(&ctx), black_box(&req), false));
            }
        }));
    }
    let start = Instant::now();
    barrier.wait();
    for reader in readers {
        reader.join().unwrap();
    }
    println!(
        "eight_reader_role_checks_per_sec={:.0}",
        800_000.0 / start.elapsed().as_secs_f64()
    );
    // Skew: one role holds all objects, as happens with reporting/admin roles.
    let mut skew = (*auth).clone();
    let role = AccountKey::new("role_999", "%");
    skew.grants.for_each_mut(|grant| {
        if matches!(grant.scope, PrivilegeScope::Table { .. }) {
            grant.grantee = role.clone();
        }
    });
    skew.rebuild_runtime_indexes();
    measure(
        "skew_100000_role_allow",
        100_000,
        || skew.account_has_privilege(black_box(&ctx), black_box(&req), false),
        true,
    );
    let denied = RequiredPrivilege {
        privilege: Privilege::Delete,
        scope: req.scope.clone(),
    };
    measure(
        "skew_100000_role_deny",
        100_000,
        || skew.account_has_privilege(black_box(&ctx), black_box(&denied), false),
        false,
    );
    measure(
        "skew_legacy_scan",
        1000,
        || {
            skew.grants
                .iter()
                .any(|g| g.grantee == role && g.privilege == req.privilege && g.scope == req.scope)
        },
        true,
    );
    let mut all_roles = (*auth).clone();
    for n in 0..999 {
        all_roles.role_edges.push(RoleEdge {
            from_role: AccountKey::new(format!("role_{n}"), "%"),
            to_account: ctx.authenticated_as.clone(),
            admin_option: false,
        });
    }
    all_roles.rebuild_runtime_indexes();
    let mut all_context = ctx.clone();
    all_context.active_roles = all_roles
        .roles_granted_directly(&ctx.authenticated_as)
        .into();
    assert_eq!(all_context.active_roles.len(), 1000);
    measure(
        "one_thousand_active_roles_allow",
        10_000,
        || all_roles.account_has_privilege(black_box(&all_context), black_box(&req), false),
        true,
    );
    measure(
        "one_thousand_active_roles_deny",
        10_000,
        || all_roles.account_has_privilege(black_box(&all_context), black_box(&denied), false),
        false,
    );
    if let Some(dir) = std::env::args().skip_while(|a| a != "--sql-dir").nth(1) {
        sql_bench(&dir);
    }
}

fn sql_bench(dir: &str) {
    use mydb::engine::{SessionContext, StorageQueryEngine};
    assert!(
        std::path::Path::new(dir).starts_with("/tmp/mydb"),
        "benchmark data must be under /tmp/mydb"
    );
    assert!(
        !std::path::Path::new(dir).exists(),
        "use a fresh benchmark directory"
    );
    let engine = StorageQueryEngine::open(dir).unwrap();
    let mut root = SessionContext::default();
    let start = Instant::now();
    let users = (0..1000)
        .map(|i| format!("'sql_u{i}'@'%'"))
        .collect::<Vec<_>>();
    let roles = (0..1000)
        .map(|i| format!("'sql_r{i}'@'%'"))
        .collect::<Vec<_>>();
    engine
        .execute_with_session(&mut root, &format!("CREATE USER {}", users.join(",")))
        .unwrap();
    engine
        .execute_with_session(&mut root, &format!("CREATE ROLE {}", roles.join(",")))
        .unwrap();
    println!(
        "sql_create_1000_users_1000_roles_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let start = Instant::now();
    for i in 0..1000 {
        engine
            .execute_with_session(&mut root, &format!("GRANT {} TO {}", roles[i], users[i]))
            .unwrap();
    }
    println!(
        "sql_1000_role_edges_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let grantees = users
        .iter()
        .chain(roles.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    let mut latencies = Vec::new();
    let start = Instant::now();
    for i in 0..50 {
        engine
            .execute_with_session(&mut root, &format!("CREATE TABLE test.acl_{i} (id INT)"))
            .unwrap();
        let grant_start = Instant::now();
        engine
            .execute_with_session(
                &mut root,
                &format!("GRANT SELECT ON test.acl_{i} TO {grantees}"),
            )
            .unwrap();
        latencies.push(grant_start.elapsed().as_secs_f64() * 1000.0);
    }
    println!(
        "sql_100000_grants_total_seconds={:.3}",
        start.elapsed().as_secs_f64()
    );
    latencies.sort_by(f64::total_cmp);
    println!(
        "sql_grant_2000_principals_p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        latencies[25], latencies[47], latencies[49]
    );
    for sql in [
        "REVOKE SELECT ON test.acl_49 FROM 'sql_u999'@'%'",
        "GRANT SELECT ON test.acl_49 TO 'sql_u999'@'%'",
    ] {
        let start = Instant::now();
        engine.execute_with_session(&mut root, sql).unwrap();
        println!(
            "sql_single_mutation_ms={:.3} statement={sql}",
            start.elapsed().as_secs_f64() * 1000.0
        );
    }
    let snapshot = engine.database_for_replication().auth_catalog();
    assert_eq!(
        snapshot
            .accounts
            .values()
            .filter(|a| a.key.user.starts_with("sql_u"))
            .count(),
        1000
    );
    assert_eq!(
        snapshot
            .accounts
            .values()
            .filter(|a| a.key.user.starts_with("sql_r"))
            .count(),
        1000
    );
    assert_eq!(
        snapshot
            .grants
            .iter()
            .filter(|g| g.grantee.user.starts_with("sql_"))
            .count(),
        100_000
    );
    assert_eq!(
        snapshot
            .role_edges
            .iter()
            .filter(|e| e.from_role.user.starts_with("sql_r"))
            .count(),
        1000
    );
    let mut user = SessionContext::default();
    user.user = "sql_u999".into();
    user.login_user = user.user.clone();
    user.authenticated_as = AccountKey::new("sql_u999", "%");
    user.auth_epoch = snapshot.epoch;
    engine
        .execute_with_session(&mut user, "SET ROLE ALL")
        .unwrap();
    let started = Instant::now();
    for _ in 0..1000 {
        engine
            .execute_with_session(&mut user, "SELECT id FROM test.acl_49")
            .unwrap();
    }
    println!(
        "sql_role_select_ns_per_query={:.2}",
        started.elapsed().as_nanos() as f64 / 1000.0
    );
    drop(snapshot);
    engine.shutdown().unwrap();
    drop(engine);
    let recovered = StorageQueryEngine::open(dir).unwrap();
    let snapshot = recovered.database_for_replication().auth_catalog();
    assert_eq!(
        snapshot
            .accounts
            .values()
            .filter(|a| a.key.user.starts_with("sql_u"))
            .count(),
        1000
    );
    assert_eq!(
        snapshot
            .accounts
            .values()
            .filter(|a| a.key.user.starts_with("sql_r"))
            .count(),
        1000
    );
    assert_eq!(
        snapshot
            .grants
            .iter()
            .filter(|g| g.grantee.user.starts_with("sql_"))
            .count(),
        100_000
    );
    assert_eq!(
        snapshot
            .role_edges
            .iter()
            .filter(|e| e.from_role.user.starts_with("sql_r"))
            .count(),
        1000
    );
    println!("sql_reopen_verified=true");
    recovered.shutdown().unwrap();
}

/// Fixed principals and changing unrelated ACL cardinality isolate the cost of
/// sparse SQL DCL. Fixture installation uses the normal durable auth mutation
/// API; measured operations go through SQL parsing, validation and WAL commit.
fn scale_bench(base: &str) {
    use mydb::engine::{SessionContext, StorageQueryEngine};
    assert!(std::path::Path::new(base).starts_with("/tmp/mydb"));
    for count in [1usize, 1_000, 100_000, 1_000_000] {
        let path = format!("{base}/{count}");
        assert!(
            !std::path::Path::new(&path).exists(),
            "use fresh scale directories"
        );
        let engine = StorageQueryEngine::open(&path).unwrap();
        let db = engine.database_for_replication().clone();
        let grantor = AccountKey::new("root", "localhost");
        let started = Instant::now();
        db.mutate_auth_catalog(|auth| {
            for n in 0..2000 {
                let key = AccountKey::new(format!("scale_{n}"), "%");
                let mut account =
                    UserAccount::new(key.clone(), "mysql_native_password".into(), String::new());
                account.is_role = n >= 1000;
                account.account_locked = n >= 1000;
                auth.accounts.insert(account_id(&key), account);
            }
            for n in 0..1000 {
                auth.role_edges.push(RoleEdge {
                    from_role: AccountKey::new(format!("scale_{}", n + 1000), "%"),
                    to_account: AccountKey::new(format!("scale_{n}"), "%"),
                    admin_option: false,
                });
            }
            for n in 0..count {
                auth.grants.push(GrantEntry {
                    updated_ms: benchmark_timestamp_ms(),
                    grantor: grantor.clone(),
                    grantee: AccountKey::new(format!("scale_{}", n % 2000), "%"),
                    privilege: Privilege::Select,
                    scope: PrivilegeScope::Table {
                        database: "test".into(),
                        table: format!("scale_object_{}", n / 2000),
                    },
                    grant_option: false,
                    origin: GrantOrigin::Explicit,
                });
            }
            Ok(())
        })
        .unwrap();
        println!(
            "scale_grants={count} fixture_seconds={:.3}",
            started.elapsed().as_secs_f64()
        );
        let mut root = SessionContext::default();
        engine
            .execute_with_session(&mut root, "CREATE TABLE scale_probe(id INT)")
            .unwrap();
        let grant = "GRANT SELECT ON test.scale_probe TO 'scale_0'@'%'";
        let revoke = "REVOKE SELECT ON test.scale_probe FROM 'scale_0'@'%'";
        let mut grants = Vec::new();
        let mut revokes = Vec::new();
        for i in 0..110 {
            for (sql, latencies) in [(grant, &mut grants), (revoke, &mut revokes)] {
                let t = Instant::now();
                engine.execute_with_session(&mut root, sql).unwrap();
                if i >= 10 {
                    latencies.push(t.elapsed().as_secs_f64() * 1000.0);
                }
            }
        }
        for (name, mut samples) in [("grant", grants), ("revoke", revokes)] {
            samples.sort_by(f64::total_cmp);
            println!("scale_grants={count} operation={name} samples=100 p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",samples[49],samples[94],samples[98],samples[99]);
        }
        assert_eq!(
            db.auth_catalog()
                .grants
                .iter()
                .filter(|g| g.grantee.user.starts_with("scale_"))
                .count(),
            count
        );
        drop(db);
        engine.shutdown().unwrap();
        drop(engine);
        let reopened = StorageQueryEngine::open(&path).unwrap();
        assert_eq!(
            reopened
                .database_for_replication()
                .auth_catalog()
                .grants
                .iter()
                .filter(|g| g.grantee.user.starts_with("scale_"))
                .count(),
            count
        );
        reopened.shutdown().unwrap();
        println!("scale_grants={count} reopen_verified=true");
    }
}
