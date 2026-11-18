use super::*;
use crate::engine::StorageQueryEngine;
use std::io::Write;
use std::time::{Duration, Instant};

#[test]
fn peer_disconnect_cancels_inline_query_and_releases_transaction_locks() {
    check_disconnect(Some("SELECT BENCHMARK(1000000000,SHA2('abc',256))"));
}

#[test]
fn peer_disconnect_cancels_blocking_worker_and_releases_transaction_locks() {
    check_disconnect(Some("SELECT SLEEP(120)"));
}

#[test]
fn peer_disconnect_releases_idle_transaction_locks() {
    check_disconnect(None);
}

fn check_disconnect(sql: Option<&str>) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let registry = Arc::new(SessionRegistry::new());
    let engine = Arc::new(
        StorageQueryEngine::open_full(dir.path(), Default::default(), Arc::clone(&registry))
            .unwrap(),
    );
    engine
        .execute("CREATE TABLE disconnect_txn(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO disconnect_txn VALUES (1,0)")
        .unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let server_engine = Arc::clone(&engine);
    let server_registry = Arc::clone(&registry);
    let server = crate::util::thread_spawn::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let (stream, peer) = listener.accept().await.unwrap();
                let mut session =
                    Session::new(4248, peer, stream, None, server_engine, server_registry);
                // Skip authentication, retaining the real protocol command loop,
                // transaction engine, cancellation handle and session destructor.
                session.authenticated = true;
                session.context.current_database = "test".into();
                let handle = session.registry.register(
                    4248,
                    "root".into(),
                    "localhost".into(),
                    "test".into(),
                );
                session.kill_flag = Arc::clone(handle.kill_flag());
                session
                    .context
                    .set_cancel_handle(Some(handle.cancel_handle()));
                session.handle = Some(Arc::clone(&handle));
                for sql in ["BEGIN", "UPDATE disconnect_txn SET v=99 WHERE id=1"] {
                    session
                        .engine
                        .execute_with_session(&mut session.context, sql)
                        .unwrap();
                }
                ready_tx.send(handle).unwrap();
                let _ = session.command_loop_with_disconnect().await;
                session.close_authenticated_session().await.unwrap();
                drop(session);
                finished_tx.send(()).unwrap();
            });
    });

    let handle = ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    if let Some(sql) = sql {
        let length = (sql.len() + 1) as u32;
        client.write_all(&length.to_le_bytes()[..3]).unwrap();
        client.write_all(&[0, COM_QUERY]).unwrap();
        client.write_all(sql.as_bytes()).unwrap();
        let started = Instant::now();
        while !registry
            .snapshot()
            .iter()
            .any(|entry| entry.info.as_deref() == Some(sql))
        {
            if started.elapsed() > Duration::from_secs(5) {
                handle.kill_connection();
                drop(client);
                server.join().unwrap();
                panic!("query did not enter the command loop");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    client.shutdown(std::net::Shutdown::Both).unwrap();
    drop(client);
    let completed = finished_rx.recv_timeout(Duration::from_secs(2));
    // Bound the failing regression too: an explicit KILL must still let the
    // old synchronous command finish so the test never leaves a spinning thread.
    if completed.is_err() {
        handle.kill_connection();
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    server.join().unwrap();
    assert!(
        completed.is_ok(),
        "peer close did not cancel and clean up the session"
    );
    assert!(!registry.snapshot().iter().any(|entry| entry.id == 4248));

    let result = engine
        .execute("SELECT v FROM disconnect_txn WHERE id=1")
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![crate::types::Value::Int(0)]]);
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "SET innodb_lock_wait_timeout=1")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE disconnect_txn SET v=v+1 WHERE id=1")
        .expect("disconnect rollback must release the row lock");
}
