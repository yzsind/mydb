use super::*;
use crate::engine::executor::StorageQueryEngine;
use crate::mysql::packet::{read_packet, Packet};
use crate::mysql::protocol::{CLIENT_DEPRECATE_EOF, CLIENT_PROTOCOL_41};
use tokio::net::{TcpListener, TcpStream};

struct Harness {
    session: Session<StorageQueryEngine>,
    client: TcpStream,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn new(extra_flags: u32) -> Self {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let registry = Arc::new(SessionRegistry::new());
        let engine = Arc::new(
            StorageQueryEngine::open_full(dir.path(), Default::default(), Arc::clone(&registry))
                .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut session = Session::new(1, peer, stream, None, engine, registry);
        // This fixture skips the handshake and models an explicitly selected database.
        session.context.current_database = "test".to_string();
        session.capabilities = NegotiatedCapabilities::new(
            CLIENT_PROTOCOL_41 | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS | extra_flags,
            crate::mysql::protocol::server_capabilities(false),
        );
        Self {
            session,
            client,
            _dir: dir,
        }
    }

    fn setup(&mut self, sql: &str) {
        self.session
            .engine
            .execute_with_session(&mut self.session.context, sql)
            .unwrap();
    }

    async fn query(&mut self, sql: &str) {
        self.session.handle_query(sql.as_bytes()).await.unwrap();
    }

    async fn packet(&mut self, seq: u8) -> Packet {
        let packet = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            read_packet(&mut self.client),
        )
        .await
        .expect("missing response packet")
        .unwrap()
        .unwrap();
        assert_eq!(packet.sequence_id, seq);
        packet
    }

    async fn ok(&mut self, seq: u8, affected: u8, insert_id: u8, more: bool) -> (u16, u16) {
        let packet = self.packet(seq).await;
        assert_eq!(
            &packet.payload[..3],
            &[0, affected, insert_id],
            "{:?}",
            packet.payload
        );
        let status = u16::from_le_bytes(packet.payload[3..5].try_into().unwrap());
        let warnings = u16::from_le_bytes(packet.payload[5..7].try_into().unwrap());
        assert_eq!(status & SERVER_MORE_RESULTS_EXISTS != 0, more);
        (status, warnings)
    }

    async fn error(&mut self, seq: u8, code: u16) {
        let packet = self.packet(seq).await;
        assert_eq!(packet.payload[0], 0xff);
        assert_eq!(
            u16::from_le_bytes(packet.payload[1..3].try_into().unwrap()),
            code
        );
    }

    async fn scalar(&mut self, mut seq: u8, value: &str, more: bool) -> u8 {
        assert_eq!(self.packet(seq).await.payload, [1]);
        seq = seq.wrapping_add(1);
        self.packet(seq).await; // column definition
        seq = seq.wrapping_add(1);
        if !self.session.capabilities.has(CLIENT_DEPRECATE_EOF) {
            assert_eq!(self.packet(seq).await.payload[0], 0xfe);
            seq = seq.wrapping_add(1);
        }
        let row = self.packet(seq).await;
        assert_eq!(row.payload[0] as usize, value.len());
        assert_eq!(&row.payload[1..], value.as_bytes());
        seq = seq.wrapping_add(1);
        let end = self.packet(seq).await;
        assert_eq!(end.payload[0], 0xfe);
        let status = u16::from_le_bytes(end.payload[3..5].try_into().unwrap());
        assert_eq!(status & SERVER_MORE_RESULTS_EXISTS != 0, more);
        seq.wrapping_add(1)
    }
}

#[tokio::test]
async fn multi_query_counts_and_warm_cache() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE TABLE t (id INT PRIMARY KEY, k INT)");
    h.setup("INSERT INTO t VALUES (1,1)");
    for _ in 0..3 {
        h.query("UPDATE t SET k=2 WHERE id=1; UPDATE t SET k=2 WHERE id=-1; INSERT INTO t VALUES (2,1),(3,1)").await;
        h.ok(1, 1, 0, true).await;
        h.ok(2, 0, 0, true).await;
        h.ok(3, 2, 0, false).await;
        h.query("SELECT SUM(k) FROM t").await;
        h.scalar(1, "4", false).await;
        h.setup("DELETE FROM t WHERE id>1");
        h.setup("UPDATE t SET k=1 WHERE id=1");
    }
}

#[tokio::test]
async fn multi_query_error_preserves_prefix_and_stops_suffix() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE TABLE t (id INT PRIMARY KEY)");
    for (middle, code) in [
        ("INSERT INTO t VALUES (1)", 1062),
        ("nonsense SQL", 1064),
        ("SELECT 'unterminated", 1064),
    ] {
        h.setup("DELETE FROM t");
        h.query(&format!(
            "INSERT INTO t VALUES (1); {middle}; INSERT INTO t VALUES (2)"
        ))
        .await;
        h.ok(1, 1, 0, true).await;
        h.error(2, code).await;
        h.query("SELECT COUNT(*) FROM t").await;
        h.scalar(1, "1", false).await;
    }
}

#[tokio::test]
async fn multi_query_mixed_results_and_statement_state() {
    for flags in [0, CLIENT_DEPRECATE_EOF] {
        let mut h = Harness::new(flags).await;
        h.setup("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, k INT)");
        h.query("START TRANSACTION; INSERT INTO t(k) VALUES (1); SELECT COUNT(*) FROM t; INSERT INTO t(k) VALUES (2); ROLLBACK; SELECT COUNT(*) FROM t").await;
        let (status, _) = h.ok(1, 0, 0, true).await;
        assert_ne!(status & SERVER_STATUS_IN_TRANS, 0);
        h.ok(2, 1, 1, true).await;
        let seq = h.scalar(3, "1", true).await;
        h.ok(seq, 1, 2, true).await;
        let (status, _) = h.ok(seq + 1, 0, 0, true).await;
        assert_eq!(status & SERVER_STATUS_IN_TRANS, 0);
        h.scalar(seq + 2, "0", false).await;
    }
}

#[tokio::test]
async fn multi_query_quotes_comments_routines_and_sql_mode() {
    let mut h = Harness::new(0).await;
    h.query("SELECT ';quoted'; /* inert ; */ SELECT 2; -- trailing ;\n")
        .await;
    let seq = h.scalar(1, ";quoted", true).await;
    h.scalar(seq, "2", false).await;
    h.query("/*!80000 SELECT 3; SELECT 4 */; SELECT 5").await;
    let seq = h.scalar(1, "3", true).await;
    let seq = h.scalar(seq, "4", true).await;
    h.scalar(seq, "5", false).await;
    h.query("CREATE PROCEDURE p() BEGIN SELECT 6; SELECT 7; END; CALL p(); SELECT 8")
        .await;
    h.ok(1, 0, 0, true).await;
    let seq = h.scalar(2, "6", true).await;
    let seq = h.scalar(seq, "7", true).await;
    h.ok(seq, 0, 0, true).await;
    h.scalar(seq + 1, "8", false).await;
    h.query("SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 'a\\'; SELECT 9")
        .await;
    h.ok(1, 0, 0, true).await;
    let seq = h.scalar(2, "a\\", true).await;
    h.scalar(seq, "9", false).await;
}

#[tokio::test]
async fn multi_query_sequence_wraps_and_resets() {
    let mut h = Harness::new(0).await;
    h.query(&vec!["SET @x=1"; 260].join(";")).await;
    for i in 0..260 {
        h.ok((i as u8).wrapping_add(1), 0, 0, i < 259).await;
    }
    h.query("SET @x=2").await;
    h.ok(1, 0, 0, false).await;
}

#[tokio::test]
async fn multi_query_found_rows_warnings_and_capability_toggle() {
    let mut h = Harness::new(CLIENT_FOUND_ROWS).await;
    h.setup("CREATE TABLE t (id INT PRIMARY KEY, k INT)");
    h.setup("INSERT INTO t VALUES (1,1)");
    h.query("UPDATE t SET k=1 WHERE id=1; UPDATE t SET k=1 WHERE id=0; INSERT IGNORE INTO t VALUES (1,2); UPDATE t SET k=2 WHERE id=1").await;
    h.ok(1, 1, 0, true).await;
    h.ok(2, 0, 0, true).await;
    let (_, warnings) = h.ok(3, 0, 0, true).await;
    assert_eq!(warnings, 1);
    let (_, warnings) = h.ok(4, 1, 0, false).await;
    assert_eq!(warnings, 0);
    h.session.handle_set_option(&[1, 0]).await.unwrap();
    h.ok(1, 0, 0, false).await;
    h.query("INSERT INTO t VALUES (2,1); INSERT INTO t VALUES (3,1)")
        .await;
    h.error(1, 1064).await;
    h.session.handle_set_option(&[0, 0]).await.unwrap();
    h.ok(1, 0, 0, false).await;
    h.query("INSERT INTO t VALUES (2,1); INSERT INTO t VALUES (3,1)")
        .await;
    h.ok(1, 1, 0, true).await;
    h.ok(2, 1, 0, false).await;
}

#[tokio::test]
async fn multi_query_local_infile_continues_sequence() {
    use crate::mysql::packet::write_packet;
    let mut h = Harness::new(crate::mysql::protocol::CLIENT_LOCAL_FILES).await;
    h.setup("SET GLOBAL local_infile=ON");
    h.setup("CREATE TABLE t (id INT)");
    let session = &mut h.session;
    let client = &mut h.client;
    let (result, ()) = tokio::join!(
        session.handle_query(
            b"SET @x=1; LOAD DATA LOCAL INFILE 'batch.txt' INTO TABLE t; SELECT COUNT(*) FROM t"
        ),
        async {
            let first = read_packet(client).await.unwrap().unwrap();
            assert_eq!(first.sequence_id, 1);
            assert_eq!(first.payload[0], 0);
            let request = read_packet(client).await.unwrap().unwrap();
            assert_eq!(request.sequence_id, 2);
            assert_eq!(request.payload[0], 0xfb);
            write_packet(client, 3, b"1\n2\n").await.unwrap();
            write_packet(client, 4, b"").await.unwrap();
        }
    );
    result.unwrap();
    h.ok(5, 2, 0, true).await;
    h.scalar(6, "2", false).await;
}

#[tokio::test]
async fn multi_query_coalesced_updates_keep_logical_counts_and_error_prefix() {
    for flags in [0, CLIENT_FOUND_ROWS] {
        let mut h = Harness::new(flags).await;
        h.setup("CREATE TABLE t (id INT PRIMARY KEY, k TINYINT)");
        h.setup("INSERT INTO t VALUES (1,1),(2,0)");
        h.setup("SET autocommit=0");
        for _ in 0..3 {
            h.query("UPDATE t SET k=2 WHERE id=1; UPDATE t SET k=2 WHERE id=1; UPDATE t SET k=2 WHERE id=99; UPDATE t SET k=3 WHERE id=2").await;
            h.ok(1, 1, 0, true).await;
            h.ok(2, u8::from(flags != 0), 0, true).await;
            h.ok(3, 0, 0, true).await;
            h.ok(4, 1, 0, false).await;
            h.setup("ROLLBACK");
        }
        h.query("UPDATE t SET k=2 WHERE id=1; UPDATE t SET k=1000 WHERE id=1; UPDATE t SET k=3 WHERE id=2").await;
        h.ok(1, 1, 0, true).await;
        h.error(2, 1264).await;
        h.query("SELECT SUM(k) FROM t").await;
        h.scalar(1, "2", false).await;
        h.setup("ROLLBACK");
    }
}

#[tokio::test]
async fn multi_query_compiled_batch_retains_insert_ids_and_per_update_warnings() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE TABLE ids (id INT PRIMARY KEY AUTO_INCREMENT, k INT)");
    let (results, result) = h
        .session
        .engine
        .execute_text_dml_batch(
            &mut h.session.context,
            "INSERT INTO ids(k) VALUES (1); INSERT INTO ids(k) VALUES (2)",
        )
        .expect("compiled DML batch must remain enabled");
    result.unwrap();
    assert_eq!(
        results.iter().map(|r| r.insert_id).collect::<Vec<_>>(),
        [1, 2]
    );
    h.query("INSERT INTO ids(k) VALUES (3); INSERT INTO ids(k) VALUES (4)")
        .await;
    h.ok(1, 1, 3, true).await;
    h.ok(2, 1, 4, false).await;

    h.setup("CREATE TABLE t (id INT PRIMARY KEY, k TINYINT)");
    h.setup("INSERT INTO t VALUES (1,1)");
    h.setup("SET sql_mode=''");
    h.setup("SET autocommit=0");
    let sql =
        "UPDATE t SET k=1000 WHERE id=1; UPDATE t SET k=2 WHERE id=1; UPDATE t SET k=2 WHERE id=1";
    let (results, result) = h
        .session
        .engine
        .execute_text_dml_batch(&mut h.session.context, sql)
        .expect("coalesced UPDATE batch must remain enabled");
    result.unwrap();
    assert_eq!(
        results
            .iter()
            .map(|r| (r.affected_rows, r.warnings.len()))
            .collect::<Vec<_>>(),
        [(1, 1), (1, 0), (0, 0)]
    );
    h.setup("ROLLBACK");
    h.query(sql).await;
    assert_eq!(h.ok(1, 1, 0, true).await.1, 1);
    assert_eq!(h.ok(2, 1, 0, true).await.1, 0);
    assert_eq!(h.ok(3, 0, 0, false).await.1, 0);
    h.setup("ROLLBACK");
}

#[tokio::test(flavor = "current_thread")]
async fn cached_text_select_waits_for_partition_ddl_without_blocking_runtime() {
    use crate::storage::{MdlDuration, MdlLockType, MdlObjectKey, MdlRequest};
    let mut h = Harness::new(0).await;
    h.setup("CREATE TABLE t(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2");
    h.setup("INSERT INTO t VALUES(1,10)");
    for _ in 0..3 {
        h.query("SELECT v FROM t WHERE id=1").await;
        h.scalar(1, "10", false).await;
    }
    let engine = h.session.engine.clone();
    engine
        .database()
        .acquire_mdl(
            9901,
            Some(9901),
            MdlDuration::Explicit,
            vec![MdlRequest {
                key: MdlObjectKey::table("test", "t"),
                lock_type: MdlLockType::Exclusive,
            }],
            "test/partition-publish",
        )
        .unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut session = SessionContext::default();
        session.connection_id = 9901;
        let rebuilt = engine.execute_with_session(
            &mut session,
            "ALTER TABLE t REBUILD PARTITION ALL, LOCK=EXCLUSIVE",
        );
        let updated = engine.execute_with_session(&mut session, "UPDATE t SET v=20 WHERE id=1");
        engine.database().release_mdl_owner(9901);
        rebuilt.unwrap();
        updated.unwrap();
    });
    h.query("SELECT v FROM t WHERE id=1").await;
    h.scalar(1, "20", false).await;
    release.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cached_binary_select_waits_for_partition_ddl_without_blocking_runtime() {
    use crate::storage::{MdlDuration, MdlLockType, MdlObjectKey, MdlRequest};
    async fn result(h: &mut Harness, expected: i32) {
        assert_eq!(h.packet(1).await.payload, [1]);
        h.packet(2).await;
        assert_eq!(h.packet(3).await.payload[0], 0xfe);
        let row = h.packet(4).await.payload.clone();
        assert_eq!(&row[..2], &[0, 0]);
        assert_eq!(i32::from_le_bytes(row[2..6].try_into().unwrap()), expected);
        assert_eq!(h.packet(5).await.payload[0], 0xfe);
    }
    let mut h = Harness::new(0).await;
    h.setup("CREATE TABLE t(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2");
    h.setup("INSERT INTO t VALUES(1,10)");
    h.session
        .handle_stmt_prepare(b"SELECT v FROM t WHERE id=?")
        .await
        .unwrap();
    let prepared = h.packet(1).await.payload.clone();
    assert_eq!(prepared[0], 0);
    assert_eq!(u16::from_le_bytes(prepared[5..7].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(prepared[7..9].try_into().unwrap()), 1);
    let id = u32::from_le_bytes(prepared[1..5].try_into().unwrap());
    for seq in 2..=5 {
        h.packet(seq).await;
    }
    let mut payload = id.to_le_bytes().to_vec();
    payload.extend_from_slice(&[0, 1, 0, 0, 0, 0, 1, 3, 0]);
    payload.extend_from_slice(&1i32.to_le_bytes());
    for _ in 0..3 {
        h.session.handle_stmt_execute(&payload).await.unwrap();
        result(&mut h, 10).await;
    }
    let engine = h.session.engine.clone();
    engine
        .database()
        .acquire_mdl(
            9902,
            Some(9902),
            MdlDuration::Explicit,
            vec![MdlRequest {
                key: MdlObjectKey::table("test", "t"),
                lock_type: MdlLockType::Exclusive,
            }],
            "test/partition-publish",
        )
        .unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut session = SessionContext::default();
        session.connection_id = 9902;
        let rebuilt = engine.execute_with_session(
            &mut session,
            "ALTER TABLE t REBUILD PARTITION ALL, LOCK=EXCLUSIVE",
        );
        let updated = engine.execute_with_session(&mut session, "UPDATE t SET v=20 WHERE id=1");
        engine.database().release_mdl_owner(9902);
        rebuilt.unwrap();
        updated.unwrap();
    });
    h.session.handle_stmt_execute(&payload).await.unwrap();
    result(&mut h, 20).await;
    release.await.unwrap();
}

#[tokio::test]
async fn audit1022_self_kill_query_does_not_cancel_the_next_batch_statement() {
    let mut h = Harness::new(0).await;
    h.session.connection_id = 100_001;
    h.session.context.connection_id = 100_001;
    let handle = h.session.registry.register_authenticated(
        100_001,
        "root".into(),
        "localhost".into(),
        "test".into(),
        crate::engine::status_counters::SessionStatus::new_untracked(),
        crate::engine::auth::AccountKey::new("root", "localhost"),
    );
    h.session
        .context
        .set_cancel_handle(Some(handle.cancel_handle()));
    h.session.kill_flag = Arc::clone(handle.kill_flag());
    h.session.handle = Some(handle);
    h.query("KILL QUERY 100001; SELECT 7").await;
    h.ok(1, 0, 0, true).await;
    h.scalar(2, "7", false).await;
}

#[tokio::test]
async fn audit1022_self_kill_connection_sends_a_final_ok_packet() {
    let mut h = Harness::new(0).await;
    h.session.connection_id = 100_001;
    h.session.context.connection_id = 100_001;
    let handle = h.session.registry.register_authenticated(
        100_001,
        "root".into(),
        "localhost".into(),
        "test".into(),
        crate::engine::status_counters::SessionStatus::new_untracked(),
        crate::engine::auth::AccountKey::new("root", "localhost"),
    );
    h.session
        .context
        .set_cancel_handle(Some(handle.cancel_handle()));
    h.session.kill_flag = Arc::clone(handle.kill_flag());
    h.session.handle = Some(handle);
    h.query("KILL 100001; SELECT 7").await;
    h.ok(1, 0, 0, false).await;
    assert!(h.session.context.close_after_response);
}

#[tokio::test]
async fn txn_audit_prepared_release_closes_before_next_command() {
    use tokio::io::AsyncWriteExt;
    let mut h = Harness::new(0).await;
    h.session
        .handle_stmt_prepare(b"COMMIT RELEASE")
        .await
        .unwrap();
    let packet = h.packet(1).await;
    assert_eq!(packet.payload[0], 0);
    let id = &packet.payload[1..5];
    let mut execute = vec![COM_STMT_EXECUTE];
    execute.extend_from_slice(id);
    execute.extend_from_slice(&[0, 1, 0, 0, 0]);
    let mut command = vec![execute.len() as u8, 0, 0, 0];
    command.extend_from_slice(&execute);
    h.client.write_all(&command).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), h.session.command_loop())
        .await
        .expect("RELEASE must end the command loop immediately")
        .unwrap();
    assert_eq!(h.packet(1).await.payload[0], 0);
    assert!(h.session.normal_close);
}

#[tokio::test]
async fn txn_audit_read_only_status() {
    let mut h = Harness::new(0).await;
    h.setup("START TRANSACTION READ ONLY");
    assert_ne!(h.session.server_status() & 0x2000, 0);
    h.setup("COMMIT");
    assert_eq!(h.session.server_status() & 0x2000, 0);
    h.setup("START TRANSACTION READ WRITE");
    assert_eq!(h.session.server_status() & 0x2000, 0);
    h.setup("ROLLBACK");
}

#[tokio::test]
async fn txn_audit_idle_xa_retains_admission() {
    let mut h = Harness::new(0).await;
    h.setup("XA START 'admission'");
    h.session.acquire_txn_admission().await;
    if crate::util::exec_permit::global().is_none() {
        h.setup("XA ROLLBACK 'admission'");
        return;
    }
    assert!(h.session.txn_admission.is_some());
    h.setup("XA END 'admission'");
    h.session.release_txn_admission_if_idle();
    assert!(h.session.txn_admission.is_some());
    h.setup("XA ROLLBACK 'admission'");
    h.session.release_txn_admission_if_idle();
    assert!(h.session.txn_admission.is_none());
}

#[tokio::test]
async fn routine_audit_1028_call_streams_before_body_finishes() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE PROCEDURE early_result() BEGIN SELECT 17; DO SLEEP(1); SELECT 23; END");
    let query = h.session.handle_query(b"CALL early_result()");
    tokio::pin!(query);
    let first = tokio::select! {
        packet = read_packet(&mut h.client) => packet.unwrap().unwrap(),
        result = &mut query => panic!("CALL finished before client received the first result: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(700)) => panic!("CALL retained its first result"),
    };
    assert_eq!(first.sequence_id, 1);
    assert_eq!(first.payload, [1]);
    // The session future must be polled to make network progress.
    query.await.unwrap();
}

#[tokio::test]
async fn routine_audit_1028_nested_call_results_keep_sequence_and_error() {
    for flags in [0, CLIENT_DEPRECATE_EOF] {
        let mut h = Harness::new(flags).await;
        h.setup("CREATE PROCEDURE child_results() BEGIN SELECT 2; SELECT 3; END");
        h.setup("CREATE PROCEDURE parent_results() BEGIN SELECT 1; CALL child_results(); SELECT 4; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='after results'; END");
        h.query("SELECT 0; CALL parent_results(); SELECT 99").await;
        let mut seq = h.scalar(1, "0", true).await;
        for expected in ["1", "2", "3", "4"] {
            seq = h.scalar(seq, expected, true).await;
        }
        h.error(seq, 1644).await;
    }
}

#[tokio::test]
async fn routine_audit_1028_streaming_loop_keeps_all_result_sets() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE PROCEDURE many_results() BEGIN DECLARE i INT DEFAULT 0; WHILE i<64 DO SELECT i; SET i=i+1; END WHILE; END");
    h.query("CALL many_results()").await;
    let mut seq = 1;
    for i in 0..64 {
        seq = h.scalar(seq, &i.to_string(), true).await;
    }
    h.ok(seq, 0, 0, false).await;
}

#[tokio::test]
async fn routine_audit_1028_kill_unblocks_backpressured_call() {
    let mut h = Harness::new(0).await;
    h.setup("CREATE PROCEDURE backpressured() BEGIN LOOP SELECT REPEAT('x',100); END LOOP; END");
    let handle = h
        .session
        .registry
        .register(1, "root".into(), "localhost".into(), "test".into());
    h.session
        .context
        .set_cancel_handle(Some(handle.cancel_handle()));
    h.session.kill_flag = Arc::clone(handle.kill_flag());
    h.session.handle = Some(Arc::clone(&handle));
    // Keep the client socket open without reading until the send buffer fills.
    let call = h.session.handle_query(b"CALL backpressured()");
    let kill = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        handle.peer_disconnected();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(call, kill)
    })
    .await
    .expect("KILL must wake the final socket flush and the bounded producer");
    result.unwrap();
}
