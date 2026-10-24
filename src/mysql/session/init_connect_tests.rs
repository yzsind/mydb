use super::*;
use crate::engine::executor::StorageQueryEngine;
use crate::mysql::packet::{read_packet, write_packet};
use crate::mysql::protocol::{
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS, CLIENT_CONNECT_WITH_DB, CLIENT_PLUGIN_AUTH,
    CLIENT_SECURE_CONNECTION,
};
use crate::types::Value;
use tokio::net::TcpListener;

fn execute(engine: &StorageQueryEngine, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(&mut SessionContext::default(), sql)
        .unwrap()
}

struct Client {
    stream: TcpStream,
    server: tokio::task::JoinHandle<()>,
}

impl Client {
    async fn connect(engine: Arc<StorageQueryEngine>, user: &str, database: Option<&str>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, peer) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            let mut session = Session::new(
                10,
                peer,
                socket,
                None,
                engine,
                Arc::new(SessionRegistry::new()),
            );
            session.run().await.unwrap();
        });
        let mut client = Self { stream, server };
        assert_eq!(client.packet().await.sequence_id, 0);
        let flags = CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
            | if database.is_some() {
                CLIENT_CONNECT_WITH_DB
            } else {
                0
            };
        let mut response = Vec::new();
        response.extend_from_slice(&flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        response.extend_from_slice(&[0; 23]);
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.push(0); // empty password
        if let Some(database) = database {
            response.extend_from_slice(database.as_bytes());
            response.push(0);
        }
        response.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client.stream, 1, &response)
            .await
            .unwrap();
        let ok = client.packet().await;
        assert_eq!(ok.sequence_id, 2);
        assert_eq!(ok.payload[0], 0, "{:?}", ok.payload);
        client
    }

    async fn packet(&mut self) -> Packet {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            read_packet(&mut self.stream),
        )
        .await
        .unwrap()
        .unwrap()
        .expect("connection closed before response")
    }

    async fn command(&mut self, command: u8, data: &[u8]) -> Packet {
        let mut payload = vec![command];
        payload.extend_from_slice(data);
        write_packet(&mut self.stream, 0, &payload).await.unwrap();
        let packet = self.packet().await;
        assert_eq!(packet.sequence_id, 1);
        packet
    }

    async fn scalar(&mut self, sql: &str) -> Vec<u8> {
        assert_eq!(self.command(COM_QUERY, sql.as_bytes()).await.payload, [1]);
        self.packet().await; // column
        assert_eq!(self.packet().await.payload[0], 0xfe); // EOF
        let row = self.packet().await.payload.clone();
        assert_eq!(self.packet().await.payload[0], 0xfe);
        row
    }

    async fn quit(mut self) {
        write_packet(&mut self.stream, 0, &[COM_QUIT])
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), self.server)
            .await
            .unwrap()
            .unwrap();
    }

    async fn rejected(mut self) {
        let packet = self.command(COM_QUERY, b"SELECT 1").await;
        assert_eq!(packet.payload[0], 0xff);
        assert_eq!(
            u16::from_le_bytes(packet.payload[1..3].try_into().unwrap()),
            1184
        );
        assert!(String::from_utf8_lossy(&packet.payload).contains("init_connect command failed"));
        assert!(read_packet(&mut self.stream).await.unwrap().is_none());
        tokio::time::timeout(std::time::Duration::from_secs(5), self.server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn init_connect_mysql80_login_lifecycle() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    for sql in [
        "CREATE USER 'ordinary'@'%' IDENTIFIED BY ''",
        "CREATE USER 'admin'@'%' IDENTIFIED BY ''",
        "CREATE USER 'superuser'@'%' IDENTIFIED BY ''",
        "CREATE USER 'expired'@'%' IDENTIFIED BY '' PASSWORD EXPIRE",
        "CREATE ROLE 'connection_role'",
        "GRANT CONNECTION_ADMIN ON *.* TO 'connection_role'",
        "GRANT 'connection_role' TO 'admin'@'%'",
        "SET DEFAULT ROLE 'connection_role' TO 'admin'@'%'",
        "GRANT SUPER ON *.* TO 'superuser'@'%'",
        "CREATE TABLE test.init_audit (id INT PRIMARY KEY, value INT)",
        "GRANT SELECT, INSERT, UPDATE ON test.* TO 'ordinary'@'%'",
        "SET GLOBAL init_connect='SET @initialized=41; SET @text=''a;b''; SET SESSION sql_select_limit=7; SELECT @initialized; INSERT INTO test.init_audit VALUES (1,1)'",
    ] { execute(&engine, sql); }

    // Client has no CLIENT_MULTI_STATEMENTS/MULTI_RESULTS. Init result packets
    // must never leak, and the selected database/session state must survive.
    let mut ordinary = Client::connect(engine.clone(), "ordinary", Some("test")).await;
    assert_eq!(ordinary.scalar("SELECT @initialized+1").await, b"\x0242");
    assert_eq!(ordinary.scalar("SELECT @text").await, b"\x03a;b");
    assert_eq!(ordinary.scalar("SELECT @@sql_select_limit").await, b"\x017");
    assert_eq!(ordinary.scalar("SELECT DATABASE()").await, b"\x04test");

    execute(&engine, "SET GLOBAL init_connect='SET @initialized=99; UPDATE test.init_audit SET value=value+1 WHERE id=1'");
    assert_eq!(ordinary.scalar("SELECT @initialized").await, b"\x0241");
    let mut next = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(next.scalar("SELECT @initialized").await, b"\x0299");
    next.quit().await;

    // RESET and CHANGE_USER reset state without executing init_connect again.
    assert_eq!(
        ordinary.command(COM_RESET_CONNECTION, &[]).await.payload[0],
        0
    );
    assert_eq!(ordinary.scalar("SELECT @initialized").await, [0xfb]);
    let mut change = b"ordinary\0\0test\0".to_vec();
    change.extend_from_slice(&crate::charset::UTF8MB4_0900_AI_CI_ID.to_le_bytes());
    change.extend_from_slice(b"caching_sha2_password\0");
    assert_eq!(
        ordinary.command(COM_CHANGE_USER, &change).await.payload[0],
        0
    );
    assert_eq!(ordinary.scalar("SELECT @initialized").await, [0xfb]);
    ordinary.quit().await;

    // Invalid SQL is accepted as configuration, but rejects only new ordinary
    // connections; default-role CONNECTION_ADMIN, SUPER and expired passwords skip it.
    execute(
        &engine,
        "ALTER USER 'ordinary'@'%' WITH MAX_USER_CONNECTIONS 1",
    );
    execute(&engine, "SET GLOBAL init_connect='THIS IS INVALID SQL'");
    Client::connect(engine.clone(), "ordinary", None)
        .await
        .rejected()
        .await;
    for user in ["admin", "superuser", "expired"] {
        let mut client = Client::connect(engine.clone(), user, None).await;
        assert_eq!(client.command(COM_PING, &[]).await.payload[0], 0);
        client.quit().await;
    }

    // No privilege escalation; a committed prefix remains, an open transaction
    // rolls back during failed-session cleanup, and the suffix never executes.
    execute(&engine, "SET GLOBAL init_connect='UPDATE test.init_audit SET value=3 WHERE id=1; START TRANSACTION; UPDATE test.init_audit SET value=4 WHERE id=1; SELECT * FROM mysql.user; UPDATE test.init_audit SET value=5 WHERE id=1'");
    Client::connect(engine.clone(), "ordinary", None)
        .await
        .rejected()
        .await;
    let QueryResponse::ResultSet(result) =
        execute(&engine, "SELECT value FROM test.init_audit WHERE id=1")
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3)]]);

    execute(&engine, "SET GLOBAL init_connect='USE test; SET autocommit=0; UPDATE init_audit SET value=6 WHERE id=1'");
    let mut transaction = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(transaction.scalar("SELECT @@autocommit").await, b"\x010");
    assert_eq!(
        transaction
            .scalar("SELECT value FROM init_audit WHERE id=1")
            .await,
        b"\x016"
    );
    transaction.quit().await;
    let QueryResponse::ResultSet(result) =
        execute(&engine, "SELECT value FROM test.init_audit WHERE id=1")
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(3)]]);

    // SYSTEM_VARIABLES_ADMIN alone does not exempt a user. SQL may modify its
    // own configuration without deadlock; this connection completes its snapshot.
    execute(
        &engine,
        "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'ordinary'@'%'",
    );
    execute(
        &engine,
        "SET GLOBAL init_connect='SET GLOBAL init_connect=''SET @snapshot=2''; SET @snapshot=1'",
    );
    let mut snapshot = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(snapshot.scalar("SELECT @snapshot").await, b"\x011");
    snapshot.quit().await;
    let mut following = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(following.scalar("SELECT @snapshot").await, b"\x012");
    following.quit().await;

    execute(
        &engine,
        "SET GLOBAL init_connect='SELECT CAST(''abc'' AS UNSIGNED)'",
    );
    let mut warning = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(warning.scalar("SELECT @@warning_count").await, b"\x011");
    warning.quit().await;

    // Clearing restores ordinary connections, including after multiple failures.
    execute(&engine, "SET GLOBAL init_connect=DEFAULT");
    let mut recovered = Client::connect(engine.clone(), "ordinary", None).await;
    assert_eq!(recovered.scalar("SELECT @initialized").await, [0xfb]);
    recovered.quit().await;
}

#[test]
fn init_connect_sequential_parser_modes_and_routines() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let mut session = SessionContext::default();
    execute_init_connect_sql(&engine, &mut session,
        "SET sql_mode='ANSI_QUOTES'; SET @a=1; SET @a=@a+1; SELECT \"a\" FROM (SELECT 3 AS a) t; SET sql_mode='NO_BACKSLASH_ESCAPES'; SET @s='a\\'; SET @b=4").unwrap();
    assert_eq!(session.user_variable("a"), Some(&Value::Int(2)));
    assert_eq!(
        session.user_variable("s"),
        Some(&Value::String("a\\".into()))
    );
    assert_eq!(session.user_variable("b"), Some(&Value::Int(4)));
    execute_init_connect_sql(&engine, &mut session,
        "CREATE PROCEDURE init_proc() BEGIN SET @a=5; SELECT 1; SET @a=@a+1; END; CALL init_proc(); /*!80000 SET @b=7 */; -- comment\n SET @c='x;y'").unwrap();
    assert_eq!(session.user_variable("a"), Some(&Value::Int(6)));
    assert_eq!(session.user_variable("b"), Some(&Value::Int(7)));
    assert_eq!(
        session.user_variable("c"),
        Some(&Value::String("x;y".into()))
    );
    let err =
        execute_init_connect_sql(&engine, &mut session, "SET @prefix=1; SELECT 'unterminated")
            .unwrap_err();
    assert_eq!(err.code(), 1064);
    assert_eq!(session.user_variable("prefix"), Some(&Value::Int(1)));
    execute_init_connect_sql(&engine, &mut session, " ; /* empty */; -- empty\n").unwrap();
}

#[test]
fn init_connect_drains_streams_and_propagates_deferred_errors() {
    use crate::engine::api::{ResultCursor, StreamingResultSet};
    #[derive(Debug)]
    struct Cursor {
        remaining: usize,
        fail: bool,
        consumed: Arc<AtomicU64>,
    }
    impl ResultCursor for Cursor {
        fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
            if self.remaining == 0 {
                return if self.fail {
                    Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "deferred failure",
                    ))
                } else {
                    Ok(None)
                };
            }
            self.remaining -= 1;
            self.consumed.fetch_add(1, Ordering::Relaxed);
            Ok(Some(vec![Value::Int(1)]))
        }
    }
    for fail in [false, true] {
        let consumed = Arc::new(AtomicU64::new(0));
        let response = QueryResponse::MultiResult(vec![
            QueryResponse::StreamingResultSet(StreamingResultSet {
                columns: vec![],
                found_rows: None,
                cursor: Box::new(Cursor {
                    remaining: 1000,
                    fail,
                    consumed: consumed.clone(),
                }),
            }),
            QueryResponse::Ok,
        ]);
        let result = discard_init_connect_response(response, &mut SessionContext::default());
        assert_eq!(result.is_err(), fail);
        assert_eq!(consumed.load(Ordering::Relaxed), 1000);
    }
}
