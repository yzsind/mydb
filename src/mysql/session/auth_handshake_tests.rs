use super::*;
use crate::engine::executor::StorageQueryEngine;
use crate::mysql::packet::{read_packet, write_packet};
use crate::mysql::protocol::CLIENT_SECURE_CONNECTION;
use sha1::{Digest, Sha1};
use tokio::net::TcpListener;

fn native_token(password: &str, nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(stage1);
    let mut hash = Sha1::new();
    hash.update(nonce);
    hash.update(stage2);
    stage1
        .iter()
        .zip(hash.finalize())
        .map(|(a, b)| a ^ b)
        .collect()
}

async fn packet(stream: &mut TcpStream) -> Packet {
    tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(stream))
        .await
        .expect("authentication response timed out")
        .unwrap()
        .expect("connection closed before authentication response")
}

async fn native_login(
    engine: Arc<StorageQueryEngine>,
    user: &str,
    password: &str,
    deferred: bool,
    accepted: bool,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, peer) = listener.accept().await.unwrap();
    let server = tokio::spawn(async move {
        Session::new(
            1,
            peer,
            stream,
            None,
            engine,
            Arc::new(SessionRegistry::new()),
        )
        .run()
        .await
    });

    let hello = packet(&mut client).await;
    assert_eq!(hello.sequence_id, 0);
    assert!(hello.payload.ends_with(b"caching_sha2_password\0"));
    let offset = hello.payload[1..].iter().position(|b| *b == 0).unwrap() + 2;
    let mut nonce = hello.payload[offset + 4..offset + 12].to_vec();
    nonce.extend_from_slice(&hello.payload[offset + 31..offset + 43]);
    let initial = if deferred {
        Vec::new()
    } else {
        native_token(password, &nonce)
    };
    let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let mut response = Vec::new();
    response.extend_from_slice(&flags.to_le_bytes());
    response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
    response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
    response.extend_from_slice(&[0; 23]);
    response.extend_from_slice(user.as_bytes());
    response.push(0);
    response.push(initial.len() as u8);
    response.extend_from_slice(&initial);
    response.extend_from_slice(b"mysql_native_password\0");
    write_packet(&mut client, 1, &response).await.unwrap();

    // MariaDB 5.5 defers credentials when it cannot use the advertised plugin.
    // An explicitly empty password has the same wire representation.
    let switches = deferred || password.is_empty();
    let final_sequence = if switches {
        let switch = packet(&mut client).await;
        assert_eq!(switch.sequence_id, 2);
        let prefix = b"\xfemysql_native_password\0";
        assert!(switch.payload.starts_with(prefix), "{:?}", switch.payload);
        let switched_nonce = &switch.payload[prefix.len()..];
        assert_eq!(switched_nonce.len(), 21);
        assert_eq!(switched_nonce[20], 0);
        assert_ne!(&switched_nonce[..20], nonce);
        write_packet(
            &mut client,
            3,
            &native_token(password, &switched_nonce[..20]),
        )
        .await
        .unwrap();
        4
    } else {
        2
    };

    let result = packet(&mut client).await;
    assert_eq!(result.sequence_id, final_sequence);
    assert_eq!(result.payload[0], if accepted { 0x00 } else { 0xff });
    if accepted {
        write_packet(&mut client, 0, b"\x03SELECT CURRENT_USER()")
            .await
            .unwrap();
        assert_eq!(packet(&mut client).await.payload, [1]);
        packet(&mut client).await; // column definition
        assert_eq!(packet(&mut client).await.payload[0], 0xfe);
        let identity = format!("{user}@%");
        let mut row = vec![identity.len() as u8];
        row.extend_from_slice(identity.as_bytes());
        assert_eq!(packet(&mut client).await.payload, row);
        assert_eq!(packet(&mut client).await.payload[0], 0xfe);
        write_packet(&mut client, 0, &[COM_QUIT]).await.unwrap();
    } else {
        assert_eq!(
            u16::from_le_bytes(result.payload[1..3].try_into().unwrap()),
            1045
        );
        assert_eq!(&result.payload[3..9], b"#28000");
    }
    drop(client);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("authentication session did not exit")
        .expect("authentication session panicked");
    assert_eq!(outcome.is_ok(), accepted);
}

#[tokio::test]
async fn handshake_deferred_native_credentials_verify_password_and_identity() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    for (user, password) in [("native", "secret"), ("empty", "")] {
        engine
            .execute_with_session(
                &mut SessionContext::default(),
                &format!("CREATE USER '{user}'@'%' IDENTIFIED WITH mysql_native_password BY '{password}'"),
            )
            .unwrap();
        for deferred in [true, false] {
            for supplied in ["secret", "wrong", ""] {
                native_login(
                    engine.clone(),
                    user,
                    supplied,
                    deferred,
                    supplied == password,
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn handshake_deferred_native_credentials_do_not_charge_a_failed_login() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
    engine
        .execute_with_session(
            &mut SessionContext::default(),
            "CREATE USER 'native'@'%' IDENTIFIED WITH mysql_native_password BY 'secret' \
             FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1",
        )
        .unwrap();
    for _ in 0..2 {
        native_login(engine.clone(), "native", "secret", true, true).await;
    }
}
