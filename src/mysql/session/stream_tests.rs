//! Packet-level differential coverage for the shared inline stream lifecycle.
use super::*;
use crate::engine::api::VecResultCursor;
use crate::types::{ColumnSource, ColumnType, Value};

struct UnusedEngine;
impl QueryEngine for UnusedEngine {
    fn execute(&self, _: &mut SessionContext, _: &str) -> Result<QueryResponse, QueryError> {
        unreachable!("the writer test supplies its own result")
    }
}

async fn capture_result(
    result: ResultSet,
    streaming: bool,
    encoding: StreamRowEncoding,
    flags: u32,
) -> (u8, Vec<u8>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, peer) = listener.accept().await.unwrap();
    let mut session = Session::new(
        73,
        peer,
        stream,
        None,
        Arc::new(UnusedEngine),
        Arc::new(SessionRegistry::new()),
    );
    session.capabilities = NegotiatedCapabilities::new(flags, flags);
    session.context.character_set_results = Some("utf8mb4".into());
    let next = if streaming {
        let result = StreamingResultSet {
            columns: result.columns,
            found_rows: Some(77),
            cursor: Box::new(VecResultCursor::new(result.rows)),
        };
        let next = session
            .write_streaming_result_set_inline(result, SERVER_STATUS_AUTOCOMMIT, 250, encoding)
            .await
            .unwrap();
        assert_eq!(session.context.found_rows, 77);
        next
    } else {
        match encoding {
            StreamRowEncoding::Text(_) => session
                .write_text_result_set_parts_with_status_seq(
                    &result.columns,
                    &result.rows,
                    SERVER_STATUS_AUTOCOMMIT,
                    250,
                )
                .await
                .unwrap(),
            StreamRowEncoding::Binary(_) => session
                .write_binary_result_set_with_status_seq(&result, SERVER_STATUS_AUTOCOMMIT, 250)
                .await
                .unwrap(),
        }
    };
    drop(session);
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.read_to_end(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    (next, bytes)
}

#[tokio::test]
async fn inline_text_and_binary_streams_match_materialized_packets() {
    for flags in [
        CLIENT_PROTOCOL_41,
        CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF,
    ] {
        for encoding in [
            StreamRowEncoding::Text(crate::charset::resolve_result_charset(Some("utf8mb4"))),
            StreamRowEncoding::Binary(None),
        ] {
            for rows in [
                Vec::new(),
                vec![
                    vec![Value::Int(7), Value::String("汉字".into())],
                    vec![Value::Null, Value::String("".into())],
                ],
            ] {
                let result = ResultSet {
                    columns: vec![
                        Column {
                            name: "n".into(),
                            column_type: ColumnType::LongLong,
                            source: ColumnSource {
                                flags: crate::mysql::protocol::ZEROFILL_FLAG,
                                length: Some(5),
                                ..Default::default()
                            },
                        },
                        Column {
                            name: "s".into(),
                            column_type: ColumnType::VarString,
                            source: Default::default(),
                        },
                    ],
                    rows,
                };
                let materialized = capture_result(result.clone(), false, encoding, flags).await;
                let streaming = capture_result(result, true, encoding, flags).await;
                assert_eq!(
                    streaming, materialized,
                    "packet order, sequence wrap, row encoding and terminator must agree"
                );
            }
        }
    }
}
