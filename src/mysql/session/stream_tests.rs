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
    capture_result_with_vector_format(result, streaming.then_some(false), encoding, flags, false)
        .await
}

async fn capture_result_with_vector_format(
    result: ResultSet,
    streaming: Option<bool>,
    encoding: StreamRowEncoding,
    flags: u32,
    vector_text: bool,
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
    session.context.vector_result_format_text = vector_text;
    let next = if let Some(offloaded) = streaming {
        let result = StreamingResultSet {
            columns: result.columns,
            found_rows: Some(77),
            cursor: Box::new(VecResultCursor::new(result.rows)),
        };
        let next = if offloaded {
            session
                .write_streaming_result_set_offloaded(
                    result,
                    SERVER_STATUS_AUTOCOMMIT,
                    250,
                    encoding,
                )
                .await
                .unwrap()
        } else {
            session
                .write_streaming_result_set_inline(result, SERVER_STATUS_AUTOCOMMIT, 250, encoding)
                .await
                .unwrap()
        };
        assert_eq!(session.context.found_rows, 77);
        next
    } else {
        match encoding {
            StreamRowEncoding::Text(_) | StreamRowEncoding::VectorText(_) => session
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
async fn vector_result_format_matches_all_text_writers_and_preserves_binary() {
    let vector: Value = Value::Vector(crate::vector::VectorValue::from(
        [1.0f32, -2.5, 3.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>(),
    ));
    let result = ResultSet {
        columns: vec![Column {
            name: "v".into(),
            column_type: ColumnType::Vector,
            source: Default::default(),
        }],
        rows: vec![vec![vector], vec![Value::Null]],
    };
    for flags in [
        CLIENT_PROTOCOL_41,
        CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF,
    ] {
        let encoding =
            StreamRowEncoding::Text(crate::charset::resolve_result_charset(Some("utf8mb4")));
        let mut expected_text = result.clone();
        expected_text.columns =
            crate::mysql::protocol::vector_text_result_columns(&result.columns, true).into_owned();
        expected_text.rows[0][0] = Value::String("[1,-2.5,3]".into());
        let text_packets = capture_result(expected_text, false, encoding, flags).await;
        let native_packets = capture_result(result.clone(), false, encoding, flags).await;
        assert_ne!(text_packets, native_packets);
        for mode in [None, Some(false), Some(true)] {
            assert_eq!(
                capture_result_with_vector_format(result.clone(), mode, encoding, flags, true)
                    .await,
                text_packets
            );
            assert_eq!(
                capture_result_with_vector_format(result.clone(), mode, encoding, flags, false)
                    .await,
                native_packets
            );
            let binary = StreamRowEncoding::Binary(None);
            let expected_binary = capture_result(result.clone(), false, binary, flags).await;
            assert_eq!(
                capture_result_with_vector_format(result.clone(), mode, binary, flags, true).await,
                expected_binary
            );
        }
    }
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
