use super::*;
use crate::engine::api::ResultCursor;
use crate::types::{ColumnSource, ColumnType, Value};
use std::sync::atomic::AtomicUsize;
use tokio::io::DuplexStream;

#[derive(Debug)]
struct NeverPull(Arc<AtomicUsize>);
impl ResultCursor for NeverPull {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("metadata must be sent before pulling rows")
    }
}
struct HeaderSession {
    codec: PacketCodec,
    stream: DuplexStream,
    context: SessionContext,
    handle: Option<Arc<SessionHandle>>,
    capabilities: u32,
}
impl HeaderSession {
    fn client_capabilities(&self) -> u32 {
        self.capabilities
    }
    fn result_fallback_collation_id(&self) -> u16 {
        255
    }
}

async fn render_header(
    columns: Vec<Column>,
    capabilities: u32,
    metadata_full: bool,
    vector_text: bool,
    encoding: StreamRowEncoding,
    first_sequence_id: u8,
    shared: bool,
) -> io::Result<(Vec<u8>, Vec<Column>)> {
    let (wire, mut client) = tokio::io::duplex(1024 * 1024);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut result = StreamingResultSet {
        columns,
        found_rows: None,
        cursor: Box::new(NeverPull(counter.clone())),
    };
    let mut session = HeaderSession {
        codec: PacketCodec::plain(),
        stream: wire,
        context: SessionContext {
            resultset_metadata_full: metadata_full,
            vector_result_format_text: vector_text,
            ..Default::default()
        },
        handle: None,
        capabilities,
    };
    let server_status = SERVER_STATUS_AUTOCOMMIT;
    if shared {
        let (_encoding, _capabilities, _notify, mut batch, sequence_id) =
            begin_streaming_result!(session, result, encoding, server_status, first_sequence_id);
        assert!(try_stage_stream_packet(&mut batch, sequence_id, b"row", None)?.is_some());
        batch.flush().await?;
    } else {
        // The pre-refactor protocol sequence is the wire-format oracle.
        let encoding = encoding.with_vector_text(vector_text);
        if let std::borrow::Cow::Owned(columns) = crate::mysql::protocol::vector_text_result_columns(
            &result.columns,
            matches!(encoding, StreamRowEncoding::VectorText(_)),
        ) {
            result.columns = columns;
        }
        let mut batch = session.codec.batch_writer(&mut session.stream);
        let sequence_id = write_result_set_header_batched(
            &mut batch,
            &result.columns,
            capabilities,
            metadata_full,
            255,
            server_status,
            first_sequence_id,
        )
        .await?;
        batch.flush().await?;
        batch.begin_streaming_rows();
        batch.write_packet_seq(sequence_id, b"row").await?;
        batch.flush().await?;
    }
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    drop(session);
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await?;
    Ok((bytes, result.columns))
}

#[tokio::test]
async fn streaming_header_preserves_optional_metadata_encoding_and_sequence_rollover(
) -> io::Result<()> {
    for len in [0, 1, 260] {
        let columns = (0..len)
            .map(|i| Column {
                name: format!("汉{i}"),
                column_type: if i % 3 == 0 {
                    ColumnType::Vector
                } else {
                    ColumnType::LongLong
                },
                source: ColumnSource::default(),
            })
            .collect::<Vec<_>>();
        for capabilities in [
            CLIENT_PROTOCOL_41,
            CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF,
            CLIENT_PROTOCOL_41 | CLIENT_OPTIONAL_RESULTSET_METADATA,
            CLIENT_PROTOCOL_41 | CLIENT_OPTIONAL_RESULTSET_METADATA | CLIENT_DEPRECATE_EOF,
        ] {
            for metadata_full in [false, true] {
                for vector_text in [false, true] {
                    for encoding in [
                        StreamRowEncoding::Text(None),
                        StreamRowEncoding::Binary(None),
                    ] {
                        let (a, ac) = render_header(
                            columns.clone(),
                            capabilities,
                            metadata_full,
                            vector_text,
                            encoding,
                            253,
                            false,
                        )
                        .await?;
                        let (b, bc) = render_header(
                            columns.clone(),
                            capabilities,
                            metadata_full,
                            vector_text,
                            encoding,
                            253,
                            true,
                        )
                        .await?;
                        assert_eq!(a, b);
                        assert_eq!(ac.len(), bc.len());
                        for (x, y) in ac.iter().zip(&bc) {
                            assert_eq!(
                                (&x.name, x.column_type, &x.source),
                                (&y.name, y.column_type, &y.source)
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn streaming_header_io_failure_never_pulls_cursor() {
    let (wire, client) = tokio::io::duplex(1024);
    drop(client);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut result = StreamingResultSet {
        columns: vec![Column {
            name: "v".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }],
        found_rows: None,
        cursor: Box::new(NeverPull(counter.clone())),
    };
    let mut session = HeaderSession {
        codec: PacketCodec::plain(),
        stream: wire,
        context: SessionContext::default(),
        handle: None,
        capabilities: CLIENT_PROTOCOL_41,
    };
    let outcome: io::Result<()> = async {
        let (_encoding, _caps, _notify, _batch, _sequence) = begin_streaming_result!(
            session,
            result,
            StreamRowEncoding::Text(None),
            SERVER_STATUS_AUTOCOMMIT,
            0
        );
        Ok(())
    }
    .await;
    assert_eq!(outcome.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}
