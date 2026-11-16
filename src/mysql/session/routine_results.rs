//! Encode CALL output on its owning worker and drain bounded packet batches
//! concurrently on the connection task. No live cursor or session crosses the
//! channel; nested statements and handlers keep their execution context.
use super::*;
use crate::engine::api::{ResultCursor, VecResultCursor};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
pub(super) use crate::engine::executor::routines::result_sink::with_sink;
use crate::engine::executor::routines::result_sink::ResultSink;
use tokio::sync::mpsc;

struct PacketBatch {
    packets: Vec<(u8, Vec<u8>)>,
    next_sequence: u8,
    charge: ControlledMemoryCharge,
}

pub(super) struct PacketReceiver(mpsc::Receiver<PacketBatch>);

struct Encoder {
    sender: mpsc::Sender<PacketBatch>,
    batch: PacketBatch,
    binary: bool,
    capabilities: u32,
    bytes: usize,
}

impl Encoder {
    fn packet(&mut self, session: &SessionContext, payload: Vec<u8>) -> Result<(), QueryError> {
        session.check_cancelled()?;
        self.batch
            .charge
            .try_grow(payload.capacity() + std::mem::size_of::<(u8, Vec<u8>)>())?;
        let sequence = self.batch.next_sequence;
        let frames = payload.len() / crate::mysql::packet::MAX_PAYLOAD_LEN + 1;
        self.batch.next_sequence = sequence.wrapping_add(frames as u8);
        self.bytes = self.bytes.saturating_add(payload.len());
        self.batch.packets.push((sequence, payload));
        if self.bytes >= 64 * 1024 {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryError> {
        if self.batch.packets.is_empty() {
            return Ok(());
        }
        let next = PacketBatch {
            packets: Vec::new(),
            next_sequence: self.batch.next_sequence,
            charge: ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::ProtocolTransient,
                0,
            )?,
        };
        let batch = std::mem::replace(&mut self.batch, next);
        self.bytes = 0;
        self.sender
            .blocking_send(batch)
            .map_err(|_| crate::engine::cancel::interrupted_error())
    }

    fn response(
        &mut self,
        session: &mut SessionContext,
        response: QueryResponse,
    ) -> Result<(), QueryError> {
        let required = if self.binary {
            CLIENT_PS_MULTI_RESULTS
        } else {
            CLIENT_MULTI_RESULTS
        };
        if self.capabilities & required == 0 {
            return Err(error(
                crate::error::catalog::ER_SP_BADSELECT,
                "Stored procedure requires negotiated multi-result support",
            ));
        }
        let streaming = matches!(response, QueryResponse::StreamingResultSet(_));
        let (columns, mut cursor, found_rows): (_, Box<dyn ResultCursor>, _) = match response {
            QueryResponse::ResultSet(result) => (
                result.columns,
                Box::new(VecResultCursor::new(result.rows)),
                None,
            ),
            QueryResponse::SharedResultSet(result) => (
                result.columns.to_vec(),
                Box::new(VecResultCursor::new(result.rows)),
                None,
            ),
            QueryResponse::StreamingResultSet(result) => {
                (result.columns, result.cursor, result.found_rows)
            }
            _ => unreachable!("routine sink only receives result sets"),
        };
        let charset =
            crate::charset::resolve_result_charset(session.character_set_results.as_deref());
        let encoding = if self.binary {
            StreamRowEncoding::Binary(charset)
        } else {
            StreamRowEncoding::Text(charset).with_vector_text(session.vector_result_format_text)
        };
        let columns = crate::mysql::protocol::vector_text_result_columns(
            &columns,
            matches!(encoding, StreamRowEncoding::VectorText(_)),
        );
        let status = server_status_for_context(session) | SERVER_MORE_RESULTS_EXISTS;
        let send_metadata = self.capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA == 0
            || session.resultset_metadata_full;
        let mut count = Vec::new();
        if self.capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0 {
            count.push(u8::from(send_metadata));
        }
        write_lenenc_int(&mut count, columns.len() as u64);
        self.packet(session, count)?;
        let fallback = session
            .character_set_results
            .as_deref()
            .and_then(|charset| crate::charset::default_collation_for_charset(charset).ok())
            .and_then(|name| crate::charset::resolve_collation(name).ok())
            .map_or(0, |collation| collation.id);
        if send_metadata {
            for column in columns.iter() {
                self.packet(
                    session,
                    column_definition_packet_full_with_fallback(
                        &column.name,
                        column.column_type,
                        &column.source,
                        fallback,
                    ),
                )?;
            }
            if self.capabilities & CLIENT_DEPRECATE_EOF == 0 {
                self.packet(session, eof_packet_with_status(status).to_vec())?;
            }
        }
        // Flush before pulling a lazy cursor; materialized small results can
        // send metadata, rows and EOF together without an extra wake/syscall.
        if streaming {
            self.flush()?;
        }
        let zerofill = if matches!(encoding, StreamRowEncoding::Binary(_)) {
            None
        } else {
            crate::mysql::protocol::zerofill_pad_spec(&columns)
        };
        let mut sent = 0u64;
        let result = (|| {
            while let Some(row) = cursor.next_row()? {
                session.check_cancelled()?;
                let mut payload = Vec::new();
                encode_stream_row(&row, &columns, zerofill.as_deref(), encoding, &mut payload);
                self.packet(session, payload)?;
                sent = sent.saturating_add(1);
            }
            session.check_cancelled()
        })();
        crate::engine::api::append_statement_warnings(cursor.take_warnings());
        if let Err(err) = result {
            self.flush()?;
            return Err(err);
        }
        session.found_rows = found_rows.unwrap_or(sent);
        let warnings = crate::engine::api::statement_warning_count().min(u16::MAX as usize) as u16;
        let end = if self.capabilities & CLIENT_DEPRECATE_EOF != 0 {
            eof_as_ok_packet_with_status_warnings(status, warnings)
        } else {
            eof_packet_with_status_warnings(status, warnings).to_vec()
        };
        self.packet(session, end)?;
        self.flush()
    }
}

fn may_emit_routine_results(sql: &str, mode: crate::sql_lex::SqlLexMode) -> bool {
    if let Some(word) = simple_first_keyword(sql) {
        return word.eq_ignore_ascii_case(b"CALL") || word.eq_ignore_ascii_case(b"EXECUTE");
    }
    crate::sql_lex::leading_keyword_ranges(sql, mode, crate::parser::MYSQL_VERSION_ID, 1)
        .ok()
        .and_then(|words| words.first().cloned())
        .is_some_and(|range| {
            sql[range.clone()].eq_ignore_ascii_case("CALL")
                || sql[range].eq_ignore_ascii_case("EXECUTE")
        })
}

impl<E: QueryEngine> Session<E> {
    pub(super) fn routine_packet_stream(
        &self,
        sql: &str,
        binary: bool,
    ) -> (Option<ResultSink>, Option<PacketReceiver>) {
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(
            crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode),
        );
        if !may_emit_routine_results(sql, mode) {
            return (None, None);
        }
        let (sender, receiver) = mpsc::channel(2);
        let mut encoder = Encoder {
            sender,
            binary,
            capabilities: self.client_capabilities(),
            bytes: 0,
            batch: PacketBatch {
                packets: Vec::new(),
                next_sequence: self
                    .text_batch
                    .map_or(self.command_response_sequence, |p| p.sequence_id),
                charge: ControlledMemoryCharge::try_new(
                    ControlledMemoryCategory::ProtocolTransient,
                    0,
                )
                .expect("zero-byte protocol reservation cannot fail"),
            },
        };
        (
            Some(Box::new(move |session, response| {
                encoder.response(session, response)
            })),
            Some(PacketReceiver(receiver)),
        )
    }

    async fn write_routine_packet_batch(&mut self, batch: &PacketBatch) -> io::Result<()> {
        let handle = self.handle.as_ref().map(Arc::clone);
        let write = async {
            let mut writer = self.codec.batch_writer(&mut self.stream);
            for (sequence, payload) in &batch.packets {
                writer.write_packet(*sequence, payload).await?;
            }
            writer.finish().await
        };
        tokio::pin!(write);
        if let Some(handle) = handle {
            loop {
                if handle.kill_flag().load(Ordering::Acquire) {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "connection killed during routine result write",
                    ));
                }
                tokio::select! {
                    biased;
                    result = &mut write => return result,
                    _ = handle.notified() => {},
                }
            }
        } else {
            write.await
        }
    }

    pub(super) async fn finish_routine_job<T>(
        &mut self,
        job: crate::util::blocking_pool::BlockingHandle<T>,
        receiver: Option<PacketReceiver>,
    ) -> Result<T, crate::util::blocking_pool::BlockingError> {
        if let Some(PacketReceiver(mut receiver)) = receiver {
            // Earlier statements in COM_QUERY may have staged OK packets.
            let mut write_failed = self.flush_text_batch_packets().await.is_err();
            while !write_failed {
                let Some(batch) = receiver.recv().await else {
                    break;
                };
                write_failed = self.write_routine_packet_batch(&batch).await.is_err();
                if !write_failed {
                    if let Some(progress) = self.text_batch.as_mut() {
                        progress.sequence_id = batch.next_sequence;
                    } else {
                        self.routine_response_sequence = Some(batch.next_sequence);
                    }
                }
            }
            if write_failed {
                if let Some(handle) = &self.handle {
                    handle.peer_disconnected();
                }
                self.kill_flag.store(true, Ordering::Release);
            }
            // Unblock the producer on transport failure, then recover its real
            // session before ordinary teardown can roll back and release MDL.
            drop(receiver);
        }
        job.await
    }
}
