use super::*;
use crate::util::catalog_admission::CatalogAdmission;
use std::sync::{mpsc, Mutex};
use std::time::Duration;

/// Model statistics publication with a bounded writer so the broken inline
/// route fails without leaving the test reactor permanently deadlocked.
struct PublishingEngine {
    admission: Arc<CatalogAdmission>,
    writers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    executor: Mutex<Option<std::thread::ThreadId>>,
}
impl QueryEngine for PublishingEngine {
    fn catalog_admission(&self) -> Option<&CatalogAdmission> {
        Some(&self.admission)
    }
    fn execute(&self, _: &mut SessionContext, _: &str) -> Result<QueryResponse, QueryError> {
        Ok(QueryResponse::Ok)
    }
    fn execute_prepared_statement(
        &self,
        _: &mut SessionContext,
        _: &str,
        statement: Statement,
    ) -> Option<Result<QueryResponse, QueryError>> {
        *self.executor.lock().unwrap() = Some(std::thread::current().id());
        if !matches!(statement, Statement::Explain { .. }) {
            return Some(Ok(QueryResponse::Ok));
        }
        let admission = self.admission.clone();
        let (sent, received) = mpsc::sync_channel(1);
        self.writers
            .lock()
            .unwrap()
            .push(std::thread::spawn(move || {
                let _publication = admission.write();
                let _ = sent.send(());
            }));
        Some(
            received
                .recv_timeout(Duration::from_secs(1))
                .map(|_| QueryResponse::Ok)
                .map_err(|_| {
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT
                        .error("statistics publication blocked by its own catalog read admission")
                }),
        )
    }
}

#[tokio::test(flavor = "current_thread")]
async fn prepared_explain_releases_catalog_read_admission_before_statistics_publication() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    let _client = client.unwrap();
    let (stream, peer) = accepted.unwrap();
    let engine = Arc::new(PublishingEngine {
        admission: Arc::new(CatalogAdmission::default()),
        writers: Mutex::new(Vec::new()),
        executor: Mutex::new(None),
    });
    let mut session = Session::new(
        1,
        peer,
        stream,
        None,
        engine.clone(),
        Arc::new(SessionRegistry::new()),
    );
    let reactor = std::thread::current().id();
    for sql in [
        "EXPLAIN SELECT id FROM t WHERE id>?",
        "EXPLAIN FORMAT=TREE SELECT id FROM t WHERE id>?",
        "EXPLAIN FORMAT=JSON SELECT id FROM t WHERE id>?",
        "EXPLAIN ANALYZE SELECT id FROM t WHERE id>?",
        "EXPLAIN SELECT id FROM t ORDER BY id+?",
        "EXPLAIN WITH q AS (SELECT id FROM t WHERE id>?) SELECT * FROM q",
        "EXPLAIN SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE id>?)",
        "EXPLAIN INSERT INTO t(id) VALUES(?)",
        "EXPLAIN UPDATE t SET id=? WHERE id=1",
        "EXPLAIN DELETE FROM t WHERE id>?",
    ] {
        let mut statement = parse_statement(sql).unwrap();
        crate::parser::number_statement_placeholders(&mut statement, &mut 0);
        let result = session
            .execute_prepared_statement_blocking(
                Arc::from(sql),
                statement,
                vec![crate::types::Value::Int(0)],
                None,
                crate::engine::perf_events::prepared_statement_texts(sql),
                false,
            )
            .await
            .unwrap();
        for writer in engine.writers.lock().unwrap().drain(..) {
            writer.join().unwrap();
        }
        assert!(result.is_ok(), "{sql}: {result:?}");
        assert_ne!(
            *engine.executor.lock().unwrap(),
            Some(reactor),
            "{sql} ran on the reactor"
        );
        assert!(
            session.txn_admission.is_none(),
            "read-only EXPLAIN acquired transaction admission"
        );
    }
    // Keep the latency-sensitive fallback for ordinary reads on the reactor.
    for sql in ["SELECT 1", "SELECT id FROM t WHERE id=1", "SHOW TABLES"] {
        let result = session
            .execute_prepared_statement_blocking(
                Arc::from(sql),
                parse_statement(sql).unwrap(),
                Vec::new(),
                None,
                crate::engine::perf_events::prepared_statement_texts(sql),
                false,
            )
            .await
            .unwrap();
        assert!(result.is_ok());
        assert_eq!(
            *engine.executor.lock().unwrap(),
            Some(reactor),
            "{sql} left the read fast path"
        );
    }
}
