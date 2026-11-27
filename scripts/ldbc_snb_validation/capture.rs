#[cfg(not(native_snapshot))]
mod file_export;
#[cfg(native_snapshot)]
mod native_snapshot;

use mydb::engine::{QueryResponse, SessionContext, StorageQueryEngine};
use mydb::storage::{Database, IsolationLevel, RowId, TableSchema, TxnView};
use mydb::types::{QueryError, Value};
use mydb::wal::{decode_frame_for_replication, WalPayload, WalReader};
use serde_json::{json, Value as Json};
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn number(value: &Value) -> u64 {
    match value {
        Value::UInt(n) => *n,
        Value::Int(n) => u64::try_from(*n).expect("unsigned graph key"),
        _ => panic!("invalid graph key {value:?}"),
    }
}

fn token(value: &Value) -> &str {
    match value {
        Value::Bytes(data) => std::str::from_utf8(data).unwrap(),
        _ => panic!("invalid graph label"),
    }
}

fn write_io(error: io::Error) -> QueryError {
    mydb::error::catalog::ER_GET_ERRNO.error(format!("graph export: {error}"))
}

const MAX_ELEMENT_ID: u64 = 100_000_000;
const GRAPH_TABLE: &str = "__mydb_graph_snb";

fn element_id(value: &Value) -> io::Result<u32> {
    let id = number(value);
    if id > MAX_ELEMENT_ID {
        return Err(io::Error::other("element ID exceeds the validation budget"));
    }
    Ok(id as u32)
}

fn scan_shards<S: Send>(
    db: &Arc<Database>,
    table: &TableSchema,
    scan: impl Fn(TxnView, usize, u64, u64) -> io::Result<S> + Sync,
) -> Result<(u64, Vec<S>)> {
    let view = db.read_only_statement_view(IsolationLevel::RepeatableRead, None);
    let _snapshot = db.register_statement_snapshot(view.snapshot_scn);
    let pages = db.heap_page_count_by_name(&format!("test.{GRAPH_TABLE}"))?;
    if table.is_partitioned() || pages == 0 {
        return Err("expected a nonempty unpartitioned graph heap".into());
    }
    let workers = thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8)
        .min(pages as usize);
    let shard_count = (workers * 4).min(pages as usize);
    let next = AtomicUsize::new(0);
    let mut shards = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut output = Vec::new();
                    loop {
                        let shard = next.fetch_add(1, Ordering::Relaxed);
                        if shard >= shard_count {
                            return Ok(output);
                        }
                        let lo = shard as u64 * pages / shard_count as u64;
                        let hi = (shard as u64 + 1) * pages / shard_count as u64;
                        output.push((shard, scan(view, shard, lo, hi)?));
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| io::Error::other("snapshot worker panicked"))?
            })
            .collect::<io::Result<Vec<_>>>()
    })?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    shards.sort_unstable_by_key(|(id, _)| *id);
    Ok((
        view.snapshot_scn,
        shards.into_iter().map(|(_, shard)| shard).collect(),
    ))
}

fn publish_snapshot(out: &Path, meta: &Json) -> io::Result<()> {
    let pending = out.join("snapshot.pending");
    serde_json::to_writer(File::create(&pending)?, meta)?;
    fs::rename(pending, out.join("snapshot.json"))?;
    eprintln!("snapshot: {meta}");
    Ok(())
}

fn read_write(
    payload: WalPayload,
    txn: u64,
    writes: &mut Vec<(String, RowId, u64)>,
    commits: &mut Vec<(u64, u64)>,
    incompatible: &mut Vec<(u64, Option<String>)>,
) {
    match payload {
        WalPayload::HeapInsert {
            path,
            page_id,
            slot_id,
            ..
        } => writes.push((path, RowId { page_id, slot_id }, txn)),
        WalPayload::HeapInsertBatch { path, inserts } => {
            writes.extend(inserts.into_iter().map(|r| {
                (
                    path.clone(),
                    RowId {
                        page_id: r.page_id,
                        slot_id: r.slot_id,
                    },
                    txn,
                )
            }))
        }
        WalPayload::TxnBatch { records } => {
            for record in records {
                read_write(record, txn, writes, commits, incompatible);
            }
        }
        WalPayload::CommitTxn { commit_scn, .. } => commits.push((txn, commit_scn)),
        WalPayload::HeapDelete { path, .. }
        | WalPayload::HeapUpdate { path, .. }
        | WalPayload::HeapUpdateLogical { path, .. } => incompatible.push((txn, Some(path))),
        WalPayload::RollbackTo { .. } | WalPayload::AbortTxn => incompatible.push((txn, None)),
        _ => (),
    }
}

fn graph_heap(path: &str, table_id: &str) -> bool {
    let p = Path::new(path);
    p.parent()
        .and_then(Path::file_name)
        .and_then(|x| x.to_str())
        == Some(table_id)
        && p.extension().and_then(|s| s.to_str()) == Some("heap")
}

fn delta(
    db: &Arc<Database>,
    schema: &TableSchema,
    reader: &mut WalReader,
    durable_lsn: u64,
    last_scn: u64,
) -> Result<(u64, Vec<Json>)> {
    let mut writes = Vec::new();
    let mut commits = Vec::new();
    let mut incompatible = Vec::new();
    let graph_dir = schema.id.to_string();
    // WalReader emits at least one complete frame. A one-frame read avoids
    // probing the unwritten segment tail and losing our current offset.
    while let Some(chunk) = reader
        .next_durable_chunk(durable_lsn, 1)
        .map_err(|e| format!("read durable WAL: {e:?}"))?
    {
        let mut bytes = chunk.bytes.as_slice();
        while !bytes.is_empty() {
            let frame = decode_frame_for_replication(bytes)?;
            bytes = &bytes[frame.frame_len..];
            read_write(
                frame.payload,
                frame.header.txn_id,
                &mut writes,
                &mut commits,
                &mut incompatible,
            );
        }
    }
    let writers: HashSet<_> = writes
        .iter()
        .filter(|(p, _, _)| graph_heap(p, &graph_dir))
        .map(|(_, _, tx)| *tx)
        .collect();
    if writers.len() != 1 {
        return Err(format!("unexpected graph writers: {writers:?}").into());
    }
    commits.retain(|(tx, _)| writers.contains(tx));
    let [(txn, scn)] = commits.as_slice() else {
        return Err(format!("expected one graph WAL commit, got {commits:?}").into());
    };
    if incompatible.iter().any(|(writer, path)| {
        writer == txn || path.as_deref().is_some_and(|p| graph_heap(p, &graph_dir))
    }) {
        return Err(format!("unexpected in-place change or abort associated with graph transaction {txn}: {incompatible:?}").into());
    }
    if *scn <= last_scn {
        return Err(format!("invalid append transaction {txn} at SCN {scn}").into());
    }
    let current = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    if current.snapshot_scn < *scn {
        return Err(format!("commit SCN {scn} has not become visible").into());
    }
    let _snapshot = db.register_statement_snapshot(*scn);
    let view = TxnView::read_only(*scn, IsolationLevel::RepeatableRead);
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let mut paths = BTreeSet::new();
    for (path, row_id, writer) in writes {
        if writer != *txn {
            continue;
        }
        if !graph_heap(&path, &graph_dir) || !seen.insert(row_id) {
            return Err(
                format!("unexpected WAL heap insert {path} {row_id:?} from {writer}").into(),
            );
        }
        paths.insert(path);
        let r = db
            .read_row(schema, row_id, view)?
            .ok_or_else(|| format!("committed heap row missing {row_id:?}"))?;
        let v = &r.values;
        let id = number(&v[0]);
        rows.push(match number(&v[1]) {
            0 => json!([
                "node",
                id,
                serde_json::from_str::<Json>(&v[7].to_display_string())?
            ]),
            role @ (1 | 2) => json!([
                "edge",
                id,
                role,
                number(&v[2]),
                number(&v[3]),
                serde_json::from_str::<Json>(&v[7].to_display_string())?
            ]),
            role @ (3 | 4) => json!(["label", role, id, number(&v[2]), token(&v[5])]),
            6 => json!(["index", id]),
            role => return Err(format!("unexpected graph row role {role}").into()),
        });
    }
    if paths.len() != 1 {
        return Err("unexpected multiple graph heap generations".into());
    }
    Ok((*scn, rows))
}

fn open_graph(data: &Path) -> Result<(StorageQueryEngine, Arc<TableSchema>)> {
    if !data.starts_with("/tmp/mydb") && !data.starts_with("/private/tmp/mydb") {
        return Err("validation requires a scratch data directory".into());
    }
    let engine = StorageQueryEngine::open(data)?;
    let db = engine.database_for_replication();
    let schema = db.table_in_database("test", GRAPH_TABLE)?;
    if schema.table_type != "GRAPH STORAGE" {
        return Err("expected an internal graph relation".into());
    }
    Ok((engine, schema))
}

fn capture_updates(
    engine: StorageQueryEngine,
    schema: Arc<TableSchema>,
    out: &Path,
    writes: &Path,
    mut last_scn: u64,
    start: Instant,
) -> Result<()> {
    let exported = start.elapsed();
    let db = engine.database_for_replication();
    let wal = db.wal_log();
    let next_lsn = wal.lock().next_lsn();
    let mut reader =
        WalReader::open(&wal, next_lsn).map_err(|e| format!("open WAL reader: {e:?}"))?;
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "SESSION SET GRAPH snb")?;
    let mut ledger = BufWriter::new(File::create(out.join("commits.jsonl"))?);
    let mut count = 0;
    let mut statement_seconds = 0.0;
    let mut commit_seconds = 0.0;
    let mut delta_seconds = 0.0;
    for line in BufRead::lines(io::BufReader::new(File::open(writes)?)) {
        let line: Json = serde_json::from_str(&line?)?;
        let statement_started = Instant::now();
        engine.execute_with_session(&mut session, "START TRANSACTION")?;
        for statement in line["statements"].as_array().ok_or("missing statements")? {
            let text = statement[0].as_str().ok_or("missing SQL")?;
            match engine.execute_with_session(&mut session, text)? {
                QueryResponse::OkAffected { affected_rows }
                    if Some(affected_rows) == statement[1].as_u64() =>
                {
                    ()
                }
                _ => {
                    return Err(
                        format!("unexpected affected rows at {}: {text}", line["ordinal"]).into(),
                    )
                }
            }
        }
        statement_seconds += statement_started.elapsed().as_secs_f64();
        let commit_started = Instant::now();
        engine.execute_with_session(&mut session, "COMMIT")?;
        commit_seconds += commit_started.elapsed().as_secs_f64();
        let delta_started = Instant::now();
        let durable_lsn = wal.lock().durable_lsn();
        let (scn, rows) = delta(db, &schema, &mut reader, durable_lsn, last_scn)
            .map_err(|e| format!("ordinal {}: {e}", line["ordinal"]))?;
        last_scn = scn;
        serde_json::to_writer(
            &mut ledger,
            &json!({"ordinal":line["ordinal"],"scn":scn,"durable_lsn":durable_lsn,"rows":rows}),
        )?;
        writeln!(ledger)?;
        count += 1;
        if count % 64 == 0 {
            ledger.flush()?;
        }
        delta_seconds += delta_started.elapsed().as_secs_f64();
        if count % 1000 == 0 {
            eprintln!("captured {count} commits");
        }
    }
    ledger.flush()?;
    let committed = start.elapsed();
    engine.shutdown()?;
    serde_json::to_writer(
        File::create(out.join("capture.json"))?,
        &json!({"open_export_seconds":exported.as_secs_f64(),"write_capture_seconds":(committed-exported).as_secs_f64(),"total_seconds":start.elapsed().as_secs_f64(),"commits":count,
                 "statement_seconds":statement_seconds,"commit_seconds":commit_seconds,"delta_seconds":delta_seconds}),
    )?;
    Ok(())
}

#[cfg(not(native_snapshot))]
fn run(data: &Path, out: &Path, writes: &Path) -> Result<()> {
    let start = Instant::now();
    fs::create_dir(out)?;
    let (engine, schema) = open_graph(data)?;
    let scn = file_export::export(engine.database_for_replication(), &schema, out)?;
    capture_updates(engine, schema, out, writes, scn, start)
}

#[cfg(not(native_snapshot))]
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 4 {
        return Err("usage: capture DATA_DIR FRESH_OUTPUT_DIR WRITES_JSONL".into());
    }
    run(
        Path::new(&args[1]),
        Path::new(&args[2]),
        Path::new(&args[3]),
    )
}
