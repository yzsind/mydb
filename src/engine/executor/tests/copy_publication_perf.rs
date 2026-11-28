use super::support::query_rows;
use super::*;

#[test]
#[ignore = "local paired COPY publication lock, WAL and sync regression probe"]
fn copy_publication_lock_probe() {
    for (shape, count, bytes, vector) in [
        ("scalar", 1000, 64, false),
        ("scalar", 10000, 64, false),
        ("scalar", 100000, 64, false),
        ("wide", 1000, 1024, false),
        ("wide", 10000, 1024, false),
        ("lob", 100, 20000, false),
        ("lob", 1000, 20000, false),
        ("vector", 1000, 128, true),
        ("vector", 10000, 128, true),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,n INT,b LONGTEXT,v VECTOR(8) NOT NULL,INDEX ni(n){})",if vector {",INDEX vi(v) USING HNSW WITH(m=8,ef_construction=64)"}else{""})).unwrap();
        insert_fixture_rows(
            &engine,
            "test.t",
            (1..=count).map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id),
                    Value::String(
                        (0..bytes)
                            .map(|j| {
                                char::from(b'a' + ((id as usize * 17 + j * 13 + j / 31) % 26) as u8)
                            })
                            .collect(),
                    ),
                    Value::Vector(
                        crate::engine::vector::codec::parse_text(&format!(
                            "[{},{},{},{},{},{},{},{}]",
                            id % 97,
                            id % 89,
                            id % 83,
                            id % 79,
                            id % 73,
                            id % 71,
                            id % 67,
                            id % 61
                        ))
                        .unwrap(),
                    ),
                ]
            }),
        );
        engine.checkpoint().unwrap();
        let before = engine.database().storage_status();
        let copy = engine.database().ddl_copy_perf_snapshot();
        let start = Instant::now();
        engine
            .execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE")
            .unwrap();
        let elapsed = start.elapsed().as_micros();
        let after = engine.database().storage_status();
        let finish = engine.database().ddl_copy_perf_snapshot();
        eprintln!("COPY_PUBLICATION_PROBE shape={shape} rows={count} bytes_per_row={bytes} elapsed_us={elapsed} hold_us={} wal_bytes={} wal_syncs={} tail_rows={}",finish[22]-copy[22],after.wal_bytes.saturating_sub(before.wal_bytes),after.wal_syncs-before.wal_syncs,finish[24]-copy[24]);
        assert_eq!(
            query_rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(count)]]
        );
    }
}

#[test]
#[ignore = "local actual writer latency across COPY publication"]
fn copy_publication_writer_probe() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,n INT,b VARCHAR(2000),INDEX ni(n))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (1..=10000).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id),
                Value::String("quartz".repeat(128)),
            ]
        }),
    );
    engine.checkpoint().unwrap();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let go = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker = engine.clone();
    let stop = done.clone();
    let active = go.clone();
    let (send, recv) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 33701;
        s.sysvars.lock_wait_timeout = Some(1);
        let mut samples = Vec::new();
        let mut commits = 0;
        let mut errors = 0;
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            let started = Instant::now();
            let result = worker.execute_with_session(&mut s, "UPDATE t SET n=n+1 WHERE id=1");
            let elapsed = started.elapsed().as_micros() as u64;
            samples.push(elapsed);
            match result {
                Ok(_) => {
                    if active.load(std::sync::atomic::Ordering::Acquire) {
                        commits += 1
                    }
                }
                Err(err) => {
                    assert_eq!(err.code(), 1205);
                    errors += 1
                }
            };
            let _ = send.send(());
            std::thread::sleep(Duration::from_millis(1));
        }
        (samples, commits, errors)
    });
    recv.recv_timeout(Duration::from_secs(10)).unwrap();
    go.store(true, std::sync::atomic::Ordering::Release);
    let before = engine.database().ddl_copy_perf_snapshot();
    let start = Instant::now();
    let result = engine.execute("ALTER TABLE t MODIFY n BIGINT,LOCK=NONE");
    let elapsed = start.elapsed().as_micros();
    go.store(false, std::sync::atomic::Ordering::Release);
    done.store(true, std::sync::atomic::Ordering::Release);
    let (mut samples, commits, errors) = writer.join().unwrap();
    result.unwrap();
    samples.sort_unstable();
    eprintln!("COPY_WRITER_PROBE elapsed_us={elapsed} hold_us={} commits={commits} errors={errors} writer_p95_us={} writer_max_us={}",engine.database().ddl_copy_perf_snapshot()[22]-before[22],samples[samples.len()*95/100],samples.last().unwrap());
}
