use crate::util::thread_spawn as thread;
use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::OnceLock;

use chrono::Local;

pub fn log_message(message: impl fmt::Display) {
    let line = format_log_line(message);
    enqueue_or_write(line);
}

const LOG_QUEUE_CAPACITY: usize = 16_384;
const STDERR_QUEUE_CAPACITY: usize = 1_024;

fn log_sender() -> &'static SyncSender<String> {
    static SENDER: OnceLock<SyncSender<String>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<String>(LOG_QUEUE_CAPACITY);
        let builder = thread::Builder::new().name("mydb-log-writer".to_string());
        builder
            .spawn(move || {
                for line in rx {
                    write_line(line);
                }
            })
            .expect("failed to spawn mydb log writer");
        tx
    })
}

fn stderr_sender() -> Option<&'static SyncSender<String>> {
    static SENDER: OnceLock<Option<SyncSender<String>>> = OnceLock::new();
    SENDER
        .get_or_init(|| {
            let (tx, rx) = mpsc::sync_channel::<String>(STDERR_QUEUE_CAPACITY);
            thread::Builder::new()
                .name("mydb-stderr-writer".to_string())
                .spawn(move || {
                    let stderr = std::io::stderr();
                    for line in rx {
                        // Neither a blocked sink nor a broken pipe may stop the
                        // file writer. Release the stderr lock between lines.
                        let _ = writeln!(stderr.lock(), "{line}");
                    }
                })
                .ok()
                .map(|_| tx)
        })
        .as_ref()
}

fn enqueue_or_write(line: String) {
    match log_sender().try_send(line) {
        Ok(()) => {}
        // Channel full: drop the line (record the loss) rather than
        // fall back to a synchronous `eprintln!` on the caller's thread.
        // The caller is often a tokio worker in the middle of a request
        // (slow-sql / general-log / warning), and stderr writes can
        // block on the terminal/pipe backpressure for hundreds of µs to
        // tens of ms — profiling sysbench oltp_update_index saw this
        // path stealing ~7.7% of wallclock. Losing a log line is far
        // preferable to stalling the write hot path; the dropped
        // counter emits a single "N lines suppressed" summary the next
        // time the writer catches up.
        Err(TrySendError::Full(line)) => {
            let bucket = if line.contains("[slow-sql]") {
                &SLOW_LOG_DROPPED
            } else {
                &OTHER_LOG_DROPPED
            };
            bucket.fetch_add(1, Ordering::Relaxed);
        }
        // Disconnected only if the writer thread crashed/exited. This is
        // effectively unreachable in a running server (the writer is
        // static-lifetime), but stay conservative and fall back to a
        // direct file write. The terminal copy still uses its own queue.
        Err(TrySendError::Disconnected(line)) => {
            write_line(line);
        }
    }
}

static SLOW_LOG_DROPPED: AtomicU64 = AtomicU64::new(0);
static OTHER_LOG_DROPPED: AtomicU64 = AtomicU64::new(0);
static STDERR_LOG_DROPPED: AtomicU64 = AtomicU64::new(0);

fn write_line(line: String) {
    let slow = SLOW_LOG_DROPPED.swap(0, Ordering::Relaxed);
    let other = OTHER_LOG_DROPPED.swap(0, Ordering::Relaxed);
    if slow > 0 || other > 0 {
        let summary = format_log_line(format_args!(
            "[log] suppressed slow-sql={slow} other={other} lines while error-log writer was saturated"
        ));
        write_line_inner(summary);
    }
    write_line_inner(line);
}

fn write_line_inner(line: String) {
    crate::engine::logging::record_error_log_line("Note", "Server", &line);
    // Transfer ownership after recording the file/event copy. No string clone
    // or blocking terminal operation is needed on the file writer's thread.
    let result = match stderr_sender() {
        Some(sender) => sender.try_send(line),
        None => Err(TrySendError::Disconnected(line)),
    };
    match result {
        Ok(()) => {
            let dropped = STDERR_LOG_DROPPED.swap(0, Ordering::Relaxed);
            if dropped > 0 {
                let summary = format_log_line(format_args!(
                    "[log] suppressed stderr={dropped} lines while stderr writer was saturated or unavailable; error-log file output continued"
                ));
                crate::engine::logging::record_error_log_line("Note", "Server", &summary);
            }
        }
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            if STDERR_LOG_DROPPED.fetch_add(1, Ordering::Relaxed) == 0 {
                let summary = format_log_line(
                    "[log] stderr writer is saturated or unavailable; suppressing terminal copies while error-log file output continues",
                );
                // Report the start of suppression even if stderr never recovers.
                // Do not enqueue this notice back onto the saturated sink.
                crate::engine::logging::record_error_log_line("Note", "Server", &summary);
            }
        }
    }
}

fn format_log_line(message: impl fmt::Display) -> String {
    format_log_line_with_timestamp(&current_timestamp(), message)
}

fn current_timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn format_log_line_with_timestamp(timestamp: &str, message: impl fmt::Display) -> String {
    format!("[{timestamp}] {message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_line_prefixes_timestamp() {
        let line = format_log_line_with_timestamp(
            "2026-05-27 12:34:56.789",
            format_args!("[slow-sql] conn=42"),
        );

        assert_eq!(line, "[2026-05-27 12:34:56.789] [slow-sql] conn=42");
    }

    #[cfg(unix)]
    fn error_log_with_unavailable_stderr(blocked: bool) -> String {
        use std::io::Write;
        use std::os::fd::OwnedFd;
        use std::os::unix::net::UnixStream;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let path = dir.path().join("error.log");
        let (reader, mut writer) = UnixStream::pair().unwrap();
        if blocked {
            writer.set_nonblocking(true).unwrap();
            loop {
                match writer.write(&[b'x'; 8192]) {
                    Ok(0) => panic!("stderr socket unexpectedly closed"),
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => panic!("failed to fill stderr socket: {err}"),
                }
            }
            writer.set_nonblocking(false).unwrap();
        } else {
            drop(reader);
        }
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "util::logging::tests::unavailable_stderr_child",
                    "--nocapture",
                ])
                .env("MYDB_LOGGING_TEST_DIR", dir.path())
                .env("MYDB_LOG_ERROR", &path)
                .env("MYDB_GENERAL_LOG", "off")
                .env("MYDB_SLOW_QUERY_LOG", "off")
                .env("MYDB_LOG_OUTPUT", "NONE")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(OwnedFd::from(writer)))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let contents = std::fs::read_to_string(&path).unwrap_or_default();
            if contents.contains("[logging-test] sentinel") {
                assert_eq!(contents.matches("[logging-test] line=").count(), 2048);
                return contents;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "logging subprocess exited before recording the sentinel"
            );
            assert!(
                Instant::now() < deadline,
                "error-log file stalled with {} stderr after recording {} lines",
                if blocked { "blocked" } else { "disconnected" },
                contents.matches("[logging-test] line=").count()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(unix)]
    #[test]
    fn blocked_stderr_does_not_block_error_log_file() {
        let contents = error_log_with_unavailable_stderr(true);
        assert!(contents.contains("suppressing terminal copies"));
    }

    #[cfg(unix)]
    #[test]
    fn disconnected_stderr_does_not_block_error_log_file() {
        error_log_with_unavailable_stderr(false);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess fixture with its own stderr and logging service"]
    fn unavailable_stderr_child() {
        let dir = std::env::var_os("MYDB_LOGGING_TEST_DIR").unwrap();
        let _service = crate::engine::logging::SqlLogService::open(
            std::path::PathBuf::from(dir),
            &crate::engine::logging::QueryLogConfig::default(),
        )
        .unwrap();
        for i in 0..2048 {
            log_message(format_args!("[logging-test] line={i}"));
        }
        log_message("[logging-test] sentinel");
        loop {
            thread::park();
        }
    }
}
