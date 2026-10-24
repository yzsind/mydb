use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::thread;

use chrono::Local;

pub fn log_message(message: impl fmt::Display) {
    let line = format_log_line(message);
    enqueue_or_write(line);
}

const LOG_QUEUE_CAPACITY: usize = 16_384;

fn log_sender() -> &'static SyncSender<String> {
    static SENDER: OnceLock<SyncSender<String>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<String>(LOG_QUEUE_CAPACITY);
        let builder = thread::Builder::new().name("mydb-log-writer".to_string());
        builder
            .spawn(move || {
                for line in rx {
                    write_line(&line);
                }
            })
            .expect("failed to spawn mydb log writer");
        tx
    })
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
        // direct synchronous write so a truly rare failure mode still
        // leaves diagnostics on stderr.
        Err(TrySendError::Disconnected(line)) => {
            write_line(&line);
        }
    }
}

static SLOW_LOG_DROPPED: AtomicU64 = AtomicU64::new(0);
static OTHER_LOG_DROPPED: AtomicU64 = AtomicU64::new(0);

fn write_line(line: &str) {
    let slow = SLOW_LOG_DROPPED.swap(0, Ordering::Relaxed);
    let other = OTHER_LOG_DROPPED.swap(0, Ordering::Relaxed);
    if slow > 0 || other > 0 {
        let summary = format_log_line(format_args!(
            "[log] suppressed slow-sql={slow} other={other} lines while stderr/error-log writer was saturated"
        ));
        write_line_inner(&summary);
    }
    write_line_inner(line);
}

fn write_line_inner(line: &str) {
    eprintln!("{line}");
    crate::engine::logging::record_error_log_line("Note", "Server", line);
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
}
