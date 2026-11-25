//! The forwarder thread: the one thread that hands core records to the sink.
//!
//! - It takes up to [`FORWARD_BATCH`] records at a time and hands the batch
//!   to the sink in force: one stderr line per record, or the JavaScript sink
//!   (`js_sink`). A batch that the JavaScript sink does not take goes to
//!   stderr.
//! - It counts every record it handles, so that an abort can wait for the
//!   channel to empty.
//! - After a batch it reports the records that a full channel lost, as one
//!   WARNING record of the bridge logger.
//! - A panic in the forwarder is a panic in a core background task: the
//!   process panic rule applies. The batch and the records still in the
//!   channel go to stderr first.
//! - It ends only when the channel closes. The sender lives in a static, so
//!   it runs for the life of the process.

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;

use super::bridge::{CoreLogRecord, DropCounts, LogSink, LOG_CHANNEL_CAPACITY};
use super::js_sink::{self, Handed};
use super::levels::RecordLevel;
use crate::process_core::panic_rule::{background_task_panicked, take_panic_cause, PanicReport};

/// Most records handed to the sink at a time.
pub const FORWARD_BATCH: usize = 256;

/// Name of the forwarder thread.
pub const FORWARDER_THREAD_NAME: &str = "horsies-log-forwarder";

/// Start the forwarder thread. The thread is detached: it runs for the life
/// of the process.
pub fn start_forwarder(
    records: Receiver<CoreLogRecord>,
    sink: &Arc<LogSink>,
) -> std::io::Result<()> {
    sink.mark_forwarder_running();
    let thread_sink = Arc::clone(sink);
    let spawned = std::thread::Builder::new()
        .name(FORWARDER_THREAD_NAME.to_owned())
        .spawn(move || {
            run(&records, &thread_sink);
            thread_sink.mark_forwarder_stopped();
        });
    match spawned {
        Ok(_detached) => Ok(()),
        Err(error) => {
            sink.mark_forwarder_stopped();
            Err(error)
        }
    }
}

/// The service loop. It ends when the channel closes or the forwarder
/// panics.
fn run(records: &Receiver<CoreLogRecord>, sink: &LogSink) {
    loop {
        let Some(batch) = receive(records) else {
            return;
        };
        let delivered = std::panic::catch_unwind(AssertUnwindSafe(|| {
            deliver(sink, &batch);
            report_drop_counts(sink);
        }));
        match delivered {
            Ok(()) => sink.records_handled(batch.len()),
            Err(payload) => {
                let cause = take_panic_cause(payload.as_ref());
                // Spill first and mark the forwarder stopped: under the abort
                // rule the panic call below does not return, and its drain
                // wait must not wait for this thread.
                sink.records_handled(spill_all(&batch, records));
                sink.mark_forwarder_stopped();
                background_task_panicked(PanicReport {
                    subject: format!("thread {FORWARDER_THREAD_NAME}"),
                    cause,
                });
                return;
            }
        }
    }
}

/// Wait for a record, then take up to [`FORWARD_BATCH`]. `None` when the
/// channel closed.
fn receive(records: &Receiver<CoreLogRecord>) -> Option<Vec<CoreLogRecord>> {
    let first = records.recv().ok()?;
    let mut batch = Vec::with_capacity(FORWARD_BATCH);
    batch.push(first);
    while batch.len() < FORWARD_BATCH {
        match records.try_recv() {
            Ok(record) => batch.push(record),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
    Some(batch)
}

/// Hand one batch to the sink in force. A batch that the JavaScript sink
/// does not take goes to stderr, with one line that says why.
fn deliver(sink: &LogSink, batch: &[CoreLogRecord]) {
    let handed = match sink.stderr_only() {
        true => Handed::NoJavaScriptSink,
        false => js_sink::hand_batch(sink, batch),
    };
    match handed {
        Handed::Taken => {}
        Handed::NoJavaScriptSink => write_stderr(batch),
        Handed::NotTaken { reason } => {
            let _write_failure_ignored = writeln!(
                std::io::stderr(),
                "horsies: {} log records go to stderr: {reason}",
                batch.len()
            );
            write_stderr(batch);
        }
    }
}

/// Write each record of `batch` to stderr, one line each.
fn write_stderr(batch: &[CoreLogRecord]) {
    let mut stderr = std::io::stderr().lock();
    for record in batch {
        let _write_failure_ignored = writeln!(stderr, "{}", record.stderr_line());
    }
}

/// Report the records that a full channel lost since the last report, as one
/// WARNING record of the bridge logger.
fn report_drop_counts(sink: &LogSink) {
    let DropCounts {
        dropped_low,
        spilled_high,
    } = sink.take_drop_counts();
    if dropped_low == 0 && spilled_high == 0 {
        return;
    }
    let report = CoreLogRecord::bridge(
        RecordLevel::Warning,
        format!(
            "the core log channel was full: {dropped_low} TRACE/DEBUG/INFO records were \
             dropped and {spilled_high} WARNING/ERROR records were written to stderr"
        ),
    );
    deliver(sink, std::slice::from_ref(&report));
}

/// Write `batch` and the records in the channel, at most one channel's
/// capacity, to stderr; the count of records taken.
fn spill_all(batch: &[CoreLogRecord], records: &Receiver<CoreLogRecord>) -> usize {
    write_stderr(batch);
    let left: Vec<CoreLogRecord> = records.try_iter().take(LOG_CHANNEL_CAPACITY).collect();
    write_stderr(&left);
    batch.len() + left.len()
}
