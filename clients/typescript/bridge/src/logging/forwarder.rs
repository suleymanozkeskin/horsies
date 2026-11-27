//! The forwarder thread: the one thread that hands core records to the sink.
//!
//! - It takes up to [`FORWARD_BATCH`] records at a time and hands the batch
//!   to the sink in force: one stderr line per record, or the JavaScript sink
//!   (`js_sink`). A batch that the JavaScript sink does not take goes to
//!   stderr.
//! - It counts every record it handles, so that an abort can wait for the
//!   channel to empty.
//! - After a batch it reports the records that were lost (a full channel, or
//!   a sink that did not answer within its bound), as one WARNING record of
//!   the bridge logger, when the bridge logger's threshold lets it through.
//! - A panic in the forwarder is a panic in a core background task: the
//!   process panic rule applies. The batch and the records still in the
//!   channel go to stderr first. A build with the cargo feature
//!   `test-probes` reads the probe of `forwarder_probe` before each batch, so
//!   a test can cause such a panic.
//! - It ends only when the channel closes. The sender lives in a static, so
//!   it runs for the life of the process.

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;

use super::bridge::{CoreLogRecord, DropCounts, LogSink, LOG_CHANNEL_CAPACITY};
use super::format::{current_format, notice_line_in, record_line, LogFormatKind};
use super::js_sink::{self, Handed, NotTaken, SINK_ANSWER_BOUND};
use super::levels::{RecordLevel, LEVELS};
use super::names::LoggerName;
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
            #[cfg(feature = "test-probes")]
            super::forwarder_probe::FORWARDER_PANIC_PROBE.panic_if_armed();
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

/// Hand one batch to the sink in force, and act on how it ended.
fn deliver(sink: &LogSink, batch: &[CoreLogRecord]) {
    let handed = match sink.stderr_only() {
        true => Handed::NoJavaScriptSink,
        false => js_sink::hand_batch(sink, batch),
    };
    settle_batch(sink, batch, handed);
}

/// Act on how the hand of `batch` ended: nothing for a batch that the sink
/// has, a count for an abandoned batch, and stderr for the rest (with one
/// line that says why the sink did not take it).
fn settle_batch(sink: &LogSink, batch: &[CoreLogRecord], handed: Handed) {
    match handed {
        Handed::Taken | Handed::InSinkUnanswered => {}
        Handed::Abandoned => sink.records_abandoned(batch.len()),
        Handed::NoJavaScriptSink => write_stderr(batch),
        Handed::NotTaken(reason) => {
            let _write_failure_ignored = writeln!(
                std::io::stderr(),
                "{}",
                not_taken_notice(current_format(), batch.len(), &reason)
            );
            write_stderr(batch);
        }
    }
}

/// The notice line that `count` records go to stderr because the sink did
/// not take them for `reason`, in `format`. Pure but for the clock, the
/// thread name and the process id.
fn not_taken_notice(format: LogFormatKind, count: usize, reason: &NotTaken) -> String {
    notice_line_in(
        format,
        format!("horsies: {count} log records go to stderr: {reason}"),
    )
}

/// Write each record of `batch` to stderr, one line each, in the format of
/// the process.
fn write_stderr(batch: &[CoreLogRecord]) {
    let mut stderr = std::io::stderr().lock();
    for record in batch {
        let _write_failure_ignored = writeln!(stderr, "{}", record_line(record));
    }
}

/// Report the records lost since the last report, as one WARNING record of
/// the bridge logger.
fn report_drop_counts(sink: &LogSink) {
    if let Some(report) = drop_report(sink.take_drop_counts()) {
        deliver(sink, std::slice::from_ref(&report));
    }
}

/// The WARNING record of the lost records, or `None` when none was lost or
/// the threshold of the bridge logger refuses a WARNING. Pure but for the
/// read of the thresholds.
fn drop_report(counts: DropCounts) -> Option<CoreLogRecord> {
    let DropCounts {
        dropped_low,
        spilled_high,
        abandoned,
    } = counts;
    let lost = dropped_low != 0 || spilled_high != 0 || abandoned != 0;
    match lost && LEVELS.enabled(LoggerName::Bridge, RecordLevel::Warning) {
        false => None,
        true => Some(CoreLogRecord::bridge(
            RecordLevel::Warning,
            format!(
                "core log records were lost: {dropped_low} TRACE/DEBUG/INFO records were \
                 dropped in a full channel, {spilled_high} WARNING/ERROR records were written \
                 to stderr, and {abandoned} records were dropped because the log sink did not \
                 answer within {} ms",
                SINK_ANSWER_BOUND.as_millis()
            ),
        )),
    }
}

/// Write `batch` and the records in the channel, at most one channel's
/// capacity, to stderr; the count of records taken.
fn spill_all(batch: &[CoreLogRecord], records: &Receiver<CoreLogRecord>) -> usize {
    write_stderr(batch);
    let left: Vec<CoreLogRecord> = records.try_iter().take(LOG_CHANNEL_CAPACITY).collect();
    write_stderr(&left);
    batch.len() + left.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_that_the_sink_did_not_take_has_a_notice_in_the_format_of_the_process() {
        assert_eq!(
            not_taken_notice(LogFormatKind::TextLogs, 3, &NotTaken::SinkThrew),
            format!(
                "horsies: 3 log records go to stderr: {}",
                NotTaken::SinkThrew
            )
        );
        let value: serde_json::Value = serde_json::from_str(&not_taken_notice(
            LogFormatKind::JsonLogs,
            3,
            &NotTaken::SinkThrew,
        ))
        .expect("the JSON notice is one JSON object");
        assert_eq!(value["level"], "WARNING");
        assert_eq!(value["logger"], "horsies.bridge");
        assert_eq!(
            value["message"],
            format!(
                "horsies: 3 log records go to stderr: {}",
                NotTaken::SinkThrew
            )
        );
    }

    fn record(message: &str) -> CoreLogRecord {
        CoreLogRecord::bridge(RecordLevel::Error, message.to_owned())
    }

    #[test]
    fn no_loss_has_no_report() {
        assert_eq!(
            drop_report(DropCounts {
                dropped_low: 0,
                spilled_high: 0,
                abandoned: 0
            }),
            None
        );
    }

    #[test]
    fn a_loss_is_one_warning_of_the_bridge_logger_with_each_count() {
        let report = drop_report(DropCounts {
            dropped_low: 3,
            spilled_high: 2,
            abandoned: 7,
        })
        .expect("the default threshold lets a WARNING through");
        assert_eq!(report.logger, LoggerName::Bridge);
        assert_eq!(report.level, RecordLevel::Warning);
        assert!(
            report.message.contains("3 TRACE/DEBUG/INFO"),
            "{}",
            report.message
        );
        assert!(
            report.message.contains("2 WARNING/ERROR"),
            "{}",
            report.message
        );
        assert!(
            report.message.contains("7 records were dropped"),
            "{}",
            report.message
        );
    }

    #[test]
    fn an_abandoned_batch_counts_its_records_as_dropped() {
        let (sink, _records) = LogSink::new(1);
        settle_batch(&sink, &[record("a"), record("b")], Handed::Abandoned);
        assert_eq!(sink.take_drop_counts().abandoned, 2);
    }

    #[test]
    fn a_batch_that_the_sink_has_counts_nothing() {
        let (sink, _records) = LogSink::new(1);
        settle_batch(&sink, &[record("a")], Handed::Taken);
        settle_batch(&sink, &[record("b")], Handed::InSinkUnanswered);
        assert_eq!(
            sink.take_drop_counts(),
            DropCounts {
                dropped_low: 0,
                spilled_high: 0,
                abandoned: 0
            }
        );
    }

    #[test]
    fn a_batch_that_the_sink_did_not_take_counts_nothing() {
        let (sink, _records) = LogSink::new(1);
        settle_batch(&sink, &[record("a")], Handed::NotTaken(NotTaken::SinkThrew));
        assert_eq!(sink.take_drop_counts().abandoned, 0);
    }

    #[test]
    fn a_spill_takes_the_batch_and_the_records_in_the_channel() {
        let (sender, receiver) = std::sync::mpsc::sync_channel(4);
        for message in ["c", "d", "e"] {
            sender.send(record(message)).expect("the channel has room");
        }
        assert_eq!(spill_all(&[record("a"), record("b")], &receiver), 5);
        assert!(receiver.try_recv().is_err());
    }
}
