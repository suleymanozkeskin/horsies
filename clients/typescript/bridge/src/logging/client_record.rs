//! A WARNING record that the package writes on a logger of the core log:
//! a failure that the package reports and does not answer (for example a
//! core handle of `check --live` that did not close). It goes to the sink
//! like a core record, at the threshold of its logger.

use napi_derive::napi;

use super::bridge::{started_log, CoreLogRecord};
use super::levels::RecordLevel;
use super::names::LoggerName;
use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::process_core::runtime::{CallRefusal, StartFailure};

/// Write `message` as a WARNING record of `logger`. Starts the log bridge;
/// a forwarder that cannot start throws `RUNTIME_START_FAILED`.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn log_client_warning(logger: LoggerName, message: String) -> Envelope<()> {
    sync_call("log_client_warning", move || match started_log() {
        Err(failure) => Envelope::Thrown(
            CallRefusal::StartFailed(StartFailure::LogForwarderStartFailed {
                cause: failure.cause,
            })
            .thrown(),
        ),
        Ok(log) => {
            log.push_record(client_warning(logger, message));
            Envelope::Value(())
        }
    })
}

/// The WARNING record of `message` on `logger`, created now on this thread.
/// Pure but for the clock and the thread name.
fn client_warning(logger: LoggerName, message: String) -> CoreLogRecord {
    let mut record = CoreLogRecord::bridge(RecordLevel::Warning, message);
    record.logger = logger;
    record
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_record_is_a_warning_of_its_logger_with_no_field() {
        let record = client_warning(LoggerName::App, "the handle did not close".to_owned());
        assert_eq!(record.logger, LoggerName::App);
        assert_eq!(record.level, RecordLevel::Warning);
        assert_eq!(record.message, "the handle did not close");
        assert!(record.fields.is_empty());
    }
}
