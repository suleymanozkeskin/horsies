//! The panic guard of a bridge call.
//!
//! - A panic that unwinds through the Node-API boundary aborts the process.
//!   So every sync `#[napi]` function runs its body inside [`sync_call`]: a
//!   panic becomes `CORE_PANIC` in the envelope, with the subject and the
//!   cause, and the process runs on.
//! - Every operation on the core runtime runs inside [`guard`]: the core
//!   subscriber is its default dispatcher, a panic of the operation becomes
//!   `CORE_PANIC` (the core runs on), and a stop of the core ends the
//!   operation early with `CORE_STOPPED`.
//! - JavaScript cannot leave a call, so the only early end is a stop of the
//!   core. What remains true then depends on the [`CallKind`]: a read changed
//!   nothing; a write may have committed.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use tracing::instrument::WithSubscriber;

use crate::call::thrown::{Envelope, NativeThrown, StateAfter};
use crate::logging::levels::RecordLevel;
use crate::process_core::panic_rule::{take_panic_cause, PanicReport};
use crate::process_core::runtime::Core;

/// Run the body of a sync bridge function. A panic of the body becomes
/// `CORE_PANIC { subject, cause }`.
pub fn sync_call<T>(subject: &'static str, body: impl FnOnce() -> Envelope<T>) -> Envelope<T> {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(envelope) => envelope,
        Err(payload) => Envelope::Thrown(NativeThrown::CorePanic {
            subject: subject.to_owned(),
            cause: take_panic_cause(payload.as_ref()),
        }),
    }
}

/// Whether an operation changes database state. It decides what remains
/// true when a stop of the core ends the operation early.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallKind {
    /// Reads and waits: nothing changed.
    Read,
    /// Writes: the write may have committed or not.
    Write,
}

impl CallKind {
    /// What remains true after an early end. Pure.
    pub const fn state_after(self) -> StateAfter {
        match self {
            Self::Read => StateAfter::NoChange,
            Self::Write => StateAfter::OutcomeUnknown,
        }
    }
}

/// How one guarded operation ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Guarded<T> {
    Done(T),
    /// The operation panicked; the core runs on.
    Panicked {
        cause: String,
    },
    /// A core background task panicked while the operation ran.
    CoreStopped(PanicReport),
}

impl<T> Guarded<T> {
    /// The envelope of the end of the operation `subject` of `kind`. Pure.
    pub fn envelope(self, subject: &'static str, kind: CallKind) -> Envelope<T> {
        match self {
            Self::Done(value) => Envelope::Value(value),
            Self::Panicked { cause } => Envelope::Thrown(NativeThrown::CorePanic {
                subject: subject.to_owned(),
                cause,
            }),
            Self::CoreStopped(report) => Envelope::Thrown(NativeThrown::CoreStopped {
                subject: report.subject,
                cause: report.cause,
                state_after: kind.state_after(),
            }),
        }
    }
}

/// Run `operation` with the core subscriber as the default dispatcher, catch
/// its panic, and end early when the core stops. A panic is logged as one
/// ERROR record of the bridge logger.
pub async fn guard<T, F>(core: &'static Core, subject: &'static str, operation: F) -> Guarded<T>
where
    F: Future<Output = T> + Send,
{
    let operation = AssertUnwindSafe(operation)
        .catch_unwind()
        .with_subscriber(core.log().dispatch().clone());
    let mut stop = core.stop_receiver();
    tokio::select! {
        biased;
        outcome = operation => match outcome {
            Ok(value) => Guarded::Done(value),
            Err(payload) => {
                let cause = take_panic_cause(payload.as_ref());
                core.log().push_bridge_record(
                    RecordLevel::Error,
                    format!("panic in core operation {subject}: {cause}"),
                );
                Guarded::Panicked { cause }
            }
        },
        report = stopped(core, &mut stop) => Guarded::CoreStopped(report),
    }
}

/// Wait until the core stops, and return the first panic report.
async fn stopped(core: &Core, stop: &mut tokio::sync::watch::Receiver<bool>) -> PanicReport {
    // The borrow of the watched value ends in this statement: it must not be
    // held across an await, or the future would not be `Send`.
    let signal_seen = stop.wait_for(|stopped| *stopped).await.is_ok();
    if !signal_seen {
        // The sender lives in the core, which lives for the process.
        std::future::pending::<()>().await;
    }
    match core.stopped_report() {
        Some(report) => report.clone(),
        None => unreachable!("the stop signal is sent only after the first report is kept"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_core::runtime::standalone_core_for_tests;

    #[test]
    fn a_body_that_returns_gives_its_envelope() {
        assert_eq!(
            sync_call("core_state", || Envelope::Value(7_u32)),
            Envelope::Value(7)
        );
    }

    #[test]
    fn a_body_that_panics_gives_core_panic_with_the_subject() {
        let envelope: Envelope<u32> = sync_call("core_state", || panic!("planned panic"));
        match envelope {
            Envelope::Thrown(NativeThrown::CorePanic { subject, cause }) => {
                assert_eq!(subject, "core_state");
                assert!(cause.starts_with("planned panic"), "{cause}");
            }
            other => panic!("a panic is CORE_PANIC, got {other:?}"),
        }
    }

    #[test]
    fn a_read_changes_nothing_and_a_write_has_an_unknown_outcome() {
        assert_eq!(CallKind::Read.state_after(), StateAfter::NoChange);
        assert_eq!(CallKind::Write.state_after(), StateAfter::OutcomeUnknown);
    }

    /// An operation body that panics, with the type of a value.
    fn planned_operation_panic() -> u32 {
        panic!("planned operation panic")
    }

    #[test]
    fn an_operation_that_ends_gives_its_value() {
        let core = standalone_core_for_tests();
        let guarded = core
            .runtime_handle()
            .block_on(guard(core, "ping_database", async { 5_u32 }));
        assert_eq!(
            guarded.envelope("ping_database", CallKind::Read),
            Envelope::Value(5)
        );
    }

    #[test]
    fn an_operation_that_panics_gives_core_panic_and_the_core_runs_on() {
        let core = standalone_core_for_tests();
        let guarded: Guarded<u32> =
            core.runtime_handle()
                .block_on(guard(core, "ping_database", async {
                    planned_operation_panic()
                }));
        match guarded.envelope("ping_database", CallKind::Read) {
            Envelope::Thrown(NativeThrown::CorePanic { subject, cause }) => {
                assert_eq!(subject, "ping_database");
                assert!(cause.starts_with("planned operation panic"), "{cause}");
            }
            other => panic!("a panic is CORE_PANIC, got {other:?}"),
        }
        assert!(core.stopped_report().is_none());
    }

    #[test]
    fn a_stop_of_the_core_ends_a_write_with_an_unknown_outcome() {
        let core = standalone_core_for_tests();
        core.stop(PanicReport {
            subject: "thread horsies-log-forwarder".to_owned(),
            cause: "planned background panic".to_owned(),
        });
        let guarded: Guarded<u32> = core.runtime_handle().block_on(guard(
            core,
            "install_schema",
            std::future::pending::<u32>(),
        ));
        assert_eq!(
            guarded.envelope("install_schema", CallKind::Write),
            Envelope::Thrown(NativeThrown::CoreStopped {
                subject: "thread horsies-log-forwarder".to_owned(),
                cause: "planned background panic".to_owned(),
                state_after: StateAfter::OutcomeUnknown,
            })
        );
    }
}
