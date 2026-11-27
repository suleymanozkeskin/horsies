//! `probePanicLogForwarder`: a test export that makes the log forwarder of
//! this process panic at its next batch: a panic in a core background task.
//! The end-to-end tests of a task child call it from task code; the package
//! does not.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.
//!
//! - The call arms the probe with the panic text and pushes one bridge
//!   record, so the forwarder takes a batch even when the process logs
//!   nothing else. It returns at once.
//! - The forwarder reads the probe before it hands each batch to the sink
//!   (`forwarder::run`). An armed probe panics there with its text, once.
//!   The forwarder catches the panic, writes the batch and the records in
//!   the channel to stderr, and gives the panic to the process panic rule:
//!   under `ABORT_PROCESS` (a worker process or a task child) the process
//!   writes its abort line and aborts; under `STOP_CORE` its core stops.
//! - With no running log bridge the call throws `CLIENT_DEFECT`: no
//!   forwarder runs to panic, and nothing is armed.

use std::sync::{Mutex, MutexGuard, PoisonError};

use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::logging::bridge::{running_log, CoreLogRecord};
use crate::logging::levels::RecordLevel;

/// The probe that the forwarder of this process reads at each batch.
pub static FORWARDER_PANIC_PROBE: PanicProbe = PanicProbe::new();

/// The subject of the probe in its refusal.
const PROBE_SUBJECT: &str = "probePanicLogForwarder";

/// The message of the record that wakes the forwarder.
const WAKE_MESSAGE: &str = "the forwarder panic probe is armed: the forwarder panics at this batch";

/// Whether the forwarder panics at its next batch.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ProbeState {
    NotArmed,
    Armed { text: String },
}

/// A panic that a forwarder takes at its next batch.
pub struct PanicProbe {
    state: Mutex<ProbeState>,
}

impl PanicProbe {
    /// A probe that is not armed.
    const fn new() -> Self {
        Self {
            state: Mutex::new(ProbeState::NotArmed),
        }
    }

    /// Arm the probe: the next `panic_if_armed` panics with `text`. A probe
    /// that is armed again keeps the later text.
    fn arm(&self, text: String) {
        *self.lock() = ProbeState::Armed { text };
    }

    /// Panic with the text of an armed probe, which is then not armed. A
    /// probe that is not armed does nothing.
    pub fn panic_if_armed(&self) {
        // The guard ends with this statement, so the panic below does not
        // poison the lock.
        let state = std::mem::replace(&mut *self.lock(), ProbeState::NotArmed);
        match state {
            ProbeState::NotArmed => {}
            ProbeState::Armed { text } => panic!("{text}"),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ProbeState> {
        // The state is one value that a panic cannot leave half written.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Make the log forwarder of this process panic with `text` at its next
/// batch. Arms the probe and pushes one bridge record; returns at once.
/// Throws `CLIENT_DEFECT`, and arms nothing, when no log bridge runs in this
/// process.
#[napi(
    js_name = "probePanicLogForwarder",
    ts_return_type = "NativeEnvelope<undefined>"
)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_panic_log_forwarder(text: String) -> Envelope<()> {
    sync_call("probe_panic_log_forwarder", || {
        let Some(log) = running_log() else {
            return Envelope::Thrown(NativeThrown::ClientDefect {
                subject: PROBE_SUBJECT.to_owned(),
                rule: "it is called in a process whose log forwarder runs".to_owned(),
            });
        };
        FORWARDER_PANIC_PROBE.arm(text);
        log.sink().push(CoreLogRecord::bridge(
            RecordLevel::Warning,
            WAKE_MESSAGE.to_owned(),
        ));
        Envelope::Value(())
    })
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;
    use std::time::Duration;

    use super::*;
    use crate::logging::forwarder::FORWARDER_THREAD_NAME;
    use crate::process_core::panic_rule::{set_panic_rule, PanicRule};
    use crate::process_core::runtime::{core_for_call, core_status, pure_call_refusal};
    use crate::process_core::runtime::{CallRefusal, CoreStatus};

    /// The child test to run, set in the environment of a child process. The
    /// panic rule, the core and the log bridge are per process, so the tests
    /// that start them run in a child process of the test binary.
    const CHILD_TEST_ENV: &str = "HORSIES_TYPESCRIPT_FORWARDER_PROBE_CHILD_TEST";

    /// The abort signal number on Linux and macOS.
    const SIGABRT: i32 = 6;

    /// The panic text of the tests.
    const PLANNED: &str = "planned forwarder panic of a probe test";

    /// The longest wait of a child for the end that the panic brings.
    const PANIC_END_WAIT: Duration = Duration::from_secs(5);

    /// The interval of the reads of that wait.
    const PANIC_END_POLL: Duration = Duration::from_millis(10);

    fn in_child(test: &str) -> bool {
        std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == test)
    }

    /// Run one test of this binary in a child process.
    fn run_child(test: &str) -> std::process::Output {
        let binary = std::env::current_exe().expect("the test binary has a path");
        std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST_ENV, test)
            .output()
            .expect("the child test process runs")
    }

    /// Wait at most `PANIC_END_WAIT` until the core of this process stopped.
    fn wait_for_the_stop() -> CoreStatus {
        let polls = PANIC_END_WAIT.as_millis() / PANIC_END_POLL.as_millis();
        for _poll in 0..polls {
            match core_status() {
                CoreStatus::Stopped => return CoreStatus::Stopped,
                CoreStatus::NotStarted | CoreStatus::Running | CoreStatus::Inherited => {
                    std::thread::sleep(PANIC_END_POLL);
                }
            }
        }
        core_status()
    }

    #[test]
    fn an_armed_probe_panics_once_with_its_text() {
        let probe = PanicProbe::new();
        probe.arm(PLANNED.to_owned());
        let caught = std::panic::catch_unwind(|| probe.panic_if_armed());
        let payload = caught.expect_err("an armed probe panics");
        assert_eq!(
            payload.downcast_ref::<String>().map(String::as_str),
            Some(PLANNED)
        );
        probe.panic_if_armed();
    }

    #[test]
    fn a_probe_that_is_not_armed_does_nothing() {
        let probe = PanicProbe::new();
        probe.panic_if_armed();
        assert_eq!(*probe.lock(), ProbeState::NotArmed);
    }

    #[test]
    fn no_log_child() {
        if !in_child("logging::forwarder_probe::tests::no_log_child") {
            return;
        }
        assert!(
            running_log().is_none(),
            "no test of this child started the log"
        );
        assert_eq!(
            probe_panic_log_forwarder(PLANNED.to_owned()),
            Envelope::Thrown(NativeThrown::ClientDefect {
                subject: PROBE_SUBJECT.to_owned(),
                rule: "it is called in a process whose log forwarder runs".to_owned(),
            })
        );
        assert_eq!(*FORWARDER_PANIC_PROBE.lock(), ProbeState::NotArmed);
    }

    #[test]
    fn with_no_log_forwarder_the_probe_is_a_client_defect_and_arms_nothing() {
        let output = run_child("logging::forwarder_probe::tests::no_log_child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("test logging::forwarder_probe::tests::no_log_child ... ok"),
            "{stdout}"
        );
    }

    #[test]
    fn stop_rule_child() {
        if !in_child("logging::forwarder_probe::tests::stop_rule_child") {
            return;
        }
        assert!(core_for_call().is_ok(), "the core starts");
        assert_eq!(
            probe_panic_log_forwarder(PLANNED.to_owned()),
            Envelope::Value(())
        );
        assert_eq!(wait_for_the_stop(), CoreStatus::Stopped);
        match pure_call_refusal() {
            Some(CallRefusal::Stopped(report)) => {
                assert_eq!(report.subject, format!("thread {FORWARDER_THREAD_NAME}"));
                assert!(report.cause.starts_with(PLANNED), "{}", report.cause);
            }
            other => panic!("a stopped core refuses every call, got {other:?}"),
        }
    }

    #[test]
    fn under_stop_core_the_probe_stops_the_core_with_the_forwarder_as_the_subject() {
        let output = run_child("logging::forwarder_probe::tests::stop_rule_child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("test logging::forwarder_probe::tests::stop_rule_child ... ok"),
            "{stdout}"
        );
    }

    #[test]
    fn abort_rule_child() {
        if !in_child("logging::forwarder_probe::tests::abort_rule_child") {
            return;
        }
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        assert!(core_for_call().is_ok(), "the core starts");
        assert_eq!(
            probe_panic_log_forwarder(PLANNED.to_owned()),
            Envelope::Value(())
        );
        // The forwarder aborts the process within this wait; the parent test
        // fails on a normal exit.
        std::thread::sleep(PANIC_END_WAIT);
    }

    #[test]
    fn under_abort_process_the_probe_aborts_after_the_abort_line() {
        let output = run_child("logging::forwarder_probe::tests::abort_rule_child");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.signal(), Some(SIGABRT), "{stderr}");
        assert!(
            stderr.contains(&format!(
                "aborts: panic in core background task thread {FORWARDER_THREAD_NAME}: {PLANNED}"
            )),
            "{stderr}"
        );
    }
}
