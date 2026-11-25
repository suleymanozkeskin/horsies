//! The panic rule of the process: what a panic in a core background task
//! does, and what a defect of the package does.
//!
//! - `STOP_CORE`, the default, the rule of a process that the core does not
//!   own (a producer inside a web server): the core stops in this process,
//!   and every later call and every call in flight ends with `CoreStopped`.
//!   A defect of the package throws.
//! - `ABORT_PROCESS`, the rule of a worker process: the panic or the defect
//!   goes to stderr and to the log sink, the log drains for at most
//!   [`ABORT_LOG_DRAIN_BOUND`], and the process aborts; its supervisor
//!   restarts it.
//!
//! The entry point sets the rule once, before the first core call. The start
//! of the core fixes it.
//!
//! A chained panic hook keeps the cause of each panic, with its source
//! location, on its thread, so the catch site can report it.

use std::cell::RefCell;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::{Once, OnceLock};
use std::time::Duration;

use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::logging::bridge::{running_log, DrainOutcome, LogBridge};
use crate::logging::levels::RecordLevel;

/// How long an aborting process waits for the log forwarder to hand the
/// records in the channel to the sink. A full channel is 8,192 records in
/// batches of 256; at tens of microseconds per record, the forwarder empties
/// it well inside this bound.
pub const ABORT_LOG_DRAIN_BOUND: Duration = Duration::from_secs(2);

/// What a panic in a core background task does to this process.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanicRule {
    /// Stop the core in this process; every later call throws `CoreStopped`.
    StopCore,
    /// Write the panic to stderr and to the log sink, drain the log for a
    /// bounded time, and abort the process.
    AbortProcess,
}

/// The rule was set to another rule before, or fixed by the start of the
/// core. `current` is the rule in force.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PanicRuleFixed {
    pub current: PanicRule,
}

/// The first panic of a core background task: what failed, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PanicReport {
    pub subject: String,
    pub cause: String,
}

/// The thread that ends the process under the abort rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbortingThread {
    /// The JavaScript thread: it cannot run the JavaScript sink while it
    /// waits, so the records go to stderr.
    JavaScript,
    /// A core thread or the forwarder: the JavaScript sink can still take
    /// records.
    Core,
}

/// The process panic rule: set once by the entry point, or fixed at
/// `StopCore` by the start of the core.
static PANIC_RULE: OnceLock<PanicRule> = OnceLock::new();

static PANIC_HOOK: Once = Once::new();

thread_local! {
    /// Cause of the last panic on this thread, written by the panic hook so
    /// that the catch site can report the source location.
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Set the panic rule of this process. Setting the rule in force again
/// succeeds and changes nothing; another rule is refused once a rule is set
/// or the core started. Starts nothing.
pub fn set_panic_rule(rule: PanicRule) -> Result<(), PanicRuleFixed> {
    let current = *PANIC_RULE.get_or_init(|| rule);
    match current == rule {
        true => Ok(()),
        false => Err(PanicRuleFixed { current }),
    }
}

/// The panic rule in force: the rule set or fixed, else the default
/// `StopCore`. Starts nothing.
pub fn panic_rule() -> PanicRule {
    PANIC_RULE.get().copied().unwrap_or(PanicRule::StopCore)
}

/// Fix the rule at the start of the core: the rule set before, else
/// `StopCore`. A rule set after this point could miss a panic of a task that
/// the start runs.
pub fn fix_panic_rule() -> PanicRule {
    *PANIC_RULE.get_or_init(|| PanicRule::StopCore)
}

/// Set the panic rule of this process. The entry point calls it before the
/// first core call. Throws `PANIC_RULE_FIXED` for another rule once a rule
/// is set or the core started. Starts nothing.
#[napi(js_name = "setPanicRule", ts_return_type = "NativeEnvelope<undefined>")]
pub fn set_panic_rule_export(rule: PanicRule) -> Envelope<()> {
    sync_call("set_panic_rule", || match set_panic_rule(rule) {
        Ok(()) => Envelope::Value(()),
        Err(fixed) => Envelope::Thrown(NativeThrown::PanicRuleFixed {
            current: fixed.current,
        }),
    })
}

/// The package found a defect of its own (a thrown `CoreDefect`). Under
/// `ABORT_PROCESS` the defect goes to stderr and to the log sink, the log
/// drains, and the process aborts: this call does not return. Under
/// `STOP_CORE` it returns at once, and the package throws the defect.
/// Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn report_defect(code: String, message: String) -> Envelope<()> {
    sync_call("report_defect", || match panic_rule() {
        PanicRule::StopCore => Envelope::Value(()),
        PanicRule::AbortProcess => abort_process(
            &format!("defect {code} of the horsies package: {message}"),
            AbortingThread::JavaScript,
        ),
    })
}

/// Apply the process panic rule to a panic in a core background task. Under
/// `AbortProcess` this does not return.
pub fn background_task_panicked(report: PanicReport) {
    match panic_rule() {
        PanicRule::StopCore => crate::process_core::runtime::stop_after_background_panic(report),
        PanicRule::AbortProcess => abort_process(
            &format!(
                "panic in core background task {}: {}",
                report.subject, report.cause
            ),
            AbortingThread::Core,
        ),
    }
}

/// Write `what` to stderr, push one ERROR record, wait for the forwarder
/// within [`ABORT_LOG_DRAIN_BOUND`], and abort the process.
fn abort_process(what: &str, thread: AbortingThread) -> ! {
    let _write_failure_ignored = writeln!(
        std::io::stderr(),
        "horsies: worker process {} aborts: {what}",
        std::process::id()
    );
    if let Some(log) = running_log() {
        match thread {
            AbortingThread::JavaScript => log.sink().switch_to_stderr(),
            AbortingThread::Core => {}
        }
        flush_log_before_abort(log, what);
    }
    std::process::abort()
}

fn flush_log_before_abort(log: &LogBridge, what: &str) {
    log.push_bridge_record(
        RecordLevel::Error,
        format!("the worker process aborts: {what}"),
    );
    let unsent = match log.wait_for_drain(ABORT_LOG_DRAIN_BOUND) {
        DrainOutcome::Drained => return,
        DrainOutcome::ForwarderStopped { left } => {
            format!("the log forwarder is not running; {left} log records")
        }
        DrainOutcome::BoundReached { left } => format!(
            "{left} log records were still in the channel after {} ms and",
            ABORT_LOG_DRAIN_BOUND.as_millis()
        ),
    };
    let _write_failure_ignored = writeln!(
        std::io::stderr(),
        "horsies: {unsent} did not reach the log sink before the abort"
    );
}

/// Chain a hook that keeps the cause of each panic on its thread, so that
/// the catch site can report the source location and not only the message.
/// The previous hook still runs, so the panic still goes to stderr.
pub fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            let cause = describe_panic(info);
            let _thread_ending = LAST_PANIC.try_with(|slot| {
                if let Ok(mut kept) = slot.try_borrow_mut() {
                    *kept = Some(cause);
                }
            });
            previous(info);
        }));
    });
}

fn describe_panic(info: &PanicHookInfo<'_>) -> String {
    let message = payload_text(info.payload());
    match info.location() {
        Some(location) => format!("{message} (at {}:{})", location.file(), location.line()),
        None => format!("{message} (at an unknown location)"),
    }
}

fn payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "the panic payload is not text".to_owned(),
    }
}

/// The cause that the hook kept on this thread, or the payload text when the
/// hook did not run (the hook is installed at the start of the core, and a
/// hook installed later may replace it).
pub fn take_panic_cause(payload: &(dyn std::any::Any + Send)) -> String {
    let kept = LAST_PANIC
        .try_with(|slot| match slot.try_borrow_mut() {
            Ok(mut kept) => kept.take(),
            Err(_already_borrowed) => None,
        })
        .ok()
        .flatten();
    match kept {
        Some(cause) => cause,
        None => payload_text(payload),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;

    use super::*;
    use crate::process_core::runtime::{core_for_call, core_status, pure_call_refusal};
    use crate::process_core::runtime::{CallRefusal, CoreStatus};

    /// The child test to run, set in the environment of a child process. The
    /// panic rule and the core are per process, so the tests that set them
    /// run in a child process of the test binary.
    const CHILD_TEST_ENV: &str = "HORSIES_TYPESCRIPT_PROCESS_CORE_CHILD_TEST";

    /// The abort signal number on Linux and macOS.
    const SIGABRT: i32 = 6;

    /// The panic message of the child tests.
    const PLANNED_PANIC: &str = "planned background panic";

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

    fn start_core() {
        assert!(core_for_call().is_ok(), "the core starts");
    }

    /// Panic on a new thread and hand the panic to the rule, as the catch
    /// site of a core background task does.
    fn panic_in_background_task() {
        let task = std::thread::Builder::new()
            .name("rule-test-task".to_owned())
            .spawn(|| {
                let caught = std::panic::catch_unwind(|| panic!("{PLANNED_PANIC}"));
                if let Err(payload) = caught {
                    background_task_panicked(PanicReport {
                        subject: "thread rule-test-task".to_owned(),
                        cause: take_panic_cause(payload.as_ref()),
                    });
                }
            })
            .expect("the task thread starts");
        let _joined = task.join();
    }

    /// The last line of the bridge logger on stderr.
    fn last_bridge_line(stderr: &str) -> &str {
        stderr
            .lines()
            .rev()
            .find(|line| line.contains(" ERROR horsies.bridge "))
            .unwrap_or_default()
    }

    #[test]
    fn abort_rule_child() {
        if !in_child("process_core::panic_rule::tests::abort_rule_child") {
            return;
        }
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        assert_eq!(
            set_panic_rule(PanicRule::StopCore),
            Err(PanicRuleFixed {
                current: PanicRule::AbortProcess
            })
        );
        start_core();
        panic_in_background_task();
        // Under the abort rule the line above does not return; the parent
        // test fails on a normal exit.
    }

    #[test]
    fn a_worker_process_aborts_on_a_background_panic_after_its_log_record() {
        let output = run_child("process_core::panic_rule::tests::abort_rule_child");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.signal(), Some(SIGABRT), "{stderr}");
        assert!(
            stderr.contains("aborts: panic in core background task thread rule-test-task: "),
            "{stderr}"
        );
        assert!(
            last_bridge_line(&stderr).contains(&format!(
                "the worker process aborts: panic in core background task thread \
                 rule-test-task: {PLANNED_PANIC}"
            )),
            "{stderr}"
        );
    }

    #[test]
    fn defect_abort_child() {
        if !in_child("process_core::panic_rule::tests::defect_abort_child") {
            return;
        }
        assert_eq!(set_panic_rule(PanicRule::AbortProcess), Ok(()));
        start_core();
        let _never = report_defect("CLIENT_DEFECT".to_owned(), "taskId: a UUID".to_owned());
    }

    #[test]
    fn a_defect_in_a_worker_process_aborts_after_its_log_record() {
        let output = run_child("process_core::panic_rule::tests::defect_abort_child");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.signal(), Some(SIGABRT), "{stderr}");
        assert!(
            last_bridge_line(&stderr).contains(
                "the worker process aborts: defect CLIENT_DEFECT of the horsies package: \
                 taskId: a UUID"
            ),
            "{stderr}"
        );
    }

    #[test]
    fn stop_rule_child() {
        if !in_child("process_core::panic_rule::tests::stop_rule_child") {
            return;
        }
        assert_eq!(
            report_defect("CLIENT_DEFECT".to_owned(), "taskId: a UUID".to_owned()),
            Envelope::Value(())
        );
        start_core();
        assert_eq!(panic_rule(), PanicRule::StopCore);
        assert_eq!(
            set_panic_rule(PanicRule::AbortProcess),
            Err(PanicRuleFixed {
                current: PanicRule::StopCore
            })
        );
        panic_in_background_task();
        assert_eq!(core_status(), CoreStatus::Stopped);
        match pure_call_refusal() {
            Some(CallRefusal::Stopped(report)) => {
                assert_eq!(report.subject, "thread rule-test-task");
                assert!(report.cause.starts_with(PLANNED_PANIC), "{}", report.cause);
            }
            other => panic!("a stopped core refuses every call, got {other:?}"),
        }
    }

    #[test]
    fn a_producer_process_stops_its_core_on_a_background_panic() {
        let output = run_child("process_core::panic_rule::tests::stop_rule_child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("test process_core::panic_rule::tests::stop_rule_child ... ok"),
            "{stdout}"
        );
    }

    #[test]
    fn a_text_payload_is_the_cause_when_no_hook_ran() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("broken invariant");
        assert_eq!(take_panic_cause(payload.as_ref()), "broken invariant");
    }

    #[test]
    fn an_owned_text_payload_is_the_cause() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("owned text"));
        assert_eq!(payload_text(payload.as_ref()), "owned text");
    }

    #[test]
    fn a_payload_that_is_not_text_is_named() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(
            payload_text(payload.as_ref()),
            "the panic payload is not text"
        );
    }

    #[test]
    fn the_hook_keeps_the_source_location() {
        install_panic_hook();
        let caught = std::panic::catch_unwind(|| panic!("kept by the hook"));
        let payload = caught.expect_err("the closure panics");
        let cause = take_panic_cause(payload.as_ref());
        assert!(cause.starts_with("kept by the hook (at "), "{cause}");
        assert!(cause.contains("panic_rule.rs"), "{cause}");
    }
}
