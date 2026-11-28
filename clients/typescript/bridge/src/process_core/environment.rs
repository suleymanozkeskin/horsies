//! The Node-API environments of the process.
//!
//! - Only the main thread of a process loads the addon: the package's loader
//!   checks `isMainThread` first, and loads the file once. This module also
//!   counts the environments that register the addon, so a load that
//!   bypasses the loader fails at once: its registration throws, and its
//!   `require` or `process.dlopen` gets no export object. The refusal names
//!   its cause: another thread (a `worker_threads` isolate), or a second load
//!   on the thread of the first one.
//! - The first environment registers one cleanup hook. When the environment
//!   ends, the hook marks the environment closing (a later core call into
//!   JavaScript is not run), ends every pending call into JavaScript with
//!   `NotRun { envClosing }`, switches the log forwarder to stderr, and waits
//!   for the forwarder to write the records in the channel, up to
//!   [`ENVIRONMENT_LOG_DRAIN_BOUND`]. At the bound it writes one stderr line
//!   with the count of records left. It does not stop the core runtime: the
//!   runtime lives in a static, and a drop would wait for its threads.
//! - This is the one `#[napi(module_exports)]` function of the addon: napi-rs
//!   keeps one such hook, the last one registered.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::OnceLock;
use std::thread::ThreadId;
use std::time::Duration;

use napi::bindgen_prelude::Object;
use napi::{Env, Status};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::logging::bridge::{running_log, DrainOutcome};

/// The Node-API environments that registered this addon in the process.
static ENVIRONMENTS: AtomicU32 = AtomicU32::new(0);

/// The thread of the first registration: the main thread of the process.
static FIRST_THREAD: OnceLock<ThreadId> = OnceLock::new();

/// The main environment ended: no call into JavaScript runs any more.
static ENVIRONMENT_CLOSING: AtomicBool = AtomicBool::new(false);

/// How long the cleanup hook of the main environment waits for the log
/// forwarder to write the records in the channel. A full channel is 8,192
/// records in batches of 256; at tens of microseconds per record, the
/// forwarder empties it well inside this bound.
pub const ENVIRONMENT_LOG_DRAIN_BOUND: Duration = Duration::from_secs(2);

/// Where a later registration comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LaterRegistration {
    /// Another thread: a `worker_threads` isolate.
    OtherThread,
    /// The thread of the first registration: a second load of the file.
    SameThread,
}

/// Count the environment and, for the first one, register the cleanup hook.
/// A later environment's registration throws with its cause.
#[napi(module_exports)]
pub fn register_environment(_exports: Object, env: Env) -> napi::Result<()> {
    let count = ENVIRONMENTS.fetch_add(1, Ordering::AcqRel) + 1;
    let this_thread = std::thread::current().id();
    match count {
        1 => {
            let _first = FIRST_THREAD.set(this_thread);
            let _hook = env.add_env_cleanup_hook((), |()| environment_closed())?;
            Ok(())
        }
        _ => Err(napi::Error::new(
            Status::GenericFailure,
            refusal_text(later_registration(FIRST_THREAD.get(), this_thread), count),
        )),
    }
}

/// Where a later registration on `this_thread` comes from. Pure.
fn later_registration(first: Option<&ThreadId>, this_thread: ThreadId) -> LaterRegistration {
    match first {
        Some(first) if *first == this_thread => LaterRegistration::SameThread,
        Some(_) | None => LaterRegistration::OtherThread,
    }
}

/// The text of the refusal of the registration number `count`. Pure.
fn refusal_text(registration: LaterRegistration, count: u32) -> String {
    let pid = std::process::id();
    match registration {
        LaterRegistration::OtherThread => format!(
            "ADDON_OUTSIDE_MAIN_THREAD: the native addon of horsies runs on the main thread of \
             a process only; this is Node-API environment {count} of process {pid}, on another \
             thread"
        ),
        LaterRegistration::SameThread => format!(
            "the native addon of horsies was loaded twice in process {pid} (Node-API \
             environment {count}); load it once, through the package loader"
        ),
    }
}

/// The cleanup hook of the main environment: no later call into JavaScript
/// runs, every pending call ends with `NotRun { envClosing }`, the log
/// forwarder writes to stderr, and the hook waits for it within
/// [`ENVIRONMENT_LOG_DRAIN_BOUND`]. A drain that does not end writes one
/// stderr line with the count of records left. It returns in every case.
pub fn environment_closed() {
    ENVIRONMENT_CLOSING.store(true, Ordering::SeqCst);
    crate::call::callback::end_pending_calls();
    let Some(log) = running_log() else {
        return;
    };
    log.sink().switch_to_stderr();
    let outcome = log.wait_for_drain(ENVIRONMENT_LOG_DRAIN_BOUND);
    if let Some(line) = undrained_line(outcome) {
        let _write_failure_ignored = writeln!(std::io::stderr(), "{line}");
    }
}

/// The stderr line for a drain that did not end with every record written,
/// or `None` for a drain that did. Pure.
fn undrained_line(outcome: DrainOutcome) -> Option<String> {
    match outcome {
        DrainOutcome::Drained => None,
        DrainOutcome::ForwarderStopped { left } => Some(format!(
            "horsies: the log forwarder is not running; {left} log records did not reach the \
             log sink before the end of the environment"
        )),
        DrainOutcome::BoundReached { left } => Some(format!(
            "horsies: {left} log records were still in the channel after {} ms and did not \
             reach the log sink before the end of the environment",
            ENVIRONMENT_LOG_DRAIN_BOUND.as_millis()
        )),
    }
}

/// Whether the main environment ended. A core call into JavaScript after it
/// is not run.
pub fn environment_closing() -> bool {
    ENVIRONMENT_CLOSING.load(Ordering::SeqCst)
}

/// The count of Node-API environments that registered this addon in the
/// process: 1 in a process that follows the main-thread rule. A test reads
/// it. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<number>")]
pub fn environment_count() -> Envelope<u32> {
    sync_call("environment_count", || {
        Envelope::Value(ENVIRONMENTS.load(Ordering::Acquire))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::bridge::started_log;
    use crate::logging::levels::RecordLevel;

    /// The child test to run, set in the environment of a child process. The
    /// closing flag and the log bridge are per process, so the test that ends
    /// the environment runs in a child process.
    const CHILD_TEST_ENV: &str = "HORSIES_TYPESCRIPT_ENVIRONMENT_CHILD_TEST";

    /// Records that the child test writes before the environment ends.
    const CHILD_RECORD_COUNT: usize = 50;

    const CHILD_TEST_NAME: &str = "process_core::environment::tests::drained_environment_child";

    #[test]
    fn drained_environment_child() {
        if !std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == CHILD_TEST_NAME) {
            return;
        }
        let log = started_log().expect("the log bridge starts");
        for number in 0..CHILD_RECORD_COUNT {
            log.push_bridge_record(RecordLevel::Warning, format!("record-{number:03}"));
        }
        environment_closed();
        assert!(environment_closing());
        assert_eq!(log.sink().drain_state(), DrainOutcome::Drained);
    }

    #[test]
    fn the_end_of_the_environment_waits_for_every_log_record() {
        let binary = std::env::current_exe().expect("the test binary has a path");
        let output = std::process::Command::new(binary)
            .args([
                CHILD_TEST_NAME,
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_TEST_ENV, CHILD_TEST_NAME)
            .output()
            .expect("the child test process runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains(&format!("test {CHILD_TEST_NAME} ... ok")),
            "{stdout}"
        );
        for number in 0..CHILD_RECORD_COUNT {
            assert!(
                stderr.contains(&format!("record-{number:03}")),
                "record {number} is not on stderr: {stderr}"
            );
        }
    }

    #[test]
    fn a_drained_channel_writes_no_line() {
        assert_eq!(undrained_line(DrainOutcome::Drained), None);
    }

    #[test]
    fn a_stopped_forwarder_writes_a_line_with_the_count_left() {
        let line =
            undrained_line(DrainOutcome::ForwarderStopped { left: 7 }).expect("a line is written");
        assert!(line.contains("not running; 7 log records"), "{line}");
    }

    #[test]
    fn a_drain_that_reached_its_bound_writes_a_line_with_the_count_left_and_the_bound() {
        let line =
            undrained_line(DrainOutcome::BoundReached { left: 9 }).expect("a line is written");
        assert!(line.contains("9 log records"), "{line}");
        assert!(line.contains("after 2000 ms"), "{line}");
    }

    #[test]
    fn a_later_registration_on_the_first_thread_is_a_second_load() {
        let this_thread = std::thread::current().id();
        assert_eq!(
            later_registration(Some(&this_thread), this_thread),
            LaterRegistration::SameThread
        );
        assert!(refusal_text(LaterRegistration::SameThread, 2).contains("loaded twice"));
    }

    #[test]
    fn a_later_registration_on_another_thread_is_outside_the_main_thread() {
        let first = std::thread::current().id();
        let other = std::thread::spawn(|| std::thread::current().id())
            .join()
            .expect("the thread ends");
        assert_eq!(
            later_registration(Some(&first), other),
            LaterRegistration::OtherThread
        );
        assert!(refusal_text(LaterRegistration::OtherThread, 2)
            .starts_with("ADDON_OUTSIDE_MAIN_THREAD"));
    }
}
