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
//!   JavaScript is not run) and switches the log forwarder to stderr. It does
//!   not stop the core runtime: the runtime lives in a static, and a drop
//!   would wait for its threads.
//! - This is the one `#[napi(module_exports)]` function of the addon: napi-rs
//!   keeps one such hook, the last one registered.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::OnceLock;
use std::thread::ThreadId;

use napi::bindgen_prelude::Object;
use napi::{Env, Status};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::logging::bridge::running_log;

/// The Node-API environments that registered this addon in the process.
static ENVIRONMENTS: AtomicU32 = AtomicU32::new(0);

/// The thread of the first registration: the main thread of the process.
static FIRST_THREAD: OnceLock<ThreadId> = OnceLock::new();

/// The main environment ended: no call into JavaScript runs any more.
static ENVIRONMENT_CLOSING: AtomicBool = AtomicBool::new(false);

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

/// The cleanup hook of the main environment.
fn environment_closed() {
    ENVIRONMENT_CLOSING.store(true, Ordering::SeqCst);
    if let Some(log) = running_log() {
        log.sink().switch_to_stderr();
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
