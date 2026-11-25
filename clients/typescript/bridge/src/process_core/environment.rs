//! The Node-API environments of the process.
//!
//! - Only the main thread of a process loads the addon: the package's loader
//!   checks `isMainThread` first. This module also counts the environments
//!   that register the addon, so a load that bypasses the loader (a
//!   `worker_threads` isolate that opens the file itself) fails at once: its
//!   registration throws, and its `require` gets no export object.
//! - The first environment registers one cleanup hook. When the environment
//!   ends, the hook marks the environment closing (a later core call into
//!   JavaScript is not run) and switches the log forwarder to stderr. It does
//!   not stop the core runtime: the runtime lives in a static, and a drop
//!   would wait for its threads.
//! - This is the one `#[napi(module_exports)]` function of the addon: napi-rs
//!   keeps one such hook, the last one registered.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use napi::bindgen_prelude::Object;
use napi::{Env, Status};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::logging::bridge::running_log;

/// The Node-API environments that registered this addon in the process.
static ENVIRONMENTS: AtomicU32 = AtomicU32::new(0);

/// The main environment ended: no call into JavaScript runs any more.
static ENVIRONMENT_CLOSING: AtomicBool = AtomicBool::new(false);

/// Count the environment and, for the first one, register the cleanup hook.
/// A second environment's registration throws `ADDON_OUTSIDE_MAIN_THREAD`.
#[napi(module_exports)]
pub fn register_environment(_exports: Object, env: Env) -> napi::Result<()> {
    let count = ENVIRONMENTS.fetch_add(1, Ordering::AcqRel) + 1;
    match count {
        1 => {
            let _hook = env.add_env_cleanup_hook((), |()| environment_closed())?;
            Ok(())
        }
        _ => Err(napi::Error::new(
            Status::GenericFailure,
            format!(
                "ADDON_OUTSIDE_MAIN_THREAD: the native addon of horsies runs on the main thread \
                 of a process only; this is Node-API environment {count} of process {}",
                std::process::id()
            ),
        )),
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
#[allow(
    dead_code,
    reason = "the callback primitive of the call area reads it before each call into JavaScript"
)]
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
