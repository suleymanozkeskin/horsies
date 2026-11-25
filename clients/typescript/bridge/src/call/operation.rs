//! The operation spawn: how an async bridge function runs a core operation.
//!
//! 1. The sync `#[napi]` function takes the core for the call
//!    (`core_for_call`), reserves one of [`MAX_CALLS_IN_FLIGHT`] slots, and
//!    creates a deferred and its Promise.
//! 2. It spawns the operation inside [`guard`] on the core runtime and
//!    returns the Promise.
//! 3. The spawned task frees the slot and resolves the deferred from the
//!    core thread with the envelope.
//!
//! - A refusal (the core cannot start, is stopped, or the bound is reached)
//!   resolves the Promise at once with its thrown code; the operation does
//!   not start.
//! - The deferred is a referenced threadsafe function until it settles, so
//!   the Promise keeps the event loop alive while the operation runs.
//! - Rust never rejects the Promise: it resolves with an envelope.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};

use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::{Env, JsDeferred};

use crate::call::guard::{guard, CallKind};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::process_core::runtime::{core_for_call, Core};

/// Most core operations in flight per process. Only the main thread of a
/// process loads the addon, so the bound is per process.
pub const MAX_CALLS_IN_FLIGHT: u32 = 65_536;

/// The operations in flight.
static CALLS_IN_FLIGHT: AtomicU32 = AtomicU32::new(0);

/// The resolver of the deferred of an operation: it runs on the JavaScript
/// thread and gives the envelope.
pub type EnvelopeResolver<T> = Box<dyn FnOnce(Env) -> napi::Result<Envelope<T>> + Send>;

/// One reserved slot of [`MAX_CALLS_IN_FLIGHT`]; the drop frees it.
pub struct CallSlot(());

impl Drop for CallSlot {
    fn drop(&mut self) {
        let _previous = CALLS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Reserve one slot, or refuse `subject` with `TOO_MANY_PENDING_CALLS`.
pub fn reserve_slot(subject: &'static str) -> Result<CallSlot, NativeThrown> {
    CALLS_IN_FLIGHT
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |in_flight| {
            match in_flight < MAX_CALLS_IN_FLIGHT {
                true => Some(in_flight + 1),
                false => None,
            }
        })
        .map(|_previous| CallSlot(()))
        .map_err(|_at_the_bound| NativeThrown::TooManyPendingCalls {
            subject: subject.to_owned(),
            bound: MAX_CALLS_IN_FLIGHT,
        })
}

/// Run the operation that `build` makes from the core on the core runtime,
/// and give a Promise of its envelope.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create (the Promise does not exist then). Every other end is an envelope.
pub fn spawn_operation<'env, T, Build, Operation>(
    env: &'env Env,
    subject: &'static str,
    kind: CallKind,
    build: Build,
) -> napi::Result<Object<'env>>
where
    T: ToNapiValue + Send + 'static,
    Build: FnOnce(&'static Core) -> Operation,
    Operation: Future<Output = T> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    match core_for_call().map_err(|refusal| refusal.thrown()) {
        Err(thrown) => resolve(deferred, Envelope::Thrown(thrown)),
        Ok(core) => match reserve_slot(subject) {
            Err(thrown) => resolve(deferred, Envelope::Thrown(thrown)),
            Ok(slot) => {
                let operation = build(core);
                let _detached = core.runtime_handle().spawn(async move {
                    let guarded = guard(core, subject, operation).await;
                    drop(slot);
                    resolve(deferred, guarded.envelope(subject, kind));
                });
            }
        },
    }
    Ok(promise)
}

/// Resolve `deferred` with `envelope`, on any thread.
pub fn resolve<T: ToNapiValue + Send + 'static>(
    deferred: JsDeferred<Envelope<T>, EnvelopeResolver<T>>,
    envelope: Envelope<T>,
) {
    deferred.resolve(Box::new(move |_env| Ok(envelope)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_is_reserved_below_the_bound_and_freed_at_its_drop() {
        let before = CALLS_IN_FLIGHT.load(Ordering::Acquire);
        let slot = reserve_slot("ping_database").expect("a slot below the bound");
        assert_eq!(CALLS_IN_FLIGHT.load(Ordering::Acquire), before + 1);
        drop(slot);
        assert_eq!(CALLS_IN_FLIGHT.load(Ordering::Acquire), before);
    }

    #[test]
    fn slot_bound_child() {
        if std::env::var(SLOT_BOUND_CHILD_ENV).is_err() {
            return;
        }
        let slots: Vec<CallSlot> = (0..MAX_CALLS_IN_FLIGHT)
            .map(|_| reserve_slot("ping_database").expect("a slot below the bound"))
            .collect();
        assert_eq!(
            reserve_slot("ping_database").err(),
            Some(NativeThrown::TooManyPendingCalls {
                subject: "ping_database".to_owned(),
                bound: MAX_CALLS_IN_FLIGHT,
            })
        );
        drop(slots);
        assert!(reserve_slot("ping_database").is_ok());
    }

    /// The child test of the bound runs alone in a child process: the count
    /// of slots is per process, and other tests reserve slots too.
    const SLOT_BOUND_CHILD_ENV: &str = "HORSIES_TYPESCRIPT_SLOT_BOUND_CHILD";

    #[test]
    fn the_slot_past_the_bound_is_refused_with_too_many_pending_calls() {
        let binary = std::env::current_exe().expect("the test binary has a path");
        let test = "call::operation::tests::slot_bound_child";
        let output = std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(SLOT_BOUND_CHILD_ENV, "1")
            .output()
            .expect("the child test process runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{stdout}");
        assert!(stdout.contains(&format!("test {test} ... ok")), "{stdout}");
    }
}
