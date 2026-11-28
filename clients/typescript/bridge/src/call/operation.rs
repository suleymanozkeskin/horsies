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
//! - Steps 1 and 2 after the deferred run on the JavaScript thread inside
//!   `catch_unwind`: a panic there (in the start of the core, or in the
//!   area's build of the operation) resolves the Promise with
//!   `CORE_PANIC`, which the package raises under the defect rule. A panic
//!   that unwinds through Node-API would abort the process with no record.
//! - The deferred is a referenced threadsafe function until it settles, so
//!   the Promise keeps the event loop alive while the operation runs.
//! - Rust never rejects the Promise: it resolves with an envelope.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};

use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::{Env, JsDeferred};

use crate::call::guard::{guard, CallKind};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::process_core::panic_rule::take_panic_cause;
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
    match guarded_start(subject, || start_operation(subject, build)) {
        Err(thrown) => resolve(deferred, Envelope::Thrown(thrown)),
        Ok(Started {
            core,
            slot,
            operation,
        }) => {
            let _detached = core.runtime_handle().spawn(async move {
                let guarded = guard(core, subject, operation).await;
                drop(slot);
                resolve(deferred, guarded.envelope(subject, kind));
            });
        }
    }
    Ok(promise)
}

/// An operation that may run: its core, its slot, and its future.
struct Started<Operation> {
    core: &'static Core,
    slot: CallSlot,
    operation: Operation,
}

/// The steps of an operation on the JavaScript thread: the core, a slot, and
/// the area's build of the future.
fn start_operation<Build, Operation>(
    subject: &'static str,
    build: Build,
) -> Result<Started<Operation>, NativeThrown>
where
    Build: FnOnce(&'static Core) -> Operation,
{
    let core = core_for_call().map_err(|refusal| refusal.thrown())?;
    let slot = reserve_slot(subject)?;
    let operation = build(core);
    Ok(Started {
        core,
        slot,
        operation,
    })
}

/// Run the JavaScript-thread steps of a call inside `catch_unwind`: a panic
/// there is `CORE_PANIC` with `subject` and the cause, not an unwind through
/// Node-API.
pub fn guarded_start<Value>(
    subject: &'static str,
    start: impl FnOnce() -> Result<Value, NativeThrown>,
) -> Result<Value, NativeThrown> {
    match std::panic::catch_unwind(AssertUnwindSafe(start)) {
        Ok(started) => started,
        Err(payload) => Err(NativeThrown::CorePanic {
            subject: subject.to_owned(),
            cause: take_panic_cause(payload.as_ref()),
        }),
    }
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
    fn a_start_that_returns_gives_its_value() {
        assert_eq!(guarded_start("ping_database", || Ok(3_u32)), Ok(3));
    }

    #[test]
    fn a_start_that_refuses_gives_its_thrown_code() {
        let refused = NativeThrown::RuntimeStartFailed {
            cause: "no threads".to_owned(),
        };
        assert_eq!(
            guarded_start::<u32>("ping_database", || Err(refused.clone())),
            Err(refused)
        );
    }

    #[test]
    fn a_start_that_panics_gives_core_panic_with_the_subject() {
        match guarded_start::<u32>("ping_database", || panic!("planned start panic")) {
            Err(NativeThrown::CorePanic { subject, cause }) => {
                assert_eq!(subject, "ping_database");
                assert!(cause.starts_with("planned start panic"), "{cause}");
            }
            other => panic!("a panic is CORE_PANIC, got {other:?}"),
        }
    }

    /// The child test to run, set in the environment of a child process. The
    /// count of slots is per process and other tests reserve slots too, so
    /// the tests that count it from a known start run alone in a child.
    const CHILD_TEST_ENV: &str = "HORSIES_TYPESCRIPT_OPERATION_CHILD_TEST";

    fn in_child(test: &str) -> bool {
        std::env::var(CHILD_TEST_ENV).is_ok_and(|name| name == test)
    }

    /// Run one test of this binary in a child process and check that it
    /// passed.
    fn assert_child_passed(test: &str) {
        let binary = std::env::current_exe().expect("the test binary has a path");
        let output = std::process::Command::new(binary)
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_TEST_ENV, test)
            .output()
            .expect("the child test process runs");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains(&format!("test {test} ... ok")), "{stdout}");
    }

    #[test]
    fn panicked_start_child() {
        if !in_child("call::operation::tests::panicked_start_child") {
            return;
        }
        let ended = guarded_start::<u32>("ping_database", || {
            let _slot = reserve_slot("ping_database")?;
            panic!("planned panic after the slot")
        });
        assert!(matches!(ended, Err(NativeThrown::CorePanic { .. })));
        assert_eq!(CALLS_IN_FLIGHT.load(Ordering::Acquire), 0);
    }

    #[test]
    fn a_slot_of_a_start_that_panics_is_freed() {
        assert_child_passed("call::operation::tests::panicked_start_child");
    }

    #[test]
    fn slot_bound_child() {
        if !in_child("call::operation::tests::slot_bound_child") {
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

    #[test]
    fn the_slot_past_the_bound_is_refused_with_too_many_pending_calls() {
        assert_child_passed("call::operation::tests::slot_bound_child");
    }
}
