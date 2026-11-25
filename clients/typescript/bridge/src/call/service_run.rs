//! The run form of a core call: a service run that lasts until a stop
//! request, such as a worker or the scheduler.
//!
//! - `run()` of the area returns a Promise of the end record of the run
//!   ([`spawn_service_run`]). The run is spawned on the core runtime inside
//!   `guard`: a panic in the run becomes `CORE_PANIC`, and a stop of the core
//!   `CORE_STOPPED` with an unknown outcome. It takes no slot of the bound of
//!   calls in flight: it lasts for the life of the service.
//! - [`StopSignal`] asks the run to stop: `requestStop()` of the area is
//!   sync, idempotent, returns at once, and works from any thread. A request
//!   before the run starts ends the run after its start. The run ends at its
//!   own next boundary and bounds its own drain.
//! - JavaScript owns the signals: the package installs the signal handlers
//!   and calls `requestStop()`. The bridge installs no signal handler.
//! - The Promise keeps the event loop alive until the run ends.

use std::future::Future;
use std::sync::Arc;

use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::Env;

use crate::call::guard::{guard, CallKind};
use crate::call::operation::{resolve, EnvelopeResolver};
use crate::call::thrown::Envelope;
use crate::process_core::runtime::{core_for_call, Core};

/// Asks a service run to stop.
#[derive(Clone)]
pub struct StopSignal {
    request: Arc<dyn Fn() + Send + Sync>,
}

impl StopSignal {
    /// A signal that calls `request` at each stop request. `request` must be
    /// idempotent and must return at once: it runs on the JavaScript thread.
    #[allow(
        dead_code,
        reason = "the worker and scheduler areas build the signal of their runs"
    )]
    pub fn new<F>(request: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self {
            request: Arc::new(request),
        }
    }

    /// Ask the run to stop.
    #[allow(
        dead_code,
        reason = "the requestStop exports of the worker and scheduler areas call it"
    )]
    pub fn request(&self) {
        (self.request)();
    }
}

/// Run the service run that `build` makes from the core on the core runtime,
/// and give a Promise of its envelope. A stop of the core ends it with an
/// unknown outcome.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create. Every other end is an envelope.
#[allow(
    dead_code,
    reason = "the run exports of the worker and scheduler areas call it"
)]
pub fn spawn_service_run<'env, T, Build, Run>(
    env: &'env Env,
    subject: &'static str,
    build: Build,
) -> napi::Result<Object<'env>>
where
    T: ToNapiValue + Send + 'static,
    Build: FnOnce(&'static Core) -> Run,
    Run: Future<Output = T> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    match core_for_call() {
        Err(refusal) => resolve(deferred, Envelope::Thrown(refusal.thrown())),
        Ok(core) => {
            let run = build(core);
            let _detached = core.runtime_handle().spawn(async move {
                let guarded = guard(core, subject, run).await;
                resolve(deferred, guarded.envelope(subject, CallKind::Write));
            });
        }
    }
    Ok(promise)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[test]
    fn every_request_reaches_the_run_and_returns_at_once() {
        let requests = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&requests);
        let stop = StopSignal::new(move || {
            let _previous = seen.fetch_add(1, Ordering::SeqCst);
        });
        let copy = stop.clone();
        stop.request();
        copy.request();
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }
}
