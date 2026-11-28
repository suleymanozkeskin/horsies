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
use crate::call::operation::{guarded_start, resolve, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::process_core::runtime::{core_for_call, Core};

/// Asks a service run to stop.
#[derive(Clone)]
pub struct StopSignal {
    request: Arc<dyn Fn() + Send + Sync>,
}

impl StopSignal {
    /// A signal that calls `request` at each stop request. `request` must be
    /// idempotent and must return at once: it runs on the JavaScript thread.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the worker and scheduler areas build the signal of their runs"
        )
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the requestStop exports of the worker and scheduler areas call it"
        )
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
    spawn_refusable_service_run(env, subject, |core| Ok(build(core)))
}

/// As [`spawn_service_run`], with a build that can refuse the run. The build
/// runs only after the core of the process was taken for the call, so a
/// refusal of the core comes before it and changes nothing that the build
/// owns. A refusal of the build resolves the Promise at once with its
/// thrown code; nothing runs.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create. Every other end is an envelope.
pub fn spawn_refusable_service_run<'env, T, Build, Run>(
    env: &'env Env,
    subject: &'static str,
    build: Build,
) -> napi::Result<Object<'env>>
where
    T: ToNapiValue + Send + 'static,
    Build: FnOnce(&'static Core) -> Result<Run, NativeThrown>,
    Run: Future<Output = T> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    let started = guarded_start(subject, || {
        let core = core_for_call().map_err(|refusal| refusal.thrown())?;
        Ok((core, build(core)?))
    });
    match started {
        Err(thrown) => resolve(deferred, Envelope::Thrown(thrown)),
        Ok((core, run)) => {
            let _detached = core.runtime_handle().spawn(async move {
                resolve(deferred, run_to_envelope(core, subject, run).await);
            });
        }
    }
    Ok(promise)
}

/// Run `run` inside `guard` to its end: its value, `CORE_PANIC`, or
/// `CORE_STOPPED` with an unknown outcome (a run writes).
async fn run_to_envelope<T, Run>(
    core: &'static Core,
    subject: &'static str,
    run: Run,
) -> Envelope<T>
where
    Run: Future<Output = T> + Send,
{
    guard(core, subject, run)
        .await
        .envelope(subject, CallKind::Write)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::sync::watch;

    use super::*;
    use crate::call::thrown::{NativeThrown, StateAfter};
    use crate::process_core::panic_rule::PanicReport;
    use crate::process_core::runtime::standalone_core_for_tests;

    #[test]
    fn a_run_ends_with_its_value_after_a_stop_request() {
        let core = standalone_core_for_tests();
        let (asked, mut seen) = watch::channel(false);
        let stop = StopSignal::new(move || {
            let _previous = asked.send_replace(true);
        });
        let run = async move {
            let _asked_or_closed: bool = seen.wait_for(|asked| *asked).await.is_ok();
            "drained"
        };
        stop.request();
        assert_eq!(
            core.runtime_handle()
                .block_on(run_to_envelope(core, "worker_run", run)),
            Envelope::Value("drained")
        );
    }

    #[test]
    fn a_stop_of_the_core_ends_a_run_with_an_unknown_outcome() {
        let core = standalone_core_for_tests();
        core.stop(PanicReport {
            subject: "thread horsies-log-forwarder".to_owned(),
            cause: "planned background panic".to_owned(),
        });
        let ended: Envelope<u32> = core.runtime_handle().block_on(run_to_envelope(
            core,
            "worker_run",
            std::future::pending::<u32>(),
        ));
        assert_eq!(
            ended,
            Envelope::Thrown(NativeThrown::CoreStopped {
                subject: "thread horsies-log-forwarder".to_owned(),
                cause: "planned background panic".to_owned(),
                state_after: StateAfter::OutcomeUnknown,
            })
        );
    }

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
