//! The operator's stop: `SIGTERM` and `SIGINT` ask a service run to stop,
//! as the signal handlers of Python's `horsies worker` and `horsies
//! scheduler` do.

use std::io;

use tokio::signal::unix::{signal, Signal, SignalKind};

/// Wait for the first `SIGTERM` or `SIGINT`, log it, and call `stop` once.
///
/// Installs both handlers on the runtime that polls the future; from then
/// on the default action of the two signals (end the process) does not
/// run in this process. A handler that cannot install is logged with its
/// cause, and the other one still stops the run; with neither, the future
/// never ends and only a stop request through the run's own handle stops
/// it.
pub(crate) async fn stop_on_signals<F>(service: &'static str, stop: F)
where
    F: FnOnce() + Send + 'static,
{
    let terminate = signal(SignalKind::terminate());
    let interrupt = signal(SignalKind::interrupt());
    let name = tokio::select! {
        name = next_signal(terminate, "SIGTERM") => name,
        name = next_signal(interrupt, "SIGINT") => name,
    };
    tracing::info!("received {name}, stopping the {service}");
    stop();
}

/// The name of the signal when it arrives. A handler that did not install
/// is logged and never arrives.
async fn next_signal(handler: io::Result<Signal>, name: &'static str) -> &'static str {
    match handler {
        Ok(mut handler) => match handler.recv().await {
            Some(()) => name,
            // The stream ends only when the runtime's signal driver shut
            // down; the run then ends with the runtime.
            None => std::future::pending().await,
        },
        Err(error) => {
            tracing::warn!(
                error = %error,
                "the {name} handler could not be installed; {name} does not stop the run",
            );
            std::future::pending().await
        }
    }
}
