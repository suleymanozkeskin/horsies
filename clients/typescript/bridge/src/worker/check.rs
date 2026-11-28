//! `checkLive`: the live steps of `check --live`, after the static check
//! passed (the model: the Rust client's `check_live`, `check.rs`).
//!
//! 1. Connectivity, the core's rule for every client
//!    (`PostgresBroker::check_connectivity`): connect the handle's broker,
//!    `SELECT 1` on the main URL, then on the session URL when it differs,
//!    then, in PgBouncer transaction mode, a LISTEN probe on the session
//!    URL.
//! 2. Worker connections: each worker profile of the config against the
//!    server's connection limit (none through a transaction pooler)
//!    (`check_worker_connections`).
//!
//! The check stops at the first step that refuses. It reads; it writes
//! nothing but the LISTEN probe's one NOTIFY. It installs no schema.

use std::sync::Arc;

use horsies_core::{check_worker_connections, AppConfig, ConnectivityRefusal, PostgresBroker};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::NativeThrown;
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::sends::refusals::CoreRefusal;
use crate::worker::run::{coded_refusal, thrown_now};

/// The subject of the live check in its refusals.
const LIVE_CHECK_SUBJECT: &str = "check_live";

/// The connectivity step that refused.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectivityStep {
    /// The handle could not build its broker.
    Connect,
    /// The core pools could not open with the broker's settings.
    PoolSettings,
    /// `SELECT 1` on the main URL failed.
    MainUrl,
    /// `SELECT 1` on the session URL failed.
    SessionUrl,
    /// The LISTEN probe of PgBouncer transaction mode got no NOTIFY.
    Listener,
}

impl ConnectivityStep {
    /// The step of a refusal of the core's check. Pure.
    fn of(refused: &ConnectivityRefusal) -> Self {
        match refused {
            ConnectivityRefusal::PoolSettings(_) => Self::PoolSettings,
            ConnectivityRefusal::MainUnreachable(_) => Self::MainUrl,
            ConnectivityRefusal::SessionUnreachable(_) => Self::SessionUrl,
            ConnectivityRefusal::ListenerNotCapable(_) => Self::Listener,
        }
    }
}

/// How the live check ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveCheckOutcome {
    /// Every step passed.
    Passed,
    /// The first connectivity step that refused, and the core's cause. No
    /// later step ran.
    ConnectivityRefused {
        step: ConnectivityStep,
        message: String,
    },
    /// One refusal per worker profile that does not fit the server's
    /// connection limit, in profile name order, or one refusal when the
    /// limit was not read. Each has the code `HRS-219`.
    ConnectionsRefused { refusals: Vec<CoreRefusal> },
}

/// Run the live steps on `handle` for the worker profiles of the accepted
/// app config `appConfigJson`. A config text that does not read resolves
/// with `CLIENT_DEFECT` at once: `Horsies.create` accepted it.
#[napi(ts_return_type = "Promise<NativeEnvelope<LiveCheckOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_live<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    app_config_json: String,
) -> napi::Result<Object<'env>> {
    let config = guarded_start(LIVE_CHECK_SUBJECT, || {
        serde_json::from_str::<AppConfig>(&app_config_json).map_err(|error| {
            NativeThrown::ClientDefect {
                subject: "appConfigJson".to_owned(),
                rule: format!("the accepted config text does not read: {error}"),
            }
        })
    });
    match config {
        Err(thrown) => thrown_now::<LiveCheckOutcome>(env, thrown),
        Ok(config) => {
            let connection = handle.connection();
            spawn_operation(env, LIVE_CHECK_SUBJECT, CallKind::Read, move |_core| {
                live_check(connection, config)
            })
        }
    }
}

/// The live steps, in order; the first refusal ends the check.
async fn live_check(connection: BrokerConnection, config: AppConfig) -> LiveCheckOutcome {
    let broker: Arc<PostgresBroker> = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return LiveCheckOutcome::ConnectivityRefused {
                step: ConnectivityStep::Connect,
                message: error.to_string(),
            }
        }
    };
    if let Err(refused) = broker.check_connectivity().await {
        return LiveCheckOutcome::ConnectivityRefused {
            step: ConnectivityStep::of(&refused),
            message: refused.to_string(),
        };
    }
    connections_outcome(&check_worker_connections(&config, &broker).await)
}

/// The outcome of the connection check's errors. Pure.
fn connections_outcome(errors: &[horsies_core::HorsiesError]) -> LiveCheckOutcome {
    match errors.is_empty() {
        true => LiveCheckOutcome::Passed,
        false => LiveCheckOutcome::ConnectionsRefused {
            refusals: errors.iter().map(coded_refusal).collect(),
        },
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{CheckInterruption, HorsiesError, HorsiesErrorCode, ListenProbeFailure};

    use super::*;

    #[test]
    fn a_listener_refusal_is_the_step_listener() {
        let refused = ConnectivityRefusal::ListenerNotCapable(ListenProbeFailure::Interrupted(
            CheckInterruption::Reconnected,
        ));
        assert_eq!(ConnectivityStep::of(&refused), ConnectivityStep::Listener);
    }

    #[test]
    fn no_connection_error_is_a_pass() {
        assert_eq!(connections_outcome(&[]), LiveCheckOutcome::Passed);
    }

    #[test]
    fn each_connection_error_is_one_refusal_with_its_code() {
        let errors = vec![HorsiesError::new("profile io needs 41 connections")
            .with_code(HorsiesErrorCode::CheckWorkerConnections)];
        let LiveCheckOutcome::ConnectionsRefused { refusals } = connections_outcome(&errors) else {
            panic!("an error refuses");
        };
        assert_eq!(refusals, vec![coded_refusal(&errors[0])]);
        assert_eq!(refusals[0].code, "HRS-219");
    }
}
