//! `rerunTask`: the rerun in a transaction of the app's handle (Rust
//! `Horsies::rerun_task`).
//!
//! The handle connects, and installs the schema once per handle, as a send
//! does; then `PostgresBroker::rerun_task` checks the client family, runs
//! the source read and, for an eligible source, the write in one
//! transaction on the main core pool, and commits it.

use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{RegisteredTaskCodes, RerunCommand, RerunEnqueuePolicy};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, resolve, spawn_operation, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::facades::rerun::arguments::{
    registered_task_codes, rerun_command, rerun_policy, RegisteredTaskCodesArg, RerunCommandArg,
    RerunPolicyArg,
};
use crate::facades::rerun::records::{
    app_failure, connect_failed, AppRerunFailure, RerunOutcomeRecord, RERUN_SUBJECT,
};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::values::boundary::BoundaryRefusal;
use crate::values::trace_carrier::TraceArgument;

/// How one rerun in the app's transaction ended.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppRerunAnswer {
    Done { outcome: RerunOutcomeRecord },
    Failed { error: AppRerunFailure },
}

/// The command, the policy, the trace and the declared custom codes of one
/// rerun, as the core takes them.
struct AppRequest {
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
    trace: SendTrace,
    codes: RegisteredTaskCodes,
}

/// Rerun `command` with `policy` in a transaction of `handle`, and commit
/// it. The new task stores the carrier of `trace`. `codes` are the declared
/// custom codes of the calling app's tasks: the stored retry list of the
/// source is checked against them before the write. Writes: a stop of the
/// core during the call leaves the outcome unknown.
///
/// A value that breaks a boundary rule resolves with `CLIENT_DEFECT`, and
/// nothing runs.
#[napi(ts_return_type = "Promise<NativeEnvelope<AppRerunAnswer>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn rerun_task<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    command: RerunCommandArg,
    policy: RerunPolicyArg,
    trace: TraceArgument,
    codes: RegisteredTaskCodesArg,
) -> napi::Result<Object<'env>> {
    let started = guarded_start(RERUN_SUBJECT, || {
        app_request(command, &policy, trace, &codes).map_err(NativeThrown::from)
    });
    match started {
        Ok(request) => {
            let connection = handle.connection();
            spawn_operation(env, RERUN_SUBJECT, CallKind::Write, move |_core| {
                rerun_operation(connection, request)
            })
        }
        Err(thrown) => {
            let (deferred, promise) = env
                .create_deferred::<Envelope<AppRerunAnswer>, EnvelopeResolver<AppRerunAnswer>>()?;
            resolve(deferred, Envelope::Thrown(thrown));
            Ok(promise)
        }
    }
}

/// The core's request of the arguments of one call. Pure.
///
/// Errors: the [`BoundaryRefusal`] of the first value that breaks a
/// boundary rule.
fn app_request(
    command: RerunCommandArg,
    policy: &RerunPolicyArg,
    trace: TraceArgument,
    codes: &RegisteredTaskCodesArg,
) -> Result<AppRequest, BoundaryRefusal> {
    Ok(AppRequest {
        command: rerun_command(command)?,
        policy: rerun_policy(policy)?,
        trace: trace.send_trace()?,
        codes: registered_task_codes(codes)?,
    })
}

/// Connect and install the schema through the handle, then rerun. The
/// trace carrier is checked here, on the core runtime, so a carrier that
/// breaks a bound logs its one warning through the core's logger.
async fn rerun_operation(connection: BrokerConnection, request: AppRequest) -> AppRerunAnswer {
    let broker = match connection.lazy_handle().initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return AppRerunAnswer::Failed {
                error: connect_failed(&error),
            }
        }
    };
    let command = request.command.with_trace(request.trace);
    match broker
        .rerun_task(command, request.policy, &request.codes)
        .await
    {
        Ok(outcome) => AppRerunAnswer::Done {
            outcome: outcome.into(),
        },
        Err(error) => AppRerunAnswer::Failed {
            error: app_failure(error),
        },
    }
}
