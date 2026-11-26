//! The test exports of a workflow start and a workflow handle read in a task
//! child: `probeStartWorkflowInChild` and `probeWorkflowStatusInChild`. The
//! native tests of the producer handle call them; the package does not.
//!
//! - As for the sends (`sends::child_producer_probe`), a stand-in takes the
//!   place of the handle that the child host publishes. Each probe runs the
//!   same operation as its export (`startWorkflow`, `workflowStatus`) on the
//!   choice of `ProducerHandle::chosen` for a child whose child host
//!   published the stand-in. A start keeps the child spec source of the
//!   given handle.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.

use horsies_core::HandleOperation;
use napi::bindgen_prelude::{BigInt, Object};
use napi::Env;
use napi_derive::napi;

use crate::call::guard::CallKind;
use crate::call::thrown::NativeThrown;
use crate::handle::core_handle::CoreHandle;
use crate::sends::child_producer_probe::ProbeChildProducer;
use crate::values::trace_carrier::TraceArgument;
use crate::workflows::handle_ops::{spawn_checked, status_operation, workflow_id_of};
use crate::workflows::start_resume::{start_workflow_on, NativeClientSpec, StartCall};

/// `startWorkflow` on `handle` in a task child whose child host published
/// `child`.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkflowStartOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    reason = "napi-rs passes the arguments of a JavaScript call by value; the probe takes the export's arguments"
)]
pub fn probe_start_workflow_in_child<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    child: &ProbeChildProducer,
    spec: &NativeClientSpec,
    workflow_id: String,
    sent_at_micros: BigInt,
    trace: TraceArgument,
    resend_on_transient_err: bool,
) -> napi::Result<Object<'env>> {
    let start = StartCall {
        spec,
        workflow_id,
        sent_at_micros,
        trace,
        resend_on_transient_err,
    };
    start_workflow_on(env, child.chosen_for(handle), handle, start)
}

/// `workflowStatus` on `handle` in a task child whose child host published
/// `child`.
#[napi(ts_return_type = "Promise<NativeEnvelope<WorkflowStatusRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_workflow_status_in_child<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    child: &ProbeChildProducer,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    spawn_checked(
        env,
        HandleOperation::Status.subject(),
        CallKind::Read,
        child.chosen_for(handle),
        || workflow_id_of(&workflow_id).map_err(NativeThrown::from),
        status_operation,
    )
}
