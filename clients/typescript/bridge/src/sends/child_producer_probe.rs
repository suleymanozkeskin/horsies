//! The test exports of the producer handle of a task child:
//! `probeChildProducer`, and the send and the task reads on its stand-in
//! (`probeEnqueueInChild`, `probeGetResultInChild`, `probeTaskInfoInChild`).
//! The native tests of the producer handle call them; the package does not.
//!
//! - Only the core's child host fills the core's slot of the child producer,
//!   and a Node process cannot run a child host before the child entry of
//!   the addon exists. So a stand-in takes the place of the slot's handle: a
//!   core handle of a manifest that the test gives, with the producer pools
//!   and the client family `TYPESCRIPT`, as the child host builds its
//!   handle.
//! - Each probe export runs the same operation as its export (`enqueue`,
//!   `getResult`, `taskInfo`) on the choice of `ProducerHandle::chosen` for a
//!   child whose child host published the stand-in.
//!
//! Only a build with the cargo feature `test-probes` has this module
//! (`bun run --cwd bridge build:dev`). A published addon does not export it.

use std::sync::Arc;

use horsies_core::handle::{CoreConnection as LazyHandle, HandleClose};
use horsies_core::manifest::parse_producer_manifest;
use horsies_core::{ChildProducer, ClientFamily, PoolProfile};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::spawn_operation;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::CoreHandle;
use crate::sends::producer_handle::ProducerHandle;
use crate::sends::send::{enqueue_on, EnqueueRequest, SendKind};
use crate::sends::task_reads::{result_read_on, task_info_on, WaitBound};
use crate::values::trace_carrier::TraceArgument;

/// A stand-in of the child host's producer handle. JavaScript gets it from
/// `probeChildProducer`; it has no constructor.
#[napi]
pub struct ProbeChildProducer {
    handle: Arc<LazyHandle>,
}

impl ProbeChildProducer {
    /// The handle of a call on `given` in a task child whose child host
    /// published the stand-in. Pure.
    pub(crate) fn chosen_for(&self, given: &CoreHandle) -> ProducerHandle {
        ProducerHandle::chosen(ChildProducer::InChild(Arc::clone(&self.handle)), given)
    }
}

/// How the close of a stand-in ended.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeClose {
    /// No operation connected the stand-in: nothing was closed.
    NeverConnected,
    /// An operation connected the stand-in, and its pools closed.
    Closed,
}

impl ProbeClose {
    /// The name of the core's close outcome. Pure.
    fn of(close: HandleClose) -> Self {
        match close {
            HandleClose::NeverConnected => Self::NeverConnected,
            HandleClose::Closed => Self::Closed,
        }
    }
}

#[napi]
impl ProbeChildProducer {
    /// Close the pools of the stand-in: `CLOSED` when an operation connected
    /// it, `NEVER_CONNECTED` when none did.
    #[napi(ts_return_type = "Promise<NativeEnvelope<ProbeClose>>")]
    pub fn close<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let handle = Arc::clone(&self.handle);
        spawn_operation(
            env,
            "probe_child_producer_close",
            CallKind::Write,
            move |_core| async move { ProbeClose::of(handle.close().await) },
        )
    }
}

/// A stand-in of the child host's producer handle, built from the producer
/// manifest `manifest_json`. Starts nothing. A manifest that the core
/// refuses throws `MANIFEST_REFUSED` with the field path and the cause.
#[napi(ts_return_type = "NativeEnvelope<ProbeChildProducer>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_child_producer(manifest_json: String) -> Envelope<ProbeChildProducer> {
    sync_call("probe_child_producer", || {
        match parse_producer_manifest(&manifest_json) {
            Ok(manifest) => Envelope::Value(ProbeChildProducer {
                handle: Arc::new(LazyHandle::new(
                    manifest,
                    PoolProfile::Producer,
                    ClientFamily::TypeScript,
                )),
            }),
            Err(refused) => Envelope::Thrown(NativeThrown::ManifestRefused {
                field_path: refused.field_path,
                cause: refused.cause,
            }),
        }
    })
}

/// `enqueue` on `handle` in a task child whose child host published
/// `child`.
#[napi(ts_return_type = "Promise<NativeEnvelope<EnqueueOutcome>>")]
pub fn probe_enqueue_in_child<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    child: &ProbeChildProducer,
    request: EnqueueRequest,
    send_kind: SendKind,
    trace: TraceArgument,
) -> napi::Result<Object<'env>> {
    enqueue_on(env, child.chosen_for(handle), request, send_kind, trace)
}

/// `getResult` on `handle` in a task child whose child host published
/// `child`.
#[napi(ts_return_type = "Promise<NativeEnvelope<ResultRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_get_result_in_child<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    child: &ProbeChildProducer,
    task_id: String,
    wait: WaitBound,
) -> napi::Result<Object<'env>> {
    result_read_on(env, child.chosen_for(handle), &task_id, &wait)
}

/// `taskInfo` on `handle` in a task child whose child host published
/// `child`.
#[napi(ts_return_type = "Promise<NativeEnvelope<TaskInfoRead>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn probe_task_info_in_child<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    child: &ProbeChildProducer,
    task_id: String,
) -> napi::Result<Object<'env>> {
    task_info_on(env, child.chosen_for(handle), &task_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_end_of_a_close_has_its_name() {
        assert_eq!(
            ProbeClose::of(HandleClose::NeverConnected),
            ProbeClose::NeverConnected
        );
        assert_eq!(ProbeClose::of(HandleClose::Closed), ProbeClose::Closed);
    }

    #[test]
    fn a_manifest_that_the_core_refuses_throws_manifest_refused() {
        match probe_child_producer("{}".to_owned()) {
            Envelope::Thrown(NativeThrown::ManifestRefused { .. }) => {}
            Envelope::Thrown(other) => panic!("a refused manifest, got {other:?}"),
            Envelope::Value(_) => panic!("a refused manifest, got a stand-in"),
        }
    }
}
