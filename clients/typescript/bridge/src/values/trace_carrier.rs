//! The trace argument of a send, a workflow start or a rerun.
//!
//! The package's propagator injects the carrier for the span of the call and
//! passes the trace id of that span. The core checks the bounds of the
//! carrier at the send: a carrier that breaks a bound is not stored, and it
//! never fails the send (`horsies_core::telemetry::trace_carrier`).

use std::collections::BTreeMap;

use horsies_core::telemetry::trace_carrier::{PropagatedTrace, SendTrace};
use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;

/// The trace argument of a call.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
pub enum TraceArgument {
    /// The call carries no trace: tracing is off, or the propagator wrote
    /// no key.
    NoCarrier,
    /// The map that the propagator injected into, and the trace id of the
    /// propagated span (16 bytes for a valid id).
    Carrier {
        carrier: BTreeMap<String, String>,
        trace_id: Uint8Array,
    },
}

impl TraceArgument {
    /// The core's trace of the call. The bytes of the trace id are copied
    /// out of the JavaScript array, so the caller calls it on the JavaScript
    /// thread, before the operation leaves that thread. Pure.
    #[allow(
        dead_code,
        reason = "the send, workflow and rerun operations of later areas call it"
    )]
    pub fn send_trace(self) -> SendTrace {
        match self {
            Self::NoCarrier => SendTrace::NoCarrier,
            Self::Carrier { carrier, trace_id } => SendTrace::Propagated(PropagatedTrace {
                carrier,
                trace_id: trace_id.to_vec(),
            }),
        }
    }
}
