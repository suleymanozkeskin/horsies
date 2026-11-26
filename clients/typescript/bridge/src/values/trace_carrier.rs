//! The trace argument of a send, a workflow start or a rerun.
//!
//! The package's propagator injects the carrier for the span of the call and
//! passes the trace id of that span as lower-case hexadecimal text (the form
//! of the OpenTelemetry API). The core checks the bounds of the carrier at the
//! send: a carrier that breaks a bound is not stored, and it never fails the
//! send (`horsies_core::telemetry::trace_carrier`).
//!
//! The argument holds no JavaScript memory, so an operation can move it to a
//! core thread.

use std::collections::BTreeMap;

use horsies_core::telemetry::trace_carrier::{PropagatedTrace, SendTrace};
use napi_derive::napi;

use crate::values::boundary::{bytes_of_hex, BoundaryRefusal};

/// The trace argument of a call.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceArgument {
    /// The call carries no trace: tracing is off, or the propagator wrote
    /// no key.
    NoCarrier,
    /// The map that the propagator injected into, and the trace id of the
    /// propagated span as lower-case hexadecimal text (32 characters for a
    /// valid id).
    Carrier {
        carrier: BTreeMap<String, String>,
        trace_id_hex: String,
    },
}

impl TraceArgument {
    /// The core's trace of the call, or the refusal of a trace id that is not
    /// lower-case hexadecimal text. Pure.
    #[allow(
        dead_code,
        reason = "the send, workflow and rerun operations of later areas call it"
    )]
    pub fn send_trace(self) -> Result<SendTrace, BoundaryRefusal> {
        match self {
            Self::NoCarrier => Ok(SendTrace::NoCarrier),
            Self::Carrier {
                carrier,
                trace_id_hex,
            } => Ok(SendTrace::Propagated(PropagatedTrace {
                carrier,
                trace_id: bytes_of_hex("traceIdHex", &trace_id_hex)?,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_carrier_is_no_carrier() {
        assert_eq!(
            TraceArgument::NoCarrier.send_trace(),
            Ok(SendTrace::NoCarrier)
        );
    }

    #[test]
    fn a_carrier_keeps_its_map_and_its_trace_id_bytes() {
        let carrier = BTreeMap::from([(
            "traceparent".to_owned(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_owned(),
        )]);
        let traced = TraceArgument::Carrier {
            carrier: carrier.clone(),
            trace_id_hex: "0af7651916cd43dd8448eb211c80319c".to_owned(),
        };
        assert_eq!(
            traced.send_trace(),
            Ok(SendTrace::Propagated(PropagatedTrace {
                carrier,
                trace_id: vec![
                    0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c,
                    0x80, 0x31, 0x9c
                ],
            }))
        );
    }

    #[test]
    fn a_trace_id_that_is_not_hexadecimal_text_is_refused() {
        let traced = TraceArgument::Carrier {
            carrier: BTreeMap::new(),
            trace_id_hex: "0AF7".to_owned(),
        };
        assert!(matches!(traced.send_trace(), Err(refusal) if refusal.subject == "traceIdHex"));
    }
}
