//! The trace carrier between the client's propagator and the core: the
//! injection at a send and the extraction in a worker child.
//!
//! The core stores each key as the propagator wrote it (the X-Ray
//! propagator of Python writes `X-Amzn-Trace-Id`). The injection here keeps
//! each key as written; the extraction finds a key as written first, then
//! with ASCII case ignored, as HTTP header names compare, so a carrier
//! that another client's propagator wrote reads too.

use std::collections::BTreeMap;

use horsies_core::telemetry::trace_carrier::{PropagatedTrace, SendTrace};
use horsies_core::BodyCarrier;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;

/// A carrier map that a propagator writes into, each key as written.
struct CarrierInjector<'a>(&'a mut BTreeMap<String, String>);

impl Injector for CarrierInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let _replaced = self.0.insert(key.to_owned(), value);
    }
}

/// A stored carrier map that a propagator reads.
struct CarrierExtractor<'a>(&'a BTreeMap<String, String>);

impl Extractor for CarrierExtractor<'_> {
    /// The value of `key` as written, else of the one key that equals it
    /// with ASCII case ignored. The carrier holds at most 8 keys (the
    /// core's bound), so the search is short.
    fn get(&self, key: &str) -> Option<&str> {
        match self.0.get(key) {
            Some(value) => Some(value.as_str()),
            None => self
                .0
                .iter()
                .find(|(stored, _)| stored.eq_ignore_ascii_case(key))
                .map(|(_, value)| value.as_str()),
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// The trace that a send hands the core for the span of `context`: the map
/// that `propagator` injects for `context`, with the trace id of the span.
/// [`SendTrace::NoCarrier`] when the span context is not valid (no SDK, or
/// a span that is not sampled and has no parent) or when the propagator
/// writes no key. The core checks the bounds of the map. Pure.
pub(super) fn send_trace_of(context: &Context, propagator: &dyn TextMapPropagator) -> SendTrace {
    let span_context = context.span().span_context().clone();
    match span_context.is_valid() {
        false => return SendTrace::NoCarrier,
        true => {}
    }
    let mut carrier = BTreeMap::new();
    propagator.inject_context(context, &mut CarrierInjector(&mut carrier));
    match carrier.is_empty() {
        true => SendTrace::NoCarrier,
        false => SendTrace::Propagated(PropagatedTrace {
            carrier,
            trace_id: span_context.trace_id().to_bytes().to_vec(),
        }),
    }
}

/// Where the attempt span of a run takes its parent from.
#[derive(Debug)]
pub(super) enum AttemptParent {
    /// The context that the propagator extracted from the stored carrier.
    /// It holds no span when the carrier has no key that the propagator
    /// reads: the attempt span is then a root span.
    Extracted(Context),
    /// The task row has no carrier: the attempt span is a root span.
    NoCarrier,
}

/// The parent of the attempt span of a run whose stored carrier is
/// `carrier`, read with `propagator`. Pure.
pub(super) fn attempt_parent_of(
    carrier: &BodyCarrier,
    propagator: &dyn TextMapPropagator,
) -> AttemptParent {
    match carrier {
        BodyCarrier::NoCarrier => AttemptParent::NoCarrier,
        BodyCarrier::Carrier(map) => {
            AttemptParent::Extracted(propagator.extract(&CarrierExtractor(map)))
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    use super::*;

    const TRACE_ID: u128 = 0x0af7_6519_16cd_43dd_8448_eb21_1c80_319c;
    const SPAN_ID: u64 = 0xb7ad_6b71_6920_3331;
    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn sampled_context() -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(TRACE_ID),
            SpanId::from(SPAN_ID),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    /// The carrier of a valid span is the map that the propagator wrote,
    /// with the 16 bytes of the trace id.
    #[test]
    fn a_valid_span_gives_the_map_of_the_propagator_and_its_trace_id() {
        let trace = send_trace_of(&sampled_context(), &TraceContextPropagator::new());
        assert_eq!(
            trace,
            SendTrace::Propagated(PropagatedTrace {
                carrier: BTreeMap::from([
                    ("traceparent".to_owned(), TRACEPARENT.to_owned()),
                    ("tracestate".to_owned(), String::new()),
                ]),
                trace_id: TRACE_ID.to_be_bytes().to_vec(),
            })
        );
    }

    /// No span (no SDK, no parent) gives no carrier.
    #[test]
    fn a_context_with_no_valid_span_gives_no_carrier() {
        assert_eq!(
            send_trace_of(&Context::new(), &TraceContextPropagator::new()),
            SendTrace::NoCarrier
        );
    }

    /// The injection keeps each key as the propagator wrote it.
    #[test]
    fn the_injection_keeps_each_key_as_written() {
        let mut carrier = BTreeMap::new();
        CarrierInjector(&mut carrier).set("X-Amzn-Trace-Id", "Root=1".to_owned());
        assert_eq!(
            carrier,
            BTreeMap::from([("X-Amzn-Trace-Id".to_owned(), "Root=1".to_owned())])
        );
    }

    /// The extraction finds a key as written, then with ASCII case ignored.
    #[test]
    fn the_extraction_finds_a_key_as_written_then_with_case_ignored() {
        let carrier = BTreeMap::from([
            ("X-Amzn-Trace-Id".to_owned(), "Root=1".to_owned()),
            ("traceparent".to_owned(), TRACEPARENT.to_owned()),
        ]);
        let extractor = CarrierExtractor(&carrier);
        assert_eq!(extractor.get("traceparent"), Some(TRACEPARENT));
        assert_eq!(extractor.get("x-amzn-trace-id"), Some("Root=1"));
        assert_eq!(extractor.get("tracestate"), None);
        assert_eq!(extractor.keys(), vec!["X-Amzn-Trace-Id", "traceparent"]);
    }

    /// A stored carrier gives the context of the span that it names; no
    /// carrier gives no parent.
    #[test]
    fn a_stored_carrier_gives_the_context_of_its_span() {
        let carrier = BodyCarrier::Carrier(BTreeMap::from([(
            "traceparent".to_owned(),
            TRACEPARENT.to_owned(),
        )]));
        match attempt_parent_of(&carrier, &TraceContextPropagator::new()) {
            AttemptParent::Extracted(context) => {
                let span = context.span();
                let span_context = span.span_context();
                assert_eq!(span_context.trace_id(), TraceId::from(TRACE_ID));
                assert_eq!(span_context.span_id(), SpanId::from(SPAN_ID));
                assert!(span_context.is_remote());
            }
            AttemptParent::NoCarrier => panic!("a stored carrier is extracted"),
        }
        assert!(matches!(
            attempt_parent_of(&BodyCarrier::NoCarrier, &TraceContextPropagator::new()),
            AttemptParent::NoCarrier
        ));
    }
}
