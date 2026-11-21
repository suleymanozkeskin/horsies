//! The trace carrier vectors (`trace_carrier.json`) through the core.
//!
//! The core is the one writer of the carrier text: each client hands it the
//! map that its propagator wrote, and the core checks it and encodes it
//! (`telemetry::trace_carrier::check_carrier`, `carrier_text`). Each vector
//! is the map, the trace id and the exact stored text; each refused row is a
//! map or a trace id that the check refuses, with the variant it gives.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::vector_file;
use crate::telemetry::trace_carrier::{
    carrier_text, check_carrier, CarrierCheck, CarrierRefusal, PropagatedTrace,
};

#[derive(Deserialize)]
struct TraceCarrierForm {
    vectors: Vec<CarrierVector>,
    refused: Vec<RefusedCarrier>,
}

#[derive(Deserialize)]
struct CarrierVector {
    name: String,
    carrier: BTreeMap<String, String>,
    trace_id: String,
    text: String,
}

#[derive(Deserialize)]
struct RefusedCarrier {
    name: String,
    carrier: BTreeMap<String, String>,
    trace_id: String,
    refusal: String,
}

fn form() -> TraceCarrierForm {
    serde_json::from_str(&vector_file("trace_carrier.json")).expect("trace_carrier.json parses")
}

/// The bytes of the hex text `hex` (two lowercase digits per byte).
fn hex_bytes(name: &str, hex: &str) -> Vec<u8> {
    assert!(
        hex.len().is_multiple_of(2),
        "{name}: {hex} has an odd number of digits"
    );
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16)
                .unwrap_or_else(|error| panic!("{name}: {hex} is not hex: {error}"))
        })
        .collect()
}

/// The variant name of a refusal, as the vector file names it.
fn refusal_name(refusal: &CarrierRefusal) -> &'static str {
    match refusal {
        CarrierRefusal::TooManyKeys { .. } => "TooManyKeys",
        CarrierRefusal::KeyLength { .. } => "KeyLength",
        CarrierRefusal::KeyCharacter { .. } => "KeyCharacter",
        CarrierRefusal::ValueLength { .. } => "ValueLength",
        CarrierRefusal::ValueCharacter { .. } => "ValueCharacter",
        CarrierRefusal::TextLength { .. } => "TextLength",
        CarrierRefusal::TraceIdLength { .. } => "TraceIdLength",
        CarrierRefusal::TraceIdAllZero => "TraceIdAllZero",
    }
}

#[test]
fn every_vector_is_checked_and_encoded_to_its_text() {
    let form = form();
    assert!(!form.vectors.is_empty(), "the vector file holds vectors");
    for vector in form.vectors {
        let name = vector.name.as_str();
        let trace_id = hex_bytes(name, &vector.trace_id);
        assert_eq!(carrier_text(&vector.carrier), vector.text, "{name}");
        let trace = PropagatedTrace {
            carrier: vector.carrier,
            trace_id: trace_id.clone(),
        };
        match check_carrier(&trace) {
            CarrierCheck::Accepted(checked) => {
                assert_eq!(checked.text(), vector.text, "{name}");
                assert_eq!(checked.trace_id().as_slice(), trace_id.as_slice(), "{name}");
            }
            other => panic!("{name}: expected the carrier to be accepted, got {other:?}"),
        }
    }
}

#[test]
fn every_vector_text_reads_back_to_its_carrier() {
    for vector in form().vectors {
        let read: BTreeMap<String, String> = serde_json::from_str(&vector.text)
            .unwrap_or_else(|error| panic!("{}: the text does not read: {error}", vector.name));
        assert_eq!(read, vector.carrier, "{}", vector.name);
    }
}

#[test]
fn every_refused_row_is_refused_with_its_variant() {
    let form = form();
    assert!(
        !form.refused.is_empty(),
        "the vector file holds refused rows"
    );
    for row in form.refused {
        let name = row.name.as_str();
        let trace = PropagatedTrace {
            carrier: row.carrier,
            trace_id: hex_bytes(name, &row.trace_id),
        };
        match check_carrier(&trace) {
            CarrierCheck::Refused(refusal) => {
                assert_eq!(refusal_name(&refusal), row.refusal, "{name}");
            }
            other => panic!("{name}: expected a refusal, got {other:?}"),
        }
    }
}
