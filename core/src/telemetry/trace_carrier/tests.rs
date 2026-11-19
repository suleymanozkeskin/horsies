//! The carrier check, one test per outcome; the canonical text; the one
//! warning per process; the columns of each stored trace.

use std::collections::BTreeMap;

use super::*;

/// The W3C example trace id `0af7651916cd43dd8448eb211c80319c`.
const TRACE_ID: [u8; TRACE_ID_BYTES] = [
    0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c,
];

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn carrier(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn propagated(entries: &[(&str, &str)]) -> PropagatedTrace {
    PropagatedTrace {
        carrier: carrier(entries),
        trace_id: TRACE_ID.to_vec(),
    }
}

/// A carrier of `count` keys `k0`, `k1`, ... with the value `v`.
fn propagated_keys(count: usize) -> PropagatedTrace {
    PropagatedTrace {
        carrier: (0..count)
            .map(|index| (format!("k{index}"), "v".to_owned()))
            .collect(),
        trace_id: TRACE_ID.to_vec(),
    }
}

fn accepted(trace: &PropagatedTrace) -> CheckedTraceCarrier {
    match check_carrier(trace) {
        CarrierCheck::Accepted(checked) => checked,
        other => panic!("expected an accepted carrier, got {other:?}"),
    }
}

fn refusal(trace: &PropagatedTrace) -> CarrierRefusal {
    match check_carrier(trace) {
        CarrierCheck::Refused(refusal) => refusal,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_w3c_carrier_is_stored_as_canonical_text_with_its_trace_id() {
    let checked = accepted(&propagated(&[
        ("traceparent", TRACEPARENT),
        ("tracestate", "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"),
    ]));
    assert_eq!(
        checked.text(),
        "{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\",\
         \"tracestate\":\"rojo=00f067aa0ba902b7,congo=t61rcWkgMzE\"}"
    );
    assert_eq!(checked.trace_id(), &TRACE_ID);
}

#[test]
fn the_keys_are_in_byte_order_and_keep_their_case() {
    let checked = accepted(&propagated(&[
        ("b", "2"),
        ("a", "3"),
        (
            "X-Amzn-Trace-Id",
            "Root=1-5759e988-bd862e3fe1be46a994272793",
        ),
    ]));
    assert_eq!(
        checked.text(),
        "{\"X-Amzn-Trace-Id\":\"Root=1-5759e988-bd862e3fe1be46a994272793\",\"a\":\"3\",\"b\":\"2\"}"
    );
}

#[test]
fn a_quote_and_a_backslash_are_escaped_and_every_other_character_is_itself() {
    let entries = [("x-test", "a\"b\\c d/e")];
    let checked = accepted(&propagated(&entries));
    assert_eq!(checked.text(), "{\"x-test\":\"a\\\"b\\\\c d/e\"}");
    let read: BTreeMap<String, String> =
        serde_json::from_str(checked.text()).expect("the text is a JSON object of strings");
    assert_eq!(read, carrier(&entries));
}

#[test]
fn an_empty_carrier_has_no_keys() {
    assert_eq!(check_carrier(&propagated(&[])), CarrierCheck::NoKeys);
}

#[test]
fn the_most_keys_are_stored() {
    let checked = accepted(&propagated_keys(MAX_TRACE_CARRIER_KEYS));
    let read: BTreeMap<String, String> =
        serde_json::from_str(checked.text()).expect("the text is a JSON object of strings");
    assert_eq!(read.len(), MAX_TRACE_CARRIER_KEYS);
}

#[test]
fn more_than_the_most_keys_is_refused() {
    assert_eq!(
        refusal(&propagated_keys(MAX_TRACE_CARRIER_KEYS + 1)),
        CarrierRefusal::TooManyKeys {
            keys: MAX_TRACE_CARRIER_KEYS + 1
        }
    );
}

#[test]
fn a_key_of_the_most_bytes_is_stored() {
    let key = "k".repeat(MAX_TRACE_CARRIER_KEY_BYTES);
    let _checked = accepted(&propagated(&[(key.as_str(), "v")]));
}

#[test]
fn an_empty_key_is_refused() {
    assert_eq!(
        refusal(&propagated(&[("", "v")])),
        CarrierRefusal::KeyLength { bytes: 0 }
    );
}

#[test]
fn a_key_over_the_most_bytes_is_refused() {
    let key = "k".repeat(MAX_TRACE_CARRIER_KEY_BYTES + 1);
    assert_eq!(
        refusal(&propagated(&[(key.as_str(), "v")])),
        CarrierRefusal::KeyLength {
            bytes: MAX_TRACE_CARRIER_KEY_BYTES + 1
        }
    );
}

#[test]
fn every_token_character_is_a_key_character() {
    let key = "AZaz09!#$%&'*+-.^_`|~";
    let _checked = accepted(&propagated(&[(key, "v")]));
}

#[test]
fn a_key_outside_the_token_characters_is_refused() {
    for (key, byte) in [
        ("trace parent", b' '),
        ("trace:id", b':'),
        ("trace\"id", b'"'),
        ("trac\u{e9}", 0xc3),
    ] {
        assert_eq!(
            refusal(&propagated(&[(key, "v")])),
            CarrierRefusal::KeyCharacter { byte },
            "{key:?}"
        );
    }
}

#[test]
fn a_value_of_the_most_bytes_and_an_empty_value_are_stored() {
    let value = "v".repeat(MAX_TRACE_CARRIER_VALUE_BYTES);
    let _checked = accepted(&propagated(&[("a", value.as_str()), ("b", "")]));
}

#[test]
fn a_value_over_the_most_bytes_is_refused() {
    let value = "v".repeat(MAX_TRACE_CARRIER_VALUE_BYTES + 1);
    assert_eq!(
        refusal(&propagated(&[("k", value.as_str())])),
        CarrierRefusal::ValueLength {
            bytes: MAX_TRACE_CARRIER_VALUE_BYTES + 1
        }
    );
}

#[test]
fn a_value_outside_printable_ascii_is_refused() {
    for (value, byte) in [
        ("a\tb", b'\t'),
        ("a\nb", b'\n'),
        ("a\u{7f}", 0x7f),
        ("caf\u{e9}", 0xc3),
    ] {
        assert_eq!(
            refusal(&propagated(&[("k", value)])),
            CarrierRefusal::ValueCharacter { byte },
            "{value:?}"
        );
    }
}

/// Two keys of one byte: the text is `{"a":"<first>","b":"<second>"}`, 15
/// bytes plus the two values.
fn two_values(second_bytes: usize) -> PropagatedTrace {
    let first = "v".repeat(MAX_TRACE_CARRIER_VALUE_BYTES);
    let second = "w".repeat(second_bytes);
    propagated(&[("a", first.as_str()), ("b", second.as_str())])
}

#[test]
fn a_text_of_the_most_bytes_is_stored() {
    let second = MAX_TRACE_CARRIER_BYTES - 15 - MAX_TRACE_CARRIER_VALUE_BYTES;
    let checked = accepted(&two_values(second));
    assert_eq!(checked.text().len(), MAX_TRACE_CARRIER_BYTES);
}

#[test]
fn a_text_over_the_most_bytes_is_refused() {
    let second = MAX_TRACE_CARRIER_BYTES - 15 - MAX_TRACE_CARRIER_VALUE_BYTES + 1;
    assert_eq!(
        refusal(&two_values(second)),
        CarrierRefusal::TextLength {
            bytes: MAX_TRACE_CARRIER_BYTES + 1
        }
    );
}

#[test]
fn an_escaped_character_counts_in_the_text_bound() {
    // Each quote is written as two bytes, so values within their own bound
    // can give a text over the bound.
    let first = "\"".repeat(MAX_TRACE_CARRIER_VALUE_BYTES);
    let trace = propagated(&[("a", first.as_str())]);
    assert_eq!(
        refusal(&trace),
        CarrierRefusal::TextLength {
            bytes: 2 * MAX_TRACE_CARRIER_VALUE_BYTES + 8
        }
    );
}

#[test]
fn a_trace_id_of_another_length_is_refused() {
    for bytes in [0, TRACE_ID_BYTES - 1, TRACE_ID_BYTES + 1] {
        let trace = PropagatedTrace {
            carrier: carrier(&[("traceparent", TRACEPARENT)]),
            trace_id: vec![1; bytes],
        };
        assert_eq!(refusal(&trace), CarrierRefusal::TraceIdLength { bytes });
    }
}

#[test]
fn an_all_zero_trace_id_is_refused() {
    let trace = PropagatedTrace {
        carrier: carrier(&[("traceparent", TRACEPARENT)]),
        trace_id: vec![0; TRACE_ID_BYTES],
    };
    assert_eq!(refusal(&trace), CarrierRefusal::TraceIdAllZero);
}

#[test]
fn the_key_count_is_checked_before_the_trace_id() {
    let mut trace = propagated_keys(MAX_TRACE_CARRIER_KEYS + 1);
    trace.trace_id = vec![0; TRACE_ID_BYTES];
    assert!(matches!(
        refusal(&trace),
        CarrierRefusal::TooManyKeys { .. }
    ));
}

#[test]
fn every_refusal_names_its_bound() {
    for (refusal, bound) in [
        (
            CarrierRefusal::TooManyKeys { keys: 9 },
            MAX_TRACE_CARRIER_KEYS,
        ),
        (
            CarrierRefusal::KeyLength { bytes: 0 },
            MAX_TRACE_CARRIER_KEY_BYTES,
        ),
        (
            CarrierRefusal::ValueLength { bytes: 2_000 },
            MAX_TRACE_CARRIER_VALUE_BYTES,
        ),
        (
            CarrierRefusal::TextLength { bytes: 3_000 },
            MAX_TRACE_CARRIER_BYTES,
        ),
        (CarrierRefusal::TraceIdLength { bytes: 8 }, TRACE_ID_BYTES),
    ] {
        let text = refusal.to_string();
        assert!(text.contains(&bound.to_string()), "{text}");
    }
    assert!(CarrierRefusal::KeyCharacter { byte: b' ' }
        .to_string()
        .contains("0x20"));
    assert!(CarrierRefusal::ValueCharacter { byte: b'\t' }
        .to_string()
        .contains("0x09"));
}

#[test]
fn the_log_gives_the_first_turn_once() {
    let log = CarrierRefusalLog::new();
    assert_eq!(log.turn(), RefusalTurn::First);
    assert_eq!(log.turn(), RefusalTurn::Repeated);
    assert_eq!(log.turn(), RefusalTurn::Repeated);
}

#[test]
fn a_send_with_no_carrier_stores_none_and_takes_no_turn() {
    let log = CarrierRefusalLog::new();
    assert_eq!(
        stored_trace(SendTrace::NoCarrier, &log),
        StoredTrace::NoCarrier
    );
    assert_eq!(log.turn(), RefusalTurn::First);
}

#[test]
fn an_empty_carrier_stores_none_and_takes_no_turn() {
    let log = CarrierRefusalLog::new();
    assert_eq!(
        stored_trace(SendTrace::Propagated(propagated(&[])), &log),
        StoredTrace::NoCarrier
    );
    assert_eq!(log.turn(), RefusalTurn::First);
}

#[test]
fn an_accepted_carrier_is_stored_and_takes_no_turn() {
    let log = CarrierRefusalLog::new();
    let trace = propagated(&[("traceparent", TRACEPARENT)]);
    assert_eq!(
        stored_trace(SendTrace::Propagated(trace.clone()), &log),
        StoredTrace::Carrier(accepted(&trace))
    );
    assert_eq!(log.turn(), RefusalTurn::First);
}

#[test]
fn a_refused_carrier_stores_none_and_takes_the_first_turn() {
    let log = CarrierRefusalLog::new();
    let refused = propagated_keys(MAX_TRACE_CARRIER_KEYS + 1);
    assert_eq!(
        stored_trace(SendTrace::Propagated(refused.clone()), &log),
        StoredTrace::NoCarrier
    );
    assert_eq!(
        stored_trace(SendTrace::Propagated(refused), &log),
        StoredTrace::NoCarrier
    );
    assert_eq!(log.turn(), RefusalTurn::Repeated);
}

#[test]
fn the_columns_of_each_stored_trace() {
    assert_eq!(StoredTrace::NoCarrier.columns(), ("NO_CARRIER", None, None));
    let checked = accepted(&propagated(&[("traceparent", TRACEPARENT)]));
    let stored = StoredTrace::Carrier(checked.clone());
    assert_eq!(
        stored.columns(),
        ("CARRIER", Some(checked.text()), Some(TRACE_ID.as_slice()))
    );
}

#[test]
fn every_send_starts_with_no_carrier() {
    assert_eq!(SendTrace::default(), SendTrace::NoCarrier);
    assert_eq!(StoredTrace::default(), StoredTrace::NoCarrier);
}

/// The text of a trace id is the 32 lowercase hex digits that W3C Trace
/// Context writes.
#[test]
fn a_trace_id_is_written_as_32_lowercase_hex_digits() {
    assert_eq!(trace_id_text(&TRACE_ID), "0af7651916cd43dd8448eb211c80319c");
    assert_eq!(trace_id_text(&[0xff; TRACE_ID_BYTES]), "f".repeat(32));
}

/// A stored trace id: none for a row with no trace, the text for 16 bytes,
/// and a refusal that names the length for any other length.
#[test]
fn a_stored_trace_id_is_its_text_none_or_a_length_refusal() {
    assert_eq!(stored_trace_id_text(None), Ok(None));
    assert_eq!(
        stored_trace_id_text(Some(TRACE_ID.as_slice())),
        Ok(Some("0af7651916cd43dd8448eb211c80319c".to_owned()))
    );
    for bytes in [0, 15, 17] {
        assert_eq!(
            stored_trace_id_text(Some(vec![1; bytes].as_slice())),
            Err(StoredTraceIdLength { bytes })
        );
    }
}
