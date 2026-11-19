//! The trace carrier of a send: what a client hands the core, the check of
//! its bounds, and the text that the core stores.
//!
//! A client's propagator writes a map of text to text (W3C writes
//! `traceparent` and `tracestate`; B3, Jaeger, X-Ray and Datadog write their
//! own keys). The core does not parse the map: the task child extracts it
//! with the client's own propagator. The core checks bounds only, and stores
//! the map as one canonical JSON text with the 16-byte trace id of the
//! producer span (`horsies_tasks.trace_carrier`, `trace_id`).
//!
//! Telemetry never changes a send: a carrier that breaks a bound is not
//! stored, the row gets no carrier, the send goes on, and the process logs
//! one warning that names the bound ([`StoredTrace::of_send`]).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// Most keys in one carrier.
pub const MAX_TRACE_CARRIER_KEYS: usize = 8;

/// Most bytes of one key. A key has at least one byte.
pub const MAX_TRACE_CARRIER_KEY_BYTES: usize = 128;

/// Most bytes of one value. A value may be empty.
pub const MAX_TRACE_CARRIER_VALUE_BYTES: usize = 1_024;

/// Most bytes of the stored text: the canonical JSON object of the carrier.
pub const MAX_TRACE_CARRIER_BYTES: usize = 2_048;

/// Bytes of a trace id.
pub const TRACE_ID_BYTES: usize = 16;

/// The `p_trace_kind` of a row with no carrier.
const NO_CARRIER_KIND: &str = "NO_CARRIER";

/// The `p_trace_kind` of a row with a carrier.
const CARRIER_KIND: &str = "CARRIER";

/// The trace of a send as a client hands it to the core. Every send starts
/// with [`SendTrace::NoCarrier`]; a client that traces the send passes
/// [`SendTrace::Propagated`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SendTrace {
    /// The send carries no trace.
    #[default]
    NoCarrier,
    /// The carrier that the client's propagator wrote, with the trace id of
    /// the span it propagates. The core checks it before the write.
    Propagated(PropagatedTrace),
}

/// A carrier as a propagator wrote it, before the core's check. The keys are
/// kept as written: a propagator reads back the keys it wrote (the X-Ray
/// propagator writes and reads `X-Amzn-Trace-Id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropagatedTrace {
    /// The map that the propagator injected into.
    pub carrier: BTreeMap<String, String>,
    /// The trace id of the propagated span, as bytes (16 for a valid id).
    pub trace_id: Vec<u8>,
}

/// A carrier that passed every bound, in the form the row stores. Only
/// [`check_carrier`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedTraceCarrier {
    text: String,
    trace_id: [u8; TRACE_ID_BYTES],
}

impl CheckedTraceCarrier {
    /// The canonical JSON text of the carrier ([`carrier_text`]).
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The trace id of the propagated span.
    pub fn trace_id(&self) -> &[u8; TRACE_ID_BYTES] {
        &self.trace_id
    }
}

/// The trace that a row stores.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StoredTrace {
    /// The row stores no carrier and no trace id.
    #[default]
    NoCarrier,
    /// The row stores the carrier text and the trace id.
    Carrier(CheckedTraceCarrier),
}

impl StoredTrace {
    /// The stored trace of `trace`: the checked carrier, or
    /// [`StoredTrace::NoCarrier`] when the send has none, when the
    /// propagator wrote no key, or when a bound does not hold. The first
    /// refusal of the process logs one warning that names the bound; later
    /// refusals log nothing. Never fails.
    pub fn of_send(trace: SendTrace) -> StoredTrace {
        stored_trace(trace, &PROCESS_CARRIER_REFUSALS)
    }

    /// `p_trace_kind`, `p_trace_carrier` and `p_trace_id` of the SQL
    /// functions that write a row: `NO_CARRIER` with no values, or `CARRIER`
    /// with the text and the 16-byte id. Pure.
    pub fn columns(&self) -> (&'static str, Option<&str>, Option<&[u8]>) {
        match self {
            Self::NoCarrier => (NO_CARRIER_KIND, None, None),
            Self::Carrier(carrier) => (
                CARRIER_KIND,
                Some(carrier.text.as_str()),
                Some(carrier.trace_id.as_slice()),
            ),
        }
    }
}

/// Why a carrier is not stored. Each variant names the bound that does not
/// hold and the measured number; none holds a key or a value, so a log line
/// carries no carrier content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CarrierRefusal {
    #[error("the carrier has {keys} keys; at most {max} are stored", max = MAX_TRACE_CARRIER_KEYS)]
    TooManyKeys { keys: usize },
    #[error("a key has {bytes} bytes; a key has 1 to {max} bytes", max = MAX_TRACE_CARRIER_KEY_BYTES)]
    KeyLength { bytes: usize },
    #[error(
        "a key holds the byte 0x{byte:02x}; a key holds HTTP token characters only \
         (letters, digits and !#$%&'*+-.^_`|~)"
    )]
    KeyCharacter { byte: u8 },
    #[error(
        "a value has {bytes} bytes; a value has at most {max} bytes",
        max = MAX_TRACE_CARRIER_VALUE_BYTES
    )]
    ValueLength { bytes: usize },
    #[error(
        "a value holds the byte 0x{byte:02x}; a value holds printable ASCII only (0x20 to 0x7e)"
    )]
    ValueCharacter { byte: u8 },
    #[error(
        "the carrier text has {bytes} bytes; at most {max} are stored",
        max = MAX_TRACE_CARRIER_BYTES
    )]
    TextLength { bytes: usize },
    #[error("the trace id has {bytes} bytes; a trace id has {length} bytes", length = TRACE_ID_BYTES)]
    TraceIdLength { bytes: usize },
    #[error("the trace id is all zero, which no valid span has")]
    TraceIdAllZero,
}

/// What the check of a propagated carrier found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CarrierCheck {
    /// Every bound holds.
    Accepted(CheckedTraceCarrier),
    /// The propagator wrote no key (no SDK, or a span that is not
    /// recorded): there is nothing to store and nothing to report.
    NoKeys,
    /// A bound does not hold.
    Refused(CarrierRefusal),
}

/// Checks `trace` against every bound and builds the stored text. The work
/// is bounded: the key count is checked first, then each key and value
/// length before its characters, then the text. Pure.
///
/// Outcomes: [`CarrierCheck::NoKeys`] for an empty carrier;
/// [`CarrierCheck::Refused`] with the first bound that does not hold, in the
/// order key count, trace id, then per entry in key order key length, key
/// characters, value length, value characters, then the text length;
/// [`CarrierCheck::Accepted`] otherwise.
pub fn check_carrier(trace: &PropagatedTrace) -> CarrierCheck {
    if trace.carrier.is_empty() {
        return CarrierCheck::NoKeys;
    }
    match checked_carrier(trace) {
        Ok(carrier) => CarrierCheck::Accepted(carrier),
        Err(refusal) => CarrierCheck::Refused(refusal),
    }
}

/// The bounds of [`check_carrier`] on a carrier with one key or more.
fn checked_carrier(trace: &PropagatedTrace) -> Result<CheckedTraceCarrier, CarrierRefusal> {
    let keys = trace.carrier.len();
    if keys > MAX_TRACE_CARRIER_KEYS {
        return Err(CarrierRefusal::TooManyKeys { keys });
    }
    let trace_id = checked_trace_id(&trace.trace_id)?;
    for (key, value) in &trace.carrier {
        check_key(key)?;
        check_value(value)?;
    }
    let text = carrier_text(&trace.carrier);
    match text.len() > MAX_TRACE_CARRIER_BYTES {
        true => Err(CarrierRefusal::TextLength { bytes: text.len() }),
        false => Ok(CheckedTraceCarrier { text, trace_id }),
    }
}

/// The trace id as its 16 bytes, when it has 16 bytes and one is not zero.
fn checked_trace_id(bytes: &[u8]) -> Result<[u8; TRACE_ID_BYTES], CarrierRefusal> {
    let trace_id: [u8; TRACE_ID_BYTES] = bytes
        .try_into()
        .map_err(|_| CarrierRefusal::TraceIdLength { bytes: bytes.len() })?;
    match trace_id.iter().all(|byte| *byte == 0) {
        true => Err(CarrierRefusal::TraceIdAllZero),
        false => Ok(trace_id),
    }
}

fn check_key(key: &str) -> Result<(), CarrierRefusal> {
    let bytes = key.len();
    if bytes == 0 || bytes > MAX_TRACE_CARRIER_KEY_BYTES {
        return Err(CarrierRefusal::KeyLength { bytes });
    }
    match key.bytes().find(|byte| !is_key_byte(*byte)) {
        Some(byte) => Err(CarrierRefusal::KeyCharacter { byte }),
        None => Ok(()),
    }
}

fn check_value(value: &str) -> Result<(), CarrierRefusal> {
    let bytes = value.len();
    if bytes > MAX_TRACE_CARRIER_VALUE_BYTES {
        return Err(CarrierRefusal::ValueLength { bytes });
    }
    match value.bytes().find(|byte| !is_value_byte(*byte)) {
        Some(byte) => Err(CarrierRefusal::ValueCharacter { byte }),
        None => Ok(()),
    }
}

/// Whether `byte` is a character of an HTTP token (RFC 9110, `tchar`): a
/// letter of either case, a digit, or one of ``!#$%&'*+-.^_`|~``.
fn is_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Whether `byte` is printable ASCII (0x20 to 0x7e). The space is in the
/// set: a W3C `tracestate` value and a W3C `baggage` header may hold one.
fn is_value_byte(byte: u8) -> bool {
    (0x20..=0x7e).contains(&byte)
}

/// The canonical JSON text of a carrier: one object, the keys in byte order
/// (the order of the map), no whitespace, each key and value a JSON string
/// in which `"` and `\` are escaped and every other character is written as
/// itself. The one encoder of the stored carrier for both clients; the
/// stored-form vector `trace_carrier.json` pins it. Pure.
///
/// The text is the canonical JSON form for a carrier that passed the
/// character checks: those leave no character that JSON must escape other
/// than `"` and `\`.
pub fn carrier_text(carrier: &BTreeMap<String, String>) -> String {
    let mut text = String::from("{");
    for (index, (key, value)) in carrier.iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        push_json_string(&mut text, key);
        text.push(':');
        push_json_string(&mut text, value);
    }
    text.push('}');
    text
}

fn push_json_string(text: &mut String, value: &str) {
    text.push('"');
    for character in value.chars() {
        match character {
            '"' => text.push_str("\\\""),
            '\\' => text.push_str("\\\\"),
            other => text.push(other),
        }
    }
    text.push('"');
}

/// The text of a trace id: 32 lowercase hex digits, as W3C Trace Context
/// writes it. Pure.
pub fn trace_id_text(trace_id: &[u8; TRACE_ID_BYTES]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(TRACE_ID_BYTES * 2);
    for byte in trace_id {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

/// A stored trace id that is not [`TRACE_ID_BYTES`] long: data that no
/// writer stores (the send check takes 16 bytes only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the stored trace id has {bytes} bytes; a trace id has {length} bytes", length = TRACE_ID_BYTES)]
pub struct StoredTraceIdLength {
    pub bytes: usize,
}

/// The text of a stored trace id (`trace_id` of `horsies_tasks`,
/// `horsies_workflows` and `horsies_task_history`): [`trace_id_text`], or
/// `None` for a row with no trace. Pure.
///
/// Errors: [`StoredTraceIdLength`] for a value that is not 16 bytes.
pub fn stored_trace_id_text(stored: Option<&[u8]>) -> Result<Option<String>, StoredTraceIdLength> {
    match stored {
        None => Ok(None),
        Some(bytes) => <&[u8; TRACE_ID_BYTES]>::try_from(bytes)
            .map(|trace_id| Some(trace_id_text(trace_id)))
            .map_err(|_| StoredTraceIdLength { bytes: bytes.len() }),
    }
}

/// Whether this process has logged a refused carrier.
pub(crate) struct CarrierRefusalLog {
    logged: AtomicBool,
}

/// The turn of one refusal in a [`CarrierRefusalLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefusalTurn {
    /// The first refusal of the log: it is logged.
    First,
    /// A refusal after the first: it is not logged.
    Repeated,
}

impl CarrierRefusalLog {
    pub(crate) const fn new() -> CarrierRefusalLog {
        CarrierRefusalLog {
            logged: AtomicBool::new(false),
        }
    }

    /// The turn of one more refusal: [`RefusalTurn::First`] exactly once.
    pub(crate) fn turn(&self) -> RefusalTurn {
        match self.logged.swap(true, Ordering::Relaxed) {
            false => RefusalTurn::First,
            true => RefusalTurn::Repeated,
        }
    }
}

/// The refusal log of the process: one warning per process.
static PROCESS_CARRIER_REFUSALS: CarrierRefusalLog = CarrierRefusalLog::new();

/// [`StoredTrace::of_send`] with the refusal log `log`.
pub(crate) fn stored_trace(trace: SendTrace, log: &CarrierRefusalLog) -> StoredTrace {
    match trace {
        SendTrace::NoCarrier => StoredTrace::NoCarrier,
        SendTrace::Propagated(propagated) => stored_propagated(&propagated, log),
    }
}

/// The stored trace of a propagated carrier: the checked carrier, or no
/// carrier for an empty one and for a refused one. A refusal takes a turn of
/// `log`, and the first turn logs one warning.
fn stored_propagated(propagated: &PropagatedTrace, log: &CarrierRefusalLog) -> StoredTrace {
    match check_carrier(propagated) {
        CarrierCheck::Accepted(carrier) => StoredTrace::Carrier(carrier),
        CarrierCheck::NoKeys => StoredTrace::NoCarrier,
        CarrierCheck::Refused(refusal) => {
            log_refusal(refusal, log.turn());
            StoredTrace::NoCarrier
        }
    }
}

/// Logs `refusal` at WARNING on the first turn of the process; logs nothing
/// on a later turn.
fn log_refusal(refusal: CarrierRefusal, turn: RefusalTurn) {
    match turn {
        RefusalTurn::First => {
            tracing::warn!(
                bound = ?refusal,
                "trace carrier not stored: {refusal}. The task is sent with no carrier, so its \
                 attempts start a new trace. Later carriers that break a bound are not logged.",
            );
        }
        RefusalTurn::Repeated => {}
    }
}

#[cfg(test)]
mod tests;
