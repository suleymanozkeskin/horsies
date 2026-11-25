//! The rules for values that cross the Node-API boundary.
//!
//! - An integer that can pass 2^53 - 1 crosses as a JavaScript `BigInt`. No
//!   `i64` or `u64` is the type of a `#[napi]` argument, return or field:
//!   napi-rs maps `i64` to a number, which loses digits.
//! - An instant crosses as a `BigInt` of epoch microseconds, the stored
//!   precision.
//! - A 32-bit integer argument crosses as a JavaScript number (`f64`) and is
//!   checked here ([`u32_of_number`]). A `u32` argument of napi-rs would cut
//!   a fraction (3.9 gives 3), wrap a value out of range (2^32 + 7 gives 7)
//!   and read `NaN` as 0, with no error.
//! - Bytes cross as lower-case hexadecimal text ([`bytes_of_hex`]): a
//!   `Uint8Array` points into JavaScript memory, which only the JavaScript
//!   thread may read, and an operation can move its argument to a core
//!   thread.
//! - A UUID crosses as text in lower-case hyphenated form.
//! - Text crosses as a string. napi-rs replaces a lone surrogate with U+FFFD
//!   and gives no error, so the package checks `isWellFormed()` before each
//!   call that takes user text.
//!
//! The package checks every value before a call. A value that breaks a rule
//! here is a defect of the package: the caller answers `ClientDefect` with
//! the [`BoundaryRefusal`].

use chrono::{DateTime, Utc};
use napi::bindgen_prelude::BigInt;
use uuid::Uuid;

/// The rule of a `BigInt` that must hold a `u64`.
const U64_RULE: &str = "an integer from 0 to 18446744073709551615";

/// The rule of a `BigInt` that must hold an `i64`.
const I64_RULE: &str = "an integer from -9223372036854775808 to 9223372036854775807";

/// The rule of an instant in epoch microseconds.
const INSTANT_RULE: &str =
    "epoch microseconds of an instant from year -262143 to year 262142 (UTC)";

/// The rule of a UUID text.
const UUID_RULE: &str = "a UUID in lower-case hyphenated form, for example \
                         01890a5d-ac96-774b-bcce-b302099a8057";

/// The rule of a number that must hold a `u32`.
const U32_RULE: &str = "a whole number from 0 to 4294967295";

/// The rule of bytes as text.
const HEX_RULE: &str = "lower-case hexadecimal text of whole bytes, for example 0af7";

/// A value that the package passed breaks a boundary rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryRefusal {
    /// The argument or the field, for example `taskId`.
    pub subject: String,
    /// The rule that the value breaks.
    pub rule: String,
}

impl BoundaryRefusal {
    /// The refusal of the value of `subject` by `rule`. Pure.
    pub fn new(subject: &str, rule: &str) -> Self {
        Self {
            subject: subject.to_owned(),
            rule: rule.to_owned(),
        }
    }
}

/// `value` as a `BigInt`. Pure.
pub fn bigint_of_u64(value: u64) -> BigInt {
    BigInt::from(value)
}

/// `value` as a `BigInt`. Pure.
pub fn bigint_of_i64(value: i64) -> BigInt {
    BigInt::from(i128::from(value))
}

/// The `u64` that `value` holds, or the refusal of `subject`. Pure.
pub fn u64_of_bigint(subject: &str, value: &BigInt) -> Result<u64, BoundaryRefusal> {
    let (negative, read, lossless) = value.get_u64();
    match (negative, lossless) {
        (false, true) => Ok(read),
        (true, _) | (false, false) => Err(BoundaryRefusal::new(subject, U64_RULE)),
    }
}

/// The `i64` that `value` holds, or the refusal of `subject`. Pure.
pub fn i64_of_bigint(subject: &str, value: &BigInt) -> Result<i64, BoundaryRefusal> {
    let (read, lossless) = value.get_i128();
    match (lossless, i64::try_from(read)) {
        (true, Ok(narrowed)) => Ok(narrowed),
        (true, Err(_)) | (false, _) => Err(BoundaryRefusal::new(subject, I64_RULE)),
    }
}

/// The `u32` that the number `value` holds, or the refusal of `subject`: a
/// fraction, a value out of range, `NaN` and an infinity are refused. Pure.
pub fn u32_of_number(subject: &str, value: f64) -> Result<u32, BoundaryRefusal> {
    let whole = value.is_finite() && value.fract() == 0.0;
    match (whole, value >= 0.0 && value <= f64::from(u32::MAX)) {
        // The value is whole and in range, so the conversion is exact.
        (true, true) => Ok(value as u32),
        (true, false) | (false, _) => Err(BoundaryRefusal::new(subject, U32_RULE)),
    }
}

/// The bytes of lower-case hexadecimal text, or the refusal of `subject`.
/// Pure.
pub fn bytes_of_hex(subject: &str, text: &str) -> Result<Vec<u8>, BoundaryRefusal> {
    let digits = text.as_bytes();
    if digits.len() % 2 != 0 {
        return Err(BoundaryRefusal::new(subject, HEX_RULE));
    }
    digits
        .chunks_exact(2)
        .map(|pair| match (hex_digit(pair[0]), hex_digit(pair[1])) {
            (Some(high), Some(low)) => Ok(high * 16 + low),
            (None, _) | (_, None) => Err(BoundaryRefusal::new(subject, HEX_RULE)),
        })
        .collect()
}

/// The value of one lower-case hexadecimal digit. Pure.
fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _other => None,
    }
}

/// `instant` as a `BigInt` of epoch microseconds. Pure.
pub fn epoch_micros_of(instant: DateTime<Utc>) -> BigInt {
    bigint_of_i64(instant.timestamp_micros())
}

/// The instant of a `BigInt` of epoch microseconds, or the refusal of
/// `subject`. Pure.
#[allow(
    dead_code,
    reason = "the send, workflow and facades areas read instants from JavaScript"
)]
pub fn instant_of_epoch_micros(
    subject: &str,
    micros: &BigInt,
) -> Result<DateTime<Utc>, BoundaryRefusal> {
    let micros = i64_of_bigint(subject, micros)?;
    DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or_else(|| BoundaryRefusal::new(subject, INSTANT_RULE))
}

/// The UUID of a lower-case hyphenated text, or the refusal of `subject`.
/// Another form of the same UUID (upper case, braces, no hyphens) is
/// refused: the package sends one form. Pure.
#[allow(
    dead_code,
    reason = "the send, workflow and facades areas read task and workflow ids from JavaScript"
)]
pub fn uuid_of_text(subject: &str, text: &str) -> Result<Uuid, BoundaryRefusal> {
    match Uuid::try_parse(text) {
        Ok(id) if text_of_uuid(id) == text => Ok(id),
        Ok(_) | Err(_) => Err(BoundaryRefusal::new(subject, UUID_RULE)),
    }
}

/// `id` as lower-case hyphenated text. Pure.
pub fn text_of_uuid(id: Uuid) -> String {
    id.hyphenated().to_string()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn u64_max_crosses_as_a_bigint_and_back_with_no_lost_digit() {
        let crossed = bigint_of_u64(u64::MAX);
        assert_eq!(u64_of_bigint("bytes", &crossed), Ok(u64::MAX));
    }

    #[test]
    fn a_negative_bigint_is_refused_as_a_u64() {
        let negative = bigint_of_i64(-1);
        assert_eq!(
            u64_of_bigint("bytes", &negative),
            Err(BoundaryRefusal::new("bytes", U64_RULE))
        );
    }

    #[test]
    fn a_bigint_past_u64_max_is_refused_as_a_u64() {
        let past = BigInt::from(u128::from(u64::MAX) + 1);
        assert_eq!(
            u64_of_bigint("bytes", &past),
            Err(BoundaryRefusal::new("bytes", U64_RULE))
        );
    }

    #[test]
    fn i64_min_and_max_cross_and_back() {
        for value in [i64::MIN, -1, 0, i64::MAX] {
            assert_eq!(i64_of_bigint("delay", &bigint_of_i64(value)), Ok(value));
        }
    }

    #[test]
    fn a_bigint_past_i64_max_is_refused_as_an_i64() {
        let past = BigInt::from(i128::from(i64::MAX) + 1);
        assert_eq!(
            i64_of_bigint("delay", &past),
            Err(BoundaryRefusal::new("delay", I64_RULE))
        );
    }

    #[test]
    fn an_instant_crosses_as_epoch_microseconds() {
        let instant = Utc
            .with_ymd_and_hms(2026, 11, 25, 12, 0, 0)
            .single()
            .expect("a valid instant")
            + chrono::Duration::microseconds(123_456);
        let crossed = epoch_micros_of(instant);
        assert_eq!(crossed, bigint_of_i64(1_795_608_000_123_456));
        assert_eq!(instant_of_epoch_micros("sentAt", &crossed), Ok(instant));
    }

    #[test]
    fn epoch_microseconds_past_the_instant_range_are_refused() {
        assert_eq!(
            instant_of_epoch_micros("sentAt", &bigint_of_i64(i64::MAX)),
            Err(BoundaryRefusal::new("sentAt", INSTANT_RULE))
        );
    }

    #[test]
    fn a_whole_number_in_range_is_a_u32() {
        for (number, expected) in [(0.0, 0_u32), (7.0, 7), (4_294_967_295.0, u32::MAX)] {
            assert_eq!(u32_of_number("days", number), Ok(expected), "{number}");
        }
    }

    #[test]
    fn a_number_that_napi_would_cut_or_wrap_is_refused_as_a_u32() {
        for refused in [3.9, -1.0, 4_294_967_303.0, f64::NAN, f64::INFINITY, -0.5] {
            assert_eq!(
                u32_of_number("days", refused),
                Err(BoundaryRefusal::new("days", U32_RULE)),
                "{refused}"
            );
        }
    }

    #[test]
    fn lower_case_hexadecimal_text_is_read_as_bytes() {
        assert_eq!(
            bytes_of_hex("traceIdHex", "0af7651916cd43dd8448eb211c80319c"),
            Ok(vec![
                0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80,
                0x31, 0x9c
            ])
        );
        assert_eq!(bytes_of_hex("traceIdHex", ""), Ok(Vec::new()));
    }

    #[test]
    fn hexadecimal_text_in_another_form_is_refused() {
        for refused in ["0AF7", "0af", "0g", "0a f7"] {
            assert_eq!(
                bytes_of_hex("traceIdHex", refused),
                Err(BoundaryRefusal::new("traceIdHex", HEX_RULE)),
                "{refused}"
            );
        }
    }

    #[test]
    fn a_lower_case_hyphenated_uuid_is_read() {
        let text = "01890a5d-ac96-774b-bcce-b302099a8057";
        let id = uuid_of_text("taskId", text).expect("the text is a UUID");
        assert_eq!(text_of_uuid(id), text);
    }

    #[test]
    fn another_form_of_a_uuid_is_refused() {
        for refused in [
            "01890A5D-AC96-774B-BCCE-B302099A8057",
            "01890a5dac96774bbcceb302099a8057",
            "{01890a5d-ac96-774b-bcce-b302099a8057}",
            "not a uuid",
        ] {
            assert_eq!(
                uuid_of_text("taskId", refused),
                Err(BoundaryRefusal::new("taskId", UUID_RULE)),
                "{refused}"
            );
        }
    }
}
