//! The rules for values that cross the Node-API boundary.
//!
//! - An integer that can pass 2^53 - 1 crosses as a JavaScript `BigInt`. No
//!   `i64` or `u64` is the type of a `#[napi]` argument, return or field:
//!   napi-rs maps `i64` to a number, which loses digits.
//! - An instant crosses as a `BigInt` of epoch microseconds, the stored
//!   precision.
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
