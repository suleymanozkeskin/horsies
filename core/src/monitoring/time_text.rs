//! The serialized form of the times of the monitoring models.
//!
//! - An instant is pydantic's JSON text of an aware UTC `datetime`, which
//!   the Python monitoring web app wrote (`2026-11-17T13:59:14.500000Z`).
//!   chrono's own serde form writes a whole millisecond with 3 digits
//!   (`.500Z`). The read is chrono's: any count of fraction digits.
//! - A duration is a JSON number of seconds, exact to the microsecond
//!   (`2.310417`, `30.0`, `-1.5`). chrono's own serde form of a `TimeDelta`
//!   is the pair `[seconds, nanoseconds]`. The read takes a number of
//!   seconds to the nearest microsecond.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Serialize, Serializer};

use crate::core::python_text::pydantic_json_utc;

/// Microseconds in one second.
const MICROSECONDS_PER_SECOND: i32 = 1_000_000;

/// Serialize `time` as pydantic writes it.
pub(super) fn utc<S: Serializer>(time: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&pydantic_json_utc(*time))
}

/// Serialize a time that may be absent: none, or the time as [`utc`]
/// writes it.
pub(super) fn optional_utc<S: Serializer>(
    time: &Option<DateTime<Utc>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match time {
        Some(time) => serializer.serialize_some(&PydanticTime(*time)),
        None => serializer.serialize_none(),
    }
}

/// A time that serializes as [`utc`] writes it.
struct PydanticTime(DateTime<Utc>);

impl Serialize for PydanticTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        utc(&self.0, serializer)
    }
}

/// Serialize `duration` as a JSON number of seconds, as [`seconds_number`]
/// gives it.
pub(super) fn seconds<S: Serializer>(
    duration: &TimeDelta,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64(seconds_number(*duration))
}

/// The serde `with` module of a duration that may be absent: `null`, or a
/// JSON number of seconds.
pub(super) mod optional_seconds {
    use chrono::TimeDelta;
    use serde::{de::Error as _, Deserialize, Deserializer, Serializer};

    use super::{duration_of_seconds, seconds_number};

    /// Serialize `duration`: none, or its seconds as [`seconds_number`]
    /// gives them.
    pub(in crate::monitoring) fn serialize<S: Serializer>(
        duration: &Option<TimeDelta>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(duration) => serializer.serialize_some(&seconds_number(*duration)),
            None => serializer.serialize_none(),
        }
    }

    /// Read `null` as none, and a number of seconds as its duration to the
    /// nearest microsecond. A number that [`duration_of_seconds`] refuses
    /// is an error that names the number.
    pub(in crate::monitoring) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<TimeDelta>, D::Error> {
        match Option::<f64>::deserialize(deserializer)? {
            Some(seconds) => match duration_of_seconds(seconds) {
                Some(duration) => Ok(Some(duration)),
                None => Err(D::Error::custom(format!(
                    "a duration of {seconds:?} seconds is outside the range of a duration"
                ))),
            },
            None => Ok(None),
        }
    }
}

/// The seconds of `duration` as a JSON number: the whole microseconds of
/// `duration` divided by 10^6, rounded once to the nearest `f64`. The value
/// is exact to the microsecond up to 2^53 microseconds (about 285 years),
/// so its JSON text has at most 6 fraction digits. A part finer than a
/// microsecond, which only a host clock read carries, is dropped toward
/// zero. The sign stays.
fn seconds_number(duration: TimeDelta) -> f64 {
    let microseconds = i128::from(duration.num_seconds()) * i128::from(MICROSECONDS_PER_SECOND)
        + i128::from(duration.subsec_micros());
    microseconds as f64 / f64::from(MICROSECONDS_PER_SECOND)
}

/// The duration of `seconds`, to the nearest microsecond. `None` when
/// `seconds` is not finite, or when its microseconds are outside the range
/// of `i64` (about 292,000 years).
fn duration_of_seconds(seconds: f64) -> Option<TimeDelta> {
    let microseconds = (seconds * f64::from(MICROSECONDS_PER_SECOND)).round();
    // `i64::MIN as f64` is -2^63 and `i64::MAX as f64` is 2^63, so a whole
    // number in [-2^63, 2^63) converts to `i64` exactly. NaN and the
    // infinities are outside that range.
    let convertible = i64::MIN as f64..i64::MAX as f64;
    match convertible.contains(&microseconds) {
        true => Some(TimeDelta::microseconds(microseconds as i64)),
        false => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde::Deserialize;

    use super::*;

    #[derive(Serialize)]
    struct Times {
        #[serde(serialize_with = "utc")]
        at: DateTime<Utc>,
        #[serde(serialize_with = "optional_utc")]
        maybe: Option<DateTime<Utc>>,
    }

    fn half_second() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 17, 13, 59, 14)
            .single()
            .expect("a valid time")
            + chrono::Duration::milliseconds(500)
    }

    #[test]
    fn a_time_is_written_with_six_fraction_digits() {
        let times = Times {
            at: half_second(),
            maybe: Some(half_second()),
        };

        assert_eq!(
            serde_json::to_string(&times).expect("serialize"),
            r#"{"at":"2026-11-17T13:59:14.500000Z","maybe":"2026-11-17T13:59:14.500000Z"}"#
        );
    }

    #[test]
    fn an_absent_time_is_null() {
        let times = Times {
            at: half_second(),
            maybe: None,
        };

        assert_eq!(
            serde_json::to_string(&times).expect("serialize"),
            r#"{"at":"2026-11-17T13:59:14.500000Z","maybe":null}"#
        );
    }

    #[derive(Serialize)]
    struct Waited {
        #[serde(serialize_with = "seconds")]
        wait: TimeDelta,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Measured {
        #[serde(with = "optional_seconds")]
        span: Option<TimeDelta>,
    }

    /// The JSON text of a [`Measured`] with `span`.
    fn written(span: Option<TimeDelta>) -> String {
        serde_json::to_string(&Measured { span }).expect("serialize")
    }

    /// 100 years and one microsecond.
    fn century_and_a_microsecond() -> TimeDelta {
        TimeDelta::days(36_525) + TimeDelta::microseconds(1)
    }

    #[test]
    fn a_duration_is_written_as_seconds_with_its_microseconds() {
        let duration = TimeDelta::microseconds(2_310_417);

        assert_eq!(
            serde_json::to_string(&Waited { wait: duration }).expect("serialize"),
            r#"{"wait":2.310417}"#
        );
        assert_eq!(written(Some(duration)), r#"{"span":2.310417}"#);
        assert_eq!(
            written(Some(century_and_a_microsecond())),
            r#"{"span":3155760000.000001}"#
        );
    }

    #[test]
    fn a_whole_duration_is_written_as_a_float() {
        assert_eq!(written(Some(TimeDelta::seconds(30))), r#"{"span":30.0}"#);
    }

    #[test]
    fn a_negative_duration_keeps_its_sign() {
        assert_eq!(
            written(Some(TimeDelta::milliseconds(-1_500))),
            r#"{"span":-1.5}"#
        );
    }

    /// Only a host clock read carries nanoseconds; the JSON keeps the whole
    /// microseconds, toward zero.
    #[test]
    fn a_part_finer_than_a_microsecond_is_dropped_toward_zero() {
        assert_eq!(
            written(Some(TimeDelta::nanoseconds(1_000_000_999))),
            r#"{"span":1.0}"#
        );
        assert_eq!(
            written(Some(TimeDelta::nanoseconds(-1_000_000_999))),
            r#"{"span":-1.0}"#
        );
    }

    #[test]
    fn an_absent_duration_is_null() {
        assert_eq!(written(None), r#"{"span":null}"#);
    }

    #[test]
    fn a_written_duration_reads_back_exactly() {
        for span in [
            Some(TimeDelta::microseconds(2_310_417)),
            Some(TimeDelta::milliseconds(-1_500)),
            Some(century_and_a_microsecond()),
            Some(TimeDelta::zero()),
            None,
        ] {
            let text = written(span);

            let read: Measured = serde_json::from_str(&text).expect("read the written text");

            assert_eq!(read, Measured { span }, "{text}");
        }
    }

    #[test]
    fn a_number_outside_the_range_of_a_duration_is_refused() {
        let refused = serde_json::from_str::<Measured>(r#"{"span":1e300}"#)
            .expect_err("a number past the range of a duration is refused");

        assert!(
            refused
                .to_string()
                .contains("a duration of 1e300 seconds is outside the range of a duration"),
            "{refused}"
        );
        assert_eq!(
            duration_of_seconds(9.2e12),
            Some(TimeDelta::microseconds(9_200_000_000_000_000_000))
        );
        for seconds in [9.3e12, -9.3e12, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(duration_of_seconds(seconds), None, "{seconds}");
        }
    }
}
