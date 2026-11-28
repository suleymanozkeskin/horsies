//! `StoredDateTime<T>` and `StoredTime`: a chrono date-time or time of day
//! that a task input or a task result stores in Python's text.
//!
//! Python stores a date-time of a task input or result in pydantic's JSON
//! form: the fraction of the second with 6 digits when the microsecond is not
//! 0, else no fraction (`2026-08-12T09:10:12.500000Z`). chrono's own serde
//! form writes a whole millisecond with 3 digits (`2026-08-12T09:10:12.500Z`).
//! Both clients read both texts to the same instant. A value of
//! `StoredDateTime` writes Python's text, so the two clients store the same
//! bytes for it. The time of day (`datetime.time`) has the same rule:
//! `09:10:12.500000` in Python, `09:10:12.500` in chrono's serde form, and
//! `StoredTime` writes Python's.

use chrono::{DateTime, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone, Timelike};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Nanoseconds in one second.
const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// Nanoseconds in one microsecond.
const NANOS_PER_MICROSECOND: u32 = 1_000;

/// A naive date-time with no fraction of the second.
const NAIVE_WHOLE_SECOND: &str = "%Y-%m-%dT%H:%M:%S";

/// A naive date-time with 6 fraction digits.
const NAIVE_MICROSECONDS: &str = "%Y-%m-%dT%H:%M:%S%.6f";

/// A naive date-time with 9 fraction digits.
const NAIVE_NANOSECONDS: &str = "%Y-%m-%dT%H:%M:%S%.9f";

/// A time of day with no fraction of the second.
const TIME_WHOLE_SECOND: &str = "%H:%M:%S";

/// A time of day with 6 fraction digits.
const TIME_MICROSECONDS: &str = "%H:%M:%S%.6f";

/// A time of day with 9 fraction digits.
const TIME_NANOSECONDS: &str = "%H:%M:%S%.9f";

/// A chrono date-time that is stored in Python's text.
///
/// - `StoredDateTime<DateTime<Tz>>`: RFC 3339, with the offset `Z` when it
///   is 0, else `+HH:MM` or `-HH:MM` (`2026-08-12T09:10:12.500000+02:00`).
/// - `StoredDateTime<NaiveDateTime>`: the same with no offset
///   (`2026-08-12T09:10:12.500000`).
/// - The fraction of the second: none for a whole second, 6 digits for a
///   whole microsecond, and 9 digits for a part below the microsecond, which
///   Python cannot hold.
///
/// The read is chrono's: any count of fraction digits. Use it in place of the
/// chrono type in a task's parameters, its output type, or a field of either:
/// `async fn report(day: StoredDateTime<DateTime<Utc>>)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoredDateTime<T>(pub T);

/// A chrono time of day that is stored in Python's text: `09:10:12`,
/// `09:10:12.500000`, or `09:10:12.654321001` for a part below the
/// microsecond, which Python cannot hold (the fraction of
/// [`StoredDateTime`]).
///
/// The read is chrono's: any count of fraction digits. Use it in place of
/// `chrono::NaiveTime` in a task's parameters, its output type, or a field of
/// either: `async fn open_at(time: StoredTime)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoredTime(pub NaiveTime);

/// The fraction of the second in Python's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fraction {
    /// A whole second: no fraction.
    WholeSecond,
    /// A whole microsecond: 6 digits.
    Microseconds,
    /// A part below the microsecond: 9 digits.
    Nanoseconds,
}

impl Fraction {
    /// The fraction of a time whose chrono `Timelike::nanosecond` is
    /// `nanosecond` (above 999,999,999 in a leap second). Pure.
    fn of(nanosecond: u32) -> Self {
        match (
            nanosecond % NANOS_PER_SECOND,
            nanosecond % NANOS_PER_MICROSECOND,
        ) {
            (0, _) => Self::WholeSecond,
            (_, 0) => Self::Microseconds,
            (_, _) => Self::Nanoseconds,
        }
    }

    fn seconds_format(self) -> SecondsFormat {
        match self {
            Self::WholeSecond => SecondsFormat::Secs,
            Self::Microseconds => SecondsFormat::Micros,
            Self::Nanoseconds => SecondsFormat::Nanos,
        }
    }

    fn time_pattern(self) -> &'static str {
        match self {
            Self::WholeSecond => TIME_WHOLE_SECOND,
            Self::Microseconds => TIME_MICROSECONDS,
            Self::Nanoseconds => TIME_NANOSECONDS,
        }
    }

    fn naive_pattern(self) -> &'static str {
        match self {
            Self::WholeSecond => NAIVE_WHOLE_SECOND,
            Self::Microseconds => NAIVE_MICROSECONDS,
            Self::Nanoseconds => NAIVE_NANOSECONDS,
        }
    }
}

impl<Tz: TimeZone> Serialize for StoredDateTime<DateTime<Tz>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let format = Fraction::of(self.0.nanosecond()).seconds_format();
        serializer.serialize_str(&self.0.to_rfc3339_opts(format, true))
    }
}

impl Serialize for StoredDateTime<NaiveDateTime> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let pattern = Fraction::of(self.0.nanosecond()).naive_pattern();
        serializer.collect_str(&self.0.format(pattern))
    }
}

impl Serialize for StoredTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let pattern = Fraction::of(self.0.nanosecond()).time_pattern();
        serializer.collect_str(&self.0.format(pattern))
    }
}

impl<'de, Tz> Deserialize<'de> for StoredDateTime<DateTime<Tz>>
where
    Tz: TimeZone,
    DateTime<Tz>: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DateTime::<Tz>::deserialize(deserializer).map(Self)
    }
}

impl<'de> Deserialize<'de> for StoredDateTime<NaiveDateTime> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        NaiveDateTime::deserialize(deserializer).map(Self)
    }
}

impl<'de> Deserialize<'de> for StoredTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        NaiveTime::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, NaiveDate, Utc};

    use super::*;

    /// 2026-08-12 09:10:12 UTC plus `nanoseconds`.
    fn utc(nanoseconds: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2026, 8, 12)
            .and_then(|day| day.and_hms_nano_opt(9, 10, 12, nanoseconds))
            .expect("a valid time")
            .and_utc()
    }

    fn text<T>(value: T) -> String
    where
        StoredDateTime<T>: Serialize,
    {
        serde_json::to_string(&StoredDateTime(value)).expect("a date-time encodes")
    }

    /// The texts of pydantic's `mode='json'` for the same values.
    #[test]
    fn an_instant_is_written_as_python_writes_it() {
        assert_eq!(text(utc(0)), r#""2026-08-12T09:10:12Z""#);
        assert_eq!(text(utc(500_000_000)), r#""2026-08-12T09:10:12.500000Z""#);
        assert_eq!(text(utc(654_321_000)), r#""2026-08-12T09:10:12.654321Z""#);
        assert_eq!(text(utc(1_000)), r#""2026-08-12T09:10:12.000001Z""#);
    }

    #[test]
    fn a_part_below_the_microsecond_keeps_9_digits() {
        assert_eq!(
            text(utc(654_321_001)),
            r#""2026-08-12T09:10:12.654321001Z""#
        );
    }

    #[test]
    fn an_offset_is_written_with_its_sign_and_zero_as_z() {
        let east = FixedOffset::east_opt(2 * 3600).expect("an offset");
        let west = FixedOffset::west_opt(5 * 3600 + 30 * 60).expect("an offset");
        let zero = FixedOffset::east_opt(0).expect("an offset");
        assert_eq!(
            text(utc(500_000_000).with_timezone(&east)),
            r#""2026-08-12T11:10:12.500000+02:00""#
        );
        assert_eq!(
            text(utc(500_000_000).with_timezone(&west)),
            r#""2026-08-12T03:40:12.500000-05:30""#
        );
        assert_eq!(
            text(utc(500_000_000).with_timezone(&zero)),
            r#""2026-08-12T09:10:12.500000Z""#
        );
    }

    #[test]
    fn a_naive_date_time_is_written_with_no_offset() {
        assert_eq!(
            text(utc(500_000_000).naive_utc()),
            r#""2026-08-12T09:10:12.500000""#
        );
        assert_eq!(text(utc(0).naive_utc()), r#""2026-08-12T09:10:12""#);
        assert_eq!(
            text(utc(654_321_001).naive_utc()),
            r#""2026-08-12T09:10:12.654321001""#
        );
    }

    /// chrono's text and Python's text read to the same value.
    #[test]
    fn the_read_takes_any_count_of_fraction_digits() {
        for written in [
            r#""2026-08-12T09:10:12.500Z""#,
            r#""2026-08-12T09:10:12.500000Z""#,
            r#""2026-08-12T09:10:12.5+00:00""#,
        ] {
            let read: StoredDateTime<DateTime<Utc>> =
                serde_json::from_str(written).expect("a date-time reads");
            assert_eq!(read, StoredDateTime(utc(500_000_000)), "{written}");
        }
        let naive: StoredDateTime<NaiveDateTime> =
            serde_json::from_str(r#""2026-08-12T09:10:12.500""#).expect("a date-time reads");
        assert_eq!(naive, StoredDateTime(utc(500_000_000).naive_utc()));
    }

    fn time_of_day(nanoseconds: u32) -> NaiveTime {
        NaiveTime::from_hms_nano_opt(9, 10, 12, nanoseconds).expect("a valid time")
    }

    fn time_text(value: NaiveTime) -> String {
        serde_json::to_string(&StoredTime(value)).expect("a time encodes")
    }

    #[test]
    fn a_whole_second_time_has_no_fraction() {
        assert_eq!(time_text(time_of_day(0)), r#""09:10:12""#);
    }

    /// chrono's own text is `09:10:12.500`; pydantic's is `.500000`.
    #[test]
    fn a_whole_millisecond_time_has_6_digits() {
        assert_eq!(time_text(time_of_day(500_000_000)), r#""09:10:12.500000""#);
    }

    #[test]
    fn a_microsecond_time_has_6_digits() {
        assert_eq!(time_text(time_of_day(654_321_000)), r#""09:10:12.654321""#);
        assert_eq!(time_text(time_of_day(1_000)), r#""09:10:12.000001""#);
    }

    #[test]
    fn a_time_below_the_microsecond_has_9_digits() {
        assert_eq!(
            time_text(time_of_day(654_321_001)),
            r#""09:10:12.654321001""#
        );
    }

    #[test]
    fn the_time_read_takes_any_count_of_fraction_digits() {
        for written in [
            r#""09:10:12.500""#,
            r#""09:10:12.500000""#,
            r#""09:10:12.5""#,
        ] {
            let read: StoredTime = serde_json::from_str(written).expect("a time reads");
            assert_eq!(read, StoredTime(time_of_day(500_000_000)), "{written}");
        }
    }
}
