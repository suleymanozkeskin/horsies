//! The config digest of a schedule: the SHA-256 of its pattern and timezone
//! in the digest text, which `horsies_schedule_state.config_hash` stores.
//!
//! The start write compares the stored digest with this one; a different
//! digest moves `next_run_at` to the next slot after the database time. So
//! the text is a contract. It is the core's own text, in the form that
//! Python 0.8 hashed
//! (`dumps_json({'pattern': pattern.model_dump(mode='json'), 'timezone': timezone})`),
//! so a stored digest stays valid. It does not follow the names of the
//! pattern text of the scheduler manifest (`kind`, `weekdays`,
//! `day_of_month`, `every`):
//! - Keys in this order: `pattern`, then `timezone`.
//! - The pattern holds its fields in the 0.8 order, the tag `type` first:
//!   `days` of a weekly pattern, `day` of a monthly pattern, and the four
//!   parts of an interval. The interval length is written largest unit
//!   first, with `null` for a zero part: 90 seconds is
//!   `{"type":"interval","seconds":30,"minutes":1,"hours":null,"days":null}`.
//! - A time of day is `HH:MM:SS`, with `.ffffff` (six digits of
//!   microseconds) when the microsecond is not zero. Python holds no finer
//!   part: nanoseconds below one microsecond are not in the text, and a
//!   leap second reads as second 59 with its fraction.
//! - Compact separators (`,` and `:`), and text other than ASCII as it is
//!   (`ensure_ascii=False`). Python and `serde_json` escape the same
//!   characters in a string.

use std::time::Duration;

use chrono::{NaiveTime, Timelike};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::config::schedule::{SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE};
use crate::core::config::{CronSchedule, SchedulePattern, Weekday};

/// The digest text of one schedule.
#[derive(Serialize)]
struct DigestText<'a> {
    pattern: PatternText<'a>,
    timezone: &'a str,
}

/// A pattern in the form of Python 0.8's `model_dump(mode='json')`. The
/// tag and the member names are this text's own: they do not follow the
/// pattern types.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PatternText<'a> {
    Interval {
        seconds: Option<u64>,
        minutes: Option<u64>,
        hours: Option<u64>,
        days: Option<u64>,
    },
    Hourly {
        minute: u32,
        second: u32,
    },
    Daily {
        time: String,
    },
    Weekly {
        days: &'a [Weekday],
        time: String,
    },
    Monthly {
        day: u32,
        time: String,
    },
    /// The serde form of [`CronSchedule`] is Python's form: every field and
    /// every term field is written, in Python's order. The pinned texts of
    /// the tests hold it to that.
    Cron(&'a CronSchedule),
}

impl<'a> PatternText<'a> {
    fn of(pattern: &'a SchedulePattern) -> Self {
        match pattern {
            SchedulePattern::Interval(interval) => interval_digest_parts(interval.every),
            SchedulePattern::Hourly(hourly) => Self::Hourly {
                minute: hourly.minute,
                second: hourly.second,
            },
            SchedulePattern::Daily(daily) => Self::Daily {
                time: python_time_text(daily.time),
            },
            SchedulePattern::Weekly(weekly) => Self::Weekly {
                days: &weekly.weekdays,
                time: python_time_text(weekly.time),
            },
            SchedulePattern::Monthly(monthly) => Self::Monthly {
                day: monthly.day_of_month,
                time: python_time_text(monthly.time),
            },
            SchedulePattern::Cron(cron) => Self::Cron(cron),
        }
    }
}

/// The four parts of `every`: days, hours, minutes and seconds, the largest
/// unit first, `None` for a zero part. One length has one form. A fraction
/// of a second is not in the text: the config refuses it. Pure.
fn interval_digest_parts<'a>(every: Duration) -> PatternText<'a> {
    let seconds = every.as_secs();
    let part = |value: u64| -> Option<u64> {
        match value {
            0 => None,
            _ => Some(value),
        }
    };
    PatternText::Interval {
        seconds: part(seconds % SECONDS_PER_MINUTE),
        minutes: part(seconds % SECONDS_PER_HOUR / SECONDS_PER_MINUTE),
        hours: part(seconds % SECONDS_PER_DAY / SECONDS_PER_HOUR),
        days: part(seconds / SECONDS_PER_DAY),
    }
}

/// A time of day as Python's `datetime.time.isoformat()` writes it.
fn python_time_text(time: NaiveTime) -> String {
    let microsecond = (time.nanosecond() % 1_000_000_000) / 1_000;
    match microsecond {
        0 => format!(
            "{:02}:{:02}:{:02}",
            time.hour(),
            time.minute(),
            time.second()
        ),
        _ => format!(
            "{:02}:{:02}:{:02}.{microsecond:06}",
            time.hour(),
            time.minute(),
            time.second()
        ),
    }
}

/// The text that the digest of a schedule hashes. Pure.
pub(crate) fn config_digest_text(pattern: &SchedulePattern, timezone: &str) -> String {
    let text = DigestText {
        pattern: PatternText::of(pattern),
        timezone,
    };
    // The digest types hold only text, integers, options and lists of
    // them; serializing them to a string cannot fail. A failure is a defect.
    serde_json::to_string(&text).expect("the schedule digest text serializes to JSON")
}

/// The digest of a schedule's pattern and timezone: SHA-256 of
/// [`config_digest_text`]. A change of input, queue or catch-up settings
/// does not change it. Pure.
pub(crate) fn config_digest(pattern: &SchedulePattern, timezone: &str) -> [u8; 32] {
    Sha256::digest(config_digest_text(pattern, timezone).as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::IntervalSchedule;

    /// One row of `config_digest_vectors.json`: the pattern text of the
    /// scheduler manifest and its timezone, the digest text of the pattern,
    /// its SHA-256 (lowercase hex), and what the schedule config does with
    /// the pattern.
    #[derive(serde::Deserialize)]
    struct DigestVector {
        name: String,
        pattern_json: String,
        timezone: String,
        text: String,
        digest_hex: String,
        config_check: ConfigCheck,
    }

    /// What the schedule config of each client does with a vector's pattern.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    enum ConfigCheck {
        /// The config takes the pattern.
        Accepted,
        /// The config refuses the pattern: its time of day has a fraction of
        /// a second. The digest text stays a contract vector.
        RefusedTimeWithFraction,
    }

    /// The vector table.
    #[derive(serde::Deserialize)]
    struct DigestVectors {
        vectors: Vec<DigestVector>,
    }

    /// Every pattern kind, every cron term kind and every day selector
    /// (`config_digest_vectors.json`; the Python test
    /// `clients/python/tests/unit/test_schedule_config_digest_vectors.py` pins the same
    /// table).
    fn digest_vectors() -> Vec<DigestVector> {
        let table: DigestVectors = serde_json::from_str(include_str!("config_digest_vectors.json"))
            .expect("the vector table parses");
        assert!(!table.vectors.is_empty(), "the vector table has rows");
        table.vectors
    }

    /// The pattern of a vector, read by serde alone: a time of day with a
    /// fraction of a second reads too.
    fn vector_pattern(vector: &DigestVector) -> SchedulePattern {
        serde_json::from_str(&vector.pattern_json)
            .unwrap_or_else(|error| panic!("{}: the pattern text parses: {error}", vector.name))
    }

    fn every_seconds(seconds: u64) -> SchedulePattern {
        SchedulePattern::Interval(IntervalSchedule {
            every: Duration::from_secs(seconds),
        })
    }

    fn hex(digest: &[u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_digest_text_of_each_vector_pattern_is_its_stored_text() {
        for vector in digest_vectors() {
            assert_eq!(
                config_digest_text(&vector_pattern(&vector), &vector.timezone),
                vector.text,
                "{}",
                vector.name
            );
        }
    }

    #[test]
    fn the_digest_of_each_vector_pattern_is_its_stored_digest() {
        for vector in digest_vectors() {
            assert_eq!(
                hex(&config_digest(&vector_pattern(&vector), &vector.timezone)),
                vector.digest_hex,
                "{}",
                vector.name
            );
        }
    }

    /// A manifest pattern text that Python wrote parses to the pattern whose
    /// digest is the stored digest, for every pattern kind. A text whose
    /// time of day has a fraction of a second is refused.
    #[test]
    fn the_pattern_text_of_each_vector_parses_to_a_pattern_with_the_stored_digest() {
        let vectors = digest_vectors();
        for vector in &vectors {
            let parsed =
                crate::scheduler_manifest::PatternJson::try_from(vector.pattern_json.clone());
            match (vector.config_check, parsed) {
                (ConfigCheck::Accepted, Ok(parsed)) => assert_eq!(
                    hex(&config_digest(parsed.pattern(), &vector.timezone)),
                    vector.digest_hex,
                    "{}",
                    vector.name
                ),
                (ConfigCheck::Accepted, Err(refused)) => {
                    panic!("{}: {} parses: {refused}", vector.name, vector.pattern_json)
                }
                (ConfigCheck::RefusedTimeWithFraction, Err(refused)) => assert!(
                    refused.contains("time must be a whole second"),
                    "{}: {refused}",
                    vector.name
                ),
                (ConfigCheck::RefusedTimeWithFraction, Ok(_parsed)) => panic!(
                    "{}: {} has a fraction of a second and parses",
                    vector.name, vector.pattern_json
                ),
            }
        }
        let checks: Vec<ConfigCheck> = vectors.iter().map(|vector| vector.config_check).collect();
        assert!(checks.contains(&ConfigCheck::Accepted));
        assert!(checks.contains(&ConfigCheck::RefusedTimeWithFraction));
    }

    /// The digest text writes an interval in one form: the largest unit
    /// first, `null` for a zero part. Two splits of one length that 0.8
    /// hashed apart (`seconds=90` and `minutes=1, seconds=30`) are one
    /// length now, with one digest text.
    #[test]
    fn the_digest_text_writes_an_interval_largest_unit_first() {
        let texts = [
            (
                30,
                r#"{"type":"interval","seconds":30,"minutes":null,"hours":null,"days":null}"#,
            ),
            (
                90,
                r#"{"type":"interval","seconds":30,"minutes":1,"hours":null,"days":null}"#,
            ),
            (
                90_061,
                r#"{"type":"interval","seconds":1,"minutes":1,"hours":1,"days":1}"#,
            ),
            (
                86_400,
                r#"{"type":"interval","seconds":null,"minutes":null,"hours":null,"days":1}"#,
            ),
            (
                31_536_000,
                r#"{"type":"interval","seconds":null,"minutes":null,"hours":null,"days":365}"#,
            ),
        ];
        for (seconds, pattern) in texts {
            assert_eq!(
                config_digest_text(&every_seconds(seconds), "UTC"),
                format!(r#"{{"pattern":{pattern},"timezone":"UTC"}}"#),
            );
        }
    }

    #[test]
    fn a_time_below_one_microsecond_is_not_in_the_text() {
        let nanoseconds =
            NaiveTime::from_hms_nano_opt(3, 0, 0, 500_000_999).expect("a valid time of day");
        assert_eq!(python_time_text(nanoseconds), "03:00:00.500000");
        let below = NaiveTime::from_hms_nano_opt(3, 0, 0, 999).expect("a valid time of day");
        assert_eq!(python_time_text(below), "03:00:00");
    }

    #[test]
    fn text_outside_ascii_is_written_as_it_is() {
        let pattern = every_seconds(1);
        assert_eq!(
            config_digest_text(&pattern, "Zone/Ä"),
            r#"{"pattern":{"type":"interval","seconds":1,"minutes":null,"hours":null,"days":null},"timezone":"Zone/Ä"}"#
        );
    }
}
