//! The config digest of a schedule: the SHA-256 of its pattern and timezone
//! in the text that Python's `Scheduler._compute_config_hash` hashes.
//!
//! The start write compares the stored digest with this one; a different
//! digest moves `next_run_at` to the next slot after the database time. So
//! the text is a contract, and Python is its specification:
//! `dumps_json({'pattern': pattern.model_dump(mode='json'), 'timezone': timezone})`.
//! - Keys in this order: `pattern`, then `timezone`.
//! - The pattern holds every field of its Python model in declaration order,
//!   `type` first. An unset optional field is `null` (an interval of 60
//!   seconds is `{"type":"interval","seconds":60,"minutes":null,"hours":null,"days":null}`).
//! - A time of day is `HH:MM:SS`, with `.ffffff` (six digits of
//!   microseconds) when the microsecond is not zero. Python holds no finer
//!   part: nanoseconds below one microsecond are not in the text, and a
//!   leap second reads as second 59 with its fraction.
//! - Compact separators (`,` and `:`), and text other than ASCII as it is
//!   (`ensure_ascii=False`). Python and `serde_json` escape the same
//!   characters in a string.

use chrono::{NaiveTime, Timelike};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::config::{CronSchedule, SchedulePattern, Weekday};

/// The digest text of one schedule.
#[derive(Serialize)]
struct DigestText<'a> {
    pattern: PatternText<'a>,
    timezone: &'a str,
}

/// A pattern in the form of Python's `model_dump(mode='json')`.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PatternText<'a> {
    Interval {
        seconds: Option<u32>,
        minutes: Option<u32>,
        hours: Option<u32>,
        days: Option<u32>,
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
            SchedulePattern::Interval(interval) => Self::Interval {
                seconds: interval.seconds,
                minutes: interval.minutes,
                hours: interval.hours,
                days: interval.days,
            },
            SchedulePattern::Hourly(hourly) => Self::Hourly {
                minute: hourly.minute,
                second: hourly.second,
            },
            SchedulePattern::Daily(daily) => Self::Daily {
                time: python_time_text(daily.time),
            },
            SchedulePattern::Weekly(weekly) => Self::Weekly {
                days: &weekly.days,
                time: python_time_text(weekly.time),
            },
            SchedulePattern::Monthly(monthly) => Self::Monthly {
                day: monthly.day,
                time: python_time_text(monthly.time),
            },
            SchedulePattern::Cron(cron) => Self::Cron(cron),
        }
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

    /// One row of `config_digest_vectors.json`: the text that Python's
    /// `_compute_config_hash` hashes for one pattern and timezone, its
    /// SHA-256 (lowercase hex), and what the schedule config does with the
    /// pattern.
    #[derive(serde::Deserialize)]
    struct PythonVector {
        name: String,
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
    struct PythonVectors {
        vectors: Vec<PythonVector>,
    }

    /// The pattern and the timezone of a digest text, read by serde alone:
    /// a time of day with a fraction of a second reads too.
    #[derive(serde::Deserialize)]
    struct DigestInput {
        pattern: SchedulePattern,
        timezone: String,
    }

    /// Every pattern kind, every cron term kind and every day selector
    /// (`config_digest_vectors.json`; the Python test
    /// `clients/python/tests/unit/test_schedule_config_digest_vectors.py` pins the same
    /// table).
    fn python_vectors() -> Vec<PythonVector> {
        let table: PythonVectors = serde_json::from_str(include_str!("config_digest_vectors.json"))
            .expect("the vector table parses");
        assert!(!table.vectors.is_empty(), "the vector table has rows");
        table.vectors
    }

    fn digest_input(vector: &PythonVector) -> DigestInput {
        serde_json::from_str(&vector.text)
            .unwrap_or_else(|error| panic!("{}: the digest text parses: {error}", vector.name))
    }

    fn interval(
        seconds: Option<u32>,
        minutes: Option<u32>,
        hours: Option<u32>,
        days: Option<u32>,
    ) -> SchedulePattern {
        SchedulePattern::Interval(IntervalSchedule {
            seconds,
            minutes,
            hours,
            days,
        })
    }

    fn hex(digest: &[u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_digest_text_is_the_text_python_hashes_for_every_pattern_kind() {
        for vector in python_vectors() {
            let input = digest_input(&vector);
            assert_eq!(
                config_digest_text(&input.pattern, &input.timezone),
                vector.text,
                "{}",
                vector.name
            );
        }
    }

    #[test]
    fn the_digest_is_the_digest_python_stores_for_every_pattern_kind() {
        for vector in python_vectors() {
            let input = digest_input(&vector);
            assert_eq!(
                hex(&config_digest(&input.pattern, &input.timezone)),
                vector.digest_hex,
                "{}",
                vector.name
            );
        }
    }

    /// The pattern text inside a vector's digest text: the text of Python's
    /// `pattern.model_dump(mode='json')`, the form of a scheduler manifest's
    /// `pattern_json`.
    fn pattern_text(vector: &PythonVector) -> &str {
        let start = "{\"pattern\":".len();
        let end = vector
            .text
            .rfind(",\"timezone\":")
            .expect("a digest text ends with the timezone");
        &vector.text[start..end]
    }

    /// A manifest pattern text that Python wrote parses to the pattern whose
    /// digest is the digest that Python stores, for every pattern kind. A
    /// text whose time of day has a fraction of a second is refused.
    #[test]
    fn the_pattern_text_of_each_vector_parses_to_a_pattern_with_the_python_digest() {
        let vectors = python_vectors();
        for vector in &vectors {
            let text = pattern_text(vector);
            let timezone = digest_input(vector).timezone;
            let parsed = crate::scheduler_manifest::PatternJson::try_from(text.to_owned());
            match (vector.config_check, parsed) {
                (ConfigCheck::Accepted, Ok(parsed)) => assert_eq!(
                    hex(&config_digest(parsed.pattern(), &timezone)),
                    vector.digest_hex,
                    "{}",
                    vector.name
                ),
                (ConfigCheck::Accepted, Err(refused)) => {
                    panic!("{}: {text} parses: {refused}", vector.name)
                }
                (ConfigCheck::RefusedTimeWithFraction, Err(refused)) => assert!(
                    refused.contains("time must be a whole second"),
                    "{}: {refused}",
                    vector.name
                ),
                (ConfigCheck::RefusedTimeWithFraction, Ok(_parsed)) => panic!(
                    "{}: {text} has a fraction of a second and parses",
                    vector.name
                ),
            }
        }
        let checks: Vec<ConfigCheck> = vectors.iter().map(|vector| vector.config_check).collect();
        assert!(checks.contains(&ConfigCheck::Accepted));
        assert!(checks.contains(&ConfigCheck::RefusedTimeWithFraction));
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
        let pattern = interval(Some(1), None, None, None);
        assert_eq!(
            config_digest_text(&pattern, "Zone/Ä"),
            r#"{"pattern":{"type":"interval","seconds":1,"minutes":null,"hours":null,"days":null},"timezone":"Zone/Ä"}"#
        );
    }
}
