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
    use crate::core::config::{
        CronEnumTerm, CronNumericTerm, DailySchedule, DaySelector, HourlySchedule,
        IntervalSchedule, Month, MonthlySchedule, WeeklySchedule,
    };

    /// One contract vector: a pattern, its timezone, and the text and the
    /// digest (lowercase hex) of Python's `Scheduler._compute_config_hash`
    /// for the same pattern, written by Python 3.13 with pydantic 2.13.
    struct PythonVector {
        pattern: SchedulePattern,
        timezone: &'static str,
        text: &'static str,
        digest_hex: &'static str,
    }

    fn time(hour: u32, minute: u32, second: u32, microsecond: u32) -> NaiveTime {
        NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
            .expect("a valid time of day")
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

    fn cron(
        minute: Vec<CronNumericTerm>,
        hour: Vec<CronNumericTerm>,
        month: Vec<CronEnumTerm<Month>>,
        day: DaySelector,
    ) -> SchedulePattern {
        SchedulePattern::Cron(CronSchedule {
            minute,
            hour,
            month,
            day,
        })
    }

    fn values(values: &[i64]) -> Vec<CronNumericTerm> {
        vec![CronNumericTerm::Values {
            values: values.to_vec(),
        }]
    }

    /// Every pattern kind, every cron term kind and every day selector.
    fn python_vectors() -> Vec<PythonVector> {
        vec![
            PythonVector {
                pattern: interval(Some(60), None, None, None),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"interval","seconds":60,"minutes":null,"hours":null,"days":null},"timezone":"UTC"}"#,
                digest_hex: "a7bba31d521252fd1fb6e910224d2c9a13daa9d45b4c3a1c4569f7b76a2e92e1",
            },
            PythonVector {
                pattern: interval(None, Some(30), Some(1), None),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"interval","seconds":null,"minutes":30,"hours":1,"days":null},"timezone":"UTC"}"#,
                digest_hex: "9638bca267258dd8f236c17002ff36fa5d1aa1f00290aed2f171ad2e3539f514",
            },
            PythonVector {
                pattern: SchedulePattern::Hourly(HourlySchedule {
                    minute: 15,
                    second: 30,
                }),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"hourly","minute":15,"second":30},"timezone":"UTC"}"#,
                digest_hex: "25b79bc9f85f234b97768a4bff5ee3e758a876a30a30111024cc6da579540b12",
            },
            PythonVector {
                pattern: SchedulePattern::Daily(DailySchedule {
                    time: time(3, 0, 0, 0),
                }),
                timezone: "Europe/Berlin",
                text: r#"{"pattern":{"type":"daily","time":"03:00:00"},"timezone":"Europe/Berlin"}"#,
                digest_hex: "f004ec628b9dbf160eb0b7028450f3b5c2dfe712f06df24c08a880565a56d628",
            },
            PythonVector {
                pattern: SchedulePattern::Daily(DailySchedule {
                    time: time(3, 0, 0, 500_000),
                }),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"daily","time":"03:00:00.500000"},"timezone":"UTC"}"#,
                digest_hex: "d51e89e9e42670c3bb9ba17f04413df4f87874ba7c41a878608cfa413a5a663b",
            },
            PythonVector {
                pattern: SchedulePattern::Daily(DailySchedule {
                    time: time(3, 0, 0, 5),
                }),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"daily","time":"03:00:00.000005"},"timezone":"UTC"}"#,
                digest_hex: "c47b59160294148a52abf1d2475efc967ea22695eba6d3eba419dede43563d3b",
            },
            PythonVector {
                pattern: SchedulePattern::Weekly(WeeklySchedule {
                    days: vec![Weekday::Friday, Weekday::Monday],
                    time: time(9, 0, 0, 0),
                }),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"weekly","days":["friday","monday"],"time":"09:00:00"},"timezone":"UTC"}"#,
                digest_hex: "e4c724242468a3a913a7db5dae23ec3492be34a96d15ac5c6ca628ec82765e1c",
            },
            PythonVector {
                pattern: SchedulePattern::Monthly(MonthlySchedule {
                    day: 31,
                    time: time(23, 59, 59, 0),
                }),
                timezone: "America/New_York",
                text: r#"{"pattern":{"type":"monthly","day":31,"time":"23:59:59"},"timezone":"America/New_York"}"#,
                digest_hex: "82a41dbc0651b5d208e9d5ff2d0237442f22729f7e4c4695012f31fd83a630fa",
            },
            PythonVector {
                pattern: cron(
                    vec![CronNumericTerm::Every],
                    vec![CronNumericTerm::Step { step: 6 }],
                    vec![CronEnumTerm::Every],
                    DaySelector::EveryDay,
                ),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"cron","minute":[{"kind":"every"}],"hour":[{"kind":"step","step":6}],"month":[{"kind":"every"}],"day":{"kind":"every_day"}},"timezone":"UTC"}"#,
                digest_hex: "d0b8bae8f2ccfdac8b4f2df7f494643dad14fe4303bfa5808efa54078b9b4ff9",
            },
            PythonVector {
                pattern: cron(
                    values(&[30, 0]),
                    vec![CronNumericTerm::Range {
                        start: 9,
                        end: 17,
                        step: 1,
                    }],
                    vec![
                        CronEnumTerm::EnumValues {
                            values: vec![Month::January],
                        },
                        CronEnumTerm::EnumRange {
                            start: Month::March,
                            end: Month::June,
                            step: 2,
                        },
                        CronEnumTerm::EnumStep { step: 3 },
                    ],
                    DaySelector::EitherDay {
                        day_of_month: values(&[13]),
                        day_of_week: vec![
                            CronEnumTerm::EnumValues {
                                values: vec![Weekday::Friday],
                            },
                            CronEnumTerm::EnumRange {
                                start: Weekday::Monday,
                                end: Weekday::Wednesday,
                                step: 1,
                            },
                        ],
                    },
                ),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"cron","minute":[{"kind":"values","values":[30,0]}],"hour":[{"kind":"range","start":9,"end":17,"step":1}],"month":[{"kind":"enum_values","values":["january"]},{"kind":"enum_range","start":"march","end":"june","step":2},{"kind":"enum_step","step":3}],"day":{"kind":"either_day","day_of_month":[{"kind":"values","values":[13]}],"day_of_week":[{"kind":"enum_values","values":["friday"]},{"kind":"enum_range","start":"monday","end":"wednesday","step":1}]}},"timezone":"UTC"}"#,
                digest_hex: "892071fd113171a0ee43fd6ad1e517f4d6f06ffded63fe3540ce933aa9864ec4",
            },
            PythonVector {
                pattern: cron(
                    values(&[0]),
                    values(&[0]),
                    vec![CronEnumTerm::Every],
                    DaySelector::ByMonthDay {
                        day_of_month: vec![CronNumericTerm::Range {
                            start: 1,
                            end: 7,
                            step: 2,
                        }],
                    },
                ),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"cron","minute":[{"kind":"values","values":[0]}],"hour":[{"kind":"values","values":[0]}],"month":[{"kind":"every"}],"day":{"kind":"by_month_day","day_of_month":[{"kind":"range","start":1,"end":7,"step":2}]}},"timezone":"UTC"}"#,
                digest_hex: "e61837648d9fd5122c1de3c4b3037633104952df1152a796886d28eac2277581",
            },
            PythonVector {
                pattern: cron(
                    values(&[0]),
                    values(&[0]),
                    vec![CronEnumTerm::Every],
                    DaySelector::ByWeekday {
                        day_of_week: vec![CronEnumTerm::EnumStep { step: 2 }],
                    },
                ),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"cron","minute":[{"kind":"values","values":[0]}],"hour":[{"kind":"values","values":[0]}],"month":[{"kind":"every"}],"day":{"kind":"by_weekday","day_of_week":[{"kind":"enum_step","step":2}]}},"timezone":"UTC"}"#,
                digest_hex: "41cd9d21ab96b2891acc8722a086a457d22b2a46f283b285e28e14dbbd6a74b3",
            },
            PythonVector {
                pattern: cron(
                    values(&[0]),
                    values(&[0]),
                    vec![CronEnumTerm::Every],
                    DaySelector::BothDays {
                        day_of_month: values(&[13]),
                        day_of_week: vec![CronEnumTerm::Every],
                    },
                ),
                timezone: "UTC",
                text: r#"{"pattern":{"type":"cron","minute":[{"kind":"values","values":[0]}],"hour":[{"kind":"values","values":[0]}],"month":[{"kind":"every"}],"day":{"kind":"both_days","day_of_month":[{"kind":"values","values":[13]}],"day_of_week":[{"kind":"every"}]}},"timezone":"UTC"}"#,
                digest_hex: "9896ec7bd8eb1583fbd1ecdd935eec8a1a1b2f5ac13feb12aab71a0d68a00f20",
            },
        ]
    }

    fn hex(digest: &[u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn the_digest_text_is_the_text_python_hashes_for_every_pattern_kind() {
        for vector in python_vectors() {
            assert_eq!(
                config_digest_text(&vector.pattern, vector.timezone),
                vector.text
            );
        }
    }

    #[test]
    fn the_digest_is_the_digest_python_stores_for_every_pattern_kind() {
        for vector in python_vectors() {
            assert_eq!(
                hex(&config_digest(&vector.pattern, vector.timezone)),
                vector.digest_hex,
                "{}",
                vector.text
            );
        }
    }

    /// The pattern text inside a vector's digest text: the text of Python's
    /// `pattern.model_dump(mode='json')`, the form of a scheduler manifest's
    /// `pattern_json`.
    fn pattern_text(vector: &PythonVector) -> &'static str {
        let text: &'static str = vector.text;
        let start = "{\"pattern\":".len();
        let end = text
            .rfind(",\"timezone\":")
            .expect("a digest text ends with the timezone");
        &text[start..end]
    }

    /// The pattern texts of the vectors whose time of day has a fraction of
    /// a second. The Python config refuses such a `time`, and the manifest
    /// parser refuses its text (`SchedulePattern::validate`); their digests
    /// stay contract vectors of the digest text.
    const PATTERN_TEXTS_WITH_A_FRACTION_OF_A_SECOND: [&str; 2] = [
        r#"{"type":"daily","time":"03:00:00.500000"}"#,
        r#"{"type":"daily","time":"03:00:00.000005"}"#,
    ];

    /// A manifest pattern text that Python wrote parses to the pattern whose
    /// digest is the digest that Python stores, for every pattern kind. A
    /// text whose time of day has a fraction of a second is refused.
    #[test]
    fn the_pattern_text_of_each_vector_parses_to_a_pattern_with_the_python_digest() {
        let mut refused_texts = Vec::new();
        for vector in python_vectors() {
            let text = pattern_text(&vector);
            let parsed = crate::scheduler_manifest::PatternJson::try_from(text.to_owned());
            match (
                PATTERN_TEXTS_WITH_A_FRACTION_OF_A_SECOND.contains(&text),
                parsed,
            ) {
                (false, Ok(parsed)) => assert_eq!(
                    hex(&config_digest(parsed.pattern(), vector.timezone)),
                    vector.digest_hex,
                    "{text}"
                ),
                (false, Err(refused)) => panic!("{text} parses: {refused}"),
                (true, Err(refused)) => {
                    assert!(
                        refused.contains("time must be a whole second"),
                        "{text}: {refused}"
                    );
                    refused_texts.push(text);
                }
                (true, Ok(_parsed)) => panic!("{text} has a fraction of a second and parses"),
            }
        }
        assert_eq!(refused_texts, PATTERN_TEXTS_WITH_A_FRACTION_OF_A_SECOND);
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
