//! The core's checks of a schedule declaration, which `Horsies.create` calls
//! on a main thread. Each starts no core, opens no connection and reads no
//! clock, so a task child that runs the entry before `cli.run` still lets
//! the child host shape its process runtime.
//!
//! - `checkSchedulePattern(patternJson)`: the rule of the scheduler
//!   manifest's `pattern_json` (`PatternJson`): the text is a pattern of the
//!   core, and the pattern keeps its rules (`SchedulePattern::validate`).
//!   The refusal is a value, never a throw.
//! - `checkScheduleTimezone(name)`: whether the core's zone data holds the
//!   IANA zone `name`, by the rule of the scheduler start
//!   (`name.parse::<chrono_tz::Tz>()`, `SchedulerStartError::UnknownTimezone`).
//!   The check and the start read one zone table.
//! - `schedulerConfigFacts(appConfigJson, queueNames)`: what the accepted
//!   config resolves for the schedules, by the rules of the Rust client's
//!   scheduler manifest: the payload reject bound, the rerun-input default,
//!   and per queue the priority and the retention class key of a send that
//!   chooses no retention.
//!
//! No check applies the fork and stop rule of a core call: each reads only
//! its arguments and the core's tables, so the declaration of the `schedule` section never throws.

use chrono_tz::Tz;
use horsies_core::scheduler_manifest::PatternJson;
use horsies_core::{AppConfig, CustomQueueConfig, QueueMode};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::sends::send::SendPriority;
use crate::values::boundary::bigint_of_u64;

/// The subject of `checkSchedulePattern` in its refusals.
const PATTERN_SUBJECT: &str = "check_schedule_pattern";

/// The subject of `checkScheduleTimezone` in its refusals.
const TIMEZONE_SUBJECT: &str = "check_schedule_timezone";

/// The subject of `schedulerConfigFacts` in its refusals.
const FACTS_SUBJECT: &str = "scheduler_config_facts";

/// The outcome of the core's check of one pattern text.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatternCheck {
    /// The text is a pattern of the core, and the pattern keeps its rules.
    Accepted,
    /// The text is not a pattern of the core, or the pattern breaks a rule:
    /// `cause` names the rule.
    Refused { cause: String },
}

/// Check the pattern text `patternJson` with the rule of the scheduler
/// manifest's `pattern_json`. Pure: starts no core.
#[napi(ts_return_type = "NativeEnvelope<PatternCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_schedule_pattern(pattern_json: String) -> Envelope<PatternCheck> {
    sync_call(PATTERN_SUBJECT, || {
        Envelope::Value(pattern_check(pattern_json))
    })
}

/// The check of one pattern text. Pure.
fn pattern_check(text: String) -> PatternCheck {
    match PatternJson::try_from(text) {
        Ok(_pattern) => PatternCheck::Accepted,
        Err(cause) => PatternCheck::Refused { cause },
    }
}

/// Whether the core's zone data holds a zone name.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimezoneCheck {
    /// The core's zone data holds the name, as written.
    Known,
    /// The core's zone data does not hold the name: the scheduler start
    /// would refuse the schedule (`UnknownTimezone`).
    Unknown,
}

/// Check the zone name `name` with the core's zone data. Pure: starts no
/// core.
#[napi(ts_return_type = "NativeEnvelope<TimezoneCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_schedule_timezone(name: String) -> Envelope<TimezoneCheck> {
    sync_call(TIMEZONE_SUBJECT, || Envelope::Value(timezone_check(&name)))
}

/// The check of one zone name, by the rule of the scheduler start. Pure.
fn timezone_check(name: &str) -> TimezoneCheck {
    match name.parse::<Tz>() {
        Ok(_zone) => TimezoneCheck::Known,
        Err(_unknown) => TimezoneCheck::Unknown,
    }
}

/// The payload bound that each schedule's input meets: every slot sends the
/// same input, so an input above the bound is refused at every send.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectBound {
    /// The config refuses no input by its size.
    NoBound,
    /// A send of an input text of more UTF-8 bytes than `bytes` is refused.
    Bytes { bytes: BigInt },
}

/// What the accepted config gives a send on one queue.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueSendFacts {
    pub queue_name: String,
    /// The priority of the queue: the package's priority of the one queue
    /// of the default queue mode, or the custom queue's priority in the
    /// config (the core's default for a queue that names none).
    pub priority: SendPriority,
    /// The retention class of a send that chooses no retention: the queue's
    /// mapping in the config, else the default class.
    pub retention_class_key: String,
}

/// What the accepted config resolves for the schedules.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerConfigFacts {
    pub reject_bound: RejectBound,
    /// Keep each slot's input for a rerun of its task
    /// (`broker.retain_rerun_input_default`).
    pub retain_rerun_input: bool,
    /// The facts of each asked queue, in the asked order.
    pub queues: Vec<QueueSendFacts>,
}

/// The facts of the accepted app config `appConfigJson` (the core's config
/// text that `Horsies.create` checked) for the queues `queueNames`. Pure:
/// starts no core. A text that the core's config reader refuses, and a
/// queue that the custom queue mode does not hold, throw `CLIENT_DEFECT`:
/// the package asks only for the queues of the accepted config.
#[napi(ts_return_type = "NativeEnvelope<SchedulerConfigFacts>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn scheduler_config_facts(
    app_config_json: String,
    queue_names: Vec<String>,
) -> Envelope<SchedulerConfigFacts> {
    sync_call(FACTS_SUBJECT, || {
        match config_facts(&app_config_json, &queue_names) {
            Ok(facts) => Envelope::Value(facts),
            Err(thrown) => Envelope::Thrown(thrown),
        }
    })
}

/// The facts of `queue_names` in the config of `text`. Pure.
///
/// Errors: `CLIENT_DEFECT` for a text that the core's config reader
/// refuses, and for a queue that the custom queue mode does not hold.
fn config_facts(text: &str, queue_names: &[String]) -> Result<SchedulerConfigFacts, NativeThrown> {
    let config: AppConfig =
        serde_json::from_str(text).map_err(|error| NativeThrown::ClientDefect {
            subject: "appConfigJson".to_owned(),
            rule: format!("the config text reads as the core's AppConfig: {error}"),
        })?;
    let queues = queue_names
        .iter()
        .map(|queue| queue_facts(&config, queue))
        .collect::<Result<Vec<QueueSendFacts>, NativeThrown>>()?;
    Ok(SchedulerConfigFacts {
        reject_bound: match config.payload.reject_bytes {
            None => RejectBound::NoBound,
            Some(bytes) => RejectBound::Bytes {
                bytes: bigint_of_u64(bytes),
            },
        },
        retain_rerun_input: config.database.retain_rerun_input_default,
        queues,
    })
}

/// The facts of a send on `queue`. Pure.
fn queue_facts(config: &AppConfig, queue: &str) -> Result<QueueSendFacts, NativeThrown> {
    Ok(QueueSendFacts {
        queue_name: queue.to_owned(),
        priority: queue_priority(config.queue_mode, config.custom_queues.as_deref(), queue)?,
        retention_class_key: config.retention.resolve_queue_retention(queue).class_key(),
    })
}

/// The priority of a send on `queue` in the queue mode `mode`, or the
/// `CLIENT_DEFECT` of a queue that the custom queue mode does not hold.
/// Pure.
fn queue_priority(
    mode: QueueMode,
    custom_queues: Option<&[CustomQueueConfig]>,
    queue: &str,
) -> Result<SendPriority, NativeThrown> {
    match mode {
        QueueMode::Default => Ok(SendPriority::DefaultQueue),
        QueueMode::Custom => custom_queues
            .unwrap_or_default()
            .iter()
            .find(|configured| configured.name == queue)
            .map(|configured| SendPriority::Configured {
                priority: configured.priority,
            })
            .ok_or_else(|| NativeThrown::ClientDefect {
                subject: "queueNames".to_owned(),
                rule: format!("a custom queue of the accepted config, not {queue:?}"),
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_MODE: &str =
        r#"{"database": {"database_url": "postgresql://127.0.0.1:55432/horsies"}}"#;

    const CUSTOM_MODE: &str = r#"{
        "database": {"database_url": "postgresql://127.0.0.1:55432/horsies", "retain_rerun_input_default": true},
        "queue_mode": "custom",
        "custom_queues": [{"name": "urgent", "priority": 20}, {"name": "bulk"}],
        "payload": {"reject_bytes": 4096},
        "retention": {"queue_retention": {"bulk": null}}
    }"#;

    fn facts(text: &str, queues: &[&str]) -> Result<SchedulerConfigFacts, NativeThrown> {
        let names: Vec<String> = queues.iter().map(|queue| (*queue).to_owned()).collect();
        config_facts(text, &names)
    }

    #[test]
    fn each_fixture_pattern_text_is_accepted() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../core/tests/fixtures/schedule_pattern/pattern_texts.json"
        ))
        .expect("the fixture is JSON");
        let rows = fixture["patterns"]
            .as_array()
            .expect("the fixture holds a pattern list");
        assert!(!rows.is_empty());
        for row in rows {
            let text = row["text"]
                .as_str()
                .expect("each row holds a text")
                .to_owned();
            assert_eq!(
                pattern_check(text.clone()),
                PatternCheck::Accepted,
                "{text}"
            );
        }
    }

    #[test]
    fn a_pattern_that_breaks_a_rule_is_refused_with_the_rule() {
        let PatternCheck::Refused { cause } =
            pattern_check(r#"{"kind":"hourly","minute":60,"second":0}"#.to_owned())
        else {
            panic!("minute 60 breaks the hourly rule");
        };
        assert!(
            cause.starts_with("the pattern breaks its rules: "),
            "{cause}"
        );
    }

    #[test]
    fn a_time_with_a_fraction_of_a_second_is_refused() {
        assert!(matches!(
            pattern_check(r#"{"kind":"daily","time":"09:30:00.5"}"#.to_owned()),
            PatternCheck::Refused { .. }
        ));
    }

    #[test]
    fn a_text_that_is_not_a_pattern_is_refused() {
        let PatternCheck::Refused { cause } = pattern_check(r#"{"kind":"yearly"}"#.to_owned())
        else {
            panic!("yearly is not a pattern of the core");
        };
        assert!(
            cause.starts_with("the text is not a schedule pattern: "),
            "{cause}"
        );
    }

    #[test]
    fn the_four_zones_of_the_north_american_rules_are_known() {
        for name in ["EST5EDT", "CST6CDT", "MST7MDT", "PST8PDT"] {
            assert_eq!(timezone_check(name), TimezoneCheck::Known, "{name}");
        }
    }

    #[test]
    fn a_zone_name_and_a_link_name_are_known() {
        for name in ["UTC", "Europe/Berlin", "Asia/Calcutta", "US/Eastern"] {
            assert_eq!(timezone_check(name), TimezoneCheck::Known, "{name}");
        }
    }

    #[test]
    fn an_offset_another_case_and_an_unknown_name_are_unknown() {
        for name in ["+05:00", "utc", "europe/berlin", "Nope/Zone", ""] {
            assert_eq!(timezone_check(name), TimezoneCheck::Unknown, "{name}");
        }
    }

    #[test]
    fn the_default_queue_mode_gives_the_package_priority_and_the_default_class() {
        assert_eq!(
            facts(DEFAULT_MODE, &["default"]).expect("the default queue has facts"),
            SchedulerConfigFacts {
                reject_bound: RejectBound::NoBound,
                retain_rerun_input: false,
                queues: vec![QueueSendFacts {
                    queue_name: "default".to_owned(),
                    priority: SendPriority::DefaultQueue,
                    retention_class_key: "standard_30d".to_owned(),
                }],
            }
        );
    }

    #[test]
    fn a_custom_queue_gives_its_priority_and_its_retention_mapping() {
        let facts = facts(CUSTOM_MODE, &["urgent", "bulk"]).expect("the custom queues have facts");
        assert_eq!(
            facts.reject_bound,
            RejectBound::Bytes {
                bytes: bigint_of_u64(4096)
            }
        );
        assert!(facts.retain_rerun_input);
        assert_eq!(
            facts.queues,
            vec![
                QueueSendFacts {
                    queue_name: "urgent".to_owned(),
                    priority: SendPriority::Configured { priority: 20 },
                    retention_class_key: "standard_30d".to_owned(),
                },
                QueueSendFacts {
                    queue_name: "bulk".to_owned(),
                    priority: SendPriority::Configured { priority: 1 },
                    retention_class_key: "forever".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn a_queue_that_the_custom_mode_does_not_hold_is_a_client_defect() {
        assert!(matches!(
            facts(CUSTOM_MODE, &["absent"]),
            Err(NativeThrown::ClientDefect { .. })
        ));
    }

    #[test]
    fn a_config_text_that_does_not_read_is_a_client_defect() {
        assert!(matches!(
            facts(r#"{"database": {}}"#, &[]),
            Err(NativeThrown::ClientDefect { .. })
        ));
    }
}
