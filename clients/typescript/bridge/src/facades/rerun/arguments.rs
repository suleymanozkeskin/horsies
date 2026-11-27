//! The command and the policy of a rerun as the package passes them, and
//! their conversion to the core's `RerunCommand` and `RerunEnqueuePolicy`.
//!
//! The package parses the command and the policy before every call, with
//! the bounds that the core's generated constants give. A value that breaks
//! a rule here is a defect of the package: the caller throws
//! `CLIENT_DEFECT` with the [`BoundaryRefusal`].

use std::collections::BTreeMap;

use chrono::Duration;
use horsies_core::{
    DeclaredErrorCodes, EnqueueRetention, RegisteredTaskCodes, RerunCommand, RerunEnqueuePolicy,
    RetentionDays,
};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::values::boundary::{
    i64_of_bigint, instant_of_epoch_micros, u32_of_number, uuid_of_text, BoundaryRefusal,
};

/// The good-until instant of the new task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeadlineArg {
    /// The new task expires at this instant, in epoch microseconds.
    Until { epoch_micros: BigInt },
    /// The new task has no deadline.
    NoDeadline,
}

/// The idempotency key of the new task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyKeyArg {
    /// A keyed rerun: the key holds the rerun for the reservation window.
    Keyed { key: String },
    /// A rerun with no idempotency key.
    Unkeyed,
}

/// One rerun request: the source task, and the good-until instant and the
/// idempotency key of the new task.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RerunCommandArg {
    /// The source task id, as lower-case hyphenated text.
    pub source_task_id: String,
    pub good_until: DeadlineArg,
    pub idempotency_key: IdempotencyKeyArg,
}

/// The retention of the new task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum RetentionArg {
    /// A finite period of whole days. The number is checked as a whole
    /// number from 0 to 4294967295, then as a retention period.
    Days { days: f64 },
    /// The new task is kept forever.
    Forever,
}

/// Whether the new task keeps its input for a later rerun.
#[napi(string_enum)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RerunInputArg {
    #[napi(value = "keep")]
    KeepInput,
    #[napi(value = "drop")]
    DropInput,
}

/// The enqueue policy of the new task.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct RerunPolicyArg {
    pub retention: RetentionArg,
    pub rerun_input: RerunInputArg,
    /// The reservation window of the idempotency key, in microseconds.
    pub reservation_window_micros: BigInt,
}

/// One task that the calling app lists, with the custom codes that its
/// contract declares. A contract that declares no list has no codes here.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredTaskArg {
    pub task_name: String,
    pub codes: Vec<String>,
}

/// The tasks that the calling app lists, each once. A rerun checks the
/// stored retry list of its source against them.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredTaskCodesArg {
    pub tasks: Vec<RegisteredTaskArg>,
}

/// The core's rerun request of `command`, with no trace carrier. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a source task id that is not
/// lower-case hyphenated UUID text, or of a good-until instant outside the
/// instant range.
pub fn rerun_command(command: RerunCommandArg) -> Result<RerunCommand, BoundaryRefusal> {
    let source_task_id = uuid_of_text("sourceTaskId", &command.source_task_id)?;
    let good_until = match command.good_until {
        DeadlineArg::Until { epoch_micros } => {
            Some(instant_of_epoch_micros("goodUntil", &epoch_micros)?)
        }
        DeadlineArg::NoDeadline => None,
    };
    let idempotency_key = match command.idempotency_key {
        IdempotencyKeyArg::Keyed { key } => Some(key),
        IdempotencyKeyArg::Unkeyed => None,
    };
    Ok(RerunCommand::new(
        source_task_id,
        good_until,
        idempotency_key,
    ))
}

/// The core's enqueue policy of `policy`. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a period that is not a whole number
/// of days within the retention bounds, or of a window that is not more
/// than 0 and at most 30 days.
pub fn rerun_policy(policy: &RerunPolicyArg) -> Result<RerunEnqueuePolicy, BoundaryRefusal> {
    let retention = enqueue_retention(&policy.retention)?;
    let window_micros =
        i64_of_bigint("reservationWindowMicros", &policy.reservation_window_micros)?;
    let retain_rerun_input = match policy.rerun_input {
        RerunInputArg::KeepInput => true,
        RerunInputArg::DropInput => false,
    };
    RerunEnqueuePolicy::new(
        retention,
        retain_rerun_input,
        Duration::microseconds(window_micros),
    )
    .map_err(|refused| BoundaryRefusal::new("policy", &refused.to_string()))
}

/// The core's declared custom codes of the app's tasks in `codes`. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a task name that the list holds
/// twice, or of a code set that the core refuses
/// (`DeclaredErrorCodes::parse`).
pub fn registered_task_codes(
    codes: &RegisteredTaskCodesArg,
) -> Result<RegisteredTaskCodes, BoundaryRefusal> {
    let mut tasks: BTreeMap<String, DeclaredErrorCodes> = BTreeMap::new();
    for task in &codes.tasks {
        let declared = DeclaredErrorCodes::parse(&task.codes).map_err(|refused| {
            BoundaryRefusal::new(&format!("codes.{}", task.task_name), &refused.to_string())
        })?;
        if tasks.insert(task.task_name.clone(), declared).is_some() {
            return Err(BoundaryRefusal::new(
                "codes",
                &format!("each task name once; '{}' is listed twice", task.task_name),
            ));
        }
    }
    Ok(RegisteredTaskCodes::new(tasks))
}

/// The core's retention of `retention`; the core derives its class key
/// (`EnqueueRetention::class_key`). Pure.
///
/// Errors: the [`BoundaryRefusal`] of a period that is not a whole number
/// of days within the retention bounds.
fn enqueue_retention(retention: &RetentionArg) -> Result<EnqueueRetention, BoundaryRefusal> {
    match retention {
        RetentionArg::Days { days } => {
            let days = u32_of_number("retention.days", *days)?;
            RetentionDays::parse_period(Duration::days(i64::from(days)))
                .map(EnqueueRetention::Period)
                .map_err(|refused| BoundaryRefusal::new("retention.days", &refused.to_string()))
        }
        RetentionArg::Forever => Ok(EnqueueRetention::Forever),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::values::boundary::bigint_of_i64;

    const SOURCE: &str = "01890a5d-ac96-774b-bcce-b302099a8057";

    fn command(good_until: DeadlineArg, idempotency_key: IdempotencyKeyArg) -> RerunCommandArg {
        RerunCommandArg {
            source_task_id: SOURCE.to_owned(),
            good_until,
            idempotency_key,
        }
    }

    fn policy(retention: RetentionArg, window_micros: i64) -> RerunPolicyArg {
        RerunPolicyArg {
            retention,
            rerun_input: RerunInputArg::KeepInput,
            reservation_window_micros: bigint_of_i64(window_micros),
        }
    }

    #[test]
    fn a_keyed_command_with_a_deadline_keeps_its_values() {
        let deadline = Utc
            .with_ymd_and_hms(2026, 11, 26, 12, 0, 0)
            .single()
            .expect("a valid instant")
            + Duration::microseconds(654_321);
        let converted = rerun_command(command(
            DeadlineArg::Until {
                epoch_micros: bigint_of_i64(deadline.timestamp_micros()),
            },
            IdempotencyKeyArg::Keyed {
                key: "order-7".to_owned(),
            },
        ))
        .expect("the command follows the boundary rules");
        assert_eq!(converted.source_task_id().to_string(), SOURCE);
        assert_eq!(converted.good_until(), Some(deadline));
        assert_eq!(converted.idempotency_key(), Some("order-7"));
    }

    #[test]
    fn no_deadline_and_no_key_are_none() {
        let converted = rerun_command(command(DeadlineArg::NoDeadline, IdempotencyKeyArg::Unkeyed))
            .expect("the command follows the boundary rules");
        assert_eq!(converted.good_until(), None);
        assert_eq!(converted.idempotency_key(), None);
    }

    #[test]
    fn a_source_task_id_in_another_form_is_refused() {
        let mut refused = command(DeadlineArg::NoDeadline, IdempotencyKeyArg::Unkeyed);
        refused.source_task_id = SOURCE.to_uppercase();
        assert!(matches!(
            rerun_command(refused),
            Err(refusal) if refusal.subject == "sourceTaskId"
        ));
    }

    #[test]
    fn a_deadline_outside_the_instant_range_is_refused() {
        let refused = command(
            DeadlineArg::Until {
                epoch_micros: bigint_of_i64(i64::MAX),
            },
            IdempotencyKeyArg::Unkeyed,
        );
        assert!(matches!(
            rerun_command(refused),
            Err(refusal) if refusal.subject == "goodUntil"
        ));
    }

    #[test]
    fn a_period_of_days_is_its_derived_class_key() {
        let converted = rerun_policy(&policy(RetentionArg::Days { days: 7.0 }, 86_400_000_000))
            .expect("the policy follows the boundary rules");
        assert_eq!(converted.retention_class_key(), "standard_7d");
        assert!(converted.retain_rerun_input());
        assert_eq!(converted.reservation_window(), Duration::hours(24));
    }

    #[test]
    fn forever_is_the_forever_class_and_drop_keeps_no_input() {
        let mut dropped = policy(RetentionArg::Forever, 1);
        dropped.rerun_input = RerunInputArg::DropInput;
        let converted = rerun_policy(&dropped).expect("the policy follows the boundary rules");
        assert_eq!(converted.retention_class_key(), "forever");
        assert!(!converted.retain_rerun_input());
        assert_eq!(converted.reservation_window(), Duration::microseconds(1));
    }

    #[test]
    fn a_period_that_is_not_a_retention_period_is_refused() {
        for days in [0.0, 7.5, 1826.0, -1.0] {
            assert!(
                matches!(
                    rerun_policy(&policy(RetentionArg::Days { days }, 1)),
                    Err(refusal) if refusal.subject == "retention.days"
                ),
                "{days}"
            );
        }
    }

    fn registered(task_name: &str, codes: &[&str]) -> RegisteredTaskArg {
        RegisteredTaskArg {
            task_name: task_name.to_owned(),
            codes: codes.iter().map(|code| (*code).to_owned()).collect(),
        }
    }

    #[test]
    fn each_listed_task_is_registered_with_its_codes() {
        let converted = registered_task_codes(&RegisteredTaskCodesArg {
            tasks: vec![
                registered("charge", &["CARD_DECLINED"]),
                registered("ping", &[]),
            ],
        })
        .expect("the codes follow the boundary rules");
        assert!(matches!(
            converted.of("charge"),
            horsies_core::DeclaredCodes::Registered { custom } if custom.get("CARD_DECLINED").is_some()
        ));
        assert!(matches!(
            converted.of("ping"),
            horsies_core::DeclaredCodes::Registered { custom } if custom.is_empty()
        ));
        assert!(matches!(
            converted.of("other"),
            horsies_core::DeclaredCodes::TaskNotRegistered
        ));
    }

    #[test]
    fn a_task_name_listed_twice_is_refused() {
        let refused = registered_task_codes(&RegisteredTaskCodesArg {
            tasks: vec![registered("charge", &[]), registered("charge", &[])],
        });
        assert!(matches!(refused, Err(refusal) if refusal.subject == "codes"));
    }

    #[test]
    fn a_code_set_that_the_core_refuses_is_refused() {
        let refused = registered_task_codes(&RegisteredTaskCodesArg {
            tasks: vec![registered("charge", &["TASK_TIMEOUT"])],
        });
        assert!(matches!(refused, Err(refusal) if refusal.subject == "codes.charge"));
    }

    #[test]
    fn a_window_outside_its_bounds_is_refused() {
        let thirty_days_and_one = 2_592_000_000_001;
        for window in [0, -1, thirty_days_and_one] {
            assert!(
                matches!(
                    rerun_policy(&policy(RetentionArg::Forever, window)),
                    Err(refusal) if refusal.subject == "policy"
                ),
                "{window}"
            );
        }
    }
}
