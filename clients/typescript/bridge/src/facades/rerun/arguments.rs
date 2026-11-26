//! The command and the policy of a rerun as the package passes them, and
//! their conversion to the core's `RerunTask` and `RerunEnqueuePolicy`.
//!
//! The package parses the command and the policy before every call, with
//! the bounds that the core's generated constants give. A value that breaks
//! a rule here is a defect of the package: the caller throws
//! `CLIENT_DEFECT` with the [`BoundaryRefusal`].

use chrono::Duration;
use horsies_core::{EnqueueRetention, RerunEnqueuePolicy, RerunTask, RetentionDays};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::values::boundary::{
    i64_of_bigint, instant_of_epoch_micros, u32_of_number, uuid_of_text, BoundaryRefusal,
};

/// The deadline of the new task.
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

/// The caller key of the new task.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallerKeyArg {
    /// A keyed rerun: the key holds the rerun for the reservation window.
    Keyed { key: String },
    /// A rerun with no caller key.
    Unkeyed,
}

/// One rerun request: the source task, and the deadline and the caller key
/// of the new task.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RerunCommandArg {
    /// The source task id, as lower-case hyphenated text.
    pub source_task_id: String,
    pub deadline: DeadlineArg,
    pub caller_key: CallerKeyArg,
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
    /// The reservation window of the caller key, in microseconds.
    pub reservation_window_micros: BigInt,
}

/// The core's rerun request of `command`, with no trace carrier. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a source task id that is not
/// lower-case hyphenated UUID text, or of a deadline outside the instant
/// range.
pub fn rerun_command(command: RerunCommandArg) -> Result<RerunTask, BoundaryRefusal> {
    let source_task_id = uuid_of_text("sourceTaskId", &command.source_task_id)?;
    let deadline = match command.deadline {
        DeadlineArg::Until { epoch_micros } => {
            Some(instant_of_epoch_micros("deadline", &epoch_micros)?)
        }
        DeadlineArg::NoDeadline => None,
    };
    let caller_key = match command.caller_key {
        CallerKeyArg::Keyed { key } => Some(key),
        CallerKeyArg::Unkeyed => None,
    };
    Ok(RerunTask::new(source_task_id, deadline, caller_key))
}

/// The core's enqueue policy of `policy`. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a period that is not a whole number
/// of days within the retention bounds, or of a window that is not more
/// than 0 and at most 30 days.
pub fn rerun_policy(policy: &RerunPolicyArg) -> Result<RerunEnqueuePolicy, BoundaryRefusal> {
    let class_key = retention_class_key(&policy.retention)?;
    let window_micros =
        i64_of_bigint("reservationWindowMicros", &policy.reservation_window_micros)?;
    let retain_rerun_input = match policy.rerun_input {
        RerunInputArg::KeepInput => true,
        RerunInputArg::DropInput => false,
    };
    RerunEnqueuePolicy::new(
        class_key,
        retain_rerun_input,
        Duration::microseconds(window_micros),
    )
    .map_err(|refused| BoundaryRefusal::new("policy", &refused.to_string()))
}

/// The class key of `retention`, by the core's rule
/// (`EnqueueRetention::class_key`): the package computes no key. Pure.
///
/// Errors: the [`BoundaryRefusal`] of a period that is not a whole number
/// of days within the retention bounds.
fn retention_class_key(retention: &RetentionArg) -> Result<String, BoundaryRefusal> {
    match retention {
        RetentionArg::Days { days } => {
            let days = u32_of_number("retention.days", *days)?;
            RetentionDays::parse_period(Duration::days(i64::from(days)))
                .map(|period| EnqueueRetention::Period(period).class_key())
                .map_err(|refused| BoundaryRefusal::new("retention.days", &refused.to_string()))
        }
        RetentionArg::Forever => Ok(EnqueueRetention::Forever.class_key()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::values::boundary::bigint_of_i64;

    const SOURCE: &str = "01890a5d-ac96-774b-bcce-b302099a8057";

    fn command(deadline: DeadlineArg, caller_key: CallerKeyArg) -> RerunCommandArg {
        RerunCommandArg {
            source_task_id: SOURCE.to_owned(),
            deadline,
            caller_key,
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
            CallerKeyArg::Keyed {
                key: "order-7".to_owned(),
            },
        ))
        .expect("the command follows the boundary rules");
        assert_eq!(converted.source_task_id().to_string(), SOURCE);
        assert_eq!(converted.deadline(), Some(deadline));
        assert_eq!(converted.caller_key(), Some("order-7"));
    }

    #[test]
    fn no_deadline_and_no_key_are_none() {
        let converted = rerun_command(command(DeadlineArg::NoDeadline, CallerKeyArg::Unkeyed))
            .expect("the command follows the boundary rules");
        assert_eq!(converted.deadline(), None);
        assert_eq!(converted.caller_key(), None);
    }

    #[test]
    fn a_source_task_id_in_another_form_is_refused() {
        let mut refused = command(DeadlineArg::NoDeadline, CallerKeyArg::Unkeyed);
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
            CallerKeyArg::Unkeyed,
        );
        assert!(matches!(
            rerun_command(refused),
            Err(refusal) if refusal.subject == "deadline"
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
