//! The slot health of a worker state snapshot: the `slots_down` column.
//!
//! Python reference: `DownSlot`, `AllSlotsServing`, `SlotsDown` and
//! `parse_slot_health` (`clients/python/horsies/core/models/health.py:100-259`), and the
//! boot failure causes that can clear (`clients/python/horsies/core/models/boot_failure.py`).
//!
//! This module is pure. It does no I/O.

use std::fmt;
use std::num::NonZeroU64;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Map, Value};

use crate::broker::health::{stored_value_text, stored_whole_number, STORED_COUNT_LIMIT};

pub use crate::core::boot_failure::{ShortageErrno, MAX_BOOT_FAILURE_DETAIL_CHARS};

/// A boot failure cause that can clear by itself. Only these causes keep a
/// slot down (Python `ClearableBootCause`,
/// `clients/python/horsies/core/models/boot_failure.py:167`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClearableBootCause {
    /// A boot step failed with an errno of a resource limit.
    ResourceShortage(ShortageErrno),
    /// The OS refused a thread that the child starts at boot.
    ThreadStartRefused,
}

/// The cause as short text, as Python's `describe_boot_cause`
/// (`clients/python/horsies/core/models/boot_failure.py:178-194`).
impl fmt::Display for ClearableBootCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceShortage(errno) => write!(
                formatter,
                "the OS ran short of a resource ({})",
                errno.as_str()
            ),
            Self::ThreadStartRefused => formatter.write_str("the OS refused a thread of the child"),
        }
    }
}

/// A worker slot whose last child boot failed for a cause that can clear.
///
/// The slot takes no task, and the worker's claim budget leaves it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownSlot {
    /// The slot number, below the worker's `processes`.
    pub slot: u32,
    pub cause: ClearableBootCause,
    /// The error text of the last failed boot, at most
    /// [`MAX_BOOT_FAILURE_DETAIL_CHARS`] characters.
    pub detail: String,
    pub down_since: DateTime<Utc>,
    /// The failed boots since the slot went down.
    pub failed_boots: NonZeroU64,
}

/// The down slots of a degraded worker: never empty, each slot once, in slot
/// order. Only [`parse_slot_health`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownSlots(Vec<DownSlot>);

impl DownSlots {
    /// The down slots, in slot order.
    pub fn as_slice(&self) -> &[DownSlot] {
        &self.0
    }
}

/// Whether any slot of the worker was down when it wrote the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotHealth {
    /// No slot of the worker was down.
    AllSlotsServing,
    /// The worker is degraded: these slots were down.
    SlotsDown(DownSlots),
}

/// A stored `slots_down` value does not have the shape that the worker
/// writes. `reason` names the field and the cause.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct SlotHealthUnreadable {
    pub reason: String,
}

fn unreadable(reason: impl Into<String>) -> SlotHealthUnreadable {
    SlotHealthUnreadable {
        reason: reason.into(),
    }
}

/// The slot health of a stored `slots_down` value (Python
/// `parse_slot_health`, `clients/python/horsies/core/models/health.py:159-187`).
///
/// An empty array is [`SlotHealth::AllSlotsServing`]. Each entry must name a
/// distinct slot below `processes`, a cause that can clear, a detail of at
/// most [`MAX_BOOT_FAILURE_DETAIL_CHARS`] characters, a `down_since` with an
/// offset and `failed_boots` of at least 1. Any other value is
/// [`SlotHealthUnreadable`]. Reads only.
pub fn parse_slot_health(
    value: &Value,
    processes: i32,
) -> Result<SlotHealth, SlotHealthUnreadable> {
    let Value::Array(entries) = value else {
        return Err(unreadable("slots_down is not a JSON array"));
    };
    if entries.is_empty() {
        return Ok(SlotHealth::AllSlotsServing);
    }
    if more_entries_than_processes(entries.len(), processes) {
        return Err(unreadable("slots_down lists more slots than processes"));
    }
    let mut slots = entries
        .iter()
        .map(|entry| down_slot(entry, processes))
        .collect::<Result<Vec<_>, _>>()?;
    slots.sort_by_key(|down| down.slot);
    if slots.windows(2).any(|pair| pair[0].slot == pair[1].slot) {
        return Err(unreadable("slots_down names one slot twice"));
    }
    Ok(SlotHealth::SlotsDown(DownSlots(slots)))
}

fn more_entries_than_processes(entries: usize, processes: i32) -> bool {
    match i64::try_from(entries) {
        Ok(count) => count > i64::from(processes),
        Err(_) => true,
    }
}

/// One `slots_down` entry (Python `_parse_down_slot`,
/// `clients/python/horsies/core/models/health.py:190-211`). The checks run in Python's
/// order, so the first unreadable field is the one Python names.
fn down_slot(value: &Value, processes: i32) -> Result<DownSlot, SlotHealthUnreadable> {
    let Value::Object(fields) = value else {
        return Err(unreadable("a slots_down entry is not a JSON object"));
    };
    let detail = bounded_detail(fields)?;
    Ok(DownSlot {
        slot: slot_number(fields, processes)?,
        failed_boots: failed_boots(fields)?,
        down_since: aware_instant(fields)?,
        cause: clearable_cause(fields)?,
        detail,
    })
}

fn bounded_detail(fields: &Map<String, Value>) -> Result<String, SlotHealthUnreadable> {
    match fields.get("detail") {
        Some(Value::String(text)) if text.chars().count() <= MAX_BOOT_FAILURE_DETAIL_CHARS => {
            Ok(text.clone())
        }
        _ => Err(unreadable("detail is not text within its bound")),
    }
}

fn slot_number(fields: &Map<String, Value>, processes: i32) -> Result<u32, SlotHealthUnreadable> {
    // A negative process count admits no slot, as Python's `range(processes)`.
    let below = u64::try_from(processes).unwrap_or(0);
    let slot = stored_whole_number(fields.get("slot"), "slot", 0..below).map_err(unreadable)?;
    Ok(u32::try_from(slot).expect("a slot below an i32 process count fits in u32"))
}

fn failed_boots(fields: &Map<String, Value>) -> Result<NonZeroU64, SlotHealthUnreadable> {
    let count = stored_whole_number(
        fields.get("failed_boots"),
        "failed_boots",
        1..STORED_COUNT_LIMIT,
    )
    .map_err(unreadable)?;
    Ok(NonZeroU64::new(count).expect("failed_boots is at least 1 by its range"))
}

/// An ISO instant with an offset (Python `_aware_instant`,
/// `clients/python/horsies/core/models/health.py:229-238`).
fn aware_instant(fields: &Map<String, Value>) -> Result<DateTime<Utc>, SlotHealthUnreadable> {
    let Some(Value::String(text)) = fields.get("down_since") else {
        return Err(unreadable("down_since is not text"));
    };
    match text.parse::<DateTime<FixedOffset>>() {
        Ok(instant) => Ok(instant.with_timezone(&Utc)),
        Err(_) if is_instant_without_offset(text) => {
            Err(unreadable(format!("down_since '{text}' has no offset")))
        }
        Err(_) => Err(unreadable(format!(
            "down_since '{text}' is not an ISO instant"
        ))),
    }
}

fn is_instant_without_offset(text: &str) -> bool {
    text.parse::<NaiveDateTime>().is_ok()
        || text.replacen(' ', "T", 1).parse::<NaiveDateTime>().is_ok()
        || text.parse::<NaiveDate>().is_ok()
}

/// A stored cause that can keep a slot down (Python `_clearable_cause` and
/// `parse_boot_cause`, `clients/python/horsies/core/models/health.py:241-259`,
/// `clients/python/horsies/core/models/boot_failure.py:216-249`). A known cause that
/// repeats is refused by its own reason, not as an unknown kind.
fn clearable_cause(
    fields: &Map<String, Value>,
) -> Result<ClearableBootCause, SlotHealthUnreadable> {
    let Some(Value::Object(cause)) = fields.get("cause") else {
        return Err(unreadable("cause: the cause is not a JSON object"));
    };
    match cause.get("kind").and_then(Value::as_str) {
        Some("resource_shortage") => {
            shortage_errno(cause).map(ClearableBootCause::ResourceShortage)
        }
        Some("thread_start_refused") => Ok(ClearableBootCause::ThreadStartRefused),
        Some(
            kind @ ("app_load_failed"
            | "child_pool_setup_failed"
            | "task_loop_create_failed"
            | "spawn_failed"),
        ) => Err(unreadable(format!("cause {kind} cannot keep a slot down"))),
        _ => Err(unreadable(format!(
            "cause: unknown cause kind {}",
            stored_value_text(cause.get("kind"))
        ))),
    }
}

fn shortage_errno(cause: &Map<String, Value>) -> Result<ShortageErrno, SlotHealthUnreadable> {
    let stored = cause.get("errno");
    stored
        .and_then(Value::as_str)
        .and_then(ShortageErrno::from_name)
        .ok_or_else(|| {
            unreadable(format!(
                "cause: unknown errno name {}",
                stored_value_text(stored)
            ))
        })
}

#[cfg(test)]
mod tests {
    //! One test per outcome of `parse_slot_health`. Python reference:
    //! `clients/python/tests/unit/test_health_models.py::TestSlotHealth`.
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn entry(slot: Value) -> Value {
        json!({
            "slot": slot,
            "cause": {"kind": "resource_shortage", "errno": "EMFILE"},
            "detail": "OSError: [Errno 24] Too many open files",
            "down_since": "2026-01-01T00:00:00+00:00",
            "failed_boots": 2,
        })
    }

    fn with_field(field: &str, value: Value) -> Value {
        let mut stored = entry(json!(1));
        stored[field] = value;
        json!([stored])
    }

    fn reason(value: &Value, processes: i32) -> String {
        parse_slot_health(value, processes)
            .expect_err("the value is unreadable")
            .reason
    }

    #[test]
    fn an_empty_array_is_all_slots_serving() {
        assert_eq!(
            parse_slot_health(&json!([]), 2),
            Ok(SlotHealth::AllSlotsServing)
        );
    }

    #[test]
    fn down_slots_read_every_field_in_slot_order() {
        let mut thread = entry(json!(0));
        thread["cause"] = json!({"kind": "thread_start_refused"});
        thread["down_since"] = json!("2026-01-01T02:00:00+02:00");
        let stored = json!([entry(json!(3)), thread]);

        let Ok(SlotHealth::SlotsDown(slots)) = parse_slot_health(&stored, 4) else {
            panic!("two down slots");
        };

        let midnight = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(
            slots.as_slice(),
            [
                DownSlot {
                    slot: 0,
                    cause: ClearableBootCause::ThreadStartRefused,
                    detail: "OSError: [Errno 24] Too many open files".to_owned(),
                    down_since: midnight,
                    failed_boots: NonZeroU64::new(2).unwrap(),
                },
                DownSlot {
                    slot: 3,
                    cause: ClearableBootCause::ResourceShortage(ShortageErrno::Emfile),
                    detail: "OSError: [Errno 24] Too many open files".to_owned(),
                    down_since: midnight,
                    failed_boots: NonZeroU64::new(2).unwrap(),
                },
            ]
        );
    }

    #[test]
    fn a_value_that_is_not_an_array_is_unreadable() {
        for stored in [json!({"slot": 1}), Value::Null] {
            assert_eq!(reason(&stored, 2), "slots_down is not a JSON array");
        }
    }

    #[test]
    fn more_entries_than_processes_is_unreadable() {
        let stored = json!([entry(json!(0)), entry(json!(1))]);
        assert_eq!(
            reason(&stored, 1),
            "slots_down lists more slots than processes"
        );
    }

    #[test]
    fn an_entry_that_is_not_an_object_is_unreadable() {
        assert_eq!(
            reason(&json!([1]), 2),
            "a slots_down entry is not a JSON object"
        );
    }

    #[test]
    fn a_detail_over_its_bound_or_not_text_is_unreadable() {
        let long = "x".repeat(MAX_BOOT_FAILURE_DETAIL_CHARS + 1);
        for detail in [json!(long), json!(7), Value::Null] {
            assert_eq!(
                reason(&with_field("detail", detail), 2),
                "detail is not text within its bound"
            );
        }
        let at_bound = "é".repeat(MAX_BOOT_FAILURE_DETAIL_CHARS);
        assert!(parse_slot_health(&with_field("detail", json!(at_bound)), 2).is_ok());
    }

    #[test]
    fn a_slot_outside_the_processes_or_not_a_whole_number_is_unreadable() {
        assert_eq!(
            reason(&json!([entry(json!(2))]), 2),
            "slot 2 is out of range"
        );
        assert_eq!(
            reason(&json!([entry(json!(-1))]), 2),
            "slot -1 is out of range"
        );
        assert_eq!(
            reason(&json!([entry(json!("1"))]), 2),
            "slot '1' is out of range"
        );
        assert_eq!(
            reason(&json!([entry(json!(true))]), 2),
            "slot True is not a whole number"
        );
    }

    #[test]
    fn failed_boots_below_one_is_unreadable() {
        assert_eq!(
            reason(&with_field("failed_boots", json!(0)), 2),
            "failed_boots 0 is out of range"
        );
        assert_eq!(
            reason(&with_field("failed_boots", json!("3")), 2),
            "failed_boots '3' is out of range"
        );
        assert_eq!(
            reason(&with_field("failed_boots", json!(1_u64 << 63)), 2),
            "failed_boots 9223372036854775808 is out of range"
        );
    }

    #[test]
    fn down_since_without_an_offset_or_not_iso_is_unreadable() {
        assert_eq!(
            reason(&with_field("down_since", json!("2026-01-01T00:00:00")), 2),
            "down_since '2026-01-01T00:00:00' has no offset"
        );
        assert_eq!(
            reason(&with_field("down_since", json!("yesterday")), 2),
            "down_since 'yesterday' is not an ISO instant"
        );
        assert_eq!(
            reason(&with_field("down_since", json!(0)), 2),
            "down_since is not text"
        );
    }

    #[test]
    fn a_cause_that_cannot_keep_a_slot_down_is_unreadable() {
        assert_eq!(
            reason(&with_field("cause", json!({"kind": "app_load_failed"})), 2),
            "cause app_load_failed cannot keep a slot down"
        );
        assert_eq!(
            reason(&with_field("cause", json!({"kind": "exploded"})), 2),
            "cause: unknown cause kind 'exploded'"
        );
        assert_eq!(
            reason(
                &with_field(
                    "cause",
                    json!({"kind": "resource_shortage", "errno": "ENOENT"})
                ),
                2
            ),
            "cause: unknown errno name 'ENOENT'"
        );
        assert_eq!(
            reason(&with_field("cause", json!("resource_shortage")), 2),
            "cause: the cause is not a JSON object"
        );
    }

    /// Python `TestSlotHealth::test_every_field_is_checked`, the causes that
    /// repeat (`cause-that-repeats`, `loop-cause-that-repeats`,
    /// `spawn-cause-that-repeats`): none of them keeps a slot down, and the
    /// refusal names the cause.
    #[test]
    fn each_cause_that_repeats_cannot_keep_a_slot_down() {
        for kind in [
            "app_load_failed",
            "child_pool_setup_failed",
            "task_loop_create_failed",
            "spawn_failed",
        ] {
            assert_eq!(
                reason(&with_field("cause", json!({"kind": kind})), 2),
                format!("cause {kind} cannot keep a slot down")
            );
        }
    }

    #[test]
    fn one_slot_named_twice_is_unreadable() {
        let stored = json!([entry(json!(1)), entry(json!(1))]);
        assert_eq!(reason(&stored, 2), "slots_down names one slot twice");
    }

    #[test]
    fn a_cause_describes_itself_as_python_does() {
        assert_eq!(
            ClearableBootCause::ResourceShortage(ShortageErrno::Emfile).to_string(),
            "the OS ran short of a resource (EMFILE)"
        );
        assert_eq!(
            ClearableBootCause::ThreadStartRefused.to_string(),
            "the OS refused a thread of the child"
        );
    }
}
