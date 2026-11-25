//! The event contract: the walk of the union over every type, variant and
//! bound end; the forms of every field; one test per refusal of the codec;
//! the conversions from the core's own types; the shared wire vectors; and
//! the snapshot of the contract version.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::broker::ClearableBootCause;
use crate::core::boot_failure::ShortageErrno;
use crate::core::config::bounded::BoundedU32;
use crate::core::task::Execution;
use crate::core::{BuiltInTaskCode, OperationalErrorCode, OutcomeCode, TaskErrorCode, UserCode};
use crate::worker::executor::protocol::{BoundedText, ProcessId, StopReason};
use crate::worker::executor::slots::dispatch::NotRunCause;

/// Most nesting levels of a sample. The contract nests 4: the event, the
/// end, the result and the error code of an `AttemptEnded`.
const MAX_SAMPLE_DEPTH: usize = 8;

/// The largest text bound that a field of the contract may have.
const MOST_TEXT_CHARS: usize = MAX_EVENT_TRACE_CARRIER_CHARS;

/// Which end of each bound a sample takes.
#[derive(Clone, Copy, Debug)]
enum End {
    Low,
    High,
}

/// The choices of one sample: each closed type and each code union takes
/// its value at `pick` modulo its value count.
#[derive(Clone, Copy, Debug)]
struct Sampling {
    pick: usize,
    end: End,
}

fn value_type(name: &str) -> &'static NamedType {
    EVENT_VALUE_TYPES
        .iter()
        .find(|named| named.name == name)
        .unwrap_or_else(|| panic!("{name} is not a value type of the contract"))
}

fn code_members(union: &CodeUnion) -> Vec<&'static str> {
    union
        .catalogs
        .iter()
        .flat_map(|catalog| catalog.members.iter().map(|member| member.value))
        .collect()
}

/// A sample wire value of `form`.
fn sample(form: ValueForm, sampling: Sampling, depth: usize) -> WireValue {
    assert!(depth <= MAX_SAMPLE_DEPTH, "the contract nests too deep");
    match form {
        ValueForm::Uuid => json!("0192f6a1-7b3c-7d4e-8f00-0123456789ab"),
        ValueForm::Text { max_chars } => match sampling.end {
            End::Low => json!(""),
            End::High => json!("é".repeat(max_chars)),
        },
        ValueForm::WholeNumber { min, max } => match sampling.end {
            End::Low => json!(min),
            End::High => json!(max),
        },
        ValueForm::Duration => match sampling.end {
            End::Low => json!(0_u64),
            End::High => json!(u64::MAX),
        },
        ValueForm::Time => match sampling.end {
            End::Low => json!(i64::MIN),
            End::High => json!(i64::MAX),
        },
        ValueForm::Codes(union) => {
            let members = code_members(union);
            json!(members[sampling.pick % members.len()])
        }
        ValueForm::Named(name) => match value_type(name).shape {
            NamedShape::Choice { values } => json!(values[sampling.pick % values.len()].value),
            NamedShape::Variants { variants } => {
                let variant = variants[sampling.pick % variants.len()];
                let mut items = vec![json!(variant.tag)];
                items.extend(
                    variant
                        .fields
                        .iter()
                        .map(|field| sample(field.form, sampling, depth + 1)),
                );
                WireValue::Array(items)
            }
        },
    }
}

fn event_sample(event: &EventType, sampling: Sampling) -> WireValue {
    let mut items = vec![json!(event.name)];
    items.extend(
        event
            .fields
            .iter()
            .map(|field| sample(field.form, sampling, 1)),
    );
    WireValue::Array(items)
}

/// Every field of the contract: the fields of the events, then the fields
/// of the variants of the value types.
fn all_fields() -> Vec<EventField> {
    let mut fields: Vec<EventField> = EVENT_TYPES
        .iter()
        .flat_map(|event| event.fields.iter().copied())
        .collect();
    for named in EVENT_VALUE_TYPES {
        if let NamedShape::Variants { variants } = named.shape {
            fields.extend(
                variants
                    .iter()
                    .flat_map(|variant| variant.fields.iter().copied()),
            );
        }
    }
    fields
}

/// The code unions that fields hold, each once, in the order of first use.
fn code_unions() -> Vec<CodeUnion> {
    let mut unions: Vec<CodeUnion> = Vec::new();
    for field in all_fields() {
        if let ValueForm::Codes(union) = field.form {
            if !unions.contains(union) {
                unions.push(*union);
            }
        }
    }
    unions
}

/// The most values of any closed type or code union: the walk picks each
/// value of each type at least once.
fn most_values() -> usize {
    let named = EVENT_VALUE_TYPES.iter().map(|named| match named.shape {
        NamedShape::Choice { values } => values.len(),
        NamedShape::Variants { variants } => variants.len(),
    });
    let codes = code_unions()
        .into_iter()
        .map(|union| code_members(&union).len());
    named.chain(codes).max().unwrap_or(1)
}

/// The names of the closed types that `fields` hold.
fn named_forms(fields: &[EventField]) -> Vec<&'static str> {
    fields
        .iter()
        .filter_map(|field| match field.form {
            ValueForm::Named(name) => Some(name),
            ValueForm::Uuid
            | ValueForm::Text { .. }
            | ValueForm::WholeNumber { .. }
            | ValueForm::Duration
            | ValueForm::Time
            | ValueForm::Codes(_) => None,
        })
        .collect()
}

#[test]
fn every_event_type_decodes_and_encodes_through_the_union() {
    let picks = most_values();
    for event_type in EVENT_TYPES {
        for pick in 0..picks {
            for end in [End::Low, End::High] {
                let wire = event_sample(event_type, Sampling { pick, end });
                let event = decode_event(&wire)
                    .unwrap_or_else(|error| panic!("{}: {error}\n{wire}", event_type.name));
                assert_eq!(event.event_type(), *event_type);
                assert_eq!(encode_event(&event).unwrap(), wire, "{}", event_type.name);
            }
        }
    }
}

#[test]
fn every_field_has_a_closed_or_bounded_form() {
    for field in all_fields() {
        match field.form {
            ValueForm::Uuid | ValueForm::Duration | ValueForm::Time => {}
            ValueForm::Text { max_chars } => {
                assert!((1..=MOST_TEXT_CHARS).contains(&max_chars), "{}", field.name);
            }
            ValueForm::WholeNumber { min, max } => assert!(min <= max, "{}", field.name),
            ValueForm::Codes(union) => {
                let members = code_members(union);
                let unique: BTreeSet<&str> = members.iter().copied().collect();
                assert!(!members.is_empty(), "{}", union.python_name);
                assert_eq!(unique.len(), members.len(), "{}", union.python_name);
            }
            ValueForm::Named(name) => match value_type(name).shape {
                NamedShape::Choice { values } => assert!(!values.is_empty(), "{name}"),
                NamedShape::Variants { variants } => assert!(!variants.is_empty(), "{name}"),
            },
        }
    }
}

#[test]
fn every_value_type_is_listed_once_after_the_types_it_names_and_is_used() {
    let names: Vec<&str> = EVENT_VALUE_TYPES.iter().map(|named| named.name).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len());
    for (position, named) in EVENT_VALUE_TYPES.iter().enumerate() {
        if let NamedShape::Variants { variants } = named.shape {
            for variant in variants {
                for used in named_forms(variant.fields) {
                    let used_at = names
                        .iter()
                        .position(|name| *name == used)
                        .unwrap_or_else(|| panic!("{used} is not listed"));
                    assert!(used_at < position, "{used} comes after {}", named.name);
                }
            }
        }
    }
    let used: BTreeSet<&str> = named_forms(&all_fields()).into_iter().collect();
    assert_eq!(used, unique);
}

#[test]
fn every_event_starts_with_its_time() {
    for event in EVENT_TYPES {
        let first = event.fields.first().expect("an event has fields");
        assert_eq!(
            (first.name, first.form, first.access),
            ("at", ValueForm::Time, FieldAccess::Public),
            "{}",
            event.name
        );
    }
}

#[test]
fn contract_names_are_unique_python_identifiers() {
    let identifier = |text: &str| {
        text.chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic())
            && text
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
    };
    let mut names: Vec<&str> = EVENT_TYPES.iter().map(|event| event.name).collect();
    names.extend(EVENT_VALUE_TYPES.iter().map(|named| named.name));
    for named in EVENT_VALUE_TYPES {
        if let NamedShape::Variants { variants } = named.shape {
            names.extend(variants.iter().map(|variant| variant.python_class));
        }
    }
    names.extend(code_unions().iter().map(|union| union.python_name));
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len(), "{names:?}");
    for name in names {
        assert!(identifier(name), "{name}");
    }
    for field in all_fields() {
        assert!(identifier(field.name), "{}", field.name);
    }
}

/// The wire form of `event`, with `item` of the event's items (the tag is
/// item 0) replaced by `value`.
fn with_item(event: &EventType, item: usize, value: WireValue) -> WireValue {
    let mut wire = event_sample(
        event,
        Sampling {
            pick: 0,
            end: End::Low,
        },
    );
    wire.as_array_mut().unwrap()[item] = value;
    wire
}

/// The refusal of a malformed event, or a panic.
fn malformed(wire: &WireValue) -> (&'static str, DecodeRefusal) {
    match decode_event(wire) {
        Err(EventDecodeError::Malformed { event, refusal }) => (event, refusal),
        other => panic!("{wire} is not refused as malformed: {other:?}"),
    }
}

/// The position of the field `name` of `event` among its wire items.
fn item_of(event: &EventType, name: &str) -> usize {
    1 + event
        .fields
        .iter()
        .position(|field| field.name == name)
        .unwrap_or_else(|| panic!("{} has no field {name}", event.name))
}

#[test]
fn a_value_that_is_not_an_array_with_a_tag_is_not_an_event() {
    for wire in [
        json!({"tag": "TaskClaimed"}),
        json!([]),
        json!([1, 2]),
        json!("TaskClaimed"),
    ] {
        assert_eq!(
            decode_event(&wire),
            Err(EventDecodeError::NotAnEvent),
            "{wire}"
        );
    }
}

#[test]
fn an_unknown_tag_is_named_and_cut() {
    assert_eq!(
        decode_event(&json!(["TaskStarted", 1])),
        Err(EventDecodeError::UnknownEventType {
            tag: RefusedName::cut("TaskStarted")
        })
    );
    let long = "X".repeat(MAX_REFUSED_NAME_CHARS + 10);
    match decode_event(&json!([long])) {
        Err(EventDecodeError::UnknownEventType { tag }) => {
            assert_eq!(tag.as_str().chars().count(), MAX_REFUSED_NAME_CHARS);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_missing_or_extra_item_is_an_item_count_refusal() {
    let event = <TaskClaimed as EventRecord>::TYPE;
    let mut short = event_sample(
        &event,
        Sampling {
            pick: 0,
            end: End::Low,
        },
    );
    short.as_array_mut().unwrap().pop();
    let (name, refusal) = malformed(&short);
    assert_eq!(name, "TaskClaimed");
    assert_eq!(refusal.path.names(), &[] as &[&str]);
    assert_eq!(
        refusal.cause,
        DecodeCause::ItemCount {
            expected: event.fields.len(),
            found: event.fields.len() - 1
        }
    );
    let mut long = event_sample(
        &event,
        Sampling {
            pick: 0,
            end: End::Low,
        },
    );
    long.as_array_mut().unwrap().push(json!(1));
    let (_, refusal) = malformed(&long);
    assert_eq!(
        refusal.cause,
        DecodeCause::ItemCount {
            expected: event.fields.len(),
            found: event.fields.len() + 1
        }
    );
}

#[test]
fn a_value_of_another_json_kind_is_refused_with_its_path() {
    let event = <TaskClaimed as EventRecord>::TYPE;
    let cases = [
        ("attempt", json!("2"), JsonKind::WholeNumber),
        ("attempt", json!(true), JsonKind::WholeNumber),
        ("queue_wait", json!(1.5), JsonKind::WholeNumber),
        ("task_name", json!(7), JsonKind::Text),
        ("execution", json!(null), JsonKind::Text),
    ];
    for (field, value, expected) in cases {
        let (_, refusal) = malformed(&with_item(&event, item_of(&event, field), value));
        assert_eq!(refusal.path.names(), &[field]);
        assert_eq!(refusal.cause, DecodeCause::WrongJsonKind { expected });
    }
}

#[test]
fn a_number_outside_its_bound_is_refused() {
    let claimed = <TaskClaimed as EventRecord>::TYPE;
    let cases = [
        (claimed, "attempt", json!(0), (1, i128::from(MAX_ATTEMPT))),
        (claimed, "priority", json!(101), (1, 100)),
        (claimed, "queue_wait", json!(-1), (0, i128::from(u64::MAX))),
        (
            claimed,
            "at",
            json!(u64::MAX),
            (i128::from(i64::MIN), i128::from(i64::MAX)),
        ),
        (
            <ChildStarted as EventRecord>::TYPE,
            "slot",
            json!(MAX_SLOT_INDEX + 1),
            (0, i128::from(MAX_SLOT_INDEX)),
        ),
        (
            <ChildStarted as EventRecord>::TYPE,
            "child_pid",
            json!(0),
            (1, i128::from(i32::MAX)),
        ),
    ];
    for (event, field, value, (min, max)) in cases {
        let (_, refusal) = malformed(&with_item(&event, item_of(&event, field), value));
        assert_eq!(refusal.path.names(), &[field]);
        assert_eq!(
            refusal.cause,
            DecodeCause::OutOfRange { min, max },
            "{field}"
        );
    }
}

#[test]
fn a_text_over_its_bound_is_refused() {
    let event = <TaskClaimed as EventRecord>::TYPE;
    let name = "é".repeat(MAX_EVENT_TASK_NAME_CHARS + 1);
    let (_, refusal) = malformed(&with_item(
        &event,
        item_of(&event, "task_name"),
        json!(name),
    ));
    assert_eq!(
        refusal.cause,
        DecodeCause::TextOverBound {
            chars: MAX_EVENT_TASK_NAME_CHARS + 1,
            bound: MAX_EVENT_TASK_NAME_CHARS
        }
    );
}

#[test]
fn a_uuid_in_another_form_is_refused() {
    let event = <TaskClaimed as EventRecord>::TYPE;
    for text in [
        "0192F6A1-7B3C-7D4E-8F00-0123456789AB",
        "{0192f6a1-7b3c-7d4e-8f00-0123456789ab}",
        "0192f6a17b3c7d4e8f000123456789ab",
        "not a uuid",
    ] {
        let (_, refusal) = malformed(&with_item(&event, item_of(&event, "task_id"), json!(text)));
        assert_eq!(refusal.cause, DecodeCause::NotCanonicalUuid, "{text}");
    }
}

#[test]
fn a_name_outside_a_closed_type_is_refused_with_the_type() {
    let claimed = <TaskClaimed as EventRecord>::TYPE;
    let (_, refusal) = malformed(&with_item(
        &claimed,
        item_of(&claimed, "execution"),
        json!("SHARED"),
    ));
    assert_eq!(
        refusal.cause,
        DecodeCause::UnknownName {
            type_name: "Execution",
            found: RefusedName::cut("SHARED")
        }
    );
    let ended = <AttemptEnded as EventRecord>::TYPE;
    let end = item_of(&ended, "end");
    let (_, refusal) = malformed(&with_item(&ended, end, json!(["EndedSomehow"])));
    assert_eq!(refusal.path.names(), &["end"]);
    assert_eq!(
        refusal.cause,
        DecodeCause::UnknownName {
            type_name: "AttemptEnd",
            found: RefusedName::cut("EndedSomehow")
        }
    );
    let unknown_code = json!([
        "ReportedByChild",
        ["Failed", ["BuiltIn", "NOT_A_CODE"]],
        ["NotReported"]
    ]);
    let (_, refusal) = malformed(&with_item(&ended, end, unknown_code));
    assert_eq!(
        refusal.path.to_string(),
        "end.ReportedByChild.result.Failed.error_code.BuiltIn.code"
    );
    assert_eq!(
        refusal.cause,
        DecodeCause::UnknownName {
            type_name: "BuiltInTaskCode",
            found: RefusedName::cut("NOT_A_CODE")
        }
    );
}

#[test]
fn a_variant_value_without_its_tag_or_items_is_refused() {
    let ended = <AttemptEnded as EventRecord>::TYPE;
    let end = item_of(&ended, "end");
    let (_, refusal) = malformed(&with_item(&ended, end, json!("NotKnown")));
    assert_eq!(
        refusal.cause,
        DecodeCause::WrongJsonKind {
            expected: JsonKind::Array
        }
    );
    for value in [json!([]), json!([42])] {
        let (_, refusal) = malformed(&with_item(&ended, end, value));
        assert_eq!(
            refusal.cause,
            DecodeCause::NoVariantTag {
                type_name: "AttemptEnd"
            }
        );
    }
    let (_, refusal) = malformed(&with_item(&ended, end, json!(["NotKnown"])));
    assert_eq!(refusal.path.names(), &["end", "NotKnown"]);
    assert_eq!(
        refusal.cause,
        DecodeCause::ItemCount {
            expected: 1,
            found: 0
        }
    );
}

/// A time that the wire's `i64` nanoseconds cannot hold.
fn far_future() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2300, 1, 1, 0, 0, 0).unwrap()
}

#[test]
fn a_duration_or_a_time_outside_the_wire_is_an_encode_refusal() {
    let mut ended = match vector_event("attempt_ended_not_known") {
        Event::AttemptEnded(ended) => ended,
        other => panic!("{other:?}"),
    };
    ended.run_duration = Duration::MAX;
    assert_eq!(
        encode_event(&ended.into()),
        Err(EventEncodeError {
            event: "AttemptEnded",
            refusal: EncodeRefusal {
                path: path_of(&["run_duration"]),
                cause: EncodeCause::DurationOverBound {
                    nanos: Duration::MAX.as_nanos()
                },
            },
        })
    );
    let mut recorded = match vector_event("attempt_recorded_retry") {
        Event::AttemptRecorded(recorded) => recorded,
        other => panic!("{other:?}"),
    };
    recorded.record = AttemptRecord::RetryScheduled {
        retry_count: number(1),
        next_retry_at: far_future(),
    };
    let refused = encode_event(&recorded.into()).unwrap_err();
    assert_eq!(refused.event, "AttemptRecorded");
    assert_eq!(
        refused.refusal.path.to_string(),
        "record.RetryScheduled.next_retry_at"
    );
    assert_eq!(
        refused.refusal.cause,
        EncodeCause::TimeOutOfRange { time: far_future() }
    );
}

/// The path of `names`, built as the codec builds it.
fn path_of(names: &[&'static str]) -> WirePath {
    let mut refusal = EncodeRefusal::new(EncodeCause::DurationOverBound { nanos: 0 });
    for name in names.iter().rev() {
        refusal = refusal.inside(name);
    }
    refusal.path
}

#[test]
fn the_reasons_of_the_core_convert_one_to_one() {
    let not_run = [
        (NotRunCause::PoolStopping, ReleaseReason::PoolStopping),
        (NotRunCause::PoolFailed, ReleaseReason::PoolFailed),
        (NotRunCause::QueueFull, ReleaseReason::QueueFull),
        (
            NotRunCause::NoChildOfItsKind,
            ReleaseReason::NoChildOfItsKind,
        ),
        (
            NotRunCause::RunnerBeatsFailedClosed,
            ReleaseReason::RunnerBeatsFailedClosed,
        ),
        (NotRunCause::HeldForQueueCap, ReleaseReason::HeldForQueueCap),
    ];
    for (cause, reason) in not_run {
        assert_eq!(ReleaseReason::from(cause), reason);
    }
    assert_eq!(
        SlotDownCause::from(ClearableBootCause::ResourceShortage(ShortageErrno::Enomem)),
        SlotDownCause::ResourceShortage {
            errno: ShortageErrno::Enomem
        }
    );
    assert_eq!(
        SlotDownCause::from(ClearableBootCause::ThreadStartRefused),
        SlotDownCause::ThreadStartRefused
    );
}

#[test]
fn a_task_error_code_converts_and_a_long_user_code_is_named() {
    let built_in = TaskErrorCode::BuiltIn(OutcomeCode::TaskTimeout.into());
    assert_eq!(
        AttemptErrorCode::from(&built_in),
        AttemptErrorCode::BuiltIn {
            code: OutcomeCode::TaskTimeout.into()
        }
    );
    let at_bound = "C".repeat(MAX_EVENT_USER_CODE_CHARS);
    let user = TaskErrorCode::User(UserCode::parse(at_bound.clone()).unwrap());
    assert_eq!(
        AttemptErrorCode::from(&user),
        AttemptErrorCode::User {
            code: UserCodeText::new(at_bound).unwrap()
        }
    );
    let over =
        TaskErrorCode::User(UserCode::parse("C".repeat(MAX_EVENT_USER_CODE_CHARS + 1)).unwrap());
    assert_eq!(
        AttemptErrorCode::from(&over),
        AttemptErrorCode::UserOverBound
    );
}

#[test]
fn a_worker_decision_names_the_code_of_its_attempt_row() {
    let codes = [
        (
            WorkerDecision::TimedOut,
            BuiltInTaskCode::from(OutcomeCode::TaskTimeout),
        ),
        (
            WorkerDecision::TaskBlockedLoop,
            OperationalErrorCode::TaskBlockedLoop.into(),
        ),
        (
            WorkerDecision::NeighbourBlockedLoop,
            OperationalErrorCode::NeighbourBlockedLoop.into(),
        ),
        (
            WorkerDecision::ChildLost,
            OperationalErrorCode::WorkerCrashed.into(),
        ),
        (
            WorkerDecision::KilledAfterCancel,
            OutcomeCode::TaskCancelled.into(),
        ),
        (
            WorkerDecision::LostAfterCancel,
            OutcomeCode::TaskCancelled.into(),
        ),
    ];
    for (decision, code) in codes {
        assert_eq!(decision.error_code(), code);
    }
}

#[test]
fn the_core_types_keep_their_own_names_on_the_wire() {
    for execution in Execution::ALL {
        assert_eq!(
            execution.to_wire().unwrap(),
            json!(serde_plain::to_string(&execution).unwrap())
        );
    }
    for errno in ShortageErrno::ALL {
        assert_eq!(errno.to_wire().unwrap(), json!(errno.as_str()));
    }
    // The names of `StopReason` in the `stopped_before_running` frame.
    let stop_reasons = [
        (StopReason::ClaimLost, "CLAIM_LOST"),
        (StopReason::OwnershipUnconfirmed, "OWNERSHIP_UNCONFIRMED"),
        (StopReason::WorkflowStopped, "WORKFLOW_STOPPED"),
        (StopReason::TaskExpired, "TASK_EXPIRED"),
        (StopReason::NodeNotLinked, "NODE_NOT_LINKED"),
        (StopReason::OrphanCancelled, "ORPHAN_CANCELLED"),
    ];
    for (reason, name) in stop_reasons {
        assert_eq!(reason.to_wire().unwrap(), json!(name));
    }
    let NamedShape::Choice { values } = <StopReason as EventNamedValue>::NAMED.shape else {
        panic!("StopReason is a choice");
    };
    assert_eq!(values.len(), stop_reasons.len());
}

/// The wire time of the vectors: 2023-11-14T22:13:20.123456789Z.
const VECTOR_AT: i64 = 1_700_000_000_123_456_789;

/// The trace carrier text of the vectors.
const VECTOR_CARRIER: &str =
    "{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}";

/// The worker id of the vectors.
const VECTOR_WORKER_ID: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

fn time(nanos: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(nanos)
}

fn text<const MAX_CHARS: usize>(value: &str) -> BoundedText<MAX_CHARS> {
    BoundedText::new(value.to_owned()).unwrap()
}

fn number<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
    BoundedU32::try_from(value).unwrap()
}

fn pid(value: u32) -> ProcessId {
    ProcessId::new(value).unwrap()
}

fn vector_task_id() -> Uuid {
    Uuid::parse_str("0192f6a1-7b3c-7d4e-8f00-0123456789ab").unwrap()
}

/// The event of each vector of `tests/fixtures/event_wire/vectors.json`, by
/// the vector's name.
fn vector_events() -> Vec<(&'static str, Event)> {
    let ended = |execution: Execution, attempt: u32, end: AttemptEnd, nanos: u64| AttemptEnded {
        at: time(VECTOR_AT),
        task_id: vector_task_id(),
        task_name: text("send_email"),
        queue: text("default"),
        execution,
        attempt: number(attempt),
        end,
        run_duration: Duration::from_nanos(nanos),
    };
    let recorded = |attempt: u32, record: AttemptRecord, nanos: u64| AttemptRecorded {
        at: time(VECTOR_AT),
        task_id: vector_task_id(),
        task_name: text("send_email"),
        queue: text("default"),
        attempt: number(attempt),
        record,
        record_duration: Duration::from_nanos(nanos),
    };
    vec![
        (
            "task_claimed",
            TaskClaimed {
                at: time(VECTOR_AT),
                task_id: vector_task_id(),
                task_name: text("send_email"),
                queue: text("default"),
                execution: Execution::SharedAsync,
                attempt: number(2),
                priority: number(50),
                queue_wait: Duration::from_nanos(1_500_000),
            }
            .into(),
        ),
        (
            "claim_released",
            ClaimReleased {
                at: time(VECTOR_AT),
                task_id: vector_task_id(),
                task_name: text("send_email"),
                queue: text("default"),
                reason: ReleaseReason::QueueFull,
            }
            .into(),
        ),
        (
            "attempt_placed",
            AttemptPlaced {
                at: time(VECTOR_AT),
                task_id: vector_task_id(),
                task_name: text("send_email"),
                queue: text("default"),
                execution: Execution::SharedAsync,
                attempt: number(1),
                child_kind: Execution::Isolated,
                child_pid: pid(4242),
                slot_wait: Duration::from_nanos(250_000),
            }
            .into(),
        ),
        (
            "attempt_not_started",
            AttemptNotStarted {
                at: time(VECTOR_AT),
                task_id: vector_task_id(),
                task_name: text("send_email"),
                queue: text("default"),
                attempt: number(1),
                reason: StopReason::WorkflowStopped,
            }
            .into(),
        ),
        (
            "attempt_ended_completed",
            ended(
                Execution::SharedThread,
                1,
                AttemptEnd::ReportedByChild {
                    result: ChildResult::Completed,
                    start: AttemptStart::Reported {
                        started_at: time(1_700_000_000_100_000_000),
                    },
                },
                2_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_user_code",
            ended(
                Execution::SharedAsync,
                2,
                AttemptEnd::ReportedByChild {
                    result: ChildResult::Failed {
                        error_code: AttemptErrorCode::User {
                            code: text("RATE_LIMITED"),
                        },
                    },
                    start: AttemptStart::Reported {
                        started_at: time(1_700_000_000_100_000_000),
                    },
                },
                2_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_built_in_code",
            ended(
                Execution::Isolated,
                1,
                AttemptEnd::ReportedByChild {
                    result: ChildResult::Failed {
                        error_code: AttemptErrorCode::BuiltIn {
                            code: OperationalErrorCode::TaskException.into(),
                        },
                    },
                    start: AttemptStart::NotReported,
                },
                5_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_worker_failure",
            ended(
                Execution::Isolated,
                1,
                AttemptEnd::ReportedByChild {
                    result: ChildResult::WorkerFailure,
                    start: AttemptStart::NotReported,
                },
                5_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_timed_out_in_child",
            ended(
                Execution::SharedAsync,
                3,
                AttemptEnd::ReportedByChild {
                    result: ChildResult::TimedOut,
                    start: AttemptStart::Reported {
                        started_at: time(1_700_000_000_100_000_000),
                    },
                },
                30_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_by_worker",
            ended(
                Execution::SharedAsync,
                3,
                AttemptEnd::DecidedByWorker {
                    reason: WorkerDecision::TimedOut,
                    error_code: OutcomeCode::TaskTimeout.into(),
                    trace: RunTrace::Carrier {
                        text: text(VECTOR_CARRIER),
                    },
                    placed_at: time(1_700_000_000_000_000_000),
                    child_kind: Execution::SharedAsync,
                    worker_id: text(VECTOR_WORKER_ID),
                    workflow: WorkflowPlace::NotInWorkflow,
                },
                30_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_by_worker_without_carrier",
            ended(
                Execution::SharedAsync,
                1,
                AttemptEnd::DecidedByWorker {
                    reason: WorkerDecision::NeighbourBlockedLoop,
                    error_code: OperationalErrorCode::NeighbourBlockedLoop.into(),
                    trace: RunTrace::NoCarrier,
                    placed_at: time(1_700_000_000_000_000_000),
                    child_kind: Execution::SharedAsync,
                    worker_id: text(VECTOR_WORKER_ID),
                    workflow: WorkflowPlace::NotInWorkflow,
                },
                30_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_by_worker_in_workflow",
            ended(
                Execution::SharedThread,
                2,
                AttemptEnd::DecidedByWorker {
                    reason: WorkerDecision::ChildLost,
                    error_code: OperationalErrorCode::WorkerCrashed.into(),
                    trace: RunTrace::Carrier {
                        text: text(VECTOR_CARRIER),
                    },
                    placed_at: time(1_700_000_000_000_000_000),
                    child_kind: Execution::Isolated,
                    worker_id: text(VECTOR_WORKER_ID),
                    workflow: WorkflowPlace::WorkflowNotKnown,
                },
                30_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_ended_not_known",
            ended(
                Execution::Isolated,
                1,
                AttemptEnd::NotKnown {
                    child_pid: pid(4242),
                },
                60_000_000_000,
            )
            .into(),
        ),
        (
            "attempt_recorded_retry",
            recorded(
                2,
                AttemptRecord::RetryScheduled {
                    retry_count: number(2),
                    next_retry_at: time(1_700_000_060_000_000_000),
                },
                3_000_000,
            )
            .into(),
        ),
        (
            "attempt_recorded_failed",
            recorded(3, AttemptRecord::Failed, 1_000_000).into(),
        ),
        (
            "task_recovered",
            TaskRecovered {
                at: time(VECTOR_AT),
                task_id: vector_task_id(),
                task_name: text("send_email"),
                queue: text("default"),
                action: RecoveryAction::RunningFailed,
            }
            .into(),
        ),
        (
            "child_started",
            ChildStarted {
                at: time(VECTOR_AT),
                child_kind: Execution::SharedThread,
                slot: number(3),
                child_pid: pid(4243),
                boot_duration: Duration::from_nanos(850_000_000),
            }
            .into(),
        ),
        (
            "child_replaced",
            ChildReplaced {
                at: time(VECTOR_AT),
                child_kind: Execution::Isolated,
                slot: number(0),
                child_pid: pid(4242),
                reason: ReplaceReason::MemoryLimit,
            }
            .into(),
        ),
        (
            "slot_down",
            SlotDown {
                at: time(VECTOR_AT),
                child_kind: Execution::SharedAsync,
                slot: number(1),
                cause: SlotDownCause::ResourceShortage {
                    errno: ShortageErrno::Emfile,
                },
            }
            .into(),
        ),
        (
            "slot_recovered",
            SlotRecovered {
                at: time(VECTOR_AT),
                child_kind: Execution::SharedAsync,
                slot: number(1),
            }
            .into(),
        ),
    ]
}

fn vector_event(name: &str) -> Event {
    vector_events()
        .into_iter()
        .find(|(vector, _)| *vector == name)
        .map(|(_, event)| event)
        .unwrap_or_else(|| panic!("no vector event {name}"))
}

/// The vectors file: `contract_version`, and `vectors` of `name` and
/// `wire`. The Python decode test reads the same file.
fn wire_vectors() -> WireValue {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/event_wire/vectors.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap()
}

#[test]
fn the_wire_vectors_are_the_encoding_of_the_vector_events() {
    let file = wire_vectors();
    assert_eq!(file["contract_version"], json!(EVENT_CONTRACT_VERSION));
    let vectors = file["vectors"].as_array().unwrap();
    let events = vector_events();
    let file_names: Vec<&str> = vectors
        .iter()
        .map(|vector| vector["name"].as_str().unwrap())
        .collect();
    let event_names: Vec<&str> = events.iter().map(|(name, _)| *name).collect();
    assert_eq!(file_names, event_names);
    for (vector, (name, event)) in vectors.iter().zip(&events) {
        let wire = &vector["wire"];
        assert_eq!(&encode_event(event).unwrap(), wire, "{name}");
        assert_eq!(&decode_event(wire).unwrap(), event, "{name}");
    }
    let covered: BTreeSet<&str> = events
        .iter()
        .map(|(_, event)| event.event_type().name)
        .collect();
    let all: BTreeSet<&str> = EVENT_TYPES.iter().map(|event| event.name).collect();
    assert_eq!(covered, all);
}

/// The text of a form in the snapshot.
fn form_text(form: ValueForm) -> String {
    match form {
        ValueForm::Uuid => "Uuid".to_owned(),
        ValueForm::Text { max_chars } => format!("Text({max_chars})"),
        ValueForm::WholeNumber { min, max } => format!("WholeNumber({min}..={max})"),
        ValueForm::Duration => "Duration".to_owned(),
        ValueForm::Time => "Time".to_owned(),
        ValueForm::Codes(union) => format!("Codes({})", union.python_name),
        ValueForm::Named(name) => name.to_owned(),
    }
}

fn field_text(field: &EventField) -> String {
    match field.access {
        FieldAccess::Public => format!("{}: {}", field.name, form_text(field.form)),
        FieldAccess::Internal => format!("internal {}: {}", field.name, form_text(field.form)),
    }
}

/// The contract as text: the version, each event type with its fields, each
/// closed type with its values or variants, and each code union.
fn contract_snapshot() -> String {
    let mut text = format!("version {EVENT_CONTRACT_VERSION}\n");
    for event in EVENT_TYPES {
        text.push_str(&format!("\nevent {}\n", event.name));
        for field in event.fields {
            text.push_str(&format!("  {}\n", field_text(field)));
        }
    }
    for named in EVENT_VALUE_TYPES {
        match named.shape {
            NamedShape::Choice { values } => {
                let names: Vec<&str> = values.iter().map(|value| value.value).collect();
                text.push_str(&format!("\nchoice {}: {}\n", named.name, names.join(" | ")));
            }
            NamedShape::Variants { variants } => {
                text.push_str(&format!("\nvariants {}\n", named.name));
                for variant in variants {
                    let fields: Vec<String> = variant.fields.iter().map(field_text).collect();
                    match fields.is_empty() {
                        true => text.push_str(&format!("  {}\n", variant.tag)),
                        false => {
                            text.push_str(&format!("  {}({})\n", variant.tag, fields.join(", ")));
                        }
                    }
                }
            }
        }
    }
    for union in code_unions() {
        let classes: Vec<&str> = union
            .catalogs
            .iter()
            .map(|catalog| catalog.python_class)
            .collect();
        text.push_str(&format!(
            "\ncodes {}: {}\n",
            union.python_name,
            classes.join(" | ")
        ));
    }
    text
}

/// A change of the contract fails here: the change gets a new
/// `EVENT_CONTRACT_VERSION` and the snapshot file of that version.
#[test]
fn the_contract_is_the_snapshot_of_its_version() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "tests/fixtures/event_contract/v{EVENT_CONTRACT_VERSION}.txt"
    ));
    let snapshot = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    assert_eq!(contract_snapshot(), snapshot);
    assert_eq!(EVENT_CONTRACT.version, EVENT_CONTRACT_VERSION);
    assert_eq!(EVENT_CONTRACT.events, EVENT_TYPES);
    assert_eq!(EVENT_CONTRACT.value_types, EVENT_VALUE_TYPES);
}

/// Whether `event` is an event of type `E`, by [`UnionEvent::of_event`].
fn is_of<E: UnionEvent>(event: &Event) -> bool {
    E::of_event(event).is_some()
}

/// `of_event` of each type gives the record of an event of that type, and
/// `None` for an event of every other type. The list has one entry per
/// type of the union, so a new type fails here until it is added.
/// An event type and the check that an event is of that type.
type TypeCheck = (EventTypeName, fn(&Event) -> bool);

#[test]
fn of_event_gives_the_record_of_its_own_type_only() {
    let checks: [TypeCheck; 11] = [
        (EventTypeName::TaskClaimed, is_of::<TaskClaimed>),
        (EventTypeName::ClaimReleased, is_of::<ClaimReleased>),
        (EventTypeName::AttemptPlaced, is_of::<AttemptPlaced>),
        (EventTypeName::AttemptNotStarted, is_of::<AttemptNotStarted>),
        (EventTypeName::AttemptEnded, is_of::<AttemptEnded>),
        (EventTypeName::AttemptRecorded, is_of::<AttemptRecorded>),
        (EventTypeName::TaskRecovered, is_of::<TaskRecovered>),
        (EventTypeName::ChildStarted, is_of::<ChildStarted>),
        (EventTypeName::ChildReplaced, is_of::<ChildReplaced>),
        (EventTypeName::SlotDown, is_of::<SlotDown>),
        (EventTypeName::SlotRecovered, is_of::<SlotRecovered>),
    ];
    assert_eq!(checks.len(), EventTypeName::ALL.len());
    let events = vector_events();
    for name in EventTypeName::ALL {
        assert!(
            events.iter().any(|(_, event)| event.type_name() == *name),
            "the vectors have no event of {name:?}"
        );
    }
    for (_, event) in &events {
        for (name, is_of_type) in &checks {
            assert_eq!(
                is_of_type(event),
                event.type_name() == *name,
                "{name:?} of a {:?} event",
                event.type_name()
            );
        }
    }
    let claimed = vector_event("task_claimed");
    let record = TaskClaimed::of_event(&claimed).expect("a TaskClaimed event");
    assert_eq!(Event::from(record.clone()), claimed);
}
