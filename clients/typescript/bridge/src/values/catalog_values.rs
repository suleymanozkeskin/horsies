//! The raw record of a value of a core error catalog.
//!
//! A core error value whose enum derives `ErrorClassValues` crosses as
//! `{ class, slots }`: the name of its generated class and the values of the
//! class's instance slots, by name, in the class's slot order. The generated
//! TypeScript converter of the catalog builds the public value from it and
//! adds `code` and the fixed `retryable`. An unknown class name is a defect of
//! the build.
//!
//! Slot values: text as a string; a flag as a boolean; an integer as a
//! `BigInt`; a UUID as its text; a code member as its wire value; the payload
//! of a send as a raw record of its fields; the `cause` slot as an
//! exception record.

use horsies_core::error_catalog::{
    ClientExceptionClass, ErrorClassValues, ErrorEnum, ExceptionValue, RecordValue, SlotData,
};
use horsies_core::TaskSendPayload;
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::values::boundary::{bigint_of_i64, epoch_micros_of, text_of_uuid};
use crate::values::core_failures::CauseRecord;

/// A value of a core error catalog: its generated class and its instance
/// slots.
#[napi(object)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawCatalogError {
    /// The generated class, for example `TaskActionNotFound`.
    pub class: String,
    /// The instance slots, in the class's slot order.
    pub slots: Vec<RawSlot>,
}

/// One instance slot of a catalog value.
#[napi(object)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawSlot {
    pub name: String,
    pub value: RawSlotValue,
}

/// The value of one instance slot, by the type of the slot.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawSlotValue {
    Text {
        text: String,
    },
    Flag {
        value: bool,
    },
    /// Any integer of 64 bits or less.
    Integer {
        value: BigInt,
    },
    /// A UUID, as lower-case hyphenated text.
    Uuid {
        text: String,
    },
    /// A member of a code catalog: the generated enum and the member's wire
    /// value.
    CodeMember {
        catalog: String,
        value: String,
    },
    /// The payload of a send that the retry methods send again.
    SendPayload {
        fields: Vec<RawRecordField>,
    },
    /// The `cause` slot.
    Exception {
        exception: ExceptionRecord,
    },
}

/// One field of a generated record class, with its name.
#[napi(object)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawRecordField {
    pub name: String,
    pub value: RawRecordValue,
}

/// The value of one record field. An optional field with no value is
/// `absent`. The retention class key has its own value for "retain
/// forever": in that field the core's `None` means forever, and the field's
/// default is the 30-day class, so `absent` would read as the default.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawRecordValue {
    Text {
        text: String,
    },
    Integer {
        value: BigInt,
    },
    /// An instant, in epoch microseconds.
    Instant {
        epoch_micros: BigInt,
    },
    Absent,
    /// The retention class key `None`: the task is kept forever.
    RetainForever,
}

/// The exception of an `cause` slot.
#[napi(discriminant = "kind", discriminant_case = "camelCase")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExceptionRecord {
    /// The core's class of the cause, with the operation that failed as its
    /// subject.
    CoreFailure { cause: CauseRecord },
    /// A generated exception class with its text, for example
    /// `OutcomeDecodeError`.
    Generated { class: String, text: String },
    /// A class that the client defines: a refused value (`valueError`) or a
    /// text that does not read as JSON (`serializationError`).
    ClientClass {
        class: ClientClassName,
        text: String,
    },
    /// No exception: a check of the core found the failure.
    NoCause,
}

/// A class of exception that the client defines.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientClassName {
    ValueError,
    SerializationError,
}

impl RawCatalogError {
    /// The raw record of `error`. `subject` is the operation that failed, for
    /// example `cancel_task`: the subject of a core cause in the `cause`
    /// slot. Pure.
    pub fn of<E: ErrorClassValues>(subject: &str, error: &E) -> Self {
        Self {
            class: ErrorEnum::type_name(error).to_owned(),
            slots: error
                .slot_values()
                .into_iter()
                .map(|slot| RawSlot {
                    name: slot.name.to_owned(),
                    value: slot_value(subject, slot.value),
                })
                .collect(),
        }
    }
}

/// The raw value of one slot. Pure.
fn slot_value(subject: &str, value: SlotData<'_>) -> RawSlotValue {
    match value {
        SlotData::Str(text) => RawSlotValue::Text {
            text: text.to_owned(),
        },
        SlotData::Bool(flag) => RawSlotValue::Flag { value: flag },
        SlotData::Int(number) => RawSlotValue::Integer {
            value: BigInt::from(number),
        },
        SlotData::TaskSendPayload(payload) => RawSlotValue::SendPayload {
            fields: payload_fields(payload),
        },
        SlotData::Uuid(id) | SlotData::UuidText(id) => RawSlotValue::Uuid {
            text: text_of_uuid(id),
        },
        SlotData::CodeMember { catalog, value } => RawSlotValue::CodeMember {
            catalog: catalog.type_name.to_owned(),
            value: value.to_owned(),
        },
        SlotData::Exception(exception) => RawSlotValue::Exception {
            exception: exception_record(subject, exception),
        },
    }
}

/// The fields of a send payload by name ([`TaskSendPayload::record_values`]).
/// Pure.
fn payload_fields(payload: &TaskSendPayload) -> Vec<RawRecordField> {
    payload
        .record_values()
        .into_iter()
        .map(|field| RawRecordField {
            name: field.name.to_owned(),
            value: record_value(field.value),
        })
        .collect()
}

/// The raw value of one record field. Pure.
fn record_value(value: RecordValue<'_>) -> RawRecordValue {
    match value {
        RecordValue::Str(text) => RawRecordValue::Text {
            text: text.to_owned(),
        },
        RecordValue::Int(number) => RawRecordValue::Integer {
            value: bigint_of_i64(number),
        },
        RecordValue::DateTime(instant) => RawRecordValue::Instant {
            epoch_micros: epoch_micros_of(instant),
        },
        RecordValue::OptionalStr(text) => {
            text.map_or(RawRecordValue::Absent, |text| RawRecordValue::Text {
                text: text.to_owned(),
            })
        }
        RecordValue::OptionalInt(number) => {
            number.map_or(RawRecordValue::Absent, |number| RawRecordValue::Integer {
                value: bigint_of_i64(number),
            })
        }
        RecordValue::OptionalDateTime(instant) => {
            instant.map_or(RawRecordValue::Absent, |instant| RawRecordValue::Instant {
                epoch_micros: epoch_micros_of(instant),
            })
        }
        RecordValue::OptionalRetentionClassKey(key) => key
            .map_or(RawRecordValue::RetainForever, |text| RawRecordValue::Text {
                text,
            }),
    }
}

/// The exception record of an `cause` slot. Pure.
fn exception_record(subject: &str, exception: ExceptionValue) -> ExceptionRecord {
    match exception {
        ExceptionValue::CoreFailure {
            text,
            retryable,
            cause,
        } => ExceptionRecord::CoreFailure {
            cause: CauseRecord::new(subject, text, retryable, cause),
        },
        ExceptionValue::Generated { class, text } => ExceptionRecord::Generated {
            class: class.type_name.to_owned(),
            text,
        },
        ExceptionValue::ClientClass { class, text } => ExceptionRecord::ClientClass {
            class: match class {
                ClientExceptionClass::ValueError => ClientClassName::ValueError,
                ClientExceptionClass::SerializationError => ClientClassName::SerializationError,
            },
            text,
        },
        ExceptionValue::NoCause => ExceptionRecord::NoCause,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use horsies_core::FailureCause;

    use super::*;

    #[test]
    fn an_integer_slot_of_u64_max_keeps_every_digit() {
        assert_eq!(
            slot_value("send", SlotData::Int(i128::from(u64::MAX))),
            RawSlotValue::Integer {
                value: BigInt::from(u64::MAX)
            }
        );
    }

    #[test]
    fn a_text_and_a_flag_slot_keep_their_values() {
        assert_eq!(
            slot_value("send", SlotData::Str("orders")),
            RawSlotValue::Text {
                text: "orders".to_owned()
            }
        );
        assert_eq!(
            slot_value("send", SlotData::Bool(true)),
            RawSlotValue::Flag { value: true }
        );
    }

    #[test]
    fn both_uuid_slot_forms_cross_as_text() {
        let id = uuid::Uuid::from_u128(0x0189_0a5d_ac96_774b_bcce_b302_099a_8057);
        for slot in [SlotData::Uuid(id), SlotData::UuidText(id)] {
            assert_eq!(
                slot_value("cancel_task", slot),
                RawSlotValue::Uuid {
                    text: "01890a5d-ac96-774b-bcce-b302099a8057".to_owned()
                }
            );
        }
    }

    #[test]
    fn a_core_failure_exception_carries_the_subject_of_the_operation() {
        assert_eq!(
            exception_record(
                "cancel_task",
                ExceptionValue::CoreFailure {
                    text: "connection failed: refused".to_owned(),
                    retryable: true,
                    cause: FailureCause::Connection,
                },
            ),
            ExceptionRecord::CoreFailure {
                cause: CauseRecord::Connection {
                    subject: "cancel_task".to_owned(),
                    message: "connection failed: refused".to_owned(),
                    retryable: true,
                }
            }
        );
    }

    #[test]
    fn a_client_class_exception_names_the_class_and_keeps_the_text() {
        assert_eq!(
            exception_record(
                "enqueue",
                ExceptionValue::ClientClass {
                    class: ClientExceptionClass::ValueError,
                    text: "priority must be between 1 and 100".to_owned(),
                },
            ),
            ExceptionRecord::ClientClass {
                class: ClientClassName::ValueError,
                text: "priority must be between 1 and 100".to_owned(),
            }
        );
    }

    #[test]
    fn no_cause_is_its_own_record() {
        assert_eq!(
            exception_record("enqueue", ExceptionValue::NoCause),
            ExceptionRecord::NoCause
        );
    }

    #[test]
    fn a_record_value_is_its_raw_value() {
        assert_eq!(
            record_value(RecordValue::Int(-7)),
            RawRecordValue::Integer {
                value: bigint_of_i64(-7)
            }
        );
        assert_eq!(
            record_value(RecordValue::OptionalInt(None)),
            RawRecordValue::Absent
        );
        assert_eq!(
            record_value(RecordValue::OptionalRetentionClassKey(None)),
            RawRecordValue::RetainForever
        );
        assert_eq!(
            record_value(RecordValue::OptionalRetentionClassKey(Some(
                "audit-90".to_owned()
            ))),
            RawRecordValue::Text {
                text: "audit-90".to_owned()
            }
        );
        let instant = Utc
            .with_ymd_and_hms(2026, 11, 18, 12, 0, 0)
            .single()
            .expect("a valid instant");
        assert_eq!(
            record_value(RecordValue::DateTime(instant)),
            RawRecordValue::Instant {
                epoch_micros: bigint_of_i64(instant.timestamp_micros())
            }
        );
    }
}
