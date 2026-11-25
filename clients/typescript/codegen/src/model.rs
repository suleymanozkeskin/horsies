//! The adapter over the codegen model of horsies-core.
//!
//! Some fields and types of the model have Python names: `python_module`,
//! `python_class`, `python_name`, `python_type`, `PythonValue` and
//! `PythonItem`. This module is the only one that reads them. It gives the
//! rest of the renderer neutral names, so a rename in the core changes this
//! module only. The renderer reads every other part of the model directly.
//!
//! A field name does not change a value: `python_module` holds a short
//! group name (`broker`, `task_send_errors`), which is also the stem of the
//! TypeScript module. The renderer does not read the facts that only
//! Python needs (`python_base`, the base class of a Python enum).

use horsies_core::catalog::CodeCatalog;
use horsies_core::error_catalog::{
    CodeSubset, ErrorCatalog, ErrorClass, ErrorSlot, ErrorUnion, ExceptionClass, ExceptionType,
    RecordClass, RecordField, RecordFieldType, SlotRepr, SlotType, TextNewType,
};
use horsies_core::events::{CodeUnion, VariantType};
use horsies_core::manifest_schema::{
    IntBound, ManifestField, ManifestType, PythonItem, PythonValue, TaggedVariant,
};

// Code catalogs.

/// The module of a code catalog: a group name such as `status`. Pure.
pub fn catalog_module(catalog: &CodeCatalog) -> &'static str {
    catalog.python_module
}

/// The type name of a code catalog, such as `TaskStatus`. Pure.
pub fn catalog_type_name(catalog: &CodeCatalog) -> &'static str {
    catalog.python_class
}

// Error catalogs.

/// The module of an error catalog, such as `task_send_errors`. Pure.
pub fn error_catalog_module(catalog: &ErrorCatalog) -> &'static str {
    catalog.python_module
}

/// The type name of an error class, such as `TaskSendEnqueueFailed`. Pure.
pub fn error_class_type_name(class: &ErrorClass) -> &'static str {
    class.python_class
}

/// Whether the cause slot of an error class always holds a cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CauseSlot {
    /// The slot holds a cause.
    Required,
    /// The slot holds a cause, or the value that says that no cause
    /// exists: a check of the core found the failure.
    Optional,
}

/// One slot of an error class, in neutral names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassSlot {
    /// The class has one code: the catalog member named `member`.
    CodeMember { member: &'static str },
    /// The value holds a code of the code subset named `subset`.
    CodeSubset { subset: &'static str },
    /// The state of the class never clears: `retryable` is always false.
    NeverRetryable,
    /// A field of the value.
    Field {
        name: &'static str,
        slot_type: SlotType,
        display: SlotRepr,
    },
    /// The cause of the failure.
    Cause { cause_type: CauseSlot },
}

/// The slot of the model's `slot`. Pure.
pub fn class_slot(slot: &ErrorSlot) -> ClassSlot {
    match *slot {
        ErrorSlot::CodeMember { member } => ClassSlot::CodeMember { member },
        ErrorSlot::CodeSubset { subset } => ClassSlot::CodeSubset { subset },
        ErrorSlot::NeverRetryable => ClassSlot::NeverRetryable,
        ErrorSlot::Field {
            name,
            python_type,
            repr,
        } => ClassSlot::Field {
            name,
            slot_type: python_type,
            display: repr,
        },
        ErrorSlot::Exception { python_type } => ClassSlot::Cause {
            cause_type: match python_type {
                ExceptionType::Exception => CauseSlot::Required,
                ExceptionType::ExceptionOrNoCause => CauseSlot::Optional,
            },
        },
    }
}

/// The slots of `class`, in the model's order. Pure.
pub fn class_slots(class: &ErrorClass) -> Vec<ClassSlot> {
    class.slots.iter().map(class_slot).collect()
}

/// The module of an error union: the module of its catalog. Pure.
pub fn error_union_module(union: &ErrorUnion) -> &'static str {
    union.python_module
}

/// The module of a code subset: the module of the catalog that uses it.
/// Pure.
pub fn code_subset_module(subset: &CodeSubset) -> &'static str {
    subset.python_module
}

/// The type name of an exception class of the core, such as
/// `OutcomeDecodeError`. Pure.
pub fn exception_class_type_name(class: &ExceptionClass) -> &'static str {
    class.python_class
}

/// The module of a record that a field of an error class holds. Pure.
pub fn record_module(record: &RecordClass) -> &'static str {
    record.python_module
}

/// The type name of a record, such as `TaskSendPayload`. Pure.
pub fn record_type_name(record: &RecordClass) -> &'static str {
    record.python_class
}

/// The type of a field of a record. Pure.
pub fn record_field_type(field: &RecordField) -> RecordFieldType {
    field.python_type
}

/// The module of a named text type, such as `retention_class`. Pure.
pub fn new_type_module(new_type: &TextNewType) -> &'static str {
    new_type.python_module
}

// Manifests, the workflow spec data and the schedule pattern.

/// The type of an item of a list or of a value of a map: a value that is
/// not a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    Bool,
    Str,
    Int(IntBound),
    /// A type of the same manifest module, by its type name.
    Named(&'static str),
    /// A time of day in whole seconds, as the text `HH:MM:SS`.
    TimeOfDay,
}

/// The type of a manifest field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    Str,
    Int(IntBound),
    /// A type of the same manifest module, by its type name.
    Named(&'static str),
    /// A list of items of one type.
    List(ItemType),
    /// A map from text keys to values of one type.
    Map(ItemType),
    /// An item or no value: the text holds `null`, and the reader also
    /// takes a missing member.
    Optional(ItemType),
    /// A time of day in whole seconds, as the text `HH:MM:SS`.
    TimeOfDay,
}

/// The item type of the model's `item`. Pure.
pub fn item_type(item: PythonItem) -> ItemType {
    match item {
        PythonItem::Bool => ItemType::Bool,
        PythonItem::Str => ItemType::Str,
        PythonItem::Int(bound) => ItemType::Int(bound),
        PythonItem::Named(name) => ItemType::Named(name),
        PythonItem::TimeOfDay => ItemType::TimeOfDay,
    }
}

/// The type of a manifest field. Pure.
pub fn field_value_type(field: &ManifestField) -> ValueType {
    match field.value {
        PythonValue::Bool => ValueType::Bool,
        PythonValue::Str => ValueType::Str,
        PythonValue::Int(bound) => ValueType::Int(bound),
        PythonValue::Named(name) => ValueType::Named(name),
        PythonValue::List(item) => ValueType::List(item_type(item)),
        PythonValue::Map(item) => ValueType::Map(item_type(item)),
        PythonValue::Optional(item) => ValueType::Optional(item_type(item)),
        PythonValue::TimeOfDay => ValueType::TimeOfDay,
    }
}

/// The type name of a manifest type: a record, a tagged union or a choice.
/// A field names a type by this name (`ValueType::Named`). Pure.
pub fn manifest_type_name(manifest_type: &ManifestType) -> &'static str {
    manifest_type.python_name
}

/// The type name of one variant of a tagged manifest type. Pure.
pub fn tagged_variant_type_name(variant: &TaggedVariant) -> &'static str {
    variant.python_class
}

// Events.

/// The name of the union of the catalogs of a code union, such as
/// `BuiltInTaskCode`. Pure.
pub fn code_union_alias_name(union: &CodeUnion) -> &'static str {
    union.python_name
}

/// The type name of one variant of a closed event type: the type name,
/// then the variant name. Pure.
pub fn event_variant_type_name(variant: &VariantType) -> &'static str {
    variant.python_class
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::{CodeEnum, CODE_CATALOGS};
    use horsies_core::error_catalog::{
        code_subsets, ErrorEnum, ERROR_CATALOGS, ERROR_RECORDS, ERROR_UNIONS, OUTCOME_DECODE_ERROR,
        RETENTION_CLASS_KEY, TASK_SEND_PAYLOAD,
    };
    use horsies_core::events::BUILT_IN_TASK_CODES;
    use horsies_core::manifest_schema::{MemberDefault, VariantFields};
    use horsies_core::TaskStatus;

    use super::*;

    const BOUND: IntBound = IntBound {
        name: "PRIORITY",
        min: 1,
        max: 100,
    };

    fn field(value: PythonValue) -> ManifestField {
        ManifestField {
            name: "field",
            doc: "",
            value,
            default: MemberDefault::NoDefault,
        }
    }

    #[test]
    fn a_code_catalog_has_its_module_and_type_name() {
        let catalog = <TaskStatus as CodeEnum>::CATALOG;
        assert_eq!(catalog_module(&catalog), "status");
        assert_eq!(catalog_type_name(&catalog), "TaskStatus");
        assert!(CODE_CATALOGS.contains(&catalog));
    }

    #[test]
    fn an_error_catalog_and_its_classes_have_their_names() {
        let catalog = ERROR_CATALOGS
            .iter()
            .find(|catalog| error_catalog_module(catalog) == "task_send_errors")
            .expect("the send catalog is in the model");
        assert!(catalog
            .classes
            .iter()
            .any(|class| error_class_type_name(class) == "TaskSendEnqueueFailed"));
    }

    #[test]
    fn each_error_slot_has_its_neutral_slot() {
        let class = ErrorClass {
            python_class: "Sample",
            doc: "",
            slots: &[
                ErrorSlot::CodeMember {
                    member: "ENQUEUE_FAILED",
                },
                ErrorSlot::CodeSubset {
                    subset: "BrokerOperationFailedCode",
                },
                ErrorSlot::NeverRetryable,
                ErrorSlot::Field {
                    name: "message",
                    python_type: SlotType::Str,
                    repr: SlotRepr::Hidden,
                },
                ErrorSlot::Exception {
                    python_type: ExceptionType::Exception,
                },
                ErrorSlot::Exception {
                    python_type: ExceptionType::ExceptionOrNoCause,
                },
            ],
        };
        assert_eq!(
            class_slots(&class),
            vec![
                ClassSlot::CodeMember {
                    member: "ENQUEUE_FAILED"
                },
                ClassSlot::CodeSubset {
                    subset: "BrokerOperationFailedCode"
                },
                ClassSlot::NeverRetryable,
                ClassSlot::Field {
                    name: "message",
                    slot_type: SlotType::Str,
                    display: SlotRepr::Hidden,
                },
                ClassSlot::Cause {
                    cause_type: CauseSlot::Required
                },
                ClassSlot::Cause {
                    cause_type: CauseSlot::Optional
                },
            ]
        );
    }

    #[test]
    fn unions_subsets_records_and_new_types_have_their_modules() {
        let broker = <horsies_core::BrokerOperationError as ErrorEnum>::ERROR_CATALOG;
        for union in ERROR_UNIONS {
            assert_eq!(error_union_module(union), error_catalog_module(&broker));
        }
        for subset in code_subsets() {
            assert_eq!(code_subset_module(&subset), error_catalog_module(&broker));
        }
        assert_eq!(record_module(&TASK_SEND_PAYLOAD), "task_send_errors");
        assert_eq!(record_type_name(&TASK_SEND_PAYLOAD), "TaskSendPayload");
        assert_eq!(ERROR_RECORDS, &[TASK_SEND_PAYLOAD]);
        assert_eq!(
            record_field_type(&TASK_SEND_PAYLOAD.fields[0]),
            RecordFieldType::Str
        );
        assert_eq!(new_type_module(&RETENTION_CLASS_KEY), "retention_class");
        assert_eq!(
            exception_class_type_name(&OUTCOME_DECODE_ERROR),
            "OutcomeDecodeError"
        );
    }

    #[test]
    fn each_manifest_value_has_its_value_type() {
        let cases = [
            (PythonValue::Bool, ValueType::Bool),
            (PythonValue::Str, ValueType::Str),
            (PythonValue::Int(BOUND), ValueType::Int(BOUND)),
            (PythonValue::Named("Queues"), ValueType::Named("Queues")),
            (
                PythonValue::List(PythonItem::Int(BOUND)),
                ValueType::List(ItemType::Int(BOUND)),
            ),
            (
                PythonValue::Map(PythonItem::Named("TaskManifest")),
                ValueType::Map(ItemType::Named("TaskManifest")),
            ),
            (
                PythonValue::Optional(PythonItem::Str),
                ValueType::Optional(ItemType::Str),
            ),
            (PythonValue::TimeOfDay, ValueType::TimeOfDay),
        ];
        for (value, expected) in cases {
            assert_eq!(field_value_type(&field(value)), expected);
        }
    }

    #[test]
    fn each_manifest_item_has_its_item_type() {
        let cases = [
            (PythonItem::Bool, ItemType::Bool),
            (PythonItem::Str, ItemType::Str),
            (PythonItem::Int(BOUND), ItemType::Int(BOUND)),
            (PythonItem::Named("Weekday"), ItemType::Named("Weekday")),
            (PythonItem::TimeOfDay, ItemType::TimeOfDay),
        ];
        for (item, expected) in cases {
            assert_eq!(item_type(item), expected);
        }
    }

    #[test]
    fn manifest_types_and_variants_have_their_type_names() {
        let variant = TaggedVariant {
            python_class: "CustomQueues",
            tag_value: "CUSTOM",
            doc: "",
            fields: VariantFields::Own(&[]),
        };
        assert_eq!(tagged_variant_type_name(&variant), "CustomQueues");
        let manifest_type = horsies_core::manifest_schema::PRODUCER_MANIFEST
            .type_named("ProducerManifest")
            .expect("the root is a type of its module");
        assert_eq!(manifest_type_name(manifest_type), "ProducerManifest");
    }

    #[test]
    fn code_unions_and_event_variants_have_their_names() {
        assert_eq!(
            code_union_alias_name(&BUILT_IN_TASK_CODES),
            "BuiltInTaskCode"
        );
        let variant = VariantType {
            tag: "BuiltIn",
            python_class: "AttemptErrorCodeBuiltIn",
            doc: "",
            fields: &[],
        };
        assert_eq!(event_variant_type_name(&variant), "AttemptErrorCodeBuiltIn");
    }
}
