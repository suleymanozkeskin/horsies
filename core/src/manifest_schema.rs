//! The manifest types as static data for clients.
//!
//! `#[derive(horsies_core_derive::ManifestSchema)]` on a manifest struct or enum
//! records its schema from the type itself:
//! - a struct of named fields is a record;
//! - an enum with a serde `tag` is one record per variant, with a tag field,
//!   and a union of the variants;
//! - an enum of unit variants is a set of its wire values.
//!
//! A field's type comes from [`ManifestValue`] of its Rust type. A
//! newtype with a bound implements it by hand next to its parser, so the
//! bound has one definition. A `Vec` is a list and a `BTreeMap`
//! with `String` keys a map of its values; their items are not
//! containers. `horsies-codegen` renders [`PRODUCER_MANIFEST`],
//! [`WORKER_MANIFEST`], [`SCHEDULER_MANIFEST`] and [`WORKFLOW_SPEC_DATA`]
//! for each client.
//!
//! [`SCHEDULE_PATTERN`] is the schema of the `pattern_json` text of the
//! scheduler manifest. The Python package keeps its own pattern models, so
//! the Python renderer does not render it. Its types use forms that no
//! rendered manifest uses: a variant that holds the fields of a record type
//! ([`VariantFields::OfRecord`]), a generic enum recorded once per use, a
//! field with a serde default ([`MemberDefault`]), a time of day
//! ([`ValueType::TimeOfDay`]), a length of time in whole seconds
//! ([`ValueType::Duration`]), and an integer field whose bound the type
//! that holds it names ([`bounded_int`]).

use std::collections::BTreeMap;

use chrono::NaiveTime;

use crate::core::config::schedule::{
    CronEnumTerm, CronNumericTerm, CronSchedule, DailySchedule, DaySelector, HourlySchedule,
    IntervalSchedule, Month, MonthlySchedule, SchedulePattern, Weekday, WeeklySchedule,
};
use crate::core::config::socket::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
use crate::core::config::worker_profile::{ParentCpu, ProfileRequest};
use crate::core::task::Execution;
use crate::events::EventTypeName;
use crate::manifest::{
    DatabaseManifest, PoolRecycle, ProducerManifest, SchemaMode, SendManifest, SessionDatabase,
    PRODUCER_MANIFEST_VERSION,
};
use crate::scheduler_manifest::{
    ScheduleActivation, ScheduleManifest, ScheduleSendManifest, ScheduleTaskOptions,
    SchedulerManifest, SCHEDULER_MANIFEST_VERSION,
};
use crate::worker_manifest::{
    ChildCommandManifest, ChildLaunchManifest, ChildProgram, ClaimLease, ClaimLimitsManifest,
    ClusterWideCap, CustomQueueManifest, DefaultExecutor, EventHandlers, HandlerManifest, Isolated,
    IsolatedPoolManifest, IsolatedRecycleManifest, MemoryLimit, MetricsEndpoint,
    ParentPoolManifest, PausedWorkflowExpiry, PayloadLimit, PayloadManifest, PoolMaxSize, Preload,
    PreloadLaunch, QueueConcurrency, QueueRetention, Queues, RecoveryManifest, ResilienceManifest,
    RetentionManifest, RetryAttemptLimit, SharedAsync, SharedPoolManifest, SharedRecycleManifest,
    SharedThread, TaskLimit, TaskManifest, TerminalRecordRetention, WorkerClaimManifest,
    WorkerDatabaseManifest, WorkerEndedSpans, WorkerLogLevel, WorkerManifest,
    WorkerProfileManifest, WorkerStateRetention, WORKER_MANIFEST_VERSION,
};
use crate::workflow_engine::spec_data::{
    SpecDefinitionKey, SpecJoin, SpecNode, SpecNodeBody, SpecOnError, SpecOutput,
    SpecSubWorkflowParams, SpecSuccessCase, SpecSuccessPolicy, SpecTaskOptions, WorkflowSpecData,
    WORKFLOW_SPEC_DATA_VERSION,
};

/// An inclusive integer range with the name of its constants
/// (`<name>_MIN` and `<name>_MAX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntBound {
    pub name: &'static str,
    pub min: u64,
    pub max: u64,
}

/// The type of a manifest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    Str,
    Int(IntBound),
    /// A manifest type of the same module, by its name.
    Named(&'static str),
    /// A list of items of one type (a Rust `Vec`).
    List(ItemType),
    /// A map from text keys to values of one type (a Rust `BTreeMap`
    /// with `String` keys).
    Map(ItemType),
    /// An item or no value (a Rust `Option`). The writer writes `null` for
    /// no value; the reader takes `null` and a missing member as no value.
    Optional(ItemType),
    /// A time of day in whole seconds, as the text `HH:MM:SS` (a Rust
    /// `NaiveTime`). The reader of each type that
    /// holds one refuses a fraction of a second and a UTC offset.
    TimeOfDay,
    /// A length of time in whole seconds inside the bound, as ISO 8601
    /// duration text with days, hours, minutes and seconds only (`PT30S`,
    /// `P1DT2H`; a Rust `std::time::Duration`). The writer puts the largest unit first and
    /// writes no zero part; the reader refuses years, months, weeks, a
    /// fraction and a sign.
    Duration(IntBound),
}

/// The type of an item of a list or a value of a map: a value that is not
/// a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemType {
    Bool,
    Str,
    Int(IntBound),
    Named(&'static str),
    TimeOfDay,
    Duration(IntBound),
}

impl ItemType {
    /// The item form of `value`. A container of containers is not a
    /// manifest form, and an optional value is a container here: the build
    /// fails where a manifest field has one.
    pub const fn of(value: ValueType) -> ItemType {
        match value {
            ValueType::Bool => ItemType::Bool,
            ValueType::Str => ItemType::Str,
            ValueType::Int(bound) => ItemType::Int(bound),
            ValueType::Named(name) => ItemType::Named(name),
            ValueType::TimeOfDay => ItemType::TimeOfDay,
            ValueType::Duration(bound) => ItemType::Duration(bound),
            ValueType::List(_) | ValueType::Map(_) | ValueType::Optional(_) => {
                panic!("a manifest container holds no container")
            }
        }
    }

    /// The value form of this item.
    pub const fn value(self) -> ValueType {
        match self {
            ItemType::Bool => ValueType::Bool,
            ItemType::Str => ValueType::Str,
            ItemType::Int(bound) => ValueType::Int(bound),
            ItemType::Named(name) => ValueType::Named(name),
            ItemType::TimeOfDay => ValueType::TimeOfDay,
            ItemType::Duration(bound) => ValueType::Duration(bound),
        }
    }
}

/// A Rust type that a manifest field can have.
pub trait ManifestValue {
    const VALUE_TYPE: ValueType;
}

impl ManifestValue for bool {
    const VALUE_TYPE: ValueType = ValueType::Bool;
}

impl ManifestValue for String {
    const VALUE_TYPE: ValueType = ValueType::Str;
}

impl<T: ManifestValue> ManifestValue for Vec<T> {
    const VALUE_TYPE: ValueType = ValueType::List(ItemType::of(T::VALUE_TYPE));
}

impl<T: ManifestValue> ManifestValue for BTreeMap<String, T> {
    const VALUE_TYPE: ValueType = ValueType::Map(ItemType::of(T::VALUE_TYPE));
}

impl<T: ManifestValue> ManifestValue for Option<T> {
    const VALUE_TYPE: ValueType = ValueType::Optional(ItemType::of(T::VALUE_TYPE));
}

impl ManifestValue for NaiveTime {
    const VALUE_TYPE: ValueType = ValueType::TimeOfDay;
}

/// How an integer field holds its integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntFieldForm {
    /// One integer.
    One,
    /// One integer or no value.
    Optional,
    /// A list of integers.
    List,
    /// A length of time in whole seconds (a `std::time::Duration`): the
    /// bound holds its seconds.
    WholeSeconds,
}

/// The Rust type of an integer field, or of a length of time in whole
/// seconds, whose bound the type that holds the field names
/// (`#[manifest_schema(int_bound = <IntBound constant>)]`): the bound is a
/// rule of that type, not of the field type.
pub trait IntField {
    const FORM: IntFieldForm;
}

impl IntField for u32 {
    const FORM: IntFieldForm = IntFieldForm::One;
}

impl IntField for i64 {
    const FORM: IntFieldForm = IntFieldForm::One;
}

impl IntField for Option<u32> {
    const FORM: IntFieldForm = IntFieldForm::Optional;
}

impl IntField for Vec<i64> {
    const FORM: IntFieldForm = IntFieldForm::List;
}

impl IntField for std::time::Duration {
    const FORM: IntFieldForm = IntFieldForm::WholeSeconds;
}

/// The value of a field of the Rust type `F` inside `bound`. Pure.
pub const fn bounded_int<F: IntField>(bound: IntBound) -> ValueType {
    match F::FORM {
        IntFieldForm::One => ValueType::Int(bound),
        IntFieldForm::Optional => ValueType::Optional(ItemType::Int(bound)),
        IntFieldForm::List => ValueType::List(ItemType::Int(bound)),
        IntFieldForm::WholeSeconds => ValueType::Duration(bound),
    }
}

/// What the reader takes when a text does not hold a member (serde's
/// `default = "<const fn>"`). The writer writes every member, this one
/// too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDefault {
    /// The field has no serde default.
    NoDefault,
    /// The reader takes this whole number, which is inside the field's
    /// bound.
    Int(u64),
}

impl MemberDefault {
    /// The default `default` of a field whose value is `field`. A default is
    /// modeled on an integer field only, inside the field's bound: any other
    /// default is a defect of the type, and the build stops here. Pure.
    pub const fn int(field: ValueType, default: u64) -> MemberDefault {
        match field {
            ValueType::Int(bound) => {
                assert!(
                    default >= bound.min,
                    "a field default is below the field's bound"
                );
                assert!(
                    default <= bound.max,
                    "a field default is above the field's bound"
                );
                MemberDefault::Int(default)
            }
            ValueType::Bool
            | ValueType::Str
            | ValueType::Named(_)
            | ValueType::List(_)
            | ValueType::Map(_)
            | ValueType::Optional(_)
            | ValueType::TimeOfDay
            | ValueType::Duration(_) => {
                panic!("a field default is modeled on an integer field only")
            }
        }
    }
}

/// One field of a dataclass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestField {
    /// The field name; the JSON key is the same text.
    pub name: &'static str,
    /// The field doc, from the field's `///` lines.
    pub doc: &'static str,
    pub value: ValueType,
    pub default: MemberDefault,
}

/// The members of a tagged variant after its tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantFields {
    /// The variant's own named fields; none for a unit variant.
    Own(&'static [ManifestField]),
    /// The fields of the record type of this name, in its order: a Rust
    /// newtype variant whose one field is that struct. The module lists the
    /// record type.
    OfRecord(&'static str),
}

/// The name of `item`, a record type, for a tagged variant that holds its
/// fields. Any other shape is a defect of the variant, and the build stops
/// here. Pure.
pub const fn record_name(item: ManifestType) -> &'static str {
    match item.shape {
        ManifestShape::Record { .. } => item.type_name,
        ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
            panic!("a tagged variant holds the fields of a record type only")
        }
    }
}

/// One variant of a tagged enum: a record whose tag field holds
/// `tag_value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaggedVariant {
    pub type_name: &'static str,
    pub tag_value: &'static str,
    pub doc: &'static str,
    pub fields: VariantFields,
}

/// One wire value of an enum of unit variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChoiceValue {
    pub value: &'static str,
    pub doc: &'static str,
}

/// The shape of a manifest type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestShape {
    /// A record of named fields.
    Record { fields: &'static [ManifestField] },
    /// One record per variant and a union of them.
    Tagged {
        tag_field: &'static str,
        variants: &'static [TaggedVariant],
    },
    /// A set of wire values.
    Choice { values: &'static [ChoiceValue] },
}

/// One manifest type that has its own definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestType {
    /// The name of the type: the record, or the union or set of a tagged or
    /// unit enum.
    pub type_name: &'static str,
    /// The type doc, from the type's `///` lines.
    pub doc: &'static str,
    pub shape: ManifestShape,
}

/// A manifest type that the derive recorded.
pub trait ManifestItem: ManifestValue {
    const ITEM: ManifestType;
}

/// The manifest of one process kind, as the generator renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestModule {
    /// The name of the root type, which the dump function takes.
    pub root: &'static str,
    /// The manifest version this core reads.
    pub version: u32,
    /// What reads the manifest text, as the module doc names it.
    pub reader: &'static str,
    /// The same, as the doc of the dump function names it.
    pub reader_short: &'static str,
    /// Every type of the manifest, in render order: a type comes after the
    /// types its fields name.
    pub types: &'static [ManifestType],
}

/// The producer manifest: the input of a `CoreHandle`.
pub const PRODUCER_MANIFEST: ManifestModule = ManifestModule {
    root: "ProducerManifest",
    version: PRODUCER_MANIFEST_VERSION,
    reader: "the producer handle of the core",
    reader_short: "the producer handle",
    types: &[
        <SchemaMode as ManifestItem>::ITEM,
        <SessionDatabase as ManifestItem>::ITEM,
        <PoolRecycle as ManifestItem>::ITEM,
        <TcpKeepalivesConfig as ManifestItem>::ITEM,
        <TcpUserTimeoutConfig as ManifestItem>::ITEM,
        <DatabaseManifest as ManifestItem>::ITEM,
        <SendManifest as ManifestItem>::ITEM,
        <ProducerManifest as ManifestItem>::ITEM,
    ],
};

/// The worker manifest: everything the core needs to run one worker of an
/// app.
pub const WORKER_MANIFEST: ManifestModule = ManifestModule {
    root: "WorkerManifest",
    version: WORKER_MANIFEST_VERSION,
    reader: "the worker of the core",
    reader_short: "the worker of the core",
    types: &[
        <SessionDatabase as ManifestItem>::ITEM,
        <PoolRecycle as ManifestItem>::ITEM,
        <TcpKeepalivesConfig as ManifestItem>::ITEM,
        <TcpUserTimeoutConfig as ManifestItem>::ITEM,
        <WorkerDatabaseManifest as ManifestItem>::ITEM,
        <SendManifest as ManifestItem>::ITEM,
        <TaskLimit as ManifestItem>::ITEM,
        <MemoryLimit as ManifestItem>::ITEM,
        <PoolMaxSize as ManifestItem>::ITEM,
        <DefaultExecutor as ManifestItem>::ITEM,
        <IsolatedRecycleManifest as ManifestItem>::ITEM,
        <SharedRecycleManifest as ManifestItem>::ITEM,
        <IsolatedPoolManifest as ManifestItem>::ITEM,
        <SharedPoolManifest as ManifestItem>::ITEM,
        <Isolated as ManifestItem>::ITEM,
        <SharedAsync as ManifestItem>::ITEM,
        <SharedThread as ManifestItem>::ITEM,
        <ParentPoolManifest as ManifestItem>::ITEM,
        <ClaimLimitsManifest as ManifestItem>::ITEM,
        <Preload as ManifestItem>::ITEM,
        <ParentCpu as ManifestItem>::ITEM,
        <WorkerProfileManifest as ManifestItem>::ITEM,
        <ProfileRequest as ManifestItem>::ITEM,
        <MetricsEndpoint as ManifestItem>::ITEM,
        <QueueConcurrency as ManifestItem>::ITEM,
        <CustomQueueManifest as ManifestItem>::ITEM,
        <Queues as ManifestItem>::ITEM,
        <ClaimLease as ManifestItem>::ITEM,
        <ClusterWideCap as ManifestItem>::ITEM,
        <WorkerClaimManifest as ManifestItem>::ITEM,
        <RecoveryManifest as ManifestItem>::ITEM,
        <WorkerStateRetention as ManifestItem>::ITEM,
        <TerminalRecordRetention as ManifestItem>::ITEM,
        <PausedWorkflowExpiry as ManifestItem>::ITEM,
        <QueueRetention as ManifestItem>::ITEM,
        <RetentionManifest as ManifestItem>::ITEM,
        <RetryAttemptLimit as ManifestItem>::ITEM,
        <ResilienceManifest as ManifestItem>::ITEM,
        <PayloadLimit as ManifestItem>::ITEM,
        <PayloadManifest as ManifestItem>::ITEM,
        <Execution as ManifestItem>::ITEM,
        <TaskManifest as ManifestItem>::ITEM,
        <EventTypeName as ManifestItem>::ITEM,
        <HandlerManifest as ManifestItem>::ITEM,
        <EventHandlers as ManifestItem>::ITEM,
        <WorkerEndedSpans as ManifestItem>::ITEM,
        <ChildCommandManifest as ManifestItem>::ITEM,
        <PreloadLaunch as ManifestItem>::ITEM,
        <ChildProgram as ManifestItem>::ITEM,
        <WorkerLogLevel as ManifestItem>::ITEM,
        <ChildLaunchManifest as ManifestItem>::ITEM,
        <WorkerManifest as ManifestItem>::ITEM,
    ],
};

/// The scheduler manifest: the schedules of one scheduler, each enabled one
/// with its send facts.
pub const SCHEDULER_MANIFEST: ManifestModule = ManifestModule {
    root: "SchedulerManifest",
    version: SCHEDULER_MANIFEST_VERSION,
    reader: "the scheduler of the core",
    reader_short: "the scheduler",
    types: &[
        <ScheduleTaskOptions as ManifestItem>::ITEM,
        <ScheduleSendManifest as ManifestItem>::ITEM,
        <ScheduleActivation as ManifestItem>::ITEM,
        <ScheduleManifest as ManifestItem>::ITEM,
        <SchedulerManifest as ManifestItem>::ITEM,
    ],
};

/// The schedule pattern: the text of `pattern_json` in the scheduler
/// manifest, in the form of Python's `pattern.model_dump(mode='json')`. It is
/// read inside the scheduler manifest, so its version is that manifest's.
/// The Python package keeps its own pattern models: the Python renderer does
/// not render this module.
pub const SCHEDULE_PATTERN: ManifestModule = ManifestModule {
    root: "SchedulePattern",
    version: SCHEDULER_MANIFEST_VERSION,
    reader: "the pattern parser of the scheduler manifest (`pattern_json`)",
    reader_short: "`pattern_json`",
    types: &[
        <Weekday as ManifestItem>::ITEM,
        <Month as ManifestItem>::ITEM,
        <IntervalSchedule as ManifestItem>::ITEM,
        <HourlySchedule as ManifestItem>::ITEM,
        <DailySchedule as ManifestItem>::ITEM,
        <WeeklySchedule as ManifestItem>::ITEM,
        <MonthlySchedule as ManifestItem>::ITEM,
        <CronNumericTerm as ManifestItem>::ITEM,
        <CronEnumTerm<Weekday> as ManifestItem>::ITEM,
        <CronEnumTerm<Month> as ManifestItem>::ITEM,
        <DaySelector as ManifestItem>::ITEM,
        <CronSchedule as ManifestItem>::ITEM,
        <SchedulePattern as ManifestItem>::ITEM,
    ],
};

/// A built workflow spec as data: what the Python client sends for a
/// workflow start and for the child of a sub-workflow node.
pub const WORKFLOW_SPEC_DATA: ManifestModule = ManifestModule {
    root: "WorkflowSpecData",
    version: WORKFLOW_SPEC_DATA_VERSION,
    reader: "the core's spec data parser (`parse_workflow_spec_data`)",
    reader_short: "`parse_workflow_spec_data`",
    types: &[
        <SpecDefinitionKey as ManifestItem>::ITEM,
        <SpecOnError as ManifestItem>::ITEM,
        <SpecOutput as ManifestItem>::ITEM,
        <SpecSuccessCase as ManifestItem>::ITEM,
        <SpecSuccessPolicy as ManifestItem>::ITEM,
        <SpecJoin as ManifestItem>::ITEM,
        <SpecTaskOptions as ManifestItem>::ITEM,
        <SpecSubWorkflowParams as ManifestItem>::ITEM,
        <SpecNodeBody as ManifestItem>::ITEM,
        <SpecNode as ManifestItem>::ITEM,
        <WorkflowSpecData as ManifestItem>::ITEM,
    ],
};

impl ManifestModule {
    /// The type of `type_name`, if the module has one.
    pub fn type_named(&self, type_name: &str) -> Option<&'static ManifestType> {
        self.types
            .iter()
            .find(|manifest_type| manifest_type.type_name == type_name)
    }

    /// Every integer bound of the module's fields, the items of their
    /// containers included, each name once, in the order of first use.
    pub fn int_bounds(&self) -> Vec<IntBound> {
        let mut bounds: Vec<IntBound> = Vec::new();
        for manifest_type in self.types {
            for field in type_fields(manifest_type) {
                let bound = match field.value {
                    ValueType::Int(bound)
                    | ValueType::Duration(bound)
                    | ValueType::List(ItemType::Int(bound) | ItemType::Duration(bound))
                    | ValueType::Map(ItemType::Int(bound) | ItemType::Duration(bound))
                    | ValueType::Optional(ItemType::Int(bound) | ItemType::Duration(bound)) => {
                        bound
                    }
                    ValueType::Bool
                    | ValueType::Str
                    | ValueType::Named(_)
                    | ValueType::TimeOfDay
                    | ValueType::List(
                        ItemType::Bool | ItemType::Str | ItemType::Named(_) | ItemType::TimeOfDay,
                    )
                    | ValueType::Map(
                        ItemType::Bool | ItemType::Str | ItemType::Named(_) | ItemType::TimeOfDay,
                    )
                    | ValueType::Optional(
                        ItemType::Bool | ItemType::Str | ItemType::Named(_) | ItemType::TimeOfDay,
                    ) => continue,
                };
                if !bounds.iter().any(|known| known.name == bound.name) {
                    bounds.push(bound);
                }
            }
        }
        bounds
    }
}

/// Every field that a type defines, across its variants. A variant that
/// holds the fields of a record type adds none: that record defines them.
pub fn type_fields(manifest_type: &ManifestType) -> Vec<ManifestField> {
    match manifest_type.shape {
        ManifestShape::Record { fields } => fields.to_vec(),
        ManifestShape::Tagged { variants, .. } => variants
            .iter()
            .flat_map(|variant| own_fields(variant.fields).iter().copied())
            .collect(),
        ManifestShape::Choice { .. } => Vec::new(),
    }
}

/// The fields that a variant defines itself: none for a variant that holds
/// the fields of a record type.
fn own_fields(fields: VariantFields) -> &'static [ManifestField] {
    match fields {
        VariantFields::Own(fields) => fields,
        VariantFields::OfRecord(_) => &[],
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;
    use crate::core::config::schedule::{
        parse_interval_text, CRON_ENUM_STEP, CRON_NUMERIC_STEP, CRON_NUMERIC_VALUE, HOURLY_MINUTE,
        HOURLY_SECOND, INTERVAL_EVERY_SECONDS, MONTHLY_DAY,
    };
    use crate::manifest::{parse_producer_manifest, ManifestRefused};
    use crate::scheduler_manifest::parse_scheduler_manifest;
    use crate::worker_manifest::parse_worker_manifest;

    /// Most nesting levels of a sample manifest. The worker manifest has 6.
    const MAX_SAMPLE_DEPTH: usize = 12;

    /// Which end of each integer range a sample takes.
    #[derive(Clone, Copy)]
    enum IntEnd {
        Min,
        Max,
    }

    /// A module and the parser of its root type.
    struct Parsed {
        module: ManifestModule,
        parse: fn(&str) -> Result<(), ManifestRefused>,
    }

    fn parsed_modules() -> [Parsed; 2] {
        [
            Parsed {
                module: PRODUCER_MANIFEST,
                parse: |text| parse_producer_manifest(text).map(|_| ()),
            },
            Parsed {
                module: WORKER_MANIFEST,
                parse: |text| parse_worker_manifest(text).map(|_| ()),
            },
        ]
    }

    /// The choices of one sample: every tagged enum and every choice takes
    /// its variant at `pick` modulo its variant count.
    #[derive(Clone, Copy)]
    struct Sampling {
        module: ManifestModule,
        pick: usize,
        end: IntEnd,
    }

    /// A text that every text field of a manifest takes: a database URL is
    /// not checked by the parser, and a declared error code must be
    /// `UPPER_SNAKE_CASE` and not a built-in code.
    const SAMPLE_TEXT: &str = "SAMPLE_TEXT";

    /// A time of day that every time field of a manifest takes.
    const SAMPLE_TIME_OF_DAY: &str = "09:30:00";

    /// A sample value of `value`.
    fn sample(sampling: Sampling, value: ValueType, depth: usize) -> Value {
        assert!(depth <= MAX_SAMPLE_DEPTH, "the manifest nests too deep");
        match value {
            ValueType::Bool => Value::Bool(sampling.pick % 2 == 1),
            ValueType::Str => Value::String(SAMPLE_TEXT.to_owned()),
            ValueType::Int(bound) => match sampling.end {
                IntEnd::Min => Value::from(bound.min),
                IntEnd::Max => Value::from(bound.max),
            },
            ValueType::Named(name) => {
                let manifest_type = sampling
                    .module
                    .type_named(name)
                    .unwrap_or_else(|| panic!("{name} is not a manifest type"));
                sample_type(sampling, manifest_type, depth + 1)
            }
            ValueType::List(item) => Value::Array(vec![sample(sampling, item.value(), depth + 1)]),
            ValueType::Map(item) => {
                let mut object = Map::new();
                object.insert(
                    "sample".to_owned(),
                    sample(sampling, item.value(), depth + 1),
                );
                Value::Object(object)
            }
            ValueType::Optional(item) => sample(sampling, item.value(), depth + 1),
            ValueType::TimeOfDay => Value::String(SAMPLE_TIME_OF_DAY.to_owned()),
            ValueType::Duration(bound) => match sampling.end {
                IntEnd::Min => Value::String(format!("PT{}S", bound.min)),
                IntEnd::Max => Value::String(format!("PT{}S", bound.max)),
            },
        }
    }

    fn sample_type(sampling: Sampling, manifest_type: &ManifestType, depth: usize) -> Value {
        match manifest_type.shape {
            ManifestShape::Record { fields } => {
                Value::Object(sample_fields(sampling, fields, depth))
            }
            ManifestShape::Tagged {
                tag_field,
                variants,
            } => {
                let variant = variants[sampling.pick % variants.len()];
                let mut object = match variant.fields {
                    VariantFields::Own(fields) => sample_fields(sampling, fields, depth),
                    VariantFields::OfRecord(name) => record_sample(sampling, name, depth + 1),
                };
                object.insert(
                    tag_field.to_owned(),
                    Value::String(variant.tag_value.to_owned()),
                );
                Value::Object(object)
            }
            ManifestShape::Choice { values } => {
                Value::String(values[sampling.pick % values.len()].value.to_owned())
            }
        }
    }

    /// The members of a sample of the record type `name`.
    fn record_sample(sampling: Sampling, name: &str, depth: usize) -> Map<String, Value> {
        let record = sampling
            .module
            .type_named(name)
            .unwrap_or_else(|| panic!("{name} is not a manifest type"));
        match record.shape {
            ManifestShape::Record { fields } => sample_fields(sampling, fields, depth),
            ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                panic!("{name} is not a record type")
            }
        }
    }

    fn sample_fields(
        sampling: Sampling,
        fields: &[ManifestField],
        depth: usize,
    ) -> Map<String, Value> {
        fields
            .iter()
            .map(|field| (field.name.to_owned(), sample(sampling, field.value, depth)))
            .collect()
    }

    /// The most variants of any tagged enum or choice of the module.
    fn most_variants(module: &ManifestModule) -> usize {
        module
            .types
            .iter()
            .map(|manifest_type| match manifest_type.shape {
                ManifestShape::Tagged { variants, .. } => variants.len(),
                ManifestShape::Choice { values } => values.len(),
                ManifestShape::Record { .. } => 1,
            })
            .max()
            .unwrap_or(1)
    }

    fn root_sample(sampling: Sampling) -> Value {
        sample(sampling, ValueType::Named(sampling.module.root), 0)
    }

    #[test]
    fn a_manifest_built_from_the_data_parses_for_every_variant_and_bound_end() {
        for parsed in parsed_modules() {
            for pick in 0..most_variants(&parsed.module) {
                for end in [IntEnd::Min, IntEnd::Max] {
                    let sampling = Sampling {
                        module: parsed.module,
                        pick,
                        end,
                    };
                    let text = root_sample(sampling).to_string();
                    let outcome = (parsed.parse)(&text);
                    assert!(outcome.is_ok(), "{outcome:?}\n{text}");
                }
            }
        }
    }

    #[test]
    fn a_value_past_a_bound_is_refused() {
        for parsed in parsed_modules() {
            let sample_text = root_sample(Sampling {
                module: parsed.module,
                pick: 0,
                end: IntEnd::Min,
            });
            for manifest_type in parsed.module.types {
                let ManifestShape::Record { fields } = manifest_type.shape else {
                    continue;
                };
                for field in fields {
                    let ValueType::Int(bound) = field.value else {
                        continue;
                    };
                    let outside = [
                        bound.min.checked_sub(1),
                        Some(bound.max + 1).filter(|_| bound.max < u64::from(u32::MAX)),
                    ];
                    for value in outside.into_iter().flatten() {
                        let mut document = sample_text.clone();
                        let changed = replace_field(&mut document, field.name, &Value::from(value));
                        assert!(changed, "{} is in the sample", field.name);
                        assert!(
                            (parsed.parse)(&document.to_string()).is_err(),
                            "{}.{} = {value} is outside {bound:?}",
                            parsed.module.root,
                            field.name
                        );
                    }
                }
            }
        }
    }

    /// Set every field called `name` in `document` to `value`. Bounded by
    /// the sample depth.
    fn replace_field(document: &mut Value, name: &str, value: &Value) -> bool {
        fn walk(node: &mut Value, name: &str, value: &Value, depth: usize) -> bool {
            assert!(depth <= MAX_SAMPLE_DEPTH, "the manifest nests too deep");
            match node {
                Value::Object(object) => {
                    let mut changed = false;
                    for (key, child) in object.iter_mut() {
                        if key == name {
                            *child = value.clone();
                            changed = true;
                        } else {
                            changed |= walk(child, name, value, depth + 1);
                        }
                    }
                    changed
                }
                Value::Array(items) => {
                    let mut changed = false;
                    for item in items.iter_mut() {
                        changed |= walk(item, name, value, depth + 1);
                    }
                    changed
                }
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
            }
        }
        walk(document, name, value, 0)
    }

    #[test]
    fn every_named_type_is_in_the_module_once() {
        for module in [
            PRODUCER_MANIFEST,
            WORKER_MANIFEST,
            SCHEDULER_MANIFEST,
            SCHEDULE_PATTERN,
        ] {
            for manifest_type in module.types {
                let count = module
                    .types
                    .iter()
                    .filter(|other| other.type_name == manifest_type.type_name)
                    .count();
                assert_eq!(count, 1, "{}", manifest_type.type_name);
                for field in type_fields(manifest_type) {
                    let named = match field.value {
                        ValueType::Named(name)
                        | ValueType::List(ItemType::Named(name))
                        | ValueType::Map(ItemType::Named(name))
                        | ValueType::Optional(ItemType::Named(name)) => name,
                        ValueType::Bool
                        | ValueType::Str
                        | ValueType::Int(_)
                        | ValueType::TimeOfDay
                        | ValueType::Duration(_)
                        | ValueType::List(
                            ItemType::Bool
                            | ItemType::Str
                            | ItemType::Int(_)
                            | ItemType::TimeOfDay
                            | ItemType::Duration(_),
                        )
                        | ValueType::Map(
                            ItemType::Bool
                            | ItemType::Str
                            | ItemType::Int(_)
                            | ItemType::TimeOfDay
                            | ItemType::Duration(_),
                        )
                        | ValueType::Optional(
                            ItemType::Bool
                            | ItemType::Str
                            | ItemType::Int(_)
                            | ItemType::TimeOfDay
                            | ItemType::Duration(_),
                        ) => continue,
                    };
                    assert!(module.type_named(named).is_some(), "{named}");
                }
                for record in variant_records(manifest_type) {
                    let shape = module.type_named(record).map(|found| found.shape);
                    assert!(
                        matches!(shape, Some(ManifestShape::Record { .. })),
                        "{record} is not a record type of the module"
                    );
                }
            }
            assert!(module.type_named(module.root).is_some());
        }
    }

    /// The record types whose fields the variants of `manifest_type` hold.
    fn variant_records(manifest_type: &ManifestType) -> Vec<&'static str> {
        match manifest_type.shape {
            ManifestShape::Tagged { variants, .. } => variants
                .iter()
                .filter_map(|variant| match variant.fields {
                    VariantFields::OfRecord(name) => Some(name),
                    VariantFields::Own(_) => None,
                })
                .collect(),
            ManifestShape::Record { .. } | ManifestShape::Choice { .. } => Vec::new(),
        }
    }

    #[test]
    fn the_session_database_variants_carry_their_tags() {
        let manifest_type = PRODUCER_MANIFEST
            .type_named("SessionDatabase")
            .expect("the session database is a manifest type");
        let ManifestShape::Tagged {
            tag_field,
            variants,
        } = manifest_type.shape
        else {
            panic!("SessionDatabase is a tagged enum");
        };
        assert_eq!(tag_field, "kind");
        let tags: Vec<(&str, &str)> = variants
            .iter()
            .map(|variant| (variant.type_name, variant.tag_value))
            .collect();
        assert_eq!(
            tags,
            [
                ("SessionDatabaseSameAsDatabaseUrl", "SAME_AS_DATABASE_URL"),
                ("SessionDatabaseSeparate", "SEPARATE"),
            ]
        );
    }

    #[test]
    fn integer_bounds_are_named_once() {
        let names: Vec<&str> = PRODUCER_MANIFEST
            .int_bounds()
            .iter()
            .map(|bound| bound.name)
            .collect();
        assert_eq!(
            names,
            [
                "WHOLE_SECONDS",
                "KEEPALIVE_SECONDS",
                "KEEPALIVE_PROBES",
                "TCP_USER_TIMEOUT_MS",
                "POOL_SIZE",
                "POOL_CONNECTIONS",
                "RESERVATION_WINDOW_MICROS",
                "MANIFEST_VERSION"
            ]
        );
    }

    #[test]
    fn the_worker_manifest_names_each_bound_once() {
        let names: Vec<&str> = WORKER_MANIFEST
            .int_bounds()
            .iter()
            .map(|bound| bound.name)
            .collect();
        assert_eq!(
            names,
            [
                "WHOLE_SECONDS",
                "KEEPALIVE_SECONDS",
                "KEEPALIVE_PROBES",
                "TCP_USER_TIMEOUT_MS",
                "RESERVATION_WINDOW_MICROS",
                "TASK_LIMIT",
                "MEMORY_LIMIT_MB",
                "CHILD_POOL_CONNECTIONS",
                "DEFAULT_EXECUTOR_THREADS",
                "CHILD_POOL_MIN_SIZE",
                "ISOLATED_CHILDREN",
                "SHARED_ASYNC_CHILDREN",
                "SHARED_ASYNC_SEATS",
                "SHARED_THREAD_CHILDREN",
                "SHARED_THREAD_SEATS",
                "DRAIN_TIMEOUT_MS",
                "LISTEN_PORT",
                "QUEUE_CONCURRENCY",
                "QUEUE_PRIORITY",
                "MILLISECONDS",
                "CLAIM_COUNT",
                "CLAIMED_STALE_THRESHOLD_MS",
                "STALE_THRESHOLD_MS",
                "RECOVERY_GRACE_MS",
                "RECOVERY_PASS_LIMIT",
                "CHECK_INTERVAL_MS",
                "AUDIT_INTERVAL_MS",
                "HEARTBEAT_INTERVAL_MS",
                "SNAPSHOT_INTERVAL_MS",
                "WORKER_STATE_RETENTION_HOURS",
                "TERMINAL_RECORD_RETENTION_HOURS",
                "PAUSED_AGE_MICROS",
                "RETENTION_DAYS",
                "HISTORY_LEAF_HORIZON_DAYS",
                "HEARTBEAT_LEAF_HORIZON_HOURS",
                "MAINTENANCE_INTERVAL_SECONDS",
                "SWEEP_INTERVAL_SECONDS",
                "DELETE_BATCH_SIZE",
                "RETRY_ATTEMPTS",
                "RETRY_INITIAL_MS",
                "RETRY_MAX_MS",
                "POLL_INTERVAL_MS",
                "PAYLOAD_BYTES",
                "MANIFEST_VERSION",
            ]
        );
    }

    #[test]
    fn the_scheduler_manifest_names_each_bound_once() {
        let names: Vec<&str> = SCHEDULER_MANIFEST
            .int_bounds()
            .iter()
            .map(|bound| bound.name)
            .collect();
        assert_eq!(
            names,
            [
                "QUEUE_PRIORITY",
                "MAX_CATCH_UP_RUNS",
                "MANIFEST_VERSION",
                "CHECK_INTERVAL_SECONDS",
            ]
        );
    }

    #[test]
    fn a_container_item_keeps_its_type() {
        assert_eq!(
            <Vec<String> as ManifestValue>::VALUE_TYPE,
            ValueType::List(ItemType::Str)
        );
        assert_eq!(
            <BTreeMap<String, Execution> as ManifestValue>::VALUE_TYPE,
            ValueType::Map(ItemType::Named("Execution"))
        );
        assert_eq!(ItemType::of(ValueType::Bool).value(), ValueType::Bool);
        assert_eq!(
            ItemType::of(ValueType::TimeOfDay).value(),
            ValueType::TimeOfDay
        );
    }

    #[test]
    #[should_panic(expected = "a manifest container holds no container")]
    fn an_optional_value_is_not_an_item() {
        let _refused = ItemType::of(ValueType::Optional(ItemType::Str));
    }

    const SAMPLE_BOUND: IntBound = IntBound {
        name: "SAMPLE_BOUND",
        min: 1,
        max: 9,
    };

    #[test]
    fn an_integer_field_takes_the_bound_that_its_type_names() {
        assert_eq!(
            bounded_int::<u32>(SAMPLE_BOUND),
            ValueType::Int(SAMPLE_BOUND)
        );
        assert_eq!(
            bounded_int::<Option<u32>>(SAMPLE_BOUND),
            ValueType::Optional(ItemType::Int(SAMPLE_BOUND))
        );
        assert_eq!(
            bounded_int::<Vec<i64>>(SAMPLE_BOUND),
            ValueType::List(ItemType::Int(SAMPLE_BOUND))
        );
        assert_eq!(
            bounded_int::<std::time::Duration>(SAMPLE_BOUND),
            ValueType::Duration(SAMPLE_BOUND)
        );
        assert_eq!(
            ItemType::of(ValueType::Duration(SAMPLE_BOUND)).value(),
            ValueType::Duration(SAMPLE_BOUND)
        );
    }

    #[test]
    fn a_default_inside_the_bound_of_an_integer_field_is_its_default() {
        assert_eq!(
            MemberDefault::int(ValueType::Int(SAMPLE_BOUND), 9),
            MemberDefault::Int(9)
        );
    }

    #[test]
    #[should_panic(expected = "a field default is above the field's bound")]
    fn a_default_above_the_bound_is_refused() {
        let _refused = MemberDefault::int(ValueType::Int(SAMPLE_BOUND), 10);
    }

    #[test]
    #[should_panic(expected = "a field default is modeled on an integer field only")]
    fn a_default_of_a_field_that_is_not_an_integer_is_refused() {
        let _refused = MemberDefault::int(ValueType::Str, 1);
    }

    #[test]
    #[should_panic(expected = "a tagged variant holds the fields of a record type only")]
    fn a_variant_cannot_hold_the_fields_of_a_choice() {
        let _refused = record_name(<Weekday as ManifestItem>::ITEM);
    }

    /// The pattern type `name`.
    fn pattern_type(name: &str) -> &'static ManifestType {
        SCHEDULE_PATTERN
            .type_named(name)
            .unwrap_or_else(|| panic!("{name} is not a pattern type"))
    }

    /// The fields of the pattern record type `name`.
    fn record_fields(name: &str) -> &'static [ManifestField] {
        match pattern_type(name).shape {
            ManifestShape::Record { fields } => fields,
            ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                panic!("{name} is not a record type")
            }
        }
    }

    /// The variants of the tagged pattern type `name`, with its tag field.
    fn tagged_variants(name: &str) -> (&'static str, &'static [TaggedVariant]) {
        match pattern_type(name).shape {
            ManifestShape::Tagged {
                tag_field,
                variants,
            } => (tag_field, variants),
            ManifestShape::Record { .. } | ManifestShape::Choice { .. } => {
                panic!("{name} is not a tagged type")
            }
        }
    }

    /// The name and the value of each field.
    fn field_values(fields: &[ManifestField]) -> Vec<(&'static str, ValueType)> {
        fields
            .iter()
            .map(|field| (field.name, field.value))
            .collect()
    }

    /// The name and the serde default of each field.
    fn field_defaults(fields: &[ManifestField]) -> Vec<(&'static str, MemberDefault)> {
        fields
            .iter()
            .map(|field| (field.name, field.default))
            .collect()
    }

    #[test]
    fn each_pattern_kind_holds_the_fields_of_its_record() {
        let (tag_field, variants) = tagged_variants("SchedulePattern");
        assert_eq!(tag_field, "kind");
        let kinds: Vec<(&str, &str, VariantFields)> = variants
            .iter()
            .map(|variant| (variant.type_name, variant.tag_value, variant.fields))
            .collect();
        assert_eq!(
            kinds,
            [
                (
                    "SchedulePatternInterval",
                    "interval",
                    VariantFields::OfRecord("IntervalSchedule"),
                ),
                (
                    "SchedulePatternHourly",
                    "hourly",
                    VariantFields::OfRecord("HourlySchedule"),
                ),
                (
                    "SchedulePatternDaily",
                    "daily",
                    VariantFields::OfRecord("DailySchedule"),
                ),
                (
                    "SchedulePatternWeekly",
                    "weekly",
                    VariantFields::OfRecord("WeeklySchedule"),
                ),
                (
                    "SchedulePatternMonthly",
                    "monthly",
                    VariantFields::OfRecord("MonthlySchedule"),
                ),
                (
                    "SchedulePatternCron",
                    "cron",
                    VariantFields::OfRecord("CronSchedule"),
                ),
            ]
        );
    }

    #[test]
    fn a_generic_enum_is_one_type_per_use() {
        for (name, item) in [
            ("CronEnumTermWeekday", "Weekday"),
            ("CronEnumTermMonth", "Month"),
        ] {
            let (tag_field, variants) = tagged_variants(name);
            assert_eq!(tag_field, "kind");
            let forms: Vec<(&str, Vec<(&str, ValueType)>)> = variants
                .iter()
                .map(|variant| (variant.tag_value, field_values(own_fields(variant.fields))))
                .collect();
            assert_eq!(
                forms,
                [
                    ("every", Vec::new()),
                    (
                        "enum_values",
                        vec![("values", ValueType::List(ItemType::Named(item)))],
                    ),
                    (
                        "enum_range",
                        vec![
                            ("start", ValueType::Named(item)),
                            ("end", ValueType::Named(item)),
                            ("step", ValueType::Int(CRON_ENUM_STEP)),
                        ],
                    ),
                    ("enum_step", vec![("step", ValueType::Int(CRON_ENUM_STEP))]),
                ],
                "{name}"
            );
        }
    }

    #[test]
    fn the_numeric_cron_term_takes_the_bounds_of_every_numeric_field() {
        let (tag_field, variants) = tagged_variants("CronNumericTerm");
        assert_eq!(tag_field, "kind");
        let forms: Vec<(&str, Vec<(&str, ValueType)>)> = variants
            .iter()
            .map(|variant| (variant.tag_value, field_values(own_fields(variant.fields))))
            .collect();
        assert_eq!(
            forms,
            [
                ("every", Vec::new()),
                ("step", vec![("step", ValueType::Int(CRON_NUMERIC_STEP))]),
                (
                    "values",
                    vec![("values", ValueType::List(ItemType::Int(CRON_NUMERIC_VALUE)))],
                ),
                (
                    "range",
                    vec![
                        ("start", ValueType::Int(CRON_NUMERIC_VALUE)),
                        ("end", ValueType::Int(CRON_NUMERIC_VALUE)),
                        ("step", ValueType::Int(CRON_NUMERIC_STEP)),
                    ],
                ),
            ]
        );
        assert_eq!((CRON_NUMERIC_VALUE.min, CRON_NUMERIC_VALUE.max), (0, 59));
        assert_eq!((CRON_NUMERIC_STEP.min, CRON_NUMERIC_STEP.max), (1, 59));
        assert_eq!((CRON_ENUM_STEP.min, CRON_ENUM_STEP.max), (1, 11));
    }

    #[test]
    fn the_length_of_an_interval_is_a_duration_of_whole_seconds() {
        assert_eq!(
            field_values(record_fields("IntervalSchedule")),
            [("every", ValueType::Duration(INTERVAL_EVERY_SECONDS))]
        );
        assert_eq!(
            (INTERVAL_EVERY_SECONDS.min, INTERVAL_EVERY_SECONDS.max),
            (1, 31_536_000)
        );
        assert_eq!(
            SCHEDULE_PATTERN
                .int_bounds()
                .iter()
                .map(|bound| bound.name)
                .collect::<Vec<&str>>(),
            [
                "INTERVAL_EVERY_SECONDS",
                "HOURLY_MINUTE",
                "HOURLY_SECOND",
                "MONTHLY_DAY",
                "CRON_NUMERIC_STEP",
                "CRON_NUMERIC_VALUE",
                "CRON_ENUM_STEP",
            ]
        );
    }

    #[test]
    fn an_optional_field_is_an_item_or_no_value() {
        assert_eq!(
            <Option<String> as ManifestValue>::VALUE_TYPE,
            ValueType::Optional(ItemType::Str)
        );
    }

    #[test]
    fn a_field_with_a_serde_default_records_the_default() {
        assert_eq!(
            field_values(record_fields("HourlySchedule")),
            [
                ("minute", ValueType::Int(HOURLY_MINUTE)),
                ("second", ValueType::Int(HOURLY_SECOND)),
            ]
        );
        assert_eq!(
            field_defaults(record_fields("HourlySchedule")),
            [
                ("minute", MemberDefault::NoDefault),
                ("second", MemberDefault::Int(0)),
            ]
        );
        for name in [
            "CronNumericTerm",
            "CronEnumTermWeekday",
            "CronEnumTermMonth",
        ] {
            let (_tag_field, variants) = tagged_variants(name);
            let range = variants
                .iter()
                .find(|variant| variant.tag_value.ends_with("range"))
                .unwrap_or_else(|| panic!("{name} has a range"));
            assert_eq!(
                field_defaults(own_fields(range.fields)),
                [
                    ("start", MemberDefault::NoDefault),
                    ("end", MemberDefault::NoDefault),
                    ("step", MemberDefault::Int(1)),
                ],
                "{name}"
            );
        }
    }

    #[test]
    fn a_time_of_day_field_is_a_time_of_day() {
        assert_eq!(
            field_values(record_fields("DailySchedule")),
            [("time", ValueType::TimeOfDay)]
        );
        assert_eq!(
            field_values(record_fields("WeeklySchedule")),
            [
                ("weekdays", ValueType::List(ItemType::Named("Weekday"))),
                ("time", ValueType::TimeOfDay),
            ]
        );
        assert_eq!(
            field_values(record_fields("MonthlySchedule")),
            [
                ("day_of_month", ValueType::Int(MONTHLY_DAY)),
                ("time", ValueType::TimeOfDay),
            ]
        );
        assert_eq!(
            <NaiveTime as ManifestValue>::VALUE_TYPE,
            ValueType::TimeOfDay
        );
    }

    #[test]
    fn a_weekday_and_a_month_are_their_snake_case_names() {
        let choices = |name: &str| -> Vec<&'static str> {
            match pattern_type(name).shape {
                ManifestShape::Choice { values } => {
                    values.iter().map(|choice| choice.value).collect()
                }
                ManifestShape::Record { .. } | ManifestShape::Tagged { .. } => {
                    panic!("{name} is not a choice")
                }
            }
        };
        assert_eq!(
            choices("Weekday"),
            [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ]
        );
        assert_eq!(
            choices("Month"),
            [
                "january",
                "february",
                "march",
                "april",
                "may",
                "june",
                "july",
                "august",
                "september",
                "october",
                "november",
                "december",
            ]
        );
    }

    /// The shared fixture of the schedule pattern texts: one text per
    /// pattern kind, two more interval lengths, and one cron text per day
    /// selector, in the form of Python's `pattern.model_dump(mode='json')`:
    /// every member in the order of Python's model, the tag `kind` first, a
    /// time of day as `HH:MM:SS`, an interval length as ISO 8601 text with
    /// the largest unit first. Together the texts hold every variant of
    /// every tagged type of the pattern. The Python client (`crossclient/`)
    /// and the TypeScript client read the same file.
    const PATTERN_TEXTS_FIXTURE: &str =
        include_str!("../tests/fixtures/schedule_pattern/pattern_texts.json");

    /// The `text` of each row of [`PATTERN_TEXTS_FIXTURE`], in file order.
    fn python_pattern_texts() -> Vec<String> {
        let fixture: Value =
            serde_json::from_str(PATTERN_TEXTS_FIXTURE).expect("the pattern texts fixture is JSON");
        fixture["patterns"]
            .as_array()
            .expect("the fixture has a list of patterns")
            .iter()
            .map(|row| {
                row["text"]
                    .as_str()
                    .expect("a pattern row has a text")
                    .to_owned()
            })
            .collect()
    }

    /// A scheduler manifest of one enabled schedule whose pattern text is
    /// `pattern_json`.
    fn scheduler_manifest_with(pattern_json: &str) -> Value {
        serde_json::json!({
            "manifest_version": SCHEDULER_MANIFEST_VERSION,
            "enabled": true,
            "check_interval_seconds": 5,
            "schedules": [{
                "name": "pattern",
                "activation": {
                    "kind": "ENABLED",
                    "pattern_json": pattern_json,
                    "timezone": "UTC",
                    "catch_up_missed": false,
                    "max_catch_up_runs": 1,
                    "send": {
                        "task_name": "reports.nightly",
                        "queue": "default",
                        "priority": 100,
                        "input_json": "{}",
                        "task_options": {"kind": "NO_OPTIONS"},
                        "retention_class_key": "forever",
                        "retain_rerun_input": false
                    }
                }
            }]
        })
    }

    /// The members of `value`, a JSON object of the pattern type `name`.
    fn members_of<'a>(value: &'a Value, name: &str) -> &'a Map<String, Value> {
        value
            .as_object()
            .unwrap_or_else(|| panic!("{value} is not an object of {name}"))
    }

    /// Walk `value` with the schema form `form`, and add the class of each
    /// tagged variant that it holds to `seen`. Panics where the text and the
    /// schema differ: a member name, the member order, a tag value, or the
    /// kind of a value.
    fn walk_value(form: ValueType, value: &Value, seen: &mut Vec<&'static str>, depth: usize) {
        assert!(depth <= MAX_SAMPLE_DEPTH, "the pattern nests too deep");
        match (form, value) {
            (ValueType::Bool, Value::Bool(_))
            | (ValueType::Str, Value::String(_))
            | (ValueType::Optional(_), Value::Null) => {}
            (ValueType::Int(bound), Value::Number(number)) => {
                let whole = number
                    .as_u64()
                    .unwrap_or_else(|| panic!("{number} is not a whole number"));
                assert!(
                    (bound.min..=bound.max).contains(&whole),
                    "{whole} is outside {bound:?}"
                );
            }
            (ValueType::TimeOfDay, Value::String(text)) => {
                assert!(
                    NaiveTime::parse_from_str(text, "%H:%M:%S").is_ok(),
                    "{text} is not HH:MM:SS"
                );
            }
            (ValueType::Duration(bound), Value::String(text)) => {
                let every = parse_interval_text(text)
                    .unwrap_or_else(|refused| panic!("{text} is not a duration: {refused}"));
                assert_eq!(every.subsec_nanos(), 0, "{text} is not whole seconds");
                assert!(
                    (bound.min..=bound.max).contains(&every.as_secs()),
                    "{text} is outside {bound:?}"
                );
            }
            (ValueType::Optional(item), present) => {
                walk_value(item.value(), present, seen, depth + 1);
            }
            (ValueType::List(item), Value::Array(items)) => {
                for listed in items {
                    walk_value(item.value(), listed, seen, depth + 1);
                }
            }
            (ValueType::Named(name), present) => walk_type(name, present, seen, depth + 1),
            (form, other) => panic!("{other} is not a value of {form:?}"),
        }
    }

    /// Walk `value` with the pattern type `name`.
    fn walk_type(name: &str, value: &Value, seen: &mut Vec<&'static str>, depth: usize) {
        match pattern_type(name).shape {
            ManifestShape::Choice { values } => {
                let text = value
                    .as_str()
                    .unwrap_or_else(|| panic!("{value} is not a {name}"));
                assert!(
                    values.iter().any(|choice| choice.value == text),
                    "{text} is not a {name}"
                );
            }
            ManifestShape::Record { fields } => {
                walk_members(&[], fields, members_of(value, name), seen, depth);
            }
            ManifestShape::Tagged {
                tag_field,
                variants,
            } => {
                let members = members_of(value, name);
                let tag = members
                    .get(tag_field)
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("{value} has no {tag_field} of {name}"));
                let variant = variants
                    .iter()
                    .find(|variant| variant.tag_value == tag)
                    .unwrap_or_else(|| panic!("{tag} is not a variant of {name}"));
                seen.push(variant.type_name);
                let fields = match variant.fields {
                    VariantFields::Own(fields) => fields,
                    VariantFields::OfRecord(record) => record_fields(record),
                };
                walk_members(&[tag_field], fields, members, seen, depth);
            }
        }
    }

    /// Check that the member names of `members` are `leading`, then the
    /// names of `fields`, in this order, and walk each field's value.
    fn walk_members(
        leading: &[&str],
        fields: &[ManifestField],
        members: &Map<String, Value>,
        seen: &mut Vec<&'static str>,
        depth: usize,
    ) {
        let names: Vec<&str> = members.keys().map(String::as_str).collect();
        let expected: Vec<&str> = leading
            .iter()
            .copied()
            .chain(fields.iter().map(|field| field.name))
            .collect();
        assert_eq!(names, expected);
        for field in fields {
            walk_value(field.value, &members[field.name], seen, depth + 1);
        }
    }

    #[test]
    fn each_python_pattern_text_parses_and_has_the_members_of_the_schema() {
        let mut seen: Vec<&'static str> = Vec::new();
        for text in python_pattern_texts() {
            let manifest = scheduler_manifest_with(&text).to_string();
            let parsed = parse_scheduler_manifest(&manifest);
            assert!(parsed.is_ok(), "{parsed:?}\n{text}");
            let value: Value = serde_json::from_str(&text).expect("a pattern text is JSON");
            walk_value(
                ValueType::Named(SCHEDULE_PATTERN.root),
                &value,
                &mut seen,
                0,
            );
        }
        for manifest_type in SCHEDULE_PATTERN.types {
            let ManifestShape::Tagged { variants, .. } = manifest_type.shape else {
                continue;
            };
            for variant in variants {
                assert!(
                    seen.contains(&variant.type_name),
                    "no Python text holds {}",
                    variant.type_name
                );
            }
        }
    }
}
