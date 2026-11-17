//! The manifest types as static data for clients.
//!
//! `#[derive(horsies_macros::ManifestSchema)]` on a manifest struct or enum
//! records its Python form from the type itself:
//! - a struct of named fields is a frozen dataclass;
//! - an enum with a serde `tag` is one frozen dataclass per variant, with a
//!   `Literal` tag field, and a union alias of the variants;
//! - an enum of unit variants is a `Literal` alias of its wire values.
//!
//! A field's Python type comes from [`ManifestValue`] of its Rust type. A
//! newtype with a bound implements it by hand next to its parser, so the
//! bound has one definition. A `Vec` is a Python tuple and a `BTreeMap`
//! with `String` keys a Python `dict` of its values; their items are not
//! containers. `horsies-codegen` renders [`PRODUCER_MANIFEST`] as
//! `_catalog/manifest.py`, [`WORKER_MANIFEST`] as
//! `_catalog/worker_manifest.py` and [`SCHEDULER_MANIFEST`] as
//! `_catalog/scheduler_manifest.py`.

use std::collections::BTreeMap;

use crate::core::config::socket::{TcpKeepalivesConfig, TcpUserTimeoutConfig};
use crate::core::config::worker_profile::{ParentCpu, ProfileRequest};
use crate::core::task::Execution;
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
    ClusterWideCap, CustomQueueManifest, DefaultExecutor, Isolated, IsolatedPoolManifest,
    IsolatedRecycleManifest, MemoryLimit, ParentPoolManifest, PausedWorkflowExpiry, PayloadLimit,
    PayloadManifest, PoolMaxSize, Preload, PreloadLaunch, QueueConcurrency, QueueRetention, Queues,
    RecoveryManifest, ResilienceManifest, RetentionManifest, RetryAttemptLimit, SharedAsync,
    SharedPoolManifest, SharedRecycleManifest, SharedThread, TaskLimit, TerminalRecordRetention,
    WorkerClaimManifest, WorkerDatabaseManifest, WorkerLogLevel, WorkerManifest,
    WorkerProfileManifest, WorkerStateRetention, WORKER_MANIFEST_VERSION,
};

/// An inclusive integer range with the name of its Python constants
/// (`<name>_MIN` and `<name>_MAX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntBound {
    pub name: &'static str,
    pub min: u64,
    pub max: u64,
}

/// The Python type of a manifest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonValue {
    Bool,
    Str,
    Int(IntBound),
    /// A manifest type of the same module, by its Python name.
    Named(&'static str),
    /// A tuple of items of one type (a Rust `Vec`).
    List(PythonItem),
    /// A `dict` from text keys to values of one type (a Rust `BTreeMap`
    /// with `String` keys).
    Map(PythonItem),
}

/// The Python type of an item of a tuple or a value of a `dict`: a value
/// that is not a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonItem {
    Bool,
    Str,
    Int(IntBound),
    Named(&'static str),
}

impl PythonItem {
    /// The item form of `value`. A container of containers is not a
    /// manifest form: the build fails where a manifest field has one.
    pub const fn of(value: PythonValue) -> PythonItem {
        match value {
            PythonValue::Bool => PythonItem::Bool,
            PythonValue::Str => PythonItem::Str,
            PythonValue::Int(bound) => PythonItem::Int(bound),
            PythonValue::Named(name) => PythonItem::Named(name),
            PythonValue::List(_) | PythonValue::Map(_) => {
                panic!("a manifest container holds no container")
            }
        }
    }

    /// The value form of this item.
    pub const fn value(self) -> PythonValue {
        match self {
            PythonItem::Bool => PythonValue::Bool,
            PythonItem::Str => PythonValue::Str,
            PythonItem::Int(bound) => PythonValue::Int(bound),
            PythonItem::Named(name) => PythonValue::Named(name),
        }
    }
}

/// A Rust type that a manifest field can have.
pub trait ManifestValue {
    const PYTHON: PythonValue;
}

impl ManifestValue for bool {
    const PYTHON: PythonValue = PythonValue::Bool;
}

impl ManifestValue for String {
    const PYTHON: PythonValue = PythonValue::Str;
}

impl<T: ManifestValue> ManifestValue for Vec<T> {
    const PYTHON: PythonValue = PythonValue::List(PythonItem::of(T::PYTHON));
}

impl<T: ManifestValue> ManifestValue for BTreeMap<String, T> {
    const PYTHON: PythonValue = PythonValue::Map(PythonItem::of(T::PYTHON));
}

/// One field of a dataclass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestField {
    /// The field name; the JSON key is the same text.
    pub name: &'static str,
    /// The field doc, from the field's `///` lines.
    pub doc: &'static str,
    pub value: PythonValue,
}

/// One variant of a tagged enum: a dataclass whose tag field holds
/// `tag_value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaggedVariant {
    pub python_class: &'static str,
    pub tag_value: &'static str,
    pub doc: &'static str,
    pub fields: &'static [ManifestField],
}

/// One wire value of an enum of unit variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChoiceValue {
    pub value: &'static str,
    pub doc: &'static str,
}

/// How a manifest type is written in Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestShape {
    /// A frozen dataclass.
    Record { fields: &'static [ManifestField] },
    /// One frozen dataclass per variant and a union alias.
    Tagged {
        tag_field: &'static str,
        variants: &'static [TaggedVariant],
    },
    /// A `Literal` alias.
    Choice { values: &'static [ChoiceValue] },
}

/// One manifest type that has its own Python definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestType {
    /// The class name, or the alias name of a tagged or unit enum.
    pub python_name: &'static str,
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
    /// The Python name of the root type, which the dump function takes.
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
    reader: "`horsies._core.CoreHandle`",
    reader_short: "`CoreHandle`",
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
    reader: "`horsies._core.CoreScheduler`",
    reader_short: "`CoreScheduler`",
    types: &[
        <ScheduleTaskOptions as ManifestItem>::ITEM,
        <ScheduleSendManifest as ManifestItem>::ITEM,
        <ScheduleActivation as ManifestItem>::ITEM,
        <ScheduleManifest as ManifestItem>::ITEM,
        <SchedulerManifest as ManifestItem>::ITEM,
    ],
};

impl ManifestModule {
    /// The type of `python_name`, if the module has one.
    pub fn type_named(&self, python_name: &str) -> Option<&'static ManifestType> {
        self.types
            .iter()
            .find(|manifest_type| manifest_type.python_name == python_name)
    }

    /// Every integer bound of the module's fields, the items of their
    /// containers included, each name once, in the order of first use.
    pub fn int_bounds(&self) -> Vec<IntBound> {
        let mut bounds: Vec<IntBound> = Vec::new();
        for manifest_type in self.types {
            for field in type_fields(manifest_type) {
                let bound = match field.value {
                    PythonValue::Int(bound)
                    | PythonValue::List(PythonItem::Int(bound))
                    | PythonValue::Map(PythonItem::Int(bound)) => bound,
                    PythonValue::Bool
                    | PythonValue::Str
                    | PythonValue::Named(_)
                    | PythonValue::List(
                        PythonItem::Bool | PythonItem::Str | PythonItem::Named(_),
                    )
                    | PythonValue::Map(PythonItem::Bool | PythonItem::Str | PythonItem::Named(_)) => {
                        continue
                    }
                };
                if !bounds.iter().any(|known| known.name == bound.name) {
                    bounds.push(bound);
                }
            }
        }
        bounds
    }
}

/// Every field of a type, across its variants.
pub fn type_fields(manifest_type: &ManifestType) -> Vec<ManifestField> {
    match manifest_type.shape {
        ManifestShape::Record { fields } => fields.to_vec(),
        ManifestShape::Tagged { variants, .. } => variants
            .iter()
            .flat_map(|variant| variant.fields.iter().copied())
            .collect(),
        ManifestShape::Choice { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;
    use crate::manifest::{parse_producer_manifest, ManifestRefused};
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

    /// A sample value of `value`.
    fn sample(sampling: Sampling, value: PythonValue, depth: usize) -> Value {
        assert!(depth <= MAX_SAMPLE_DEPTH, "the manifest nests too deep");
        match value {
            PythonValue::Bool => Value::Bool(sampling.pick % 2 == 1),
            PythonValue::Str => Value::String("postgresql://user:secret@localhost/db".to_owned()),
            PythonValue::Int(bound) => match sampling.end {
                IntEnd::Min => Value::from(bound.min),
                IntEnd::Max => Value::from(bound.max),
            },
            PythonValue::Named(name) => {
                let manifest_type = sampling
                    .module
                    .type_named(name)
                    .unwrap_or_else(|| panic!("{name} is not a manifest type"));
                sample_type(sampling, manifest_type, depth + 1)
            }
            PythonValue::List(item) => {
                Value::Array(vec![sample(sampling, item.value(), depth + 1)])
            }
            PythonValue::Map(item) => {
                let mut object = Map::new();
                object.insert(
                    "sample".to_owned(),
                    sample(sampling, item.value(), depth + 1),
                );
                Value::Object(object)
            }
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
                let mut object = sample_fields(sampling, variant.fields, depth);
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
        sample(sampling, PythonValue::Named(sampling.module.root), 0)
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
                    let PythonValue::Int(bound) = field.value else {
                        continue;
                    };
                    let outside = [
                        bound.min.checked_sub(1),
                        Some(bound.max + 1).filter(|_| bound.max < u64::from(u32::MAX)),
                    ];
                    for value in outside.into_iter().flatten() {
                        let mut document = sample_text.clone();
                        let changed = replace_field(&mut document, field.name, Value::from(value));
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
    fn replace_field(document: &mut Value, name: &str, value: Value) -> bool {
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
        walk(document, name, &value, 0)
    }

    #[test]
    fn every_named_type_is_in_the_module_once() {
        for module in [PRODUCER_MANIFEST, WORKER_MANIFEST, SCHEDULER_MANIFEST] {
            for manifest_type in module.types {
                let count = module
                    .types
                    .iter()
                    .filter(|other| other.python_name == manifest_type.python_name)
                    .count();
                assert_eq!(count, 1, "{}", manifest_type.python_name);
                for field in type_fields(manifest_type) {
                    let named = match field.value {
                        PythonValue::Named(name)
                        | PythonValue::List(PythonItem::Named(name))
                        | PythonValue::Map(PythonItem::Named(name)) => name,
                        PythonValue::Bool
                        | PythonValue::Str
                        | PythonValue::Int(_)
                        | PythonValue::List(
                            PythonItem::Bool | PythonItem::Str | PythonItem::Int(_),
                        )
                        | PythonValue::Map(
                            PythonItem::Bool | PythonItem::Str | PythonItem::Int(_),
                        ) => continue,
                    };
                    assert!(module.type_named(named).is_some(), "{named}");
                }
            }
            assert!(module.type_named(module.root).is_some());
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
            .map(|variant| (variant.python_class, variant.tag_value))
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
    fn a_container_item_keeps_its_python_type() {
        assert_eq!(
            <Vec<String> as ManifestValue>::PYTHON,
            PythonValue::List(PythonItem::Str)
        );
        assert_eq!(
            <BTreeMap<String, Execution> as ManifestValue>::PYTHON,
            PythonValue::Map(PythonItem::Named("Execution"))
        );
        assert_eq!(PythonItem::of(PythonValue::Bool).value(), PythonValue::Bool);
    }
}
