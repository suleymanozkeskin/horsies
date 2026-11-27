//! The contract test of the monitoring read models: the JSON schema of each
//! Rust model against the contract of its Pydantic model.
//!
//! The core writes the JSON of these models, and the Python client validates
//! it into the Pydantic models of `horsies.monitoring.models` (one
//! `model_validate_json` per read). `model_contract.json` holds the contract
//! of the Pydantic models: per model, the field names and the kind of value
//! each field holds (`clients/python/tests/monitoring_model_contract.py` writes it, and a
//! Python unit test checks that it is current). This test reduces the JSON
//! schema of each Rust model (`schemars`, a dev-dependency) to the same
//! kinds and compares them. The fields must be the same; each kind must be
//! the same, but for the differences named in [`COMPATIBLE_DIFFERENCES`],
//! where every value of the Rust kind validates into the Pydantic kind.
//!
//! The contract names each nested model, union and enum, and the TypeScript
//! client takes its type names from it. The name must be the name of the
//! Rust type. A variant of a union has the name of its Pydantic class; a
//! Rust variant has no type of its own, so its name is not compared.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde_json::{Map, Value};

use crate::broker::{
    BudgetLoad, DownSlot, ExecutionClassReport, PendingStream, WorkerExecutionClassLoad, WorkerPong,
};
use crate::monitoring::models::{
    Breakdown, BreakdownCounts, ErrorFacet, FacetValue, Facets, GroupRow, LivenessReport,
    ScheduleStateInfo, StatusCount, TaskAttemptDetail, TaskDetail, TaskListPage, TaskRowInfo,
    TaskSummary, WorkerHistoryPoint, WorkerStateInfo, WorkflowEdge, WorkflowNodeDetail,
    WorkflowNodeSummary, WorkflowRunDetail, WorkflowRunSummary,
};

/// The contract of the Pydantic models, by model name.
const PYDANTIC_CONTRACT: &str = include_str!("model_contract.json");

/// The deepest call of the comparison is at depth 19: the placement of the
/// claim of a segment of the timing of a node of a run
/// (`WorkflowRunDetail`). A deeper schema is a defect of the reduction.
const MAX_DEPTH: usize = 24;

/// A difference between a Rust field and its Pydantic field where every
/// value that the Rust model writes validates into the Pydantic field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompatibleDifference {
    /// A `Uuid` in Rust, `str` in Pydantic: the hyphenated text validates.
    UuidIntoText,
    /// A value in Rust, `T | None` in Pydantic.
    RequiredIntoOptional,
    /// An unsigned integer in Rust, `int` in Pydantic.
    UnsignedIntoInteger,
}

/// The named differences, by `<owner>.<field>`. The owner is the Rust type
/// that declares the field; a variant of a tagged enum is
/// `<enum>::<tag value>`. Any other difference fails the test, and so does
/// a named difference that no field has.
const COMPATIBLE_DIFFERENCES: &[(&str, CompatibleDifference)] = &[
    ("TaskDetail.workflow_id", CompatibleDifference::UuidIntoText),
    (
        "WorkflowRunSummary.workflow_id",
        CompatibleDifference::UuidIntoText,
    ),
    (
        "WorkflowNodeSummary.sub_workflow_id",
        CompatibleDifference::UuidIntoText,
    ),
    (
        "ScheduleStateInfo.next_run_at",
        CompatibleDifference::RequiredIntoOptional,
    ),
    ("DownSlot.slot", CompatibleDifference::UnsignedIntoInteger),
    (
        "DownSlot.failed_boots",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "BudgetLoad.capacity",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "BudgetLoad.in_flight",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "StreamCoverageState::served.workers",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "StreamCoverageState::served.capacity",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "StreamCoverageState::no_capacity.workers",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "AttemptClaimsInfo::counted.count",
        CompatibleDifference::UnsignedIntoInteger,
    ),
    (
        "SegmentClaimInfo::earlier.count",
        CompatibleDifference::UnsignedIntoInteger,
    ),
];

/// The name of a Rust type in its JSON schema.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RustName {
    /// The schema is the definition of this name in `$defs`.
    Defined(String),
    /// The schema is written in the place of its use: it has no name.
    Inline,
}

/// The kind of one Rust schema, with the schemas inside it still to read.
#[derive(Debug)]
enum RustKind {
    Nullable(Value),
    Text,
    DateTime,
    Uuid,
    Integer,
    Unsigned,
    Number,
    Boolean,
    List(Value),
    Map(Value),
    /// An enum of texts: its name and its values.
    Enum(RustName, BTreeSet<String>),
    /// A struct: the owner name of its fields, its name and its schema.
    Model {
        owner: String,
        name: RustName,
        schema: Value,
    },
    /// A tagged enum: its name, the tag name, and per tag value the variant
    /// owner and the variant schema.
    Union {
        name: String,
        tag: String,
        variants: BTreeMap<String, (String, Value)>,
    },
}

/// The string values of an enum schema: `enum`, or `oneOf` of string
/// constants. `None` for another schema.
fn enum_values(schema: &Value) -> Option<BTreeSet<String>> {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect();
    }
    let members = schema.get("oneOf")?.as_array()?;
    members
        .iter()
        .map(|member| match member.get("properties") {
            Some(_) => None,
            None => tag_constant(member).map(str::to_owned),
        })
        .collect()
}

/// The constant of a schema: `const`, or an `enum` of one value.
fn tag_constant(schema: &Value) -> Option<&str> {
    match schema.get("const") {
        Some(Value::String(value)) => Some(value.as_str()),
        _ => schema
            .get("enum")
            .and_then(Value::as_array)
            .filter(|values| values.len() == 1)
            .and_then(|values| values[0].as_str()),
    }
}

/// The tag of a tagged enum schema and its variants, by tag value.
type UnionVariants = (String, BTreeMap<String, (String, Value)>);

/// The tag and the variants of a tagged enum schema (`oneOf` of objects
/// that share one property with a constant). `None` for another schema.
fn union_variants(owner: &str, schema: &Value) -> Option<UnionVariants> {
    let members = schema.get("oneOf")?.as_array()?;
    let mut tag: Option<String> = None;
    let mut variants = BTreeMap::new();
    for member in members {
        let properties = member.get("properties")?.as_object()?;
        let (name, value) = properties
            .iter()
            .find_map(|(name, property)| tag_constant(property).map(|value| (name, value)))?;
        match &tag {
            Some(known) if known != name => return None,
            Some(_) => {}
            None => tag = Some(name.clone()),
        }
        let _ = variants.insert(
            value.to_owned(),
            (format!("{owner}::{value}"), member.clone()),
        );
    }
    tag.map(|tag| (tag, variants))
}

/// One comparison of a Rust model's schema with the Pydantic contract.
struct Comparison {
    definitions: Map<String, Value>,
    mismatches: Vec<String>,
    used: BTreeSet<&'static str>,
}

impl Comparison {
    fn definition(&self, reference: &str) -> (String, Value) {
        let name = reference
            .strip_prefix("#/$defs/")
            .unwrap_or_else(|| panic!("a reference into $defs, not {reference}"));
        let schema = self
            .definitions
            .get(name)
            .unwrap_or_else(|| panic!("no definition {name}"));
        (name.to_owned(), schema.clone())
    }

    /// Whether `<owner>.<field>` is named for `difference`; a named
    /// difference that answers yes is recorded as used.
    fn allows(&mut self, owner: &str, field: &str, difference: CompatibleDifference) -> bool {
        let key = format!("{owner}.{field}");
        match COMPATIBLE_DIFFERENCES
            .iter()
            .find(|(name, allowed)| *name == key && *allowed == difference)
        {
            Some((name, _)) => {
                let _ = self.used.insert(*name);
                true
            }
            None => false,
        }
    }

    fn mismatch(&mut self, owner: &str, field: &str, cause: &str) {
        self.mismatches.push(format!("{owner}.{field}: {cause}"));
    }

    /// Record a mismatch when the Rust type of `<owner>.<field>` does not
    /// have the name of the named kind `contract` (an enum, a model or a
    /// union of the contract).
    fn compare_name(&mut self, owner: &str, field: &str, rust: &RustName, contract: &Value) {
        let expected = contract.get("name").and_then(Value::as_str);
        match rust {
            RustName::Defined(name) if Some(name.as_str()) == expected => {}
            RustName::Defined(name) => self.mismatch(
                owner,
                field,
                &format!("type {name} in Rust, {expected:?} in Pydantic"),
            ),
            RustName::Inline => self.mismatch(
                owner,
                field,
                &format!("a type with no name in Rust, {expected:?} in Pydantic"),
            ),
        }
    }

    fn rust_kind(&self, owner: &str, schema: &Value) -> RustKind {
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            let (name, definition) = self.definition(reference);
            if let Some(values) = enum_values(&definition) {
                return RustKind::Enum(RustName::Defined(name), values);
            }
            if let Some((tag, variants)) = union_variants(&name, &definition) {
                return RustKind::Union {
                    name,
                    tag,
                    variants,
                };
            }
            return RustKind::Model {
                owner: name.clone(),
                name: RustName::Defined(name),
                schema: definition,
            };
        }
        if let Some([single]) = schema
            .get("allOf")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
        {
            if schema.get("properties").is_none() {
                return self.rust_kind(owner, single);
            }
        }
        if let Some(members) = schema.get("anyOf").and_then(Value::as_array) {
            let others: Vec<&Value> = members
                .iter()
                .filter(|member| member.get("type").and_then(Value::as_str) != Some("null"))
                .collect();
            assert!(
                members.len() == 2 && others.len() == 1,
                "{owner}: anyOf other than T | null: {schema}"
            );
            return RustKind::Nullable(others[0].clone());
        }
        if let Some(values) = enum_values(schema) {
            return RustKind::Enum(RustName::Inline, values);
        }
        match schema.get("type") {
            Some(Value::Array(types)) => {
                let others: Vec<&Value> = types.iter().filter(|kind| *kind != "null").collect();
                assert!(
                    types.len() == 2 && others.len() == 1,
                    "{owner}: a type list other than [T, null]: {schema}"
                );
                let mut inner = schema.clone();
                if let Some(object) = inner.as_object_mut() {
                    let _ = object.insert("type".to_owned(), others[0].clone());
                }
                RustKind::Nullable(inner)
            }
            Some(Value::String(kind)) => match kind.as_str() {
                "string" => match schema.get("format").and_then(Value::as_str) {
                    Some("date-time") => RustKind::DateTime,
                    Some("uuid") => RustKind::Uuid,
                    None => RustKind::Text,
                    Some(other) => panic!("{owner}: a string of format {other}"),
                },
                "integer" => match schema.get("format").and_then(Value::as_str) {
                    Some(format) if format.starts_with("uint") => RustKind::Unsigned,
                    _ => RustKind::Integer,
                },
                "number" => RustKind::Number,
                "boolean" => RustKind::Boolean,
                "array" => RustKind::List(schema.get("items").expect("an array has items").clone()),
                "object" => match schema.get("additionalProperties") {
                    Some(values @ Value::Object(_)) => RustKind::Map(values.clone()),
                    _ => RustKind::Model {
                        owner: owner.to_owned(),
                        name: RustName::Inline,
                        schema: schema.clone(),
                    },
                },
                other => panic!("{owner}: a schema of type {other}"),
            },
            _ => panic!("{owner}: a schema with no kind: {schema}"),
        }
    }

    /// The properties of a struct schema, with the properties of a
    /// flattened struct (`allOf` of references) merged in.
    fn properties(&self, schema: &Value, depth: usize) -> BTreeMap<String, Value> {
        assert!(
            depth <= MAX_DEPTH,
            "the flattened structs nest deeper than {MAX_DEPTH}"
        );
        let mut fields: BTreeMap<String, Value> = schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|object| {
                object
                    .iter()
                    .map(|(name, field)| (name.clone(), field.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for member in schema
            .get("allOf")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let merged = match member.get("$ref").and_then(Value::as_str) {
                Some(reference) => self.definition(reference).1,
                None => member.clone(),
            };
            fields.extend(self.properties(&merged, depth + 1));
        }
        fields
    }

    fn fields(
        &mut self,
        owner: &str,
        schema: &Value,
        without: Option<&str>,
        contract: &Value,
        depth: usize,
    ) {
        assert!(
            depth <= MAX_DEPTH,
            "{owner}: the schema nests deeper than {MAX_DEPTH}"
        );
        let Some(expected) = contract.as_object() else {
            return self.mismatch(
                owner,
                "*",
                &format!("the contract is not an object: {contract}"),
            );
        };
        let actual: BTreeMap<String, Value> = self
            .properties(schema, 0)
            .into_iter()
            .filter(|(name, _)| Some(name.as_str()) != without)
            .collect();
        let actual_names: BTreeSet<&str> = actual.keys().map(String::as_str).collect();
        let expected_names: BTreeSet<&str> = expected.keys().map(String::as_str).collect();
        for missing in expected_names.difference(&actual_names) {
            self.mismatch(owner, missing, "in Pydantic, not in Rust");
        }
        for extra in actual_names.difference(&expected_names) {
            self.mismatch(owner, extra, "in Rust, not in Pydantic");
        }
        for (name, field) in &actual {
            if let Some(kind) = expected.get(name) {
                self.kind(owner, name, field, kind, depth + 1);
            }
        }
    }

    fn kind(&mut self, owner: &str, field: &str, schema: &Value, contract: &Value, depth: usize) {
        assert!(
            depth <= MAX_DEPTH,
            "{owner}.{field}: deeper than {MAX_DEPTH}"
        );
        let actual = self.rust_kind(owner, schema);
        if let Some(inner) = contract.get("nullable") {
            return match actual {
                RustKind::Nullable(rust_inner) => {
                    self.kind(owner, field, &rust_inner, inner, depth + 1);
                }
                _ if self.allows(owner, field, CompatibleDifference::RequiredIntoOptional) => {
                    self.kind(owner, field, schema, inner, depth + 1);
                }
                _ => self.mismatch(owner, field, "nullable in Pydantic, not in Rust"),
            };
        }
        match (actual, contract) {
            (RustKind::Nullable(_), _) => {
                self.mismatch(owner, field, "nullable in Rust, not in Pydantic");
            }
            (RustKind::Text, Value::String(kind)) if kind == "string" => {}
            (RustKind::DateTime, Value::String(kind)) if kind == "date-time" => {}
            (RustKind::Uuid, Value::String(kind)) if kind == "uuid" => {}
            (RustKind::Uuid, Value::String(kind))
                if kind == "string"
                    && self.allows(owner, field, CompatibleDifference::UuidIntoText) => {}
            (RustKind::Integer, Value::String(kind)) if kind == "integer" => {}
            (RustKind::Unsigned, Value::String(kind))
                if kind == "integer"
                    && self.allows(owner, field, CompatibleDifference::UnsignedIntoInteger) => {}
            (RustKind::Number, Value::String(kind)) if kind == "number" => {}
            (RustKind::Boolean, Value::String(kind)) if kind == "boolean" => {}
            (RustKind::List(items), Value::Object(object)) if object.contains_key("list") => {
                self.kind(owner, field, &items, &object["list"], depth + 1);
            }
            (RustKind::Map(values), Value::Object(object)) if object.contains_key("map") => {
                self.kind(owner, field, &values, &object["map"], depth + 1);
            }
            (RustKind::Enum(name, values), Value::Object(object))
                if object.contains_key("enum") =>
            {
                let contract_enum = &object["enum"];
                self.compare_name(owner, field, &name, contract_enum);
                let expected: Option<BTreeSet<String>> =
                    contract_enum["values"].as_array().map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str().map(str::to_owned))
                            .collect()
                    });
                if expected.as_ref() != Some(&values) {
                    self.mismatch(
                        owner,
                        field,
                        &format!("enum {values:?} in Rust, {contract_enum} in Pydantic"),
                    );
                }
            }
            (
                RustKind::Model {
                    owner: model_owner,
                    name,
                    schema: model,
                },
                Value::Object(object),
            ) if object.contains_key("model") => {
                let contract_model = &object["model"];
                self.compare_name(owner, field, &name, contract_model);
                self.fields(
                    &model_owner,
                    &model,
                    None,
                    &contract_model["fields"],
                    depth + 1,
                );
            }
            (
                RustKind::Union {
                    name,
                    tag,
                    variants,
                },
                Value::Object(object),
            ) if object.contains_key("union") => {
                let contract_union = &object["union"];
                self.compare_name(owner, field, &RustName::Defined(name), contract_union);
                self.union(owner, field, &tag, &variants, contract_union, depth + 1);
            }
            (actual, expected) => {
                self.mismatch(
                    owner,
                    field,
                    &format!("{actual:?} in Rust, {expected} in Pydantic"),
                );
            }
        }
    }

    fn union(
        &mut self,
        owner: &str,
        field: &str,
        tag: &str,
        variants: &BTreeMap<String, (String, Value)>,
        contract: &Value,
        depth: usize,
    ) {
        if contract.get("tag").and_then(Value::as_str) != Some(tag) {
            return self.mismatch(
                owner,
                field,
                &format!("tag {tag} in Rust, {contract} in Pydantic"),
            );
        }
        let Some(expected) = contract.get("variants").and_then(Value::as_object) else {
            return self.mismatch(
                owner,
                field,
                &format!("no variants in Pydantic: {contract}"),
            );
        };
        let actual_values: BTreeSet<&str> = variants.keys().map(String::as_str).collect();
        let expected_values: BTreeSet<&str> = expected.keys().map(String::as_str).collect();
        if actual_values != expected_values {
            return self.mismatch(
                owner,
                field,
                &format!("variants {actual_values:?} in Rust, {expected_values:?} in Pydantic"),
            );
        }
        for (value, (variant_owner, variant)) in variants {
            self.fields(
                variant_owner,
                variant,
                Some(tag),
                &expected[value]["fields"],
                depth + 1,
            );
        }
    }
}

/// The comparison of one Rust model with its Pydantic contract.
fn compare<T: JsonSchema>(name: &str, contract: &Map<String, Value>) -> Comparison {
    let schema = serde_json::to_value(schemars::schema_for!(T)).expect("a schema serializes");
    let definitions = schema
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut comparison = Comparison {
        definitions,
        mismatches: Vec::new(),
        used: BTreeSet::new(),
    };
    match contract.get(name) {
        Some(expected) => comparison.fields(name, &schema, None, expected, 0),
        None => comparison
            .mismatches
            .push(format!("{name}: no Pydantic contract")),
    }
    comparison
}

fn contract() -> Map<String, Value> {
    match serde_json::from_str(PYDANTIC_CONTRACT) {
        Ok(Value::Object(models)) => models,
        other => panic!("model_contract.json is not an object of models: {other:?}"),
    }
}

/// The models of the contract, each with its comparison.
type Check = fn(&str, &Map<String, Value>) -> Comparison;

const CHECKS: [(&str, Check); 27] = [
    ("TaskAttemptDetail", compare::<TaskAttemptDetail>),
    ("TaskRowInfo", compare::<TaskRowInfo>),
    ("StatusCount", compare::<StatusCount>),
    ("FacetValue", compare::<FacetValue>),
    ("ErrorFacet", compare::<ErrorFacet>),
    ("Facets", compare::<Facets>),
    ("BreakdownCounts", compare::<BreakdownCounts>),
    ("GroupRow", compare::<GroupRow>),
    ("Breakdown", compare::<Breakdown>),
    ("TaskSummary", compare::<TaskSummary>),
    ("TaskListPage", compare::<TaskListPage>),
    ("TaskDetail", compare::<TaskDetail>),
    ("WorkflowRunSummary", compare::<WorkflowRunSummary>),
    ("WorkflowNodeSummary", compare::<WorkflowNodeSummary>),
    ("WorkflowEdge", compare::<WorkflowEdge>),
    ("WorkflowRunDetail", compare::<WorkflowRunDetail>),
    ("WorkflowNodeDetail", compare::<WorkflowNodeDetail>),
    ("DownSlot", compare::<DownSlot>),
    ("WorkerStateInfo", compare::<WorkerStateInfo>),
    ("BudgetLoad", compare::<BudgetLoad>),
    (
        "WorkerExecutionClassLoad",
        compare::<WorkerExecutionClassLoad>,
    ),
    ("PendingStream", compare::<PendingStream>),
    ("ExecutionClassReport", compare::<ExecutionClassReport>),
    ("WorkerPong", compare::<WorkerPong>),
    ("LivenessReport", compare::<LivenessReport>),
    ("WorkerHistoryPoint", compare::<WorkerHistoryPoint>),
    ("ScheduleStateInfo", compare::<ScheduleStateInfo>),
];

#[test]
fn every_rust_model_validates_into_its_pydantic_model() {
    let contract = contract();
    let checked: BTreeSet<&str> = CHECKS.iter().map(|(name, _)| *name).collect();
    let expected: BTreeSet<&str> = contract.keys().map(String::as_str).collect();
    assert_eq!(
        checked, expected,
        "every model of the Pydantic contract has its Rust model here"
    );

    let mut mismatches: Vec<String> = Vec::new();
    let mut used: BTreeSet<&'static str> = BTreeSet::new();
    for (name, check) in CHECKS {
        let comparison = check(name, &contract);
        mismatches.extend(comparison.mismatches);
        used.extend(comparison.used);
    }
    assert!(
        mismatches.is_empty(),
        "the Rust models differ from the Pydantic models:\n{}",
        mismatches.join("\n")
    );
    let unused: Vec<&str> = COMPATIBLE_DIFFERENCES
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !used.contains(name))
        .collect();
    assert!(
        unused.is_empty(),
        "named differences that no field has; remove them: {unused:?}"
    );
}

/// The comparison sees a difference: a Rust model with one field more than
/// its contract, and one with a kind that no named difference allows.
#[test]
fn the_comparison_sees_an_extra_field_and_an_unnamed_difference() {
    let mut contract = contract();
    let mut fewer = contract["WorkflowEdge"]
        .as_object()
        .expect("a model")
        .clone();
    let _ = fewer.remove("to_index");
    let _ = contract.insert("WorkflowEdge".to_owned(), Value::Object(fewer));
    let edge = compare::<WorkflowEdge>("WorkflowEdge", &contract);
    assert_eq!(
        edge.mismatches,
        vec!["WorkflowEdge.to_index: in Rust, not in Pydantic".to_owned()]
    );

    let mut text_count = contract["StatusCount"]
        .as_object()
        .expect("a model")
        .clone();
    let _ = text_count.insert("count".to_owned(), Value::String("string".to_owned()));
    let _ = contract.insert("StatusCount".to_owned(), Value::Object(text_count));
    let count = compare::<StatusCount>("StatusCount", &contract);
    assert_eq!(count.mismatches.len(), 1, "{:?}", count.mismatches);
    assert!(count.mismatches[0].starts_with("StatusCount.count: "));
}

/// The comparison sees a named kind whose name is not the name of the Rust
/// type.
#[test]
fn the_comparison_sees_a_name_that_is_not_the_rust_name() {
    let mut contract = contract();
    let mut budget_load = contract["BudgetLoad"].clone();
    *budget_load
        .pointer_mut("/budget/enum/name")
        .expect("the enum of budget has a name") = Value::String("Budget".to_owned());
    let _ = contract.insert("BudgetLoad".to_owned(), budget_load);

    let load = compare::<BudgetLoad>("BudgetLoad", &contract);

    assert_eq!(
        load.mismatches,
        vec!["BudgetLoad.budget: type Execution in Rust, Some(\"Budget\") in Pydantic".to_owned()]
    );
}
