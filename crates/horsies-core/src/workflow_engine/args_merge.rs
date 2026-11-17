use std::collections::HashMap;

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

use crate::core::task::result::TaskResult;
use crate::core::{OperationalErrorCode, OutcomeCode, TaskError, TaskErrorKind};

use crate::workflow_engine::error::WorkflowError;

/// The marker key of an `args_from` entry (Python `_ENGINE_KEY_TASKRESULT_ENVELOPE`,
/// `core/workflows/engine.py:143`).
pub const ARGS_FROM_ENVELOPE_KEY: &str = "__h_taskresult_envelope__";

/// The keys of an `args_from` entry, besides the marker.
pub const ARGS_FROM_ENTRY_KEYS: [&str; 3] = ["source_task_name", "source_definition_key", "inner"];

/// A dependency row of the node that receives `args_from`: every status, not only terminal ones.
pub struct DepResult {
    /// The node id of the dependency; `None` for a node stored without one.
    pub node_id: Option<String>,
    /// The task name of the dependency node (`__sub_workflow:<name>` for a sub-workflow node).
    pub task_name: String,
    /// The definition key of a sub-workflow node; `None` for a task node.
    pub sub_definition_key: Option<String>,
    pub status: String,
    pub result: Option<String>,
}

/// The stored form of one `args_from` kwarg: Python's wrap around the dependency's result envelope
/// (`core/workflows/engine.py:891-896`).
#[derive(Serialize)]
struct ArgsFromEntry<'a> {
    #[serde(rename = "__h_taskresult_envelope__")]
    marker: bool,
    source_task_name: &'a str,
    source_definition_key: Option<&'a str>,
    inner: DependencyEnvelope<'a>,
}

/// The `inner` result envelope of an `args_from` entry.
#[derive(Serialize)]
#[serde(untagged)]
enum DependencyEnvelope<'a> {
    /// The dependency's stored result envelope, as its text: each value
    /// keeps the text it was stored with, an integer outside the `i64` and
    /// `u64` range included (Python reads any integer, `loads_json`,
    /// `core/codec/json_io.py:97`).
    Stored(&'a RawValue),
    /// The error that the engine writes in place of a stored result: a
    /// SKIPPED dependency, a dependency with no stored result, or a stored
    /// result that does not read.
    Written(TaskResult<serde_json::Value>),
}

/// The members of a node input that injected members merge into, each value
/// as its text, in text order.
///
/// The stored text must pass the core's strict read rule: 300 levels of
/// room, so a user value of 200 levels inside its envelopes reads (Python
/// `core/codec/json_value.py:54-75`), and no key repeats. Only an object
/// takes named members: any other input, `null` included, is refused. The
/// client gives a node with members an object input such as `{}`; the core
/// does not read `null` as `{}`.
///
/// Errors: [`WorkflowError::Validation`] for a text that the strict read
/// refuses, and for a value that is not an object;
/// [`WorkflowError::Serialization`], logged, when serde_json does not read
/// the members of a text that the strict read took.
pub(crate) fn input_members(input: &str) -> Result<Vec<(String, &RawValue)>, WorkflowError> {
    crate::core::codec::check_json_text(input).map_err(|refusal| {
        WorkflowError::Validation(format!("stored node input refused: {refusal}"))
    })?;
    match is_object_text(input) {
        true => object_members(input).map_err(|error| {
            tracing::error!(
                error = %error,
                "failed to parse a node input for a member merge: {error}",
            );
            WorkflowError::Serialization(error)
        }),
        false => Err(WorkflowError::Validation(
            "a node input that takes args_from, workflow context or workflow meta members \
             must be a JSON object"
                .to_owned(),
        )),
    }
}

/// The members of the JSON object `text`, each value as its text, in text
/// order. The callers pass a text that the strict read rule took, or the
/// text of a member of one, so no key repeats.
///
/// Errors: the `serde_json::Error` of a text that is not one JSON object.
/// Pure.
pub(crate) fn object_members(text: &str) -> Result<Vec<(String, &RawValue)>, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let members = ObjectMembers::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(members.0)
}

/// The members of one JSON object, in text order, each value as its text.
struct ObjectMembers<'a>(Vec<(String, &'a RawValue)>);

impl<'de> Deserialize<'de> for ObjectMembers<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ObjectMembersVisitor)
    }
}

struct ObjectMembersVisitor;

impl<'de> Visitor<'de> for ObjectMembersVisitor {
    type Value = ObjectMembers<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    /// One pass over the members; the text bounds their count.
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut members: Vec<(String, &'de RawValue)> = Vec::new();
        while let Some(member) = access.next_entry::<String, &'de RawValue>()? {
            members.push(member);
        }
        Ok(ObjectMembers(members))
    }
}

/// Whether `text`, a JSON text that the strict read rule took, is an object.
/// Pure.
fn is_object_text(text: &str) -> bool {
    text.trim_start_matches(JSON_WHITESPACE).starts_with('{')
}

/// The JSON object text of `members`, in their order: each key as a JSON
/// string, each value as the text given. Pure.
pub(crate) fn object_text(members: &[(&str, &str)]) -> String {
    let mut text = String::from("{");
    for (position, (key, value)) in members.iter().enumerate() {
        if position > 0 {
            text.push(',');
        }
        text.push_str(&serde_json::Value::String((*key).to_owned()).to_string());
        text.push(':');
        text.push_str(value);
    }
    text.push('}');
    text
}

/// The stored input of a sub-workflow node, read as Python reads it before
/// the child build (`_input_kwargs`, `core/workflows/child_build.py`;
/// `enqueue_subworkflow_task`, `core/workflows/engine.py:1094-1108`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChildStaticInput<'a> {
    /// An object: the stored text, whose members are the static members.
    Members(&'a str),
    /// An empty text, or a value that is not an object: no static members.
    /// Python reads them as `{}`.
    NoMembers,
}

/// A stored node input that Python refuses before the child build: the node
/// fails with `Corrupt kwargs JSON for subworkflow task <index>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaticInputCorrupt {
    /// Why the text is refused, for the log.
    pub(crate) cause: String,
}

/// Read the stored input `text` of a sub-workflow node: an empty text has no
/// static members; any other text must pass the strict read rule
/// (`crate::core::codec::check_json_text`, Python `loads_json`) and must not
/// be `null`; an object gives its members, any other value none.
///
/// Errors: [`StaticInputCorrupt`] for a text that the strict read refuses,
/// and for `null`. Pure.
pub(crate) fn child_static_input(text: &str) -> Result<ChildStaticInput<'_>, StaticInputCorrupt> {
    if text.is_empty() {
        return Ok(ChildStaticInput::NoMembers);
    }
    crate::core::codec::check_json_text(text).map_err(|error| StaticInputCorrupt {
        cause: error.to_string(),
    })?;
    // The text passed the strict read, so its first character after the
    // whitespace names the kind of its value.
    match text
        .trim_start_matches(JSON_WHITESPACE)
        .as_bytes()
        .first()
        .copied()
    {
        Some(b'{') => Ok(ChildStaticInput::Members(text)),
        Some(b'n') => Err(StaticInputCorrupt {
            cause: "the text reads as null".to_owned(),
        }),
        Some(_) | None => Ok(ChildStaticInput::NoMembers),
    }
}

/// The whitespace characters of the JSON grammar.
const JSON_WHITESPACE: [char; 4] = [' ', '\t', '\n', '\r'];

/// The stored `args_from` text of a sub-workflow node (the CHILD_START row
/// gives a `jsonb` string as its text, any other value as its JSON text),
/// read as Python reads it (`_args_from_map`, `core/workflows/child_build.py`;
/// `enqueue_subworkflow_task`, `core/workflows/engine.py:1128-1155`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ArgsFromRead {
    /// No `args_from` members: no text, an empty text, or a value that is
    /// not an object.
    NoMembers,
    /// An object whose every value is a JSON integer: the key to index map,
    /// in text order.
    Indexes(serde_json::Map<String, serde_json::Value>),
    /// The text breaks the strict read rule, or reads as `null`: the node
    /// fails with `Corrupt args_from JSON for subworkflow task <index>`.
    Corrupt { cause: String },
    /// An object with a value that is not a JSON integer: the node fails
    /// with `args_from has non-int values for subworkflow task <index>`.
    /// `true` and `false` are refused, as Python's `_args_from_map` refuses a
    /// `bool` and `horsies_workflow_task_node_refusal` refuses them for a
    /// task node.
    NotIndexes,
}

/// Read the stored `args_from` text of a sub-workflow node ([`ArgsFromRead`]).
/// An integer is a number literal with no fraction and no exponent, whatever
/// its size; an index outside the `integer` range names no node, so the merge
/// leaves its member out. Pure.
pub(crate) fn read_args_from(text: Option<&str>) -> ArgsFromRead {
    let Some(text) = text.filter(|text| !text.is_empty()) else {
        return ArgsFromRead::NoMembers;
    };
    let value: serde_json::Value = match crate::core::codec::from_json_str(text) {
        Ok(value) => value,
        Err(error) => {
            return ArgsFromRead::Corrupt {
                cause: error.to_string(),
            }
        }
    };
    let map = match value {
        serde_json::Value::Object(map) => map,
        serde_json::Value::Null => {
            return ArgsFromRead::Corrupt {
                cause: "the text reads as null".to_owned(),
            }
        }
        serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_)
        | serde_json::Value::Array(_) => return ArgsFromRead::NoMembers,
    };
    // The literal of each value: serde_json reads an integer above the u64
    // range as a float, so the kind is read from the text.
    let literals: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
        match crate::core::codec::from_json_str(text) {
            Ok(literals) => literals,
            Err(error) => {
                return ArgsFromRead::Corrupt {
                    cause: error.to_string(),
                }
            }
        };
    match literals
        .values()
        .all(|literal| is_integer_literal(literal.get()))
    {
        true => ArgsFromRead::Indexes(map),
        false => ArgsFromRead::NotIndexes,
    }
}

/// Whether `literal`, the text of one JSON value, is an integer number: an
/// optional minus sign, then digits only. Pure.
fn is_integer_literal(literal: &str) -> bool {
    let trimmed = literal.trim_matches(JSON_WHITESPACE);
    let digits = trimmed.strip_prefix('-').unwrap_or(trimmed);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// The node input after the `args_from` merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArgsFromMerge {
    /// The input with one member per `args_from` key; the input unchanged when the node has no
    /// `args_from`.
    Merged(String),
    /// An `args_from` key is also a member of the node's static input. Nothing is merged: the node
    /// fails with [`ArgsFromConflict::node_error`] (Python `core/workflows/engine.py:834-839`,
    /// `:1151-1163`).
    StaticMemberConflict(ArgsFromConflict),
}

/// An `args_from` key that the node's static input also sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArgsFromConflict {
    /// The conflicting key.
    pub(crate) key: String,
}

/// The kind of the node whose input takes the `args_from` members. It names the node in the
/// conflict message, as Python does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergedNodeKind {
    Task,
    SubWorkflow,
}

impl ArgsFromConflict {
    /// The error that the failed node stores: `WORKFLOW_ENQUEUE_FAILED` with Python's message and
    /// `data` (`core/workflows/engine.py:472-555`, `:620-631`, `:834-839`, `:1151-1163`).
    pub(crate) fn node_error(
        &self,
        workflow_id: Uuid,
        task_index: i32,
        kind: MergedNodeKind,
    ) -> TaskError {
        let node = match kind {
            MergedNodeKind::Task => format!("task {task_index}"),
            MergedNodeKind::SubWorkflow => format!("subworkflow task {task_index}"),
        };
        TaskError {
            data: Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "task_index": task_index,
            })),
            ..TaskError::builtin(
                OperationalErrorCode::WorkflowEnqueueFailed,
                format!(
                    "args_from key '{}' conflicts with static kwarg ({node})",
                    self.key
                ),
            )
        }
    }
}

/// The `args_from` key that the static input also sets, if any.
///
/// With several, the first in the order in which Python reads the stored `args_from` (`jsonb` key
/// order: shorter keys first, then by bytes), so the core names the key that Python names.
fn static_member_conflict(
    members: &[(String, &RawValue)],
    args_from: &serde_json::Map<String, serde_json::Value>,
) -> Option<ArgsFromConflict> {
    args_from
        .keys()
        .filter(|key| members.iter().any(|(name, _)| name == *key))
        .min_by(|left, right| (left.len(), left.as_bytes()).cmp(&(right.len(), right.as_bytes())))
        .map(|key| ArgsFromConflict { key: key.clone() })
}

/// Merge args_from dependency results into the node's input.
///
/// With no args_from the input is returned unchanged. An `args_from` key that the static input
/// also sets gives [`ArgsFromMerge::StaticMemberConflict`] and merges nothing. Otherwise, for each
/// (field_name, dep_index) in args_from, the field holds Python's `args_from` wrap around the
/// dependency's result ([`dependency_result`]). A dependency index with no node row gets no field
/// (Python injects only the results that exist, `core/workflows/engine.py:841`); every node of a
/// workflow has a row from its start, so this is logged as an error.
///
/// The merged text holds the static members first, in their stored order, then the `args_from`
/// members in `args_from` order. Each static member and each stored dependency envelope keeps
/// its stored text: no value is parsed and written again.
///
/// Errors: the errors of [`input_members`]; [`WorkflowError::Serialization`] for an entry that
/// does not encode.
pub(crate) fn merge_args_from_sync(
    input: &str,
    args_from: &Option<serde_json::Value>,
    dep_results: &HashMap<i32, DepResult>,
) -> Result<ArgsFromMerge, WorkflowError> {
    let args_from_map = match args_from {
        Some(v) => match v.as_object() {
            Some(obj) if !obj.is_empty() => obj,
            _ => return Ok(ArgsFromMerge::Merged(input.to_owned())),
        },
        None => return Ok(ArgsFromMerge::Merged(input.to_owned())),
    };

    let members = input_members(input)?;
    if let Some(conflict) = static_member_conflict(&members, args_from_map) {
        return Ok(ArgsFromMerge::StaticMemberConflict(conflict));
    }

    let entries = args_from_entry_texts(args_from_map, dep_results)?;
    let mut written: Vec<(&str, &str)> = members
        .iter()
        .map(|(name, value)| (name.as_str(), value.get()))
        .collect();
    written.extend(
        entries
            .iter()
            .map(|(name, entry)| (name.as_str(), entry.as_str())),
    );
    Ok(ArgsFromMerge::Merged(object_text(&written)))
}

/// The text of one `args_from` entry per kwarg of `args_from_map` that names a terminal dependency,
/// with its kwarg, in `args_from_map` order.
///
/// Errors: [`WorkflowError::Serialization`] for an entry that does not encode.
fn args_from_entry_texts(
    args_from_map: &serde_json::Map<String, serde_json::Value>,
    dep_results: &HashMap<i32, DepResult>,
) -> Result<Vec<(String, String)>, WorkflowError> {
    let mut entries: Vec<(String, String)> = Vec::with_capacity(args_from_map.len());
    for (kwarg_name, dep_idx_value) in args_from_map {
        // An index outside the `integer` range names no node row ([`read_args_from`] took any integer).
        let Some(dep_idx) = dep_idx_value
            .as_i64()
            .and_then(|index| i32::try_from(index).ok())
        else {
            tracing::debug!(
                kwarg = %kwarg_name,
                "args_from names an index outside the integer range; the kwarg is left out",
            );
            continue;
        };
        // No row in the read: the child start reads terminal dependencies only (its `SOURCE` rows), so a
        // dependency that is not terminal is absent here too; an index with no node row is left out the same way.
        let Some(dep) = dep_results.get(&dep_idx) else {
            tracing::debug!(
                dep_idx,
                kwarg = %kwarg_name,
                "args_from names a dependency with no terminal row in the read; the kwarg is left out",
            );
            continue;
        };
        // A dependency that is not terminal (join `any` or `quorum` promoted the node first) gets no kwarg:
        // the receiving task uses its default (Python `core/workflows/engine.py:841`).
        let Some(inner) = dependency_result(dep_idx, dep) else {
            continue;
        };
        let entry = ArgsFromEntry {
            marker: true,
            source_task_name: &dep.task_name,
            source_definition_key: dep.sub_definition_key.as_deref(),
            inner,
        };
        entries.push((kwarg_name.clone(), serde_json::to_string(&entry)?));
    }
    Ok(entries)
}

/// Why an `args_from` entry of a node's kwargs does not read (Python `core/worker/child_runner.py:1573-1612`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArgsFromEntryRefused {
    #[error("args_from envelope for kwarg '{kwarg}' has the unexpected key {key:?}")]
    UnexpectedKey { kwarg: String, key: String },
    #[error("args_from envelope for kwarg '{kwarg}' is missing source_task_name")]
    MissingSourceTaskName { kwarg: String },
    #[error("args_from envelope for kwarg '{kwarg}' has a source_definition_key that is not text")]
    SourceDefinitionKeyNotText { kwarg: String },
    #[error("args_from envelope for kwarg '{kwarg}' has no inner result")]
    MissingInner { kwarg: String },
}

/// The `inner` result envelope of the kwarg `name`, as its text, when `value` is an `args_from` entry;
/// `None` when it is not one. An entry is an object with `ARGS_FROM_ENVELOPE_KEY: true`; it must have a
/// text `source_task_name`, a text or null `source_definition_key`, an `inner` value and no other key.
/// The receiving `TaskResult` field decodes the envelope with its own type (Python
/// `core/worker/child_runner.py:1362-1436`).
///
/// `value` is JSON, so its members read unless it is not an object.
///
/// Errors: [`ArgsFromEntryRefused`] for an entry that breaks a rule. Pure.
pub(crate) fn args_from_inner_text<'a>(
    name: &str,
    value: &'a RawValue,
) -> Result<Option<&'a RawValue>, ArgsFromEntryRefused> {
    let entry = match object_members(value.get()) {
        Ok(members) => members,
        Err(_not_an_object) => return Ok(None),
    };
    let member = |key: &str| {
        entry
            .iter()
            .find(|(member_name, _)| member_name == key)
            .map(|(_, text)| *text)
    };
    match member(ARGS_FROM_ENVELOPE_KEY).map(RawValue::get) {
        Some("true") => {}
        Some(_) | None => return Ok(None),
    }
    if let Some((key, _)) = entry.iter().find(|(key, _)| {
        key.as_str() != ARGS_FROM_ENVELOPE_KEY && !ARGS_FROM_ENTRY_KEYS.contains(&key.as_str())
    }) {
        return Err(ArgsFromEntryRefused::UnexpectedKey {
            kwarg: name.to_owned(),
            key: key.clone(),
        });
    }
    match member("source_task_name").map(is_json_string) {
        Some(true) => {}
        Some(false) | None => {
            return Err(ArgsFromEntryRefused::MissingSourceTaskName {
                kwarg: name.to_owned(),
            });
        }
    }
    match member("source_definition_key") {
        None => {}
        Some(key) if key.get() == "null" || is_json_string(key) => {}
        Some(_) => {
            return Err(ArgsFromEntryRefused::SourceDefinitionKeyNotText {
                kwarg: name.to_owned(),
            });
        }
    }
    member("inner")
        .map(Some)
        .ok_or_else(|| ArgsFromEntryRefused::MissingInner {
            kwarg: name.to_owned(),
        })
}

/// Whether `value` is a JSON string. A raw value starts at its first
/// character. Pure.
fn is_json_string(value: &RawValue) -> bool {
    value.get().starts_with('"')
}

/// A merged node input from which [`child_build_input`] cannot make the builder input: the text does not
/// parse, it is not an object while the node has `args_from` keys, or an `args_from` entry is malformed. The
/// engine wrote that text, so this is corrupt stored data, not a user error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredInputRefused {
    /// Why the text is refused.
    pub(crate) refusal: String,
}

/// The result of one dependency, as the `inner` envelope of its `args_from` entry. Python:
/// `get_dependency_results` (`core/workflows/engine.py:2816-2850`).
///
/// - SKIPPED: `UPSTREAM_SKIPPED` with `data.dependency_index`, also when a result is stored.
/// - COMPLETED or FAILED with a stored result: that result as its stored text, when it reads under the
///   stored result rules ([`stored_envelope`]); one that does not read gives `RESULT_DESERIALIZATION_ERROR`.
/// - COMPLETED or FAILED with a NULL or empty stored result: [`missing_result_error`] (decided: no writer stores
///   such a node, so it is stored data that does not parse).
/// - A dependency that is not terminal: `None`, and the key is left out. Python reads the dependency results
///   from terminal rows only (`core/workflows/sql.py:523-530`), and the baseline input builder
///   (`horsies_workflow_task_input`) does the same.
fn dependency_result(dep_idx: i32, dep: &DepResult) -> Option<DependencyEnvelope<'_>> {
    match (dep.status.as_str(), dep.result.as_deref()) {
        ("SKIPPED", _) => Some(DependencyEnvelope::Written(TaskResult::Err(
            upstream_skipped_error(dep_idx),
        ))),
        ("COMPLETED" | "FAILED", Some(text)) if !text.is_empty() => {
            Some(stored_envelope(dep_idx, text))
        }
        (status @ ("COMPLETED" | "FAILED"), _) => {
            Some(DependencyEnvelope::Written(TaskResult::Err(
                missing_result_error(dep_idx, dep.node_id.as_deref(), &dep.task_name, status),
            )))
        }
        (_, _) => None,
    }
}

/// The stored result `text` of the dependency `dep_idx` as an `inner` envelope: the text itself when it
/// reads as a result envelope (the reader of [`TaskResult`]: the envelope shape, the `ok` slot under the
/// rules of a user value, the `err` slot as a stored error), else `RESULT_DESERIALIZATION_ERROR`. The read
/// checks the text and keeps no value, so the envelope keeps every literal as stored. A text that
/// does not read is logged.
fn stored_envelope(dep_idx: i32, text: &str) -> DependencyEnvelope<'_> {
    let read = serde_json::from_str::<TaskResult<IgnoredAny>>(text)
        .and_then(|_reads| serde_json::from_str::<&RawValue>(text));
    match read {
        Ok(envelope) => DependencyEnvelope::Stored(envelope),
        Err(e) => {
            tracing::warn!(dep_idx, error = %e, "failed to parse dep result JSON: {e}");
            DependencyEnvelope::Written(TaskResult::Err(dep_parse_error(dep_idx, &e)))
        }
    }
}

/// The entry of a SKIPPED dependency (Python `core/workflows/engine.py:2830-2835`).
pub(crate) fn upstream_skipped_error(dep_idx: i32) -> TaskError {
    TaskError {
        exception: None,
        error_code: OutcomeCode::UpstreamSkipped.into(),
        data: Some(serde_json::json!({ "dependency_index": dep_idx })),
        message: Some("Upstream dependency was SKIPPED".to_owned()),
        kind: TaskErrorKind::Plain,
    }
}

/// The entry of a COMPLETED or FAILED dependency whose stored result is NULL or empty:
/// `RESULT_DESERIALIZATION_ERROR` naming the source node (its id, or `-` when it has none, its index and its
/// task) and its status, with `data` `{"dependency_index": <index>}`. One form for the three readers: the
/// baseline function `horsies_workflow_source_value` (`0005_workflow_functions.sql`) and Python
/// `_missing_result_entry` (`core/workflows/engine.py`) build the same entry. Pure.
pub(crate) fn missing_result_error(
    dep_idx: i32,
    node_id: Option<&str>,
    task_name: &str,
    status: &str,
) -> TaskError {
    let node = node_id.unwrap_or("-");
    TaskError {
        exception: None,
        error_code: OperationalErrorCode::ResultDeserializationError.into(),
        data: Some(serde_json::json!({ "dependency_index": dep_idx })),
        message: Some(format!(
            "Dependency node {node} (index {dep_idx}, task {task_name}) is {status} with no stored result"
        )),
        kind: TaskErrorKind::Plain,
    }
}

fn dep_parse_error(dep_idx: i32, error: &serde_json::Error) -> TaskError {
    TaskError::builtin(
        OperationalErrorCode::ResultDeserializationError,
        format!(
            "failed to parse upstream task result at index {}: {}",
            dep_idx, error
        ),
    )
}

/// The input of a child build: the node input after the `args_from` merge, with its members in the
/// order in which Python checks them, and the `args_from` keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildBuildInput {
    /// The input text: the members of the stored input, then one member per `args_from` key in
    /// [`args_from_keys`](Self::args_from_keys) order, each `args_from` entry replaced by its `inner`
    /// result envelope. For a node with no `args_from`: the stored text, unchanged.
    pub(crate) text: String,
    /// The `args_from` keys in the order in which Python reads the stored `args_from` (`jsonb` key
    /// order: shorter keys first, then by bytes). Empty for a node with no `args_from`.
    pub(crate) args_from_keys: Vec<String>,
}

impl ChildBuildInput {
    /// The stored input of a node with no `args_from`, unchanged.
    pub(crate) fn without_args_from(text: String) -> Self {
        Self {
            text,
            args_from_keys: Vec::new(),
        }
    }

    /// The input text that the child's builder decodes.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The members of [`Self::text`] that hold `args_from` results; the
    /// other members are the static members of the stored input.
    pub fn args_from_keys(&self) -> &[String] {
        &self.args_from_keys
    }
}

/// The child build input of `merged`, the text of [`ArgsFromMerge::Merged`] for the same
/// `args_from`: the one place where a child's builder input is made. Each `args_from` entry is
/// replaced by its `inner` result envelope, because the builder runs in the engine and gets the
/// result itself, not the wrap that crosses to a worker (Python `core/workflows/engine.py:1164-1173`).
/// The members are written static members first, then the `args_from` members in `jsonb` key
/// order, so a decode in text order meets every static member before any `args_from` member, as
/// Python decodes the static members before it checks the `args_from` values (`:1063-1157`).
/// Each member keeps its text from `merged`, and each `args_from` member is the text of its
/// `inner` envelope: no value is parsed and written again.
///
/// Errors: [`StoredInputRefused`] for a merged text that does not parse, that is not an object
/// while `args_from` has keys (the merge writes an object then), or that holds a malformed
/// `args_from` entry. Pure.
pub(crate) fn child_build_input(
    merged: String,
    args_from: Option<&serde_json::Value>,
) -> Result<ChildBuildInput, StoredInputRefused> {
    let args_from_keys = jsonb_key_order(args_from);
    if args_from_keys.is_empty() {
        return Ok(ChildBuildInput::without_args_from(merged));
    }
    let members = merged_members(&merged)?;
    let mut unwrapped: Vec<(&str, &str)> = Vec::with_capacity(members.len());
    for (name, value) in &members {
        let value: &RawValue = value;
        let inner = args_from_inner_text(name, value).map_err(|refusal| StoredInputRefused {
            refusal: refusal.to_string(),
        })?;
        unwrapped.push((name.as_str(), inner.unwrap_or(value).get()));
    }
    let mut written: Vec<(&str, &str)> = unwrapped
        .iter()
        .filter(|(name, _)| !args_from_keys.iter().any(|key| key == *name))
        .copied()
        .collect();
    written.extend(args_from_keys.iter().filter_map(|key| {
        unwrapped
            .iter()
            .find(|(name, _)| *name == key.as_str())
            .copied()
    }));
    let text = object_text(&written);
    Ok(ChildBuildInput {
        text,
        args_from_keys,
    })
}

/// The members of a merged node input, each value as its text, in text order.
///
/// Errors: [`StoredInputRefused`] for a text that does not parse or that is not an object. Pure.
fn merged_members(merged: &str) -> Result<Vec<(String, &RawValue)>, StoredInputRefused> {
    let whole = serde_json::from_str::<&RawValue>(merged).map_err(|error| StoredInputRefused {
        refusal: error.to_string(),
    })?;
    match is_object_text(whole.get()) {
        true => object_members(whole.get()).map_err(|error| StoredInputRefused {
            refusal: error.to_string(),
        }),
        false => Err(StoredInputRefused {
            refusal: "a node input merged with args_from members is not a JSON object".to_owned(),
        }),
    }
}

/// The keys of a stored `args_from` object in `jsonb` key order (shorter keys first, then by
/// bytes), the order in which Python reads them. Empty when `args_from` is absent, not an object,
/// or empty. Pure.
fn jsonb_key_order(args_from: Option<&serde_json::Value>) -> Vec<String> {
    let mut keys: Vec<String> = args_from
        .and_then(serde_json::Value::as_object)
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default();
    keys.sort_by(|left, right| (left.len(), left.as_bytes()).cmp(&(right.len(), right.as_bytes())));
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dep(status: &str, result: Option<String>) -> DepResult {
        DepResult {
            node_id: Some("source".to_owned()),
            task_name: "produce".to_owned(),
            sub_definition_key: None,
            status: status.to_owned(),
            result,
        }
    }

    /// The `inner` envelope of the entry of `kwarg`, after a check of the wrap.
    fn inner_of(merged: &str, kwarg: &str) -> TaskResult<serde_json::Value> {
        let parsed: serde_json::Value = serde_json::from_str(merged).unwrap();
        let entry = parsed[kwarg].as_object().expect("an args_from entry");
        assert_eq!(entry[ARGS_FROM_ENVELOPE_KEY], true);
        assert_eq!(entry.len(), 1 + ARGS_FROM_ENTRY_KEYS.len());
        serde_json::from_value(entry["inner"].clone()).unwrap()
    }

    /// The merged input; a test that reaches a conflict fails.
    fn merged(
        input: &str,
        args_from: &Option<serde_json::Value>,
        dep_results: &HashMap<i32, DepResult>,
    ) -> String {
        match merge_args_from_sync(input, args_from, dep_results).unwrap() {
            ArgsFromMerge::Merged(merged) => merged,
            ArgsFromMerge::StaticMemberConflict(conflict) => {
                panic!("unexpected args_from conflict on '{}'", conflict.key)
            }
        }
    }

    /// The child build input holds the static members first, in their stored order, and the
    /// `args_from` members after them, in `jsonb` key order; it holds the members of the merged
    /// input, each `args_from` entry replaced by its `inner` envelope. Python builds the kwargs of
    /// `build_with` in the same order: the decoded node input, then the `args_from` map as it reads
    /// it from its `jsonb` column (`python/horsies/core/workflows/child_build.py`, `_input_kwargs`).
    #[test]
    fn a_child_build_input_holds_its_static_members_first() {
        let mut deps = HashMap::new();
        for index in [0, 1] {
            deps.insert(
                index,
                dep(
                    "COMPLETED",
                    Some(serde_json::to_string(&TaskResult::Ok(json!(index))).unwrap()),
                ),
            );
        }
        let args_from = Some(json!({"bb": 0, "a": 1}));
        let merged_text = merged(r#"{"z": 1, "c": 2}"#, &args_from, &deps);

        let input = child_build_input(merged_text, args_from.as_ref()).unwrap();

        assert_eq!(input.args_from_keys, vec!["a".to_owned(), "bb".to_owned()]);
        let position = |member: &str| {
            input
                .text
                .find(&format!("\"{member}\":"))
                .unwrap_or_else(|| panic!("member {member} in {}", input.text))
        };
        assert!(position("z") < position("c"), "{}", input.text);
        assert!(position("c") < position("a"), "{}", input.text);
        assert!(position("a") < position("bb"), "{}", input.text);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&input.text).unwrap(),
            json!({
                "z": 1,
                "c": 2,
                "bb": {"__h_task_result__": true, "ok": 0, "err": null},
                "a": {"__h_task_result__": true, "ok": 1, "err": null},
            })
        );
    }

    /// With no `args_from` the child build input is the stored text, unchanged, also when it is
    /// not an object.
    #[test]
    fn a_child_build_input_without_args_from_is_the_stored_text() {
        for args_from in [None, Some(json!({}))] {
            let input = child_build_input("null".to_owned(), args_from.as_ref()).unwrap();
            assert_eq!(input, ChildBuildInput::without_args_from("null".to_owned()));
        }
    }

    #[test]
    fn an_args_from_key_that_names_a_static_member_is_a_conflict() {
        let mut deps = HashMap::new();
        deps.insert(
            0,
            dep(
                "COMPLETED",
                Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            ),
        );
        let outcome =
            merge_args_from_sync(r#"{"x": 5, "y": 6}"#, &Some(json!({"x": 0})), &deps).unwrap();
        assert_eq!(
            outcome,
            ArgsFromMerge::StaticMemberConflict(ArgsFromConflict {
                key: "x".to_owned()
            })
        );
    }

    #[test]
    fn with_several_conflicts_the_first_key_in_stored_order_is_named() {
        // jsonb key order: shorter keys first, then by bytes. Python iterates in that order.
        let outcome = merge_args_from_sync(
            r#"{"aa": 1, "b": 2, "c": 3}"#,
            &Some(json!({"aa": 0, "c": 0, "b": 0})),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            outcome,
            ArgsFromMerge::StaticMemberConflict(ArgsFromConflict {
                key: "b".to_owned()
            })
        );
    }

    #[test]
    fn the_conflict_error_follows_python() {
        let workflow_id = Uuid::nil();
        let conflict = ArgsFromConflict {
            key: "x".to_owned(),
        };
        let task = conflict.node_error(workflow_id, 3, MergedNodeKind::Task);
        assert_eq!(task.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(
            task.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (task 3)")
        );
        assert_eq!(
            task.data,
            Some(json!({"workflow_id": workflow_id.to_string(), "task_index": 3}))
        );
        let child = conflict.node_error(workflow_id, 4, MergedNodeKind::SubWorkflow);
        assert_eq!(
            child.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (subworkflow task 4)")
        );
    }

    #[test]
    fn merge_no_args_from_returns_existing() {
        let result = merged(r#"{"x": 1}"#, &None, &HashMap::new());
        assert_eq!(result, r#"{"x": 1}"#);
    }

    #[test]
    fn merge_empty_args_from_returns_existing() {
        let args_from = Some(json!({}));
        let result = merged(r#"{"x": 1}"#, &args_from, &HashMap::new());
        assert_eq!(result, r#"{"x": 1}"#);
    }

    /// Python `core/workflows/engine.py:891-896`: the kwarg holds the wrap with the source task name, the source
    /// definition key and the dependency's envelope.
    #[test]
    fn a_completed_dependency_is_stored_in_pythons_wrap() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(serde_json::Value::from(42))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));

        let result = merged("{}", &Some(json!({"answer": 0})), &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            parsed,
            json!({"answer": {
                "__h_taskresult_envelope__": true,
                "source_task_name": "produce",
                "source_definition_key": null,
                "inner": {"__h_task_result__": true, "ok": 42, "err": null},
            }})
        );
        assert!(matches!(inner_of(&result, "answer"), TaskResult::Ok(value) if value == json!(42)));
    }

    #[test]
    fn a_sub_workflow_dependency_names_its_definition_key() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(json!({"n": 1}))).unwrap();
        deps.insert(
            1,
            DepResult {
                node_id: Some("child".to_owned()),
                task_name: "__sub_workflow:child".to_owned(),
                sub_definition_key: Some("app.child".to_owned()),
                status: "COMPLETED".to_owned(),
                result: Some(stored),
            },
        );
        let result = merged("{}", &Some(json!({"child": 1})), &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["child"]["source_task_name"], "__sub_workflow:child");
        assert_eq!(parsed["child"]["source_definition_key"], "app.child");
    }

    #[test]
    fn merge_failed_dep_wraps_in_task_result_err() {
        let mut deps = HashMap::new();
        deps.insert(0, dep("FAILED", None));

        let result = merged("{}", &Some(json!({"answer": 0})), &deps);
        let TaskResult::Err(error) = inner_of(&result, "answer") else {
            panic!("a dependency with no result must give an error entry");
        };
        assert!(error.message.unwrap().contains("no stored result"));
    }

    /// Python injects only the results that exist (`core/workflows/engine.py:841`): an index with no node row
    /// gets no kwarg.
    #[test]
    fn a_dependency_with_no_row_gets_no_kwarg() {
        let result = merged("{}", &Some(json!({"answer": 0})), &HashMap::new());
        assert_eq!(result, "{}");
    }

    /// Python reads dependency results from terminal rows only (`core/workflows/sql.py:523-530`): a dependency
    /// that is not terminal when join `any` or `quorum` promotes the node gets no kwarg, and the kwargs of the
    /// terminal dependencies stay (#892).
    #[test]
    fn a_dependency_that_is_not_terminal_gets_no_kwarg() {
        let stored = serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap();
        for status in ["PENDING", "READY", "ENQUEUED", "RUNNING"] {
            let mut deps = HashMap::new();
            deps.insert(0, dep(status, None));
            deps.insert(1, dep("COMPLETED", Some(stored.clone())));
            let result = merged("{}", &Some(json!({"waiting": 0, "done": 1})), &deps);
            let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
            assert!(parsed.get("waiting").is_none(), "{status}: {result}");
            assert!(
                matches!(inner_of(&result, "done"), TaskResult::Ok(value) if value == json!(1)),
                "{status}: {result}"
            );
        }
    }

    #[test]
    fn merge_preserves_existing_input_fields() {
        let mut deps = HashMap::new();
        let stored =
            serde_json::to_string(&TaskResult::Ok(serde_json::Value::from("hello"))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));

        let args_from = Some(json!({"dep_val": 0}));
        let result = merged(r#"{"existing": true}"#, &args_from, &deps);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["existing"], true);
        assert!(
            matches!(inner_of(&result, "dep_val"), TaskResult::Ok(value) if value == json!("hello"))
        );
    }

    #[test]
    fn a_skipped_dependency_gives_upstream_skipped_with_its_index() {
        let mut deps = HashMap::new();
        deps.insert(
            3,
            dep(
                "SKIPPED",
                Some(serde_json::to_string(&TaskResult::Ok(json!(1))).unwrap()),
            ),
        );
        let result = merged("{}", &Some(json!({"answer": 3})), &deps);
        let TaskResult::Err(error) = inner_of(&result, "answer") else {
            panic!("a skipped dependency must give an error entry");
        };
        assert_eq!(error.error_code.to_string(), "UPSTREAM_SKIPPED");
        assert_eq!(
            error.message.as_deref(),
            Some("Upstream dependency was SKIPPED")
        );
        assert_eq!(error.data, Some(json!({"dependency_index": 3})));
    }

    /// The entry of `horsies_workflow_source_value` (0005) and Python `_missing_result_entry`, for a NULL and an
    /// empty stored result.
    #[test]
    fn a_terminal_dependency_with_no_result_gives_result_deserialization_error() {
        for status in ["COMPLETED", "FAILED"] {
            for result in [None, Some(String::new())] {
                let mut deps = HashMap::new();
                deps.insert(2, dep(status, result.clone()));
                let merged = merged("{}", &Some(json!({"answer": 2})), &deps);
                let TaskResult::Err(error) = inner_of(&merged, "answer") else {
                    panic!("a {status} dependency with result {result:?} must give an error entry");
                };
                let expected = format!(
                    "Dependency node source (index 2, task produce) is {status} with no stored result"
                );
                assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
                assert_eq!(error.message.as_deref(), Some(expected.as_str()));
                assert_eq!(error.data, Some(json!({"dependency_index": 2})));
            }
        }
    }

    /// A node stored without a node id is named `-`, as in the baseline function.
    #[test]
    fn a_dependency_with_no_node_id_is_named_with_a_dash() {
        let error = missing_result_error(4, None, "produce", "COMPLETED");
        assert_eq!(
            error.message.as_deref(),
            Some("Dependency node - (index 4, task produce) is COMPLETED with no stored result")
        );
        assert_eq!(error.data, Some(json!({"dependency_index": 4})));
    }

    /// A child workflow's builder gets the result envelope, not the wrap (Python
    /// `core/workflows/engine.py:1164-1173`).
    #[test]
    fn a_child_builder_reads_the_inner_envelope() {
        let mut deps = HashMap::new();
        let stored = serde_json::to_string(&TaskResult::Ok(json!(3))).unwrap();
        deps.insert(0, dep("COMPLETED", Some(stored)));
        let args_from = Some(json!({"answer": 0}));
        let merged = merged(r#"{"x": 1}"#, &args_from, &deps);
        let input = child_build_input(merged, args_from.as_ref()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&input.text).unwrap();
        assert_eq!(
            parsed,
            json!({"x": 1, "answer": {"__h_task_result__": true, "ok": 3, "err": null}})
        );
    }

    /// The stored result of `u128::MAX`.
    const U128_MAX_ENVELOPE: &str =
        r#"{"__h_task_result__":true,"ok":340282366920938463463374607431768211455,"err":null}"#;
    /// The stored result of `i64::MIN - 1`.
    const BELOW_I64_ENVELOPE: &str =
        r#"{"__h_task_result__":true,"ok":-9223372036854775809,"err":null}"#;

    /// A dependency result and a static member with an integer outside the `i64` and `u64`
    /// range keep their stored text through the merge and the child build input, as Python
    /// reads any integer (`python/horsies/core/workflows/child_build.py`, `_input_kwargs`).
    #[test]
    fn an_integer_outside_the_i64_and_u64_range_keeps_its_stored_text() {
        let mut deps = HashMap::new();
        deps.insert(0, dep("COMPLETED", Some(U128_MAX_ENVELOPE.to_owned())));
        deps.insert(1, dep("COMPLETED", Some(BELOW_I64_ENVELOPE.to_owned())));
        let args_from = Some(json!({"max": 0, "below": 1}));
        let static_members =
            r#"{"limit":340282366920938463463374607431768211455,"floor":-9223372036854775809}"#;
        let wrap = |inner: &str| {
            format!(
                r#"{{"__h_taskresult_envelope__":true,"source_task_name":"produce","source_definition_key":null,"inner":{inner}}}"#
            )
        };

        let merged_text = merged(static_members, &args_from, &deps);

        assert_eq!(
            merged_text,
            format!(
                r#"{{"limit":340282366920938463463374607431768211455,"floor":-9223372036854775809,"max":{},"below":{}}}"#,
                wrap(U128_MAX_ENVELOPE),
                wrap(BELOW_I64_ENVELOPE),
            )
        );
        let input = child_build_input(merged_text, args_from.as_ref()).unwrap();
        assert_eq!(
            input.text,
            format!(
                r#"{{"limit":340282366920938463463374607431768211455,"floor":-9223372036854775809,"max":{U128_MAX_ENVELOPE},"below":{BELOW_I64_ENVELOPE}}}"#
            )
        );
    }

    /// A merged input that holds a malformed `args_from` entry, that does not parse, or that is
    /// not an object is a refused stored input.
    #[test]
    fn a_merged_input_the_builder_cannot_read_is_a_refused_stored_input() {
        let args_from = Some(json!({"answer": 0}));
        let no_inner =
            json!({"answer": {"__h_taskresult_envelope__": true, "source_task_name": "t"}})
                .to_string();
        assert_eq!(
            child_build_input(no_inner, args_from.as_ref()),
            Err(StoredInputRefused {
                refusal: "args_from envelope for kwarg 'answer' has no inner result".to_owned(),
            })
        );
        assert!(child_build_input("{".to_owned(), args_from.as_ref()).is_err());
        assert_eq!(
            child_build_input("[1]".to_owned(), args_from.as_ref()),
            Err(StoredInputRefused {
                refusal: "a node input merged with args_from members is not a JSON object"
                    .to_owned(),
            })
        );
    }

    #[test]
    fn a_malformed_entry_names_its_kwarg() {
        let entry: &RawValue =
            serde_json::from_str(r#"{"__h_taskresult_envelope__": true, "inner": null}"#).unwrap();
        assert_eq!(
            args_from_inner_text("dep", entry).err(),
            Some(ArgsFromEntryRefused::MissingSourceTaskName {
                kwarg: "dep".to_owned()
            })
        );
    }

    /// An entry gives the text of its `inner` envelope, as written; a value that is not an
    /// object, or an object whose marker is not `true`, is not an entry.
    #[test]
    fn an_entry_gives_the_text_of_its_inner_envelope() {
        let entry: &RawValue = serde_json::from_str(
            r#"{"__h_taskresult_envelope__": true, "source_task_name": "produce", "source_definition_key": "app.child", "inner": {"ok": 1.2300}}"#,
        )
        .unwrap();
        let inner = args_from_inner_text("dep", entry)
            .unwrap()
            .map(RawValue::get);
        assert_eq!(inner, Some(r#"{"ok": 1.2300}"#));
        for value in [
            "[1]",
            "7",
            r#""text""#,
            r#"{"__h_taskresult_envelope__": false, "inner": 1}"#,
            r#"{"__h_taskresult_envelope__": "true", "inner": 1}"#,
            r#"{"inner": 1}"#,
        ] {
            let value: &RawValue = serde_json::from_str(value).unwrap();
            assert!(
                matches!(args_from_inner_text("dep", value), Ok(None)),
                "{value}"
            );
        }
    }

    #[test]
    fn merge_invalid_existing_input_is_error() {
        let args_from = Some(json!({"dep_val": 0}));
        for input in [r#"{"existing": true"#, r#"{"a":1,"a":2}"#] {
            let err = merge_args_from_sync(input, &args_from, &HashMap::new()).unwrap_err();
            assert!(matches!(err, WorkflowError::Validation(_)), "{input}");
        }
    }

    #[test]
    fn merge_into_a_non_object_input_is_refused() {
        // `null` is not read as `{}`: only an object takes named members.
        let args_from = Some(json!({"dep_val": 0}));
        for input in ["[1]", "null"] {
            let err = merge_args_from_sync(input, &args_from, &HashMap::new()).unwrap_err();
            assert!(matches!(err, WorkflowError::Validation(_)), "{input}");
        }
    }

    #[test]
    fn merge_corrupt_dep_result_wraps_parse_error() {
        for stored in [
            "{not-json",
            r#"{"__h_task_result__":true,"ok":{"__h_task_result__":true},"err":null}"#,
        ] {
            let mut deps = HashMap::new();
            deps.insert(0, dep("COMPLETED", Some(stored.to_owned())));

            let result = merged("{}", &Some(json!({"answer": 0})), &deps);
            let TaskResult::Err(error) = inner_of(&result, "answer") else {
                panic!("a stored result that does not read must give an error entry");
            };
            assert_eq!(error.error_code.to_string(), "RESULT_DESERIALIZATION_ERROR");
            assert!(error
                .message
                .unwrap()
                .contains("failed to parse upstream task result"));
        }
    }
}
