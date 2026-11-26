//! Workflow nodes: the references between nodes, the field tokens of a
//! node's input, the join rule, and the task node.
//!
//! A [`TaskNode`] comes from the generated `name::node()` or from
//! [`TaskFunction::node`]. It names its task and holds no fact of a
//! registration: the workflow builder of an app resolves the queue, the
//! options and the workflow context flag of the task against that app when
//! the node is added (`WorkflowSpecBuilder::task`). Its input type `I` and
//! output type `T` are the task's: `set` and `arg_from` take field tokens of
//! `I` only, and `arg_from` takes a source whose output type is the
//! field's, checked at compile time.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use horsies_core::core::codec::to_json_bytes_strict;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::value::RawValue;

use super::input::{InputRefused, NodeInput};
use crate::task::body::{
    decode_checked, decode_unit, encode_read_back, raw_value_of, unread_member,
    value_free_encode_text, value_parts, DecodeFailure, ValueParts,
};
use crate::{HorsiesError, HorsiesErrorCode, TaskFunction, TaskOutcome, UserErrorCode};

// ---------------------------------------------------------------------------
// Types and references
// ---------------------------------------------------------------------------

/// The output type of a node, compared when a spec names its output and
/// when a workflow is registered with an output type.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OutputType {
    id: TypeId,
    name: &'static str,
}

impl OutputType {
    pub(crate) fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// The type name, for messages.
    pub(crate) fn name(self) -> &'static str {
        self.name
    }
}

impl PartialEq for OutputType {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for OutputType {}

/// The identity of one [`WorkflowSpecBuilder`](super::WorkflowSpecBuilder):
/// a reference that one builder returns names a node of that builder only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BuilderId(u64);

impl BuilderId {
    /// A new identity, distinct from every earlier one in this process.
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// A reference to a node of one workflow builder, with no output type.
///
/// Only a builder makes one, when a node is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeRef {
    builder: BuilderId,
    index: usize,
}

impl NodeRef {
    pub(crate) fn new(builder: BuilderId, index: usize) -> Self {
        Self { builder, index }
    }

    /// The position of the node in its spec.
    pub fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn builder(&self) -> BuilderId {
        self.builder
    }
}

/// A reference to a node of one workflow builder whose output type is `T`.
///
/// Only a builder makes one, when a node is added. `arg_from` takes it with
/// a field token of the same output type.
#[derive(Debug, PartialEq, Eq)]
pub struct TypedNodeRef<T> {
    node: NodeRef,
    types: PhantomData<fn() -> T>,
}

impl<T> TypedNodeRef<T> {
    pub(crate) fn new(node: NodeRef) -> Self {
        Self {
            node,
            types: PhantomData,
        }
    }

    /// The position of the node in its spec.
    pub fn index(&self) -> usize {
        self.node.index
    }

    /// The reference without its output type.
    pub fn untyped(self) -> NodeRef {
        self.node
    }
}

impl<T> Clone for TypedNodeRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for TypedNodeRef<T> {}

impl<T> From<TypedNodeRef<T>> for NodeRef {
    fn from(value: TypedNodeRef<T>) -> Self {
        value.node
    }
}

impl<T> From<&TypedNodeRef<T>> for NodeRef {
    fn from(value: &TypedNodeRef<T>) -> Self {
        value.node
    }
}

impl From<&NodeRef> for NodeRef {
    fn from(value: &NodeRef) -> Self {
        *value
    }
}

/// The key of a node's result in a workflow, with its output type `T`
/// (Python `NodeKey`): the node id.
#[derive(Debug)]
pub struct NodeKey<T> {
    node_id: String,
    types: PhantomData<fn() -> T>,
}

impl<T> NodeKey<T> {
    /// The key of the node with id `node_id`.
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            types: PhantomData,
        }
    }

    /// The node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl<T> Clone for NodeKey<T> {
    fn clone(&self) -> Self {
        Self::new(self.node_id.clone())
    }
}

impl<T> PartialEq for NodeKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl<T> Eq for NodeKey<T> {}

/// The token of one field `F` of the input type `I` of a task or a child
/// workflow: the member name that serde reads.
///
/// `#[horsies::task]` generates one per parameter of a task
/// (`name::params::field()`), and `#[derive(WorkflowInput)]` one per field
/// of a child workflow's params type (`Params::field_name()`).
#[derive(Debug, PartialEq, Eq)]
pub struct InputField<I, F> {
    name: &'static str,
    types: PhantomData<fn() -> (I, F)>,
}

impl<I, F> InputField<I, F> {
    /// The token of the member `name`. The macros call it through
    /// `horsies::__private::input_field`.
    pub(crate) const fn raw(name: &'static str) -> Self {
        Self {
            name,
            types: PhantomData,
        }
    }

    /// The member name.
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl<I, F> Clone for InputField<I, F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I, F> Copy for InputField<I, F> {}

/// When a node with dependencies becomes ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Every dependency is terminal (the default).
    All,
    /// One dependency is COMPLETED.
    Any,
    /// `min_success` dependencies are COMPLETED: at least 1, and at most the
    /// node's dependencies (checked when the spec is built).
    Quorum { min_success: u32 },
}

// ---------------------------------------------------------------------------
// The parts every node has
// ---------------------------------------------------------------------------

/// The node id of a node: given, or made when the spec is built
/// (`<workflow name slug>:<index>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeIdChoice {
    Auto,
    Given(String),
}

/// The links of a node to the other nodes of its spec, and its join rule.
#[derive(Debug, Clone)]
pub(crate) struct NodeLinks {
    /// The nodes that this node waits for, in the order added, no repeat.
    pub(crate) dependencies: Vec<NodeRef>,
    /// The input members that take a dependency's result, by member name.
    pub(crate) args_from: BTreeMap<String, NodeRef>,
    /// The dependencies whose results the workflow context takes.
    pub(crate) workflow_context_from: Vec<NodeRef>,
    pub(crate) allow_failed_dependencies: bool,
    pub(crate) join: JoinType,
    pub(crate) node_id: NodeIdChoice,
}

impl NodeLinks {
    pub(crate) fn new() -> Self {
        Self {
            dependencies: Vec::new(),
            args_from: BTreeMap::new(),
            workflow_context_from: Vec::new(),
            allow_failed_dependencies: false,
            join: JoinType::All,
            node_id: NodeIdChoice::Auto,
        }
    }

    pub(crate) fn wait_for(&mut self, dependency: NodeRef) {
        if !self.dependencies.contains(&dependency) {
            self.dependencies.push(dependency);
        }
    }

    pub(crate) fn take_result(&mut self, member: &str, source: NodeRef) {
        self.wait_for(source);
        let _previous = self.args_from.insert(member.to_owned(), source);
    }
}

/// How a node's input is checked when the spec is built: the decode of the
/// input text (first argument) with the input type of its task or child
/// workflow, and the read-back compare of the members named in the second
/// argument, the node's own members. The other members of the text are the
/// engine members that the check adds; they are decoded and not compared.
pub(crate) type InputCheck = fn(&[u8], &[&str]) -> Result<(), DecodeFailure>;

/// The [`InputCheck`] of the input type `A`. A task with no parameters
/// (`()`) reads `null` and `{}`, as its body does, and refuses any member.
/// Another type decodes the text, and each member named in `compared`
/// must be held by the encoding of the decoded value at any depth
/// ([`unread_node_member`]). Pure.
pub(crate) fn decode_check<A: Serialize + DeserializeOwned + 'static>(
    bytes: &[u8],
    compared: &[&str],
) -> Result<(), DecodeFailure> {
    match TypeId::of::<A>() == TypeId::of::<()>() {
        true => decode_unit(bytes),
        false => {
            let decoded = decode_checked::<A>(bytes)?;
            unread_node_member(bytes, &decoded, compared)
        }
    }
}

/// The read-back compare of a node input (Python's node kwargs check with
/// `extra='forbid'`): `bytes` is the input text that decoded as `decoded`.
/// The members of `bytes` named in `compared` are compared with the
/// encoding of `decoded` ([`unread_member`]). The compare reads members as
/// texts and parses no number, so an integer of any width that the strict
/// read rule takes passes it. A member that the encoding does not hold, at
/// any depth, is a [`DecodeFailure`] at its key path and depth with
/// serde's text for a key that binds to no field (`unknown field`). The
/// callers pass a text that passed the strict read rule. Pure.
///
/// Errors: that failure; a decoded value that does not encode as JSON (the
/// value-free text of the encoder). The input text decoded, so it reads as
/// a JSON value.
pub(crate) fn unread_node_member<A: Serialize>(
    bytes: &[u8],
    decoded: &A,
    compared: &[&str],
) -> Result<(), DecodeFailure> {
    let encoded = to_json_bytes_strict(decoded).map_err(|error| DecodeFailure {
        path: ".".to_owned(),
        depth: 0,
        text: format!(
            "the decoded value does not encode as JSON: {}",
            value_free_encode_text(&error)
        ),
    })?;
    let given = match value_parts(raw_value_of(bytes)?) {
        ValueParts::Object(members) => ValueParts::Object(
            members
                .into_iter()
                .filter(|(key, _value)| compared.contains(&key.as_str()))
                .collect(),
        ),
        other @ (ValueParts::List(_) | ValueParts::Null | ValueParts::Scalar) => other,
    };
    let read = raw_value_of(&encoded)?;
    match unread_member(given, read) {
        None => Ok(()),
        Some(member) => Err(DecodeFailure {
            path: member.path,
            depth: member.depth,
            text: format!("unknown field `{}`", member.key),
        }),
    }
}

/// The input of a node from a whole value `value` of its input type: the
/// encode that a send makes, with its read-back (encode, decode, encode
/// again, compare). Pure.
///
/// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for a value that
/// does not encode or read back; `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED`
/// (HRS-026) for a value that is not a JSON object.
pub(crate) fn whole_input<V>(owner: &str, value: &V) -> Result<NodeInput, HorsiesError>
where
    V: Serialize + DeserializeOwned,
{
    let bytes = encode_read_back(value).map_err(|cause| {
        not_serializable(
            owner,
            &format!("input {}: {cause}", std::any::type_name::<V>()),
        )
    })?;
    let text = String::from_utf8(bytes)
        .map_err(|_| not_serializable(owner, "the encoded input is not UTF-8"))?;
    NodeInput::from_object_text(&text).map_err(|refused| input_refused(owner, &refused))
}

/// `input` with member `field` set to `value`, encoded with the read-back of
/// a send. Pure.
///
/// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for a value that
/// does not encode or read back, or breaks a rule of a user value;
/// `WORKFLOW_INVALID_KWARG_KEY` (HRS-019) for a member name of the engine.
pub(crate) fn input_with_member<F>(
    owner: &str,
    mut input: NodeInput,
    field: &str,
    value: &F,
) -> Result<NodeInput, HorsiesError>
where
    F: Serialize + DeserializeOwned,
{
    let bytes = encode_read_back(value)
        .map_err(|cause| not_serializable(owner, &format!("field '{field}': {cause}")))?;
    let text = String::from_utf8(bytes)
        .map_err(|_| not_serializable(owner, &format!("field '{field}': the text is not UTF-8")))?;
    let raw = RawValue::from_string(text).map_err(|error| {
        not_serializable(
            owner,
            &format!("field '{field}': {}", error.classify_text()),
        )
    })?;
    input
        .set_member(field, raw)
        .map_err(|refused| input_refused(owner, &refused))?;
    Ok(input)
}

/// The input of a node from the text of one JSON object. Pure.
///
/// Errors: as [`whole_input`], with `WORKFLOW_INVALID_KWARG_KEY` (HRS-019)
/// for a member name of the engine.
pub(crate) fn text_input(owner: &str, text: &str) -> Result<NodeInput, HorsiesError> {
    NodeInput::from_object_text(text).map_err(|refused| input_refused(owner, &refused))
}

/// The refusal of a node input, with Python's code for each cause.
pub(crate) fn input_refused(owner: &str, refused: &InputRefused) -> HorsiesError {
    match refused {
        InputRefused::NotAnObject => HorsiesError::new(format!(
            "the input of node {owner} is not a JSON object"
        ))
        .with_code(HorsiesErrorCode::WorkflowPositionalArgsNotSupported)
        .with_help(
            "a workflow node takes its input as named members: give the task a struct input or \
             several parameters",
        ),
        InputRefused::Unreadable(cause) => not_serializable(owner, cause),
        InputRefused::EngineKey(name) => HorsiesError::new(format!(
            "the input of node {owner} has the member '{name}', a name that only the engine \
             writes"
        ))
        .with_code(HorsiesErrorCode::WorkflowInvalidKwargKey)
        .with_help("keys that start with __h_ or __horsies_, and __builtin_task_code__, belong to the engine"),
    }
}

fn not_serializable(owner: &str, cause: &str) -> HorsiesError {
    HorsiesError::new(format!(
        "the input of node {owner} does not serialize: {cause}"
    ))
    .with_code(HorsiesErrorCode::WorkflowKwargsNotSerializable)
}

/// A serde_json error kind as text, with no value and no position.
trait ClassifyText {
    fn classify_text(&self) -> &'static str;
}

impl ClassifyText for serde_json::Error {
    fn classify_text(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Io => "an I/O failure",
            serde_json::error::Category::Syntax => "the text is not JSON",
            serde_json::error::Category::Data => "the value does not fit",
            serde_json::error::Category::Eof => "the text ends inside a value",
        }
    }
}

// ---------------------------------------------------------------------------
// TaskNode
// ---------------------------------------------------------------------------

/// The priority of a task node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodePriority {
    /// The configured priority of the node's queue.
    OfQueue,
    /// The priority that the node names: checked to be 1 to 100 when the
    /// spec is built.
    Given(u32),
}

/// The deadline of a task node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeDeadline {
    NoDeadline,
    At(DateTime<Utc>),
}

/// The queue of a task node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeQueue {
    /// The queue of the task's registration on the builder's app.
    OfTask,
    /// The queue that the node names. The app checks it when the workflow
    /// is registered or started.
    Given(String),
}

/// What a task node takes from the registration of its task on one app:
/// the queue, the stored options text (the retry options and the class) and
/// whether the task's input takes the workflow context. The app's catalog
/// of typed functions keeps one per task; the workflow builder reads it
/// when a node is added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRegistration {
    pub(crate) queue_name: String,
    pub(crate) task_options: Option<String>,
    pub(crate) accepts_workflow_ctx: bool,
}

/// A workflow node that runs a task with input `I` and output `T` (Python
/// `TaskNode`).
///
/// The generated `name::node()` and [`TaskFunction::node`] make one with an
/// empty input `{}`; set the input with [`Self::set_input`], [`Self::set`]
/// and [`Self::arg_from`], then add it with `WorkflowSpecBuilder::task`,
/// which resolves the task against the builder's app.
pub struct TaskNode<T, I> {
    pub(crate) task_name: String,
    pub(crate) input: NodeInput,
    pub(crate) queue: NodeQueue,
    pub(crate) priority: NodePriority,
    pub(crate) deadline: NodeDeadline,
    pub(crate) input_check: InputCheck,
    pub(crate) links: NodeLinks,
    types: PhantomData<fn() -> (T, I)>,
}

impl<T, I> std::fmt::Debug for TaskNode<T, I> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskNode")
            .field("task_name", &self.task_name)
            .field("node_id", &self.links.node_id)
            .field("queue", &self.queue)
            .field("join", &self.links.join)
            .finish_non_exhaustive()
    }
}

impl<T, I> TaskNode<T, I>
where
    I: Serialize + DeserializeOwned + 'static,
{
    /// A node of the task `task_name` with an empty input `{}`, the queue
    /// and the priority of the task, no deadline and no link. The spec
    /// build decodes its input with `I`.
    pub(crate) fn of_task(task_name: &str) -> Self {
        Self {
            task_name: task_name.to_owned(),
            input: NodeInput::empty(),
            queue: NodeQueue::OfTask,
            priority: NodePriority::OfQueue,
            deadline: NodeDeadline::NoDeadline,
            input_check: decode_check::<I>,
            links: NodeLinks::new(),
            types: PhantomData,
        }
    }
}

impl<T, I> TaskNode<T, I> {
    fn owner(&self) -> String {
        format!("'{}'", self.task_name)
    }

    /// Set the whole input of the task: the value is encoded and read back
    /// as a send does, and replaces every member set before.
    ///
    /// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for a value that
    /// does not encode or read back as `I` (a non-finite float, a key that
    /// only the engine writes); `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED`
    /// (HRS-026) for an input type that does not encode as a JSON object.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "a public builder method takes the input by value, as a send does; a \
                  reference would change every caller"
    )]
    pub fn set_input(mut self, value: I) -> Result<Self, HorsiesError>
    where
        I: Serialize + DeserializeOwned,
    {
        self.input = whole_input(&self.owner(), &value)?;
        Ok(self)
    }

    /// Set one field of the input, encoded and read back as a send does.
    ///
    /// Errors: `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031) for a value that
    /// does not encode or read back as `F`, a non-finite float included.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "a public builder method takes the field value by value, as a send does; a \
                  reference would change every caller"
    )]
    pub fn set<F>(mut self, field: InputField<I, F>, value: F) -> Result<Self, HorsiesError>
    where
        F: Serialize + DeserializeOwned,
    {
        self.input = input_with_member(&self.owner(), self.input, field.name(), &value)?;
        Ok(self)
    }

    /// Set the whole input from the text of one JSON object, as written.
    /// The spec build decodes it with `I`.
    ///
    /// Errors: `WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED` (HRS-026) for a text
    /// that is not an object; `WORKFLOW_KWARGS_NOT_SERIALIZABLE` (HRS-031)
    /// for a text that breaks the strict read rule or the rules of a user
    /// value; `WORKFLOW_INVALID_KWARG_KEY` (HRS-019) for a member name of
    /// the engine.
    pub fn input_json(mut self, text: &str) -> Result<Self, HorsiesError> {
        self.input = text_input(&self.owner(), text)?;
        Ok(self)
    }

    /// Take the result of `source` as the field `field` (Python
    /// `args_from`). The field's type is the receiving type
    /// `TaskOutcome<S, E>`, with `S` the source's output type; `E` reads the
    /// source's user codes (`UserCode` takes any). The node waits for
    /// `source`.
    pub fn arg_from<S, E>(
        mut self,
        field: InputField<I, TaskOutcome<S, E>>,
        source: TypedNodeRef<S>,
    ) -> Self {
        self.links.take_result(field.name(), source.untyped());
        self
    }

    /// Wait for `dependency`.
    pub fn waits_for<R: Into<NodeRef>>(mut self, dependency: R) -> Self {
        self.links.wait_for(dependency.into());
        self
    }

    /// Wait for each of `dependencies`.
    pub fn waits_for_all<D, R>(mut self, dependencies: D) -> Self
    where
        D: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        for dependency in dependencies {
            self.links.wait_for(dependency.into());
        }
        self
    }

    /// Give the task the results of `sources` in its workflow context. Each
    /// source must be a dependency of the node, and the task must take the
    /// context (`workflow_ctx` on the task attribute).
    pub fn workflow_context_from<D, R>(mut self, sources: D) -> Self
    where
        D: IntoIterator<Item = R>,
        R: Into<NodeRef>,
    {
        for source in sources {
            let source = source.into();
            if !self.links.workflow_context_from.contains(&source) {
                self.links.workflow_context_from.push(source);
            }
        }
        self
    }

    /// Set the node id: unique in the spec, `[A-Za-z0-9_\-:.]`, at most 128
    /// characters. Without one the spec build gives the node
    /// `<workflow name>:<index>`.
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.links.node_id = NodeIdChoice::Given(id.into());
        self
    }

    /// Run the task on `queue` in place of the task's queue. The app checks
    /// the queue when the workflow is registered or started.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = NodeQueue::Given(queue.into());
        self
    }

    /// Claim the task with `priority`, 1 (first) to 100 (last), in place of
    /// the priority of its queue.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = NodePriority::Given(priority);
        self
    }

    /// Run the node even when a dependency failed.
    pub fn allow_failed_dependencies(mut self, allow: bool) -> Self {
        self.links.allow_failed_dependencies = allow;
        self
    }

    /// Start when every dependency is terminal (the default).
    pub fn join_all(mut self) -> Self {
        self.links.join = JoinType::All;
        self
    }

    /// Start when one dependency is COMPLETED.
    pub fn join_any(mut self) -> Self {
        self.links.join = JoinType::Any;
        self
    }

    /// Start when `min_success` dependencies are COMPLETED.
    pub fn join_quorum(mut self, min_success: u32) -> Self {
        self.links.join = JoinType::Quorum { min_success };
        self
    }

    /// The deadline of the task: it expires when it has not started by then.
    pub fn good_until(mut self, deadline: DateTime<Utc>) -> Self {
        self.deadline = NodeDeadline::At(deadline);
        self
    }

    /// The key of the node's result, when the node has a given node id.
    /// A spec gives the key of every node: `WorkflowSpec::key`.
    pub fn key(&self) -> Option<NodeKey<T>> {
        match &self.links.node_id {
            NodeIdChoice::Given(id) => Some(NodeKey::new(id.clone())),
            NodeIdChoice::Auto => None,
        }
    }

    /// The task name.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}

impl<A, T, E> TaskFunction<A, T, E>
where
    A: Serialize + DeserializeOwned + 'static,
    T: Serialize + DeserializeOwned + 'static,
    E: UserErrorCode,
{
    /// A workflow node of the task with an empty input `{}` (Python
    /// `TaskNode(fn=task)`), as the generated `name::node()` makes it. The
    /// workflow builder that takes the node resolves the queue and the
    /// options of the task against its own app.
    pub fn node(&self) -> TaskNode<T, A> {
        TaskNode::of_task(self.task_name())
    }
}

/// The JSON text of `value` with the strict encoder, for an engine member of
/// the check of a node's input. Panics for a value that does not encode: the
/// callers pass values that the library builds.
pub(crate) fn engine_member_text<V: Serialize>(value: &V) -> String {
    let bytes = to_json_bytes_strict(value).unwrap_or_else(|error| {
        panic!("an engine member of a node input check does not encode: {error}")
    });
    String::from_utf8(bytes).unwrap_or_else(|_| unreachable!("serde_json writes UTF-8 text"))
}

#[cfg(test)]
pub(crate) mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct ChargeInput {
        pub(crate) order_id: u32,
        pub(crate) amount: f64,
    }

    /// A task node of `task_name` with input `I` and output `T`.
    pub(crate) fn task_node<T, I>(task_name: &str) -> TaskNode<T, I>
    where
        I: Serialize + DeserializeOwned + 'static,
    {
        TaskNode::of_task(task_name)
    }

    /// The registration of a task on queue `default`, with no options and
    /// no workflow context.
    pub(crate) fn default_registration() -> NodeRegistration {
        NodeRegistration {
            queue_name: "default".to_owned(),
            task_options: None,
            accepts_workflow_ctx: false,
        }
    }

    /// A builder of `name` on an app with no task. The spec tests add their
    /// nodes with [`add`], which gives each node a registration.
    pub(crate) fn test_builder(name: &str) -> crate::WorkflowSpecBuilder {
        crate::WorkflowSpecBuilder::new(
            name,
            std::sync::Arc::new(crate::send::TaskFunctions::new()),
        )
    }

    /// Add `node` to `builder` as the node of a task registered on queue
    /// `default`, with no options and no workflow context. The lookup of
    /// `WorkflowSpecBuilder::task` has tests of its own.
    pub(crate) fn add<T: 'static, I>(
        builder: &mut crate::WorkflowSpecBuilder,
        node: TaskNode<T, I>,
    ) -> TypedNodeRef<T> {
        builder.add_task(node, default_registration())
    }

    fn amount() -> InputField<ChargeInput, f64> {
        InputField::raw("amount")
    }

    /// The task of the tests of a generated node. No test registers it: a
    /// node needs no registration until a builder takes it.
    #[horsies::task("node_probe")]
    async fn node_probe(
        order_id: u32,
    ) -> Result<u32, horsies::TaskError<std::convert::Infallible>> {
        Ok(order_id)
    }

    /// The generated `name::node()` reads no process state: it names its
    /// task, with the queue of the task's registration and no input member.
    #[test]
    fn a_generated_node_names_its_task_and_reads_no_registration() {
        let node = node_probe::node();
        assert_eq!(node.task_name(), "node_probe");
        assert_eq!(node.queue, NodeQueue::OfTask);
        assert_eq!(node.input.to_text(), "{}");
        let node = node.queue("fast");
        assert_eq!(node.queue, NodeQueue::Given("fast".to_owned()));
    }

    #[test]
    fn set_input_stores_the_encoded_object_in_field_order() {
        let node = task_node::<u32, ChargeInput>("charge")
            .set_input(ChargeInput {
                order_id: 7,
                amount: 1.5,
            })
            .unwrap();
        assert_eq!(node.input.to_text(), r#"{"order_id":7,"amount":1.5}"#);
    }

    #[test]
    fn a_non_finite_float_is_refused_by_set() {
        let refused = task_node::<u32, ChargeInput>("charge")
            .set(amount(), f64::NAN)
            .unwrap_err();
        assert_eq!(
            refused.code,
            Some(HorsiesErrorCode::WorkflowKwargsNotSerializable)
        );
        assert!(refused.message.contains("'charge'"), "{refused}");
    }

    #[test]
    fn set_adds_a_member_to_the_input() {
        let node = task_node::<u32, ChargeInput>("charge")
            .set(amount(), 2.25)
            .unwrap();
        assert_eq!(node.input.to_text(), r#"{"amount":2.25}"#);
    }

    #[test]
    fn an_input_that_is_not_an_object_is_refused() {
        let refused = task_node::<u32, u64>("double").set_input(5).unwrap_err();
        assert_eq!(
            refused.code,
            Some(HorsiesErrorCode::WorkflowPositionalArgsNotSupported)
        );
        let refused = task_node::<u32, u64>("double")
            .input_json("[1, 2]")
            .unwrap_err();
        assert_eq!(
            refused.code,
            Some(HorsiesErrorCode::WorkflowPositionalArgsNotSupported)
        );
    }

    #[test]
    fn an_engine_member_name_is_refused() {
        let refused = task_node::<u32, ChargeInput>("charge")
            .input_json(r#"{"__h_workflow_meta__": {}}"#)
            .unwrap_err();
        assert_eq!(
            refused.code,
            Some(HorsiesErrorCode::WorkflowInvalidKwargKey)
        );
    }

    #[test]
    fn a_dependency_is_kept_once_and_arg_from_waits_for_its_source() {
        let builder = BuilderId::next();
        let first = TypedNodeRef::<u32>::new(NodeRef::new(builder, 0));
        let node = task_node::<u32, ChargeInput>("charge")
            .waits_for(first)
            .waits_for_all([first, first]);
        assert_eq!(node.links.dependencies, vec![first.untyped()]);
        let source = TypedNodeRef::<f64>::new(NodeRef::new(builder, 1));
        let field: InputField<ChargeInput, TaskOutcome<f64, std::convert::Infallible>> =
            InputField::raw("total");
        let node = node.arg_from(field, source);
        assert_eq!(
            node.links.dependencies,
            vec![first.untyped(), source.untyped()]
        );
        assert_eq!(node.links.args_from.get("total"), Some(&source.untyped()));
    }

    #[test]
    fn a_key_needs_a_given_node_id() {
        let node = task_node::<u32, ChargeInput>("charge");
        assert_eq!(node.key(), None);
        let node = node.node_id("charge-1");
        assert_eq!(node.key().unwrap().node_id(), "charge-1");
    }

    #[test]
    fn the_unit_check_reads_null_and_an_empty_object() {
        assert!(decode_check::<()>(b"{}", &[]).is_ok());
        assert!(decode_check::<()>(b"null", &[]).is_ok());
        assert!(decode_check::<()>(br#"{"a": 1}"#, &["a"]).is_err());
        assert!(decode_check::<ChargeInput>(
            br#"{"order_id": 1, "amount": 2}"#,
            &["order_id", "amount"]
        )
        .is_ok());
    }

    /// The input of [`a_node_member_of_400_digits_passes_the_key_check`]: a
    /// field that keeps the text of its value.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WideInput {
        amount: horsies_core::JsonText,
    }

    /// #1893: the read-back compare of a node input reads members as texts
    /// and parses no number, so an integer of 400 digits in a `JsonText`
    /// field passes. A compared member that the type does not read is still
    /// named.
    #[test]
    fn a_node_member_of_400_digits_passes_the_key_check() {
        let wide = "9".repeat(400);
        let input = format!(r#"{{"amount": {wide}}}"#);
        let decoded: WideInput = decode_checked(input.as_bytes()).unwrap();
        assert_eq!(decoded.amount.get(), wide);
        assert!(unread_node_member(input.as_bytes(), &decoded, &["amount"]).is_ok());
        assert!(decode_check::<WideInput>(input.as_bytes(), &["amount"]).is_ok());
        let unread = format!(r#"{{"amount": {wide}, "note": {wide}}}"#);
        let failure =
            unread_node_member(unread.as_bytes(), &decoded, &["amount", "note"]).unwrap_err();
        assert_eq!(failure.path, "note");
        assert_eq!(failure.text, "unknown field `note`");
    }

    #[test]
    fn a_token_gives_the_key_it_was_made_with_and_its_copy_gives_the_same() {
        let token: InputField<ChargeInput, u32> = crate::__private::input_field("user_id");
        let copy = token;
        assert_eq!(token.name(), "user_id");
        assert_eq!(copy.name(), "user_id");
    }

    #[test]
    fn builders_have_distinct_identities() {
        assert_ne!(BuilderId::next(), BuilderId::next());
    }
}
