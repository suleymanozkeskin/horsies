//! The body adapters: the serde boundary between the core's child host and
//! a typed task body.
//!
//! The child host gives a body its input as one JSON value and takes back a
//! JSON value or the stored error. These functions decode the input with the
//! task's input type, check that a returned value reads back with the task's
//! output type, and turn a returned `TaskError<E>` into its stored form.
//!
//! A refusal is `WORKER_SERIALIZATION_ERROR`. Its text names the type, the
//! field path and the kind of the failure, and never a value: a part of a
//! decode message that comes from the input is written as
//! [`INPUT_PLACEHOLDER`] (Python `validation_summary`,
//! `core/codec/validation_errors.py`). Its `data` holds `expected_type` and
//! `validation_error`, the same text.

use std::any::TypeId;

use horsies_core::core::codec::{
    check_json_bytes, check_json_bytes_with, to_json_bytes_strict, TextRule, MAX_JSON_DEPTH,
};
use horsies_core::core::task::RawTaskResult;
use horsies_core::{OperationalErrorCode, TaskErrorKind, TaskResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::error::TaskError;
use super::user_code::UserErrorCode;

/// Written in place of a part of a decode message that comes from the input
/// (Python `INPUT_PLACEHOLDER`).
pub(crate) const INPUT_PLACEHOLDER: &str = "<input>";

/// The text of a failure whose message may hold a value, so only its kind is
/// kept.
const UNNAMED_DECODE_FAILURE: &str = "the value does not fit the declared type";

/// The text of an encode failure whose message may hold a value.
const UNNAMED_ENCODE_FAILURE: &str =
    "the value does not encode as JSON (a Serialize impl refused it)";

/// The stored input of a task with no parameters: no keyword arguments, as
/// Python stores them and as a workflow node gives them.
const NO_PARAMETERS_INPUT: &[u8] = b"{}";

/// The stored form of a task error: the core's `TaskError`.
pub type StoredTaskError = horsies_core::TaskError;

/// The task input, decoded as `A`.
///
/// `input` is the task input as the child host passes it: one JSON value.
/// Pure. Fails with `WORKER_SERIALIZATION_ERROR` when `input` breaks the
/// strict read rule of a stored text, or does not decode as `A`. The error
/// names `A`, the field path and the kind of the failure.
pub fn decode_input<A: DeserializeOwned>(input: &[u8]) -> Result<A, StoredTaskError> {
    check_json_bytes(input).map_err(|refused| {
        serialization_error::<A>(format!("task input is not strict JSON: {refused}"))
    })?;
    decode_checked::<A>(input).map_err(|failure| {
        serialization_error::<A>(format!(
            "task input does not decode as {}: {failure}",
            std::any::type_name::<A>()
        ))
    })
}

/// The input check of a schedule and of the static check (Python
/// `encode_kwargs_read_back` and `sig.bind`, which validate with
/// `extra='forbid'`): `input` decodes as `A` by the path the body takes
/// ([`decode_input`]), the decoded value reads back to itself
/// ([`encode_read_back`]), and each member of `input` that is not `null`,
/// at any depth, is a member that the read-back holds (`unread_member`).
/// The client's task bodies give it as `validate_input`. Pure.
///
/// Fails with `WORKER_SERIALIZATION_ERROR`; the text names `A`, the field
/// path or the key path of the member, never a value.
pub fn check_input<A>(input: &[u8]) -> Result<(), StoredTaskError>
where
    A: Serialize + DeserializeOwned,
{
    let decoded = decode_input::<A>(input)?;
    let read_back_failure = |cause: String| {
        serialization_error::<A>(format!(
            "task input of type {} does not read back: {cause}",
            std::any::type_name::<A>()
        ))
    };
    let encoded = encode_read_back(&decoded).map_err(read_back_failure)?;
    let given = decode_checked::<serde_json::Value>(input)
        .map_err(|failure| read_back_failure(failure.to_string()))?;
    let read = decode_checked::<serde_json::Value>(&encoded)
        .map_err(|failure| read_back_failure(failure.to_string()))?;
    match unread_member(&given, &read) {
        None => Ok(()),
        Some(member) => Err(serialization_error::<A>(format!(
            "task input names the member '{}', which {} does not read",
            member.path,
            std::any::type_name::<A>()
        ))),
    }
}

/// A member of an input that its read-back does not hold: a key that binds
/// to no field of the input type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnreadMember {
    /// The key path, in serde_path_to_error's form: `cfg.retries`,
    /// `items[2].name`.
    pub(crate) path: String,
    /// The key of the member: the last part of `path`.
    pub(crate) key: String,
    /// The number of keys and list positions in `path`: 1 for a member of
    /// the input itself, 2 for `cfg.retries`, 3 for `items[2].name`.
    pub(crate) depth: usize,
}

/// The first member of `given` with a value that is not `null` and that
/// `read` does not hold at the same place, at any depth. `given` is an
/// input, and `read` is the encoding of the value that it decoded to.
/// Python refuses such a key at any depth (`extra='forbid'` at an input).
/// Pure.
///
/// - An object member must be a member of the object at the same place in
///   `read`; its value is then compared with the value there.
/// - A list element is compared with the element at its position in `read`.
///   When that one does not hold it, any other element of `read` may: a set
///   orders and merges its elements. When none holds it, the member that
///   the element at its position leaves unread is named.
/// - A member with the value `null` is not compared: an `Option` field
///   that is `None` may be left out of `read`.
/// - An object or a list at a place where `read` holds another kind of
///   value is not compared: its type reads one form and writes another.
///
/// The compare cannot tell a key that binds to no field from one that a
/// field reads and does not write back: an alias (`#[serde(alias)]`), a
/// field with `#[serde(skip_serializing)]`, or a value that
/// `skip_serializing_if` leaves out. Each is named as unread.
///
/// Panics: for a value nested deeper than [`MAX_JSON_DEPTH`]. The callers
/// pass text that passed the strict read rule, which bounds the depth.
pub(crate) fn unread_member(
    given: &serde_json::Value,
    read: &serde_json::Value,
) -> Option<UnreadMember> {
    unread_below(given, Some(read), "", MAX_JSON_DEPTH)
}

/// [`unread_member`] of `given` at `path`, against `read` at the same place
/// (`None`: `read` has no value there), with `levels_left` levels of
/// nesting allowed for `given` and below.
fn unread_below(
    given: &serde_json::Value,
    read: Option<&serde_json::Value>,
    path: &str,
    levels_left: usize,
) -> Option<UnreadMember> {
    use serde_json::Value;
    match (given, read) {
        (Value::Object(members), Some(Value::Object(read_members))) => {
            let below = level_below(levels_left, path);
            members
                .iter()
                .filter(|(_key, value)| !value.is_null())
                .find_map(|(key, value)| {
                    let member = member_path(path, key);
                    match read_members.get(key) {
                        None => Some(UnreadMember {
                            path: member,
                            key: key.clone(),
                            depth: member_depth(levels_left),
                        }),
                        Some(read_value) => unread_below(value, Some(read_value), &member, below),
                    }
                })
        }
        (Value::Object(members), None) => members
            .iter()
            .find(|(_key, value)| !value.is_null())
            .map(|(key, _value)| UnreadMember {
                path: member_path(path, key),
                key: key.clone(),
                depth: member_depth(levels_left),
            }),
        (Value::Array(elements), Some(Value::Array(read_elements))) => {
            let below = level_below(levels_left, path);
            elements.iter().enumerate().find_map(|(position, element)| {
                let element_path = format!("{path}[{position}]");
                unread_element(element, position, read_elements, &element_path, below)
            })
        }
        (Value::Array(elements), None) => {
            let below = level_below(levels_left, path);
            elements.iter().enumerate().find_map(|(position, element)| {
                unread_below(element, None, &format!("{path}[{position}]"), below)
            })
        }
        (Value::Object(_) | Value::Array(_), Some(_))
        | (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_), _) => None,
    }
}

/// [`unread_member`] of the list element `element` at `path`: `None` when
/// the element of `read` at `position`, or else any element of `read`,
/// holds each of its members; else the member that the element at
/// `position` leaves unread.
fn unread_element(
    element: &serde_json::Value,
    position: usize,
    read: &[serde_json::Value],
    path: &str,
    levels_left: usize,
) -> Option<UnreadMember> {
    let unread = unread_below(element, read.get(position), path, levels_left)?;
    let held_elsewhere = read
        .iter()
        .any(|read_element| unread_below(element, Some(read_element), path, levels_left).is_none());
    match held_elsewhere {
        true => None,
        false => Some(unread),
    }
}

/// The depth ([`UnreadMember::depth`]) of a member of the object that
/// [`unread_below`] reads with `levels_left` levels. The input itself is
/// read with [`MAX_JSON_DEPTH`] levels, so its members have depth 1, and
/// each object or list below takes one level and adds one part to the
/// path. `levels_left` is never more than [`MAX_JSON_DEPTH`].
fn member_depth(levels_left: usize) -> usize {
    MAX_JSON_DEPTH - levels_left + 1
}

/// The levels of nesting allowed below the object or list at `path`.
///
/// Panics: when no level is left. The strict read rule bounds the depth of
/// the callers' text at [`MAX_JSON_DEPTH`].
fn level_below(levels_left: usize, path: &str) -> usize {
    match levels_left.checked_sub(1) {
        Some(below) => below,
        None => panic!(
            "an input value at '{path}' is nested deeper than {MAX_JSON_DEPTH} levels, \
             which the strict read rule refuses before this compare"
        ),
    }
}

/// The path of member `key` of the object at `path`.
fn member_path(path: &str, key: &str) -> String {
    match path.is_empty() {
        true => key.to_owned(),
        false => format!("{path}.{key}"),
    }
}

/// The input of a task with no parameters: `{}`, the input that a send of
/// `()` and a workflow node store (a node's input is an object, and the
/// child removes the engine members from it before the decode), or `null`,
/// which the core also takes as a task input at enqueue.
///
/// Pure. Fails with `WORKER_SERIALIZATION_ERROR` naming `()` when `input`
/// breaks the strict read rule of a stored text, and for any other value.
pub fn decode_unit_input(input: &[u8]) -> Result<(), StoredTaskError> {
    check_json_bytes(input).map_err(|refused| {
        serialization_error::<()>(format!("task input is not strict JSON: {refused}"))
    })?;
    decode_unit(input).map_err(|failure| {
        serialization_error::<()>(format!("task input does not decode as (): {failure}"))
    })
}

/// `()` from the text `null` or `{}`. A non-empty object names its first
/// key in serde's words for a type with no fields; any other value names
/// only its kind. The failure is at the value itself (`.`, depth 0), also
/// for a key. Pure.
pub(crate) fn decode_unit(input: &[u8]) -> Result<(), DecodeFailure> {
    let value = decode_checked::<serde_json::Value>(input)?;
    let text = match &value {
        serde_json::Value::Null => return Ok(()),
        serde_json::Value::Object(members) => match members.keys().next() {
            None => return Ok(()),
            Some(key) => format!("unknown field `{key}`, there are no fields"),
        },
        serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_)
        | serde_json::Value::Array(_) => {
            format!("invalid type: {INPUT_PLACEHOLDER}, expected null or an empty object")
        }
    };
    Err(DecodeFailure {
        path: ".".to_owned(),
        depth: 0,
        text,
    })
}

/// The stored value of what a task body returned.
///
/// - `Ok(value)`: the encoded value, after the read-back of
///   [`encode_output`]. A value that does not read back is
///   `WORKER_SERIALIZATION_ERROR`.
/// - `Err(error)`: the stored form of `error`.
///
/// Pure.
pub fn body_result<T, E>(returned: Result<T, TaskError<E>>) -> RawTaskResult
where
    T: Serialize + DeserializeOwned,
    E: UserErrorCode,
{
    match returned {
        Ok(value) => encode_output(&value),
        Err(error) => TaskResult::Err(error.into_stored()),
    }
}

/// The encoded return value: encode `value`, check the rules of a user value
/// (at most 200 levels, no engine key), decode the text as `T`, encode the
/// decoded value again and compare the bytes (Python
/// `encode_returned_result`). Any difference is `WORKER_SERIALIZATION_ERROR`:
/// the reader of the result would get another value than the body returned.
/// Pure.
pub(crate) fn encode_output<T>(value: &T) -> RawTaskResult
where
    T: Serialize + DeserializeOwned,
{
    match encode_read_back(value) {
        Ok(bytes) => TaskResult::Ok(bytes),
        Err(text) => TaskResult::Err(serialization_error::<T>(format!(
            "task returned a value of type {}: {text}",
            std::any::type_name::<T>()
        ))),
    }
}

/// The bytes of `value` when they read back as `T` to the same bytes, else
/// the value-free cause: the strict encode, the rules of a user value, the
/// decode with `T`, the second encode and the compare. The return check of
/// a body and the input check of a send share it. Pure.
pub(crate) fn encode_read_back<T>(value: &T) -> Result<Vec<u8>, String>
where
    T: Serialize + DeserializeOwned,
{
    let first = to_json_bytes_strict(value).map_err(|error| {
        format!(
            "it does not encode as JSON: {}",
            value_free_encode_text(&error)
        )
    })?;
    check_json_bytes_with(&first, TextRule::UserValue)
        .map_err(|refused| format!("it breaks a rule of a stored value: {refused}"))?;
    let decoded = decode_checked::<T>(&first)
        .map_err(|failure| format!("it does not read back as its type: {failure}"))?;
    let second = to_json_bytes_strict(&decoded).map_err(|error| {
        format!(
            "the value read back does not encode as JSON: {}",
            value_free_encode_text(&error)
        )
    })?;
    match first == second {
        true => Ok(first),
        false => Err(
            "it does not read back as its type: the value read back encodes to other JSON \
             (a field that serde writes and does not read, or reads in another form)"
                .to_owned(),
        ),
    }
}

/// The stored text of the input of a send (Python
/// `encode_kwargs_read_back`): `{}` for a task with no parameters (the input
/// type `()`), as Python stores no keyword arguments; else
/// [`encode_read_back`] of `input`. Pure.
///
/// Errors: the value-free cause of [`encode_read_back`]. The input `()`
/// does not fail.
pub(crate) fn encode_send_input<A>(input: &A) -> Result<Vec<u8>, String>
where
    A: Serialize + DeserializeOwned + 'static,
{
    match TypeId::of::<A>() == TypeId::of::<()>() {
        true => Ok(NO_PARAMETERS_INPUT.to_vec()),
        false => encode_read_back(input),
    }
}

/// One decode failure: where it is and what it is, with no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodeFailure {
    /// The field path, `.` for the value itself (serde_path_to_error's
    /// form: `items[2].name`).
    pub(crate) path: String,
    /// The number of keys and list positions in `path`: 0 for `.`, 1 for
    /// `amount`, 2 for `cfg.retries` and for `items[2]`. It is counted from
    /// the parts of the path, not from its text, in which a key may hold
    /// `.` or `[`. serde gives a key that binds to no field the path of the
    /// key itself.
    pub(crate) depth: usize,
    /// The kind of the failure, with [`INPUT_PLACEHOLDER`] for input parts.
    pub(crate) text: String,
}

impl std::fmt::Display for DecodeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.path, self.text)
    }
}

/// Decode `bytes` as `T`. The text passed a strict read rule, so its depth is
/// bounded and serde_json's own recursion limit is off (as the core's
/// `from_json_bytes`). Pure.
pub(crate) fn decode_checked<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeFailure> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    deserializer.disable_recursion_limit();
    let decoded: Result<T, _> = serde_path_to_error::deserialize(&mut deserializer);
    let value = decoded.map_err(|error| DecodeFailure {
        path: error.path().to_string(),
        depth: error.path().iter().len(),
        text: value_free_decode_text(error.inner()),
    })?;
    deserializer.end().map_err(|error| DecodeFailure {
        path: ".".to_owned(),
        depth: 0,
        text: value_free_decode_text(&error),
    })?;
    Ok(value)
}

/// The message of a serde_json decode error with every part that comes from
/// the input replaced by [`INPUT_PLACEHOLDER`], and without its position.
///
/// serde's messages that name the input: `invalid type: <input>, expected
/// <type>`, `invalid value: <input>, expected <what>`, `unknown variant
/// <input>, expected <variants>`. The part after the last `, expected ` comes
/// from the type. `missing field`, `duplicate field`, `invalid length` and
/// `unknown field` name the schema or a key of the input object (Python
/// keeps such keys too). Any other message, such as one that a hand-written
/// `Deserialize` impl writes, keeps only its kind. Pure.
pub(crate) fn value_free_decode_text(error: &serde_json::Error) -> String {
    let message = message_without_position(error);
    for prefix in ["invalid type: ", "invalid value: "] {
        if let Some(rest) = message.strip_prefix(prefix) {
            return with_placeholder(prefix, rest);
        }
    }
    if let Some(rest) = message.strip_prefix("unknown variant ") {
        return with_placeholder("unknown variant ", rest);
    }
    let names_no_value = [
        "missing field ",
        "duplicate field ",
        "invalid length ",
        "unknown field ",
        "data did not match any variant",
    ]
    .iter()
    .any(|prefix| message.starts_with(prefix));
    match names_no_value {
        true => message,
        false => UNNAMED_DECODE_FAILURE.to_owned(),
    }
}

/// `prefix`, the placeholder, and the expectation that follows the last
/// `, expected ` of `rest`.
fn with_placeholder(prefix: &str, rest: &str) -> String {
    match rest.rsplit_once(", expected ") {
        Some((_input, expected)) => format!("{prefix}{INPUT_PLACEHOLDER}, expected {expected}"),
        None => format!("{prefix}{INPUT_PLACEHOLDER}"),
    }
}

/// The message of a serde_json encode error, kept only when it names no
/// value: the strict encoder's non-finite float refusal (its text holds the
/// float kind, `NaN` or `inf`) and a map key that is not a string. Pure.
pub(crate) fn value_free_encode_text(error: &serde_json::Error) -> String {
    let message = message_without_position(error);
    let names_no_value = message.starts_with("non-finite ") || message == "key must be a string";
    match names_no_value {
        true => message,
        false => UNNAMED_ENCODE_FAILURE.to_owned(),
    }
}

/// serde_json writes ` at line L column C` after a message with a position.
fn message_without_position(error: &serde_json::Error) -> String {
    let text = error.to_string();
    match error.line() {
        0 => text,
        line => {
            let suffix = format!(" at line {line} column {}", error.column());
            match text.strip_suffix(&suffix) {
                Some(message) => message.to_owned(),
                None => text,
            }
        }
    }
}

/// `WORKER_SERIALIZATION_ERROR` for a value of type `V`, with the
/// value-free `message` as the message and as `data.validation_error`.
fn serialization_error<V>(message: String) -> StoredTaskError {
    let mut data = serde_json::Map::new();
    let _previous = data.insert(
        "expected_type".to_owned(),
        serde_json::Value::String(std::any::type_name::<V>().to_owned()),
    );
    let _previous = data.insert(
        "validation_error".to_owned(),
        serde_json::Value::String(message.clone()),
    );
    StoredTaskError {
        exception: None,
        error_code: horsies_core::TaskErrorCode::from(
            OperationalErrorCode::WorkerSerializationError,
        ),
        data: Some(serde_json::Value::Object(data)),
        message: Some(message),
        kind: TaskErrorKind::Plain,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: u32,
        items: Vec<Item>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Item {
        name: String,
        count: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum Shape {
        Circle,
        Square,
    }

    /// Writes a field that it never reads back.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WritesMore {
        kept: u32,
        #[serde(skip_deserializing)]
        dropped: u32,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum ChargeCode {
        Declined,
    }

    impl UserErrorCode for ChargeCode {
        const CODES: &'static [&'static str] = &["DECLINED"];

        fn code(&self) -> &'static str {
            match self {
                Self::Declined => "DECLINED",
            }
        }

        fn from_code(text: &str) -> Option<Self> {
            match text {
                "DECLINED" => Some(Self::Declined),
                _ => None,
            }
        }
    }

    const MARKER: &str = "secret-marker-7f3a";

    fn bytes(value: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn text_of(error: &StoredTaskError) -> String {
        serde_json::to_string(error).unwrap()
    }

    fn assert_serialization_error(error: &StoredTaskError) {
        assert_eq!(
            error.error_code,
            horsies_core::TaskErrorCode::from(OperationalErrorCode::WorkerSerializationError)
        );
    }

    /// Python `encode_kwargs_read_back` and `sig.bind`: the schedule check
    /// takes an input that decodes and reads back, and refuses a member that
    /// the type does not read, naming the member and not its value.
    #[test]
    fn check_input_takes_a_value_that_reads_back_and_names_a_member_it_does_not_read() {
        assert!(check_input::<Order>(br#"{"id": 1, "items": []}"#).is_ok());
        let unread =
            check_input::<Order>(br#"{"id": 1, "items": [], "note": "secret"}"#).unwrap_err();
        let message = unread.message.unwrap_or_default();
        assert!(message.contains("'note'"), "{message}");
        assert!(!message.contains("secret"), "{message}");
        assert!(check_input::<Order>(br#"{"id": 1, "items": [], "note": null}"#).is_ok());
    }

    /// A task input whose nested types do not refuse an unknown key on read
    /// (no `deny_unknown_fields`), as a user's types may be.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Shipment {
        label: String,
        options: ShipOptions,
        parcels: Vec<Parcel>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ShipOptions {
        #[serde(default)]
        retries: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Parcel {
        weight: u32,
    }

    /// The message of the refusal of `input` by the schedule check of
    /// [`Shipment`]; it never holds [`MARKER`], the value of the unread key.
    fn shipment_refusal(input: serde_json::Value) -> String {
        let refused = check_input::<Shipment>(&bytes(input)).unwrap_err();
        assert_serialization_error(&refused);
        assert!(!text_of(&refused).contains(MARKER));
        refused.message.unwrap_or_default()
    }

    /// #571, Python `_validate_input` with `extra='forbid'`: the schedule
    /// check refuses a key that binds to no field inside a nested object,
    /// and names its key path.
    #[test]
    fn check_input_names_an_unread_member_of_a_nested_object() {
        let message = shipment_refusal(json!({
            "label": "a", "options": {"retriess": MARKER}, "parcels": [],
        }));
        assert!(message.contains("'options.retriess'"), "{message}");
    }

    /// #571: the same inside an element of a list.
    #[test]
    fn check_input_names_an_unread_member_of_a_list_element() {
        let message = shipment_refusal(json!({
            "label": "a", "options": {},
            "parcels": [{"weight": 1}, {"weight": 2, "wieght": MARKER}],
        }));
        assert!(message.contains("'parcels[1].wieght'"), "{message}");
    }

    /// #571: a nested member whose value is `null` is not compared, as at
    /// the top level.
    #[test]
    fn check_input_takes_a_nested_member_whose_value_is_null() {
        let input = bytes(json!({
            "label": "a", "options": {"note": null}, "parcels": [{"weight": 1, "tag": null}],
        }));
        assert!(check_input::<Shipment>(&input).is_ok());
    }

    /// The worker read keeps its rule: a stored input that holds a key the
    /// type no longer has still decodes, at any depth.
    #[test]
    fn the_worker_read_ignores_a_key_that_the_type_no_longer_has() {
        let input = bytes(json!({
            "label": "a", "options": {"removed": 1}, "parcels": [{"weight": 1, "removed": 2}],
            "removed": 3,
        }));
        let shipment: Shipment = decode_input(&input).unwrap();
        assert_eq!(shipment.parcels[0].weight, 1);
    }

    /// [`unread_member`]: a member that `read` does not hold is named by its
    /// key path and its depth at each depth: in the object, in a nested
    /// object, and in an element of a list.
    #[test]
    fn an_unread_member_is_named_by_its_key_path_at_each_depth() {
        let read = json!({"a": {"b": 1}, "list": [{"c": 1}, {"c": 2}]});
        let unread = |given: serde_json::Value| unread_member(&given, &read);
        assert_eq!(unread(json!({"a": {"b": 1}, "list": []})), None);
        assert_eq!(
            unread(json!({"x": 1})),
            Some(UnreadMember {
                path: "x".to_owned(),
                key: "x".to_owned(),
                depth: 1,
            })
        );
        assert_eq!(
            unread(json!({"a": {"b": 1, "y": 2}})),
            Some(UnreadMember {
                path: "a.y".to_owned(),
                key: "y".to_owned(),
                depth: 2,
            })
        );
        assert_eq!(
            unread(json!({"list": [{"c": 1}, {"c": 2, "z": 3}]})),
            Some(UnreadMember {
                path: "list[1].z".to_owned(),
                key: "z".to_owned(),
                depth: 3,
            })
        );
    }

    /// [`unread_member`]: a set orders and merges its elements, so a list
    /// element held by another element of `read` is not named.
    #[test]
    fn a_list_element_held_by_another_element_of_the_read_back_is_not_named() {
        let read = json!([{"name": "a"}, {"name": "b", "colour": "red"}]);
        let given = json!([{"name": "b", "colour": "red"}, {"name": "a"}, {"name": "a"}]);
        assert_eq!(unread_member(&given, &read), None);
    }

    /// [`unread_member`]: an object at a place where `read` holds another
    /// kind of value is not compared (a type that reads one form and writes
    /// another).
    #[test]
    fn a_value_that_reads_back_in_another_form_is_not_compared() {
        let read = json!({"when": "2026-01-01"});
        let given = json!({"when": {"year": 2026, "month": 1, "day": 1}});
        assert_eq!(unread_member(&given, &read), None);
    }

    /// The input of [`a_decode_failure_has_the_depth_of_its_path`]: a key
    /// that holds `.`.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct DottedKey {
        #[serde(rename = "a.b")]
        _inner: Item,
    }

    /// [`decode_checked`]: the depth of a failure is the number of keys and
    /// list positions in its path, counted from its parts, so the key `a.b`
    /// is one part. A key that binds to no field has the path of the key.
    #[test]
    fn a_decode_failure_has_the_depth_of_its_path() {
        let depth = |text: &str| {
            decode_checked::<DottedKey>(text.as_bytes())
                .unwrap_err()
                .depth
        };
        assert_eq!(depth("1"), 0);
        assert_eq!(depth(r#"{"a.b": {"name": "x", "count": 1}} 1"#), 0);
        assert_eq!(depth(r#"{"a.b": {"name": "x", "count": 1}, "c": 1}"#), 1);
        assert_eq!(depth(r#"{"a.b": {"name": "x", "count": 1, "c": 1}}"#), 2);
        let refused =
            decode_checked::<Order>(br#"{"id": 1, "items": [{"name": "x", "count": "one"}]}"#)
                .unwrap_err();
        assert_eq!(refused.path, "items[0].count");
        assert_eq!(refused.depth, 3);
    }

    #[test]
    fn check_input_refuses_a_value_that_does_not_decode() {
        let wrong_type = check_input::<Order>(br#"{"id": "one", "items": []}"#).unwrap_err();
        assert_serialization_error(&wrong_type);
        assert!(!text_of(&wrong_type).contains("one"));
    }

    #[test]
    fn an_input_of_the_type_decodes() {
        let input = bytes(json!({"id": 7, "items": [{"name": "pen", "count": 2}]}));
        let order: Order = decode_input(&input).unwrap();
        assert_eq!(order.items[0].count, 2);
    }

    #[test]
    fn null_and_an_empty_object_stay_apart() {
        decode_input::<()>(b"null").unwrap();
        let error = decode_input::<()>(b"{}").unwrap_err();
        assert_serialization_error(&error);
        let map: BTreeMap<String, u32> = decode_input(b"{}").unwrap();
        assert!(map.is_empty());
    }

    /// A task with no parameters reads `{}` (a send, a workflow node) and
    /// `null`; any other input is refused with its kind and no value.
    #[test]
    fn a_unit_input_reads_null_and_an_empty_object() {
        decode_unit_input(b"null").unwrap();
        decode_unit_input(b" { } ").unwrap();
        let member = decode_unit_input(&bytes(json!({"secret-key-7": 1}))).unwrap_err();
        assert_serialization_error(&member);
        assert!(
            member
                .message
                .as_deref()
                .unwrap()
                .contains("unknown field `secret-key-7`"),
            "{member:?}"
        );
        let number = decode_unit_input(&bytes(json!(MARKER))).unwrap_err();
        assert_serialization_error(&number);
        assert!(!number.message.as_deref().unwrap().contains(MARKER));
        assert_serialization_error(&decode_unit_input(b"{").unwrap_err());
    }

    /// Python stores a send with no keyword arguments as `{}`.
    #[test]
    fn a_send_of_a_task_with_no_parameters_stores_an_empty_object() {
        assert_eq!(encode_send_input(&()).unwrap(), b"{}".to_vec());
    }

    #[test]
    fn a_send_of_any_other_input_stores_the_read_back_text() {
        let order = Order {
            id: 7,
            items: vec![],
        };
        assert_eq!(
            encode_send_input(&order).unwrap(),
            br#"{"id":7,"items":[]}"#.to_vec()
        );
        assert!(encode_send_input(&f64::NAN).is_err());
        assert_eq!(encode_send_input(&json!(null)).unwrap(), b"null".to_vec());
    }

    #[test]
    fn a_wrong_type_names_the_path_and_never_the_value() {
        let input = bytes(json!({"id": 7, "items": [{"name": "pen", "count": MARKER}]}));
        let error = decode_input::<Order>(&input).unwrap_err();
        assert_serialization_error(&error);
        let message = error.message.clone().unwrap();
        assert!(message.contains("items[0].count"), "{message}");
        assert!(
            message.contains("invalid type: <input>, expected u32"),
            "{message}"
        );
        assert!(message.contains("Order"), "{message}");
        assert!(!text_of(&error).contains(MARKER));
        let data = error.data.unwrap();
        assert!(data["expected_type"].as_str().unwrap().contains("Order"));
        assert_eq!(data["validation_error"].as_str(), Some(message.as_str()));
    }

    #[test]
    fn an_unknown_variant_hides_the_input_text() {
        let error = decode_input::<Shape>(&bytes(json!(MARKER))).unwrap_err();
        let message = error.message.clone().unwrap();
        assert!(
            message.contains("unknown variant <input>, expected `Circle` or `Square`"),
            "{message}"
        );
        assert!(!text_of(&error).contains(MARKER));
    }

    #[test]
    fn a_missing_field_and_an_unknown_key_are_named() {
        let missing = decode_input::<Item>(&bytes(json!({"name": "pen"}))).unwrap_err();
        assert!(missing.message.unwrap().contains("missing field `count`"));
        let unknown =
            decode_input::<Item>(&bytes(json!({"name": "pen", "count": 1, "colour": "red"})))
                .unwrap_err();
        let message = unknown.message.unwrap();
        assert!(message.contains("unknown field `colour`"), "{message}");
        assert!(!message.contains("red"));
    }

    #[test]
    fn an_input_that_is_not_strict_json_is_refused() {
        let error = decode_input::<u32>(b"NaN").unwrap_err();
        assert_serialization_error(&error);
        assert!(error.message.unwrap().contains("not strict JSON"));
    }

    #[test]
    fn a_custom_deserialize_message_keeps_only_its_kind() {
        let error = <serde_json::Error as serde::de::Error>::custom(format!("bad {MARKER}"));
        assert_eq!(value_free_decode_text(&error), UNNAMED_DECODE_FAILURE);
    }

    #[test]
    fn a_returned_value_that_reads_back_is_encoded() {
        let order = Order {
            id: 1,
            items: vec![],
        };
        match body_result::<Order, ChargeCode>(Ok(order.clone())) {
            TaskResult::Ok(encoded) => assert_eq!(encoded, serde_json::to_vec(&order).unwrap()),
            TaskResult::Err(error) => panic!("refused: {error}"),
        }
    }

    #[test]
    fn a_non_finite_float_is_refused() {
        match body_result::<f64, ChargeCode>(Ok(f64::NAN)) {
            TaskResult::Ok(_) => panic!("NaN was encoded"),
            TaskResult::Err(error) => {
                assert_serialization_error(&error);
                assert!(error.message.unwrap().contains("non-finite"));
            }
        }
    }

    #[test]
    fn an_engine_key_in_the_return_is_refused() {
        let value: BTreeMap<String, u32> = BTreeMap::from([("__h_task_result__".to_owned(), 1)]);
        match body_result::<BTreeMap<String, u32>, ChargeCode>(Ok(value)) {
            TaskResult::Ok(_) => panic!("an engine key was encoded"),
            TaskResult::Err(error) => {
                assert!(error.message.unwrap().contains("rule of a stored value"));
            }
        }
    }

    #[test]
    fn a_value_that_reads_back_to_other_json_is_refused() {
        let value = WritesMore {
            kept: 1,
            dropped: 2,
        };
        match body_result::<WritesMore, ChargeCode>(Ok(value)) {
            TaskResult::Ok(_) => panic!("a value that changes on read-back was encoded"),
            TaskResult::Err(error) => {
                assert_serialization_error(&error);
                assert!(error.message.unwrap().contains("encodes to other JSON"));
            }
        }
    }

    #[test]
    fn a_value_that_does_not_decode_as_its_type_is_refused_without_the_value() {
        // Serialized as a string, read as a number.
        #[derive(Debug)]
        struct TextNumber;

        impl Serialize for TextNumber {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(MARKER)
            }
        }

        impl<'de> Deserialize<'de> for TextNumber {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _number = u64::deserialize(deserializer)?;
                Ok(Self)
            }
        }

        match body_result::<TextNumber, ChargeCode>(Ok(TextNumber)) {
            TaskResult::Ok(_) => panic!("a value that does not read back was encoded"),
            TaskResult::Err(error) => {
                assert!(error
                    .message
                    .clone()
                    .unwrap()
                    .contains("does not read back"));
                assert!(!text_of(&error).contains(MARKER));
            }
        }
    }

    #[test]
    fn a_returned_error_is_stored_with_its_user_code() {
        let returned: Result<u32, TaskError<ChargeCode>> =
            Err(TaskError::user(ChargeCode::Declined, "card declined"));
        match body_result(returned) {
            TaskResult::Ok(_) => panic!("an error was encoded as a value"),
            TaskResult::Err(error) => {
                assert_eq!(error.error_code.to_string(), "DECLINED");
                assert_eq!(error.message.as_deref(), Some("card declined"));
            }
        }
    }
}
