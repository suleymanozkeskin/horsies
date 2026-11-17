//! The static input of a workflow node: one JSON object whose members keep
//! the text and the order in which they were set.
//!
//! Every node input is an object of named members, Python's keyword input:
//! the engine appends its own members (`args_from` results, the workflow
//! context, the workflow meta) to the stored object text, and a task reads
//! the members by name. A member value follows the rules of a user value
//! (strict JSON, at most 200 levels, no key that only the engine writes).

use horsies_core::core::codec::user_value::is_engine_key;
use horsies_core::core::codec::{check_json_text_with, TextRule};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// Why a node input, or one of its members, was refused. The text names a
/// member or a rule, never a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InputRefused {
    /// The value is not a JSON object.
    NotAnObject,
    /// The text breaks the strict read rule, or a member value breaks a rule
    /// of a user value.
    Unreadable(String),
    /// A member name is a key that only the engine writes.
    EngineKey(String),
}

impl std::fmt::Display for InputRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnObject => formatter.write_str("the input is not a JSON object"),
            Self::Unreadable(cause) => formatter.write_str(cause),
            Self::EngineKey(name) => write!(
                formatter,
                "member '{name}' has a name that only the engine writes"
            ),
        }
    }
}

/// The members of a node input, in the order they were set, each with its
/// encoded text.
#[derive(Debug, Clone)]
pub(crate) struct NodeInput {
    members: Vec<(String, Box<RawValue>)>,
}

impl NodeInput {
    /// The input with no member: `{}`.
    pub(crate) fn empty() -> Self {
        Self {
            members: Vec::new(),
        }
    }

    /// The members of `text`, which must be one JSON object.
    ///
    /// Pure. Errors: [`InputRefused::Unreadable`] for a text that breaks the
    /// strict read rule (syntax, a repeated key, a non-finite number) or a
    /// member value that breaks a rule of a user value;
    /// [`InputRefused::NotAnObject`] for any other value;
    /// [`InputRefused::EngineKey`] for a member name of the engine.
    pub(crate) fn from_object_text(text: &str) -> Result<Self, InputRefused> {
        check_json_text_with(text, TextRule::Stored)
            .map_err(|refused| InputRefused::Unreadable(refused.to_string()))?;
        if !text.trim_start().starts_with('{') {
            return Err(InputRefused::NotAnObject);
        }
        let members =
            read_members(text).map_err(|error| InputRefused::Unreadable(error.to_string()))?;
        let mut input = Self::empty();
        for (name, value) in members {
            input.set_member(&name, value)?;
        }
        Ok(input)
    }

    /// Set member `name` to `value`: in place when the input has it, else
    /// after the last member.
    ///
    /// Errors: [`InputRefused::EngineKey`] for a name of the engine;
    /// [`InputRefused::Unreadable`] for a value that breaks a rule of a user
    /// value. The input is unchanged after an error.
    pub(crate) fn set_member(
        &mut self,
        name: &str,
        value: Box<RawValue>,
    ) -> Result<(), InputRefused> {
        if is_engine_key(name) {
            return Err(InputRefused::EngineKey(name.to_owned()));
        }
        check_json_text_with(value.get(), TextRule::UserValue)
            .map_err(|refused| InputRefused::Unreadable(format!("member '{name}': {refused}")))?;
        match self.members.iter_mut().find(|(member, _)| member == name) {
            Some(entry) => entry.1 = value,
            None => self.members.push((name.to_owned(), value)),
        }
        Ok(())
    }

    /// Whether the input has member `name`.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.members.iter().any(|(member, _)| member == name)
    }

    /// The member names, in order.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.members.iter().map(|(name, _)| name.as_str())
    }

    /// The object text: the members in order, each value as encoded.
    pub(crate) fn to_text(&self) -> String {
        self.text_with(&[])
    }

    /// The object text with `extra` members (name and value text) after the
    /// members of the input. The extra values are written as given: the
    /// check of a node's input uses it for the engine members, which hold
    /// engine keys.
    pub(crate) fn text_with(&self, extra: &[(&str, &str)]) -> String {
        let members = self
            .members
            .iter()
            .map(|(name, value)| (name.as_str(), value.get()))
            .chain(extra.iter().copied());
        let mut text = String::from("{");
        for (position, (name, value)) in members.enumerate() {
            if position > 0 {
                text.push(',');
            }
            text.push_str(&quoted(name));
            text.push(':');
            text.push_str(value);
        }
        text.push('}');
        text
    }
}

/// `name` as a JSON string.
fn quoted(name: &str) -> String {
    serde_json::to_string(name)
        .unwrap_or_else(|error| unreachable!("a Rust string encodes as a JSON string: {error}"))
}

/// The members of an object text in their order, each value as its text.
/// The strict reader bounded the depth, so serde_json's own recursion limit
/// is off.
fn read_members(text: &str) -> Result<Vec<(String, Box<RawValue>)>, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    deserializer.disable_recursion_limit();
    let members = OrderedMembers::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(members.0)
}

/// The members of a JSON object in text order.
struct OrderedMembers(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for OrderedMembers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(MembersVisitor)
    }
}

struct MembersVisitor;

impl<'de> Visitor<'de> for MembersVisitor {
    type Value = OrderedMembers;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut members = Vec::new();
        while let Some(member) = map.next_entry::<String, Box<RawValue>>()? {
            members.push(member);
        }
        Ok(OrderedMembers(members))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> Box<RawValue> {
        RawValue::from_string(text.to_owned()).unwrap()
    }

    #[test]
    fn the_members_keep_their_order_and_text() {
        let input = NodeInput::from_object_text(r#"{"zeta": 1, "alpha": [1.50, 2]}"#).unwrap();
        assert_eq!(input.names().collect::<Vec<_>>(), vec!["zeta", "alpha"]);
        assert_eq!(input.to_text(), r#"{"zeta":1,"alpha":[1.50, 2]}"#);
    }

    #[test]
    fn a_set_member_replaces_in_place_or_appends() {
        let mut input = NodeInput::from_object_text(r#"{"a": 1, "b": 2}"#).unwrap();
        input.set_member("a", raw("3")).unwrap();
        input.set_member("c", raw("\"x\"")).unwrap();
        assert_eq!(input.to_text(), r#"{"a":3,"b":2,"c":"x"}"#);
        assert!(input.contains("c"));
        assert!(!input.contains("d"));
    }

    #[test]
    fn only_an_object_is_a_node_input() {
        for text in ["null", "[1]", "3", "\"text\""] {
            assert_eq!(
                NodeInput::from_object_text(text).unwrap_err(),
                InputRefused::NotAnObject,
                "{text}"
            );
        }
        assert_eq!(NodeInput::from_object_text("{}").unwrap().to_text(), "{}");
    }

    #[test]
    fn a_text_that_breaks_the_strict_rule_is_unreadable() {
        for text in ["{", r#"{"a": 1, "a": 2}"#, r#"{"a": NaN}"#, ""] {
            assert!(
                matches!(
                    NodeInput::from_object_text(text),
                    Err(InputRefused::Unreadable(_))
                ),
                "{text:?}"
            );
        }
    }

    #[test]
    fn engine_names_and_engine_keys_in_values_are_refused() {
        assert_eq!(
            NodeInput::from_object_text(r#"{"__h_workflow_ctx__": {}}"#).unwrap_err(),
            InputRefused::EngineKey("__h_workflow_ctx__".to_owned())
        );
        let nested = NodeInput::from_object_text(r#"{"a": {"__h_task_result__": true}}"#);
        assert!(
            matches!(nested, Err(InputRefused::Unreadable(_))),
            "{nested:?}"
        );
        let mut input = NodeInput::empty();
        assert!(input.set_member("__horsies_x", raw("1")).is_err());
        assert!(input
            .set_member("a", raw(r#"{"__builtin_task_code__": 1}"#))
            .is_err());
        assert_eq!(input.to_text(), "{}");
    }

    #[test]
    fn extra_members_follow_the_input_as_written() {
        let input = NodeInput::from_object_text(r#"{"a": 1}"#).unwrap();
        assert_eq!(
            input.text_with(&[("dep", r#"{"__h_task_result__":true}"#)]),
            r#"{"a":1,"dep":{"__h_task_result__":true}}"#
        );
        assert_eq!(NodeInput::empty().text_with(&[("x", "1")]), r#"{"x":1}"#);
    }
}
