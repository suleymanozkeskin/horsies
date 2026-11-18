//! The members of a JSON object text, each value as its text.

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// The members of the JSON object `text`, each value as its text, in text
/// order. The callers pass a text that the strict read rule took, or the
/// text of a member of one, so no key repeats. A member value is not
/// parsed: an integer outside the `i64` and `u64` range keeps its digits.
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

#[cfg(test)]
mod tests {
    use super::object_members;

    #[test]
    fn each_member_keeps_its_text_in_text_order() {
        let text =
            r#"{"b": 340282366920938463463374607431768211455, "a": [1e-05, -9223372036854775809]}"#;
        let members: Vec<(String, &str)> = object_members(text)
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key, value.get()))
            .collect();
        assert_eq!(
            members,
            vec![
                ("b".to_owned(), "340282366920938463463374607431768211455"),
                ("a".to_owned(), "[1e-05, -9223372036854775809]"),
            ]
        );
    }

    #[test]
    fn a_text_that_is_not_one_object_is_refused() {
        for text in ["[1]", "1", r#"{"a":1} {}"#] {
            assert!(object_members(text).is_err(), "{text}");
        }
    }
}
