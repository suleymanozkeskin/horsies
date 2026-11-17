//! The receiving type of a dependency's result: `TaskOutcome<S, E>`.
//!
//! A node that takes the result of a dependency (`arg_from`) gets the
//! dependency's stored result envelope as one input member (the child
//! replaces the engine's wrap with its inner envelope, as Python's child
//! does). The field of that member has the type `TaskOutcome<S, E>`, the
//! value that a result read returns: `Succeeded(S)` for the value the
//! dependency returned, `Failed(TaskError<E>)` for its error or for a code
//! the library recorded (`UPSTREAM_SKIPPED` for a skipped dependency with
//! `allow_failed_deps`). `E` reads the user codes of the dependency's task;
//! `UserCode` reads any user code.
//!
//! The serde form is the stored envelope:
//! `{"__h_task_result__": true, "ok": <value>, "err": null}` or
//! `{"__h_task_result__": true, "ok": null, "err": {...}}`.

use std::collections::BTreeMap;

use horsies_core::{OutcomeCode, TaskResult, UserCode};
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::node::engine_member_text;
use crate::{TaskError, TaskOutcome, UserErrorCode};

impl<T, E> Serialize for TaskOutcome<T, E>
where
    T: Serialize,
    E: UserErrorCode,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Succeeded(value) => TaskResult::<&T>::Ok(value).serialize(serializer),
            Self::Failed(error) => {
                TaskResult::<&T>::Err(error.clone().into_stored()).serialize(serializer)
            }
        }
    }
}

/// The envelope is read as a JSON value first, then by the core's envelope
/// reader: the reader takes the envelope as raw text, which a JSON value
/// gives under any outer deserializer (the field path tracker of an input
/// decode included).
impl<'de, T, E> Deserialize<'de> for TaskOutcome<T, E>
where
    T: DeserializeOwned,
    E: UserErrorCode,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let envelope = serde_json::Value::deserialize(deserializer)?;
        match TaskResult::<T>::deserialize(envelope).map_err(D::Error::custom)? {
            TaskResult::Ok(value) => Ok(Self::Succeeded(value)),
            TaskResult::Err(stored) => TaskError::<E>::from_stored(stored)
                .map(Self::Failed)
                .map_err(D::Error::custom),
        }
    }
}

/// The result of a workflow with no output node (Python's outputless
/// result): the outcome of each node by node id, with the value as JSON and
/// the user codes as text, because the nodes are of many tasks.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputlessResults {
    by_node_id: BTreeMap<String, TaskOutcome<serde_json::Value, UserCode>>,
}

impl OutputlessResults {
    /// The outcome of the node `node_id`, when the result holds it.
    pub fn get(&self, node_id: &str) -> Option<&TaskOutcome<serde_json::Value, UserCode>> {
        self.by_node_id.get(node_id)
    }

    /// Every node id and its outcome, by node id.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &TaskOutcome<serde_json::Value, UserCode>)> {
        self.by_node_id
            .iter()
            .map(|(node_id, outcome)| (node_id.as_str(), outcome))
    }

    /// The number of nodes in the result.
    pub fn len(&self) -> usize {
        self.by_node_id.len()
    }

    /// Whether the result holds no node.
    pub fn is_empty(&self) -> bool {
        self.by_node_id.is_empty()
    }

    pub(crate) fn new(
        by_node_id: BTreeMap<String, TaskOutcome<serde_json::Value, UserCode>>,
    ) -> Self {
        Self { by_node_id }
    }
}

impl<'de> Deserialize<'de> for OutputlessResults {
    /// An object of node id to the stored result of that node, each read as
    /// a `TaskOutcome`. It lets an outputless definition meet the
    /// `DeserializeOwned` bound of the registration, the start and the read;
    /// the workflow handle reads the stored result of an outputless workflow
    /// through its own envelope.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::<String, TaskOutcome<serde_json::Value, UserCode>>::deserialize(deserializer)
            .map(Self::new)
    }
}

/// The text of the result member that the check of a node's input puts in
/// place of each `args_from` member: a failed dependency
/// (`UPSTREAM_SKIPPED`), which every `TaskOutcome<S, E>` reads.
pub(crate) fn placeholder_result_text() -> String {
    engine_member_text(&TaskResult::<()>::Err(horsies_core::TaskError::builtin(
        OutcomeCode::UpstreamSkipped,
        "placeholder of the input check",
    )))
}

/// The text of the workflow context that the check of a node's input puts
/// in the input of a task that takes it: a context with no result.
pub(crate) fn placeholder_context_text() -> String {
    engine_member_text(&serde_json::json!({
        "workflow_id": uuid::Uuid::nil(),
        "task_index": 0,
        "task_name": "",
        "results_by_id": {},
        "summaries_by_id": {},
    }))
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use horsies_core::BuiltInTaskCode;
    use serde_json::json;

    use super::*;
    use crate::TaskErrorCode;

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

    fn declined() -> serde_json::Value {
        json!({
            "__h_task_result__": true,
            "ok": null,
            "err": {"error_code": "DECLINED", "message": "card declined"},
        })
    }

    #[test]
    fn outputless_results_read_each_node_by_its_id() {
        let stored = json!({
            "a": {"__h_task_result__": true, "ok": {"rows": 2}, "err": null},
            "b": declined(),
        });
        let results: OutputlessResults = serde_json::from_value(stored).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results.get("a"),
            Some(&TaskOutcome::Succeeded(json!({"rows": 2})))
        );
        let Some(TaskOutcome::Failed(error)) = results.get("b") else {
            panic!("a stored error reads as Failed");
        };
        assert_eq!(error.code().text(), "DECLINED");
        assert_eq!(results.get("c"), None);
    }

    #[test]
    fn outputless_results_refuse_a_node_that_is_not_a_stored_result() {
        let refused = serde_json::from_value::<OutputlessResults>(json!({"a": 3}));
        assert!(refused.is_err());
    }

    #[test]
    fn a_stored_value_reads_as_succeeded() {
        let stored = json!({"__h_task_result__": true, "ok": 41, "err": null});
        let outcome: TaskOutcome<u32, Infallible> = serde_json::from_value(stored).unwrap();
        assert_eq!(outcome, TaskOutcome::Succeeded(41));
    }

    #[test]
    fn a_stored_user_code_reads_with_the_code_type_or_as_text() {
        let outcome: TaskOutcome<u32, ChargeCode> = serde_json::from_value(declined()).unwrap();
        let TaskOutcome::Failed(error) = outcome else {
            panic!("a stored error reads as Failed");
        };
        assert_eq!(error.code(), &TaskErrorCode::User(ChargeCode::Declined));
        let as_text: TaskOutcome<u32, UserCode> = serde_json::from_value(declined()).unwrap();
        let TaskOutcome::Failed(error) = as_text else {
            panic!("a stored error reads as Failed");
        };
        assert_eq!(error.code().text(), "DECLINED");
    }

    #[test]
    fn a_user_code_that_the_code_type_does_not_name_is_refused() {
        let refused = serde_json::from_value::<TaskOutcome<u32, Infallible>>(declined());
        assert!(refused.is_err());
    }

    #[test]
    fn an_outcome_writes_the_stored_envelope() {
        let written = serde_json::to_value(TaskOutcome::<u32, Infallible>::Succeeded(3)).unwrap();
        assert_eq!(
            written,
            json!({"__h_task_result__": true, "ok": 3, "err": null})
        );
        let failed = TaskOutcome::<u32, ChargeCode>::Failed(TaskError::user(
            ChargeCode::Declined,
            "card declined",
        ));
        let read: TaskOutcome<u32, ChargeCode> =
            serde_json::from_value(serde_json::to_value(&failed).unwrap()).unwrap();
        assert_eq!(read, failed);
    }

    #[test]
    fn the_placeholder_reads_as_every_outcome_type() {
        let text = placeholder_result_text();
        let read: TaskOutcome<Vec<String>, ChargeCode> = serde_json::from_str(&text).unwrap();
        let TaskOutcome::Failed(error) = read else {
            panic!("the placeholder is a failed dependency");
        };
        assert_eq!(
            error.code(),
            &TaskErrorCode::Library(BuiltInTaskCode::from(OutcomeCode::UpstreamSkipped))
        );
    }
}
