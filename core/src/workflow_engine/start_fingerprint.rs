//! The start command of a top-level workflow run, version 1
//! ([`StartCommandV1`]), and its fingerprint: SHA-256 of one canonical JSON
//! array. The rules are the rules of the send fingerprint
//! (`EnqueueCommandV1`, `core/src/core/history/identity/fingerprint.rs`):
//! - the canonical form is one JSON array, as `serde_json` writes it (no
//!   space between tokens); the first element is the version;
//! - an absent value is JSON `null`;
//! - a stored text goes in as one JSON string that holds its bytes as the
//!   start stores them;
//! - the key is not in the command: the workflow id is not in it, as the send
//!   key is not in the send fingerprint.
//!
//! The command is what the start writes for the top-level workflow row and
//! for the node rows of that workflow, in the stored forms of the start
//! arguments (`StartArguments::top_start_command`, `start_statement.rs`).
//! The array of version 1:
//!
//! `[1, name, definition_key, on_error, output_task_index, success_policy, nodes]`
//!
//! `nodes` holds one array per node of the top-level workflow, in node index
//! order:
//!
//! `[task_index, node_id, task_name, input, queue_name, priority, dependencies,
//! args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success,
//! task_options, retention_class_key, is_subworkflow, sub_definition_key,
//! sub_workflow_params]`
//!
//! The stored texts in it: `success_policy`; the node `input`; `dependencies`
//! (a JSON array of node indexes, in the given order); `args_from` (a JSON
//! object of key to node index, in key order); `workflow_ctx_from` (a JSON
//! array of node ids, NULL for no list and for an empty list); `task_options`
//! (with `good_until` merged in). The queue and the priority of a sub-workflow
//! node are their stored forms (`"default"`, `100`).
//!
//! Not in the command:
//! - the start call time (`sent_at`), the trace carrier and the resend
//!   setting: facts of one call, not of the command;
//! - the pre-built child workflows of the sub-workflow roots and their nodes:
//!   their ids are new on each call, and the registered builder makes them
//!   from the node input and the definition key, which are in the command;
//! - the failed-node records: the child builds make them;
//! - `max_retries`, `execution` and `good_until` of a node: the task options
//!   give them; `sub_workflow_name`: the task name gives it.
//!
//! A released version never changes its form: another form is a new version,
//! and the start compares the stored version with the digest.

use serde::ser::{Serialize, SerializeTuple, Serializer};
use sha2::{Digest, Sha256};

/// The version of the canonical form of [`StartCommandV1`].
pub(crate) const START_COMMAND_FINGERPRINT_VERSION: i16 = 1;

/// Bytes of a start command fingerprint (SHA-256).
pub(crate) const START_COMMAND_FINGERPRINT_BYTES: usize = 32;

/// Elements of the canonical array of a [`StartCommandV1`].
const COMMAND_ELEMENTS: usize = 7;

/// Elements of the canonical array of a [`StartNodeV1`].
const NODE_ELEMENTS: usize = 17;

/// The fingerprint of a start command: its version and the SHA-256 digest of
/// its canonical form. Only [`StartCommandV1::fingerprint`] makes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StartFingerprint {
    digest: [u8; START_COMMAND_FINGERPRINT_BYTES],
}

impl StartFingerprint {
    /// The version of the canonical form. Pure.
    pub(crate) fn version(&self) -> i16 {
        START_COMMAND_FINGERPRINT_VERSION
    }

    /// The SHA-256 digest of the canonical form. Pure.
    pub(crate) fn digest(&self) -> &[u8] {
        &self.digest
    }
}

/// The start command of a top-level workflow run, version 1: the values of
/// the top-level workflow row and of its node rows, in their stored forms
/// (module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartCommandV1<'a> {
    pub(crate) name: &'a str,
    pub(crate) definition_key: Option<&'a str>,
    /// `fail` or `pause`.
    pub(crate) on_error: &'a str,
    pub(crate) output_task_index: Option<i32>,
    /// The stored JSON text of the success policy.
    pub(crate) success_policy: Option<&'a str>,
    /// The nodes of the top-level workflow, in node index order.
    pub(crate) nodes: Vec<StartNodeV1<'a>>,
}

/// One node of a [`StartCommandV1`], in its stored form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartNodeV1<'a> {
    pub(crate) task_index: i32,
    pub(crate) node_id: Option<&'a str>,
    pub(crate) task_name: &'a str,
    /// The encoded input text, as the client encoded it.
    pub(crate) input: &'a str,
    pub(crate) queue_name: &'a str,
    pub(crate) priority: i32,
    /// The stored JSON array of node indexes.
    pub(crate) dependencies: &'a str,
    /// The stored JSON object of key to node index, in key order.
    pub(crate) args_from: Option<&'a str>,
    /// The stored JSON array of node ids.
    pub(crate) workflow_ctx_from: Option<&'a str>,
    pub(crate) allow_failed_deps: bool,
    pub(crate) join_type: &'a str,
    pub(crate) min_success: Option<i32>,
    /// The stored task options text, with `good_until` merged in.
    pub(crate) task_options: Option<&'a str>,
    /// The retention class key of a task node; `None` on a sub-workflow node.
    pub(crate) retention_class_key: Option<&'a str>,
    pub(crate) is_subworkflow: bool,
    pub(crate) sub_definition_key: Option<&'a str>,
    /// `GIVEN` or `NOT_GIVEN` on a sub-workflow node; `None` on a task node.
    pub(crate) sub_workflow_params: Option<&'a str>,
}

impl StartCommandV1<'_> {
    /// The fingerprint of the command: SHA-256 of its canonical form,
    /// written into the digest as it is serialized (no copy of the inputs).
    /// Pure.
    ///
    /// # Panics
    ///
    /// When the serialization fails: the command holds texts, integers and
    /// booleans only, and the digest writer never fails (a defect).
    pub(crate) fn fingerprint(&self) -> StartFingerprint {
        let mut writer = DigestWriter(Sha256::new());
        serde_json::to_writer(&mut writer, self).expect(
            "a start command of texts, integers and booleans serializes into a writer that \
             never fails",
        );
        StartFingerprint {
            digest: writer.0.finalize().into(),
        }
    }

    /// The canonical form of the command, the bytes that
    /// [`Self::fingerprint`] digests. Pure.
    #[cfg(test)]
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a start command serializes")
    }
}

impl Serialize for StartCommandV1<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut array = serializer.serialize_tuple(COMMAND_ELEMENTS)?;
        array.serialize_element(&START_COMMAND_FINGERPRINT_VERSION)?;
        array.serialize_element(self.name)?;
        array.serialize_element(&self.definition_key)?;
        array.serialize_element(self.on_error)?;
        array.serialize_element(&self.output_task_index)?;
        array.serialize_element(&self.success_policy)?;
        array.serialize_element(&self.nodes)?;
        array.end()
    }
}

impl Serialize for StartNodeV1<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut array = serializer.serialize_tuple(NODE_ELEMENTS)?;
        array.serialize_element(&self.task_index)?;
        array.serialize_element(&self.node_id)?;
        array.serialize_element(self.task_name)?;
        array.serialize_element(self.input)?;
        array.serialize_element(self.queue_name)?;
        array.serialize_element(&self.priority)?;
        array.serialize_element(self.dependencies)?;
        array.serialize_element(&self.args_from)?;
        array.serialize_element(&self.workflow_ctx_from)?;
        array.serialize_element(&self.allow_failed_deps)?;
        array.serialize_element(self.join_type)?;
        array.serialize_element(&self.min_success)?;
        array.serialize_element(&self.task_options)?;
        array.serialize_element(&self.retention_class_key)?;
        array.serialize_element(&self.is_subworkflow)?;
        array.serialize_element(&self.sub_definition_key)?;
        array.serialize_element(&self.sub_workflow_params)?;
        array.end()
    }
}

/// A writer that feeds each byte into a SHA-256 state. A write never fails.
struct DigestWriter(Sha256);

impl std::io::Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
