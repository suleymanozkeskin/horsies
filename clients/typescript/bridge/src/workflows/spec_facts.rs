//! The pure facts of the conversion of a built workflow to spec data: the
//! retention class key of each task queue, and the stored options text of a
//! task node with a deadline.
//!
//! - Both functions are pure: they start no core and open no connection.
//!   The fork and stop rules apply, as for every core call.
//! - The core reads each fact again when it parses the spec data
//!   (`parse_workflow_spec_data`), so a fact that the package changes is a
//!   refusal there, not a stored value.

use horsies_core::{RetentionConfig, TaskOptions};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;
use crate::process_core::runtime::pure_call_refusal;
use crate::sends::send::SendRules;
use crate::sends::task_options::TaskOptionsRecord;
use crate::values::boundary::{instant_of_epoch_micros, BoundaryRefusal};

/// The retention class key of each queue in `queue_names`, in that order:
/// the queue mapping of the retention config of `rules`, else the default
/// class. Pure.
#[napi(ts_return_type = "NativeEnvelope<string[]>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn queue_retention_classes(
    rules: &SendRules,
    queue_names: Vec<String>,
) -> Envelope<Vec<String>> {
    sync_call("queue_retention_classes", || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        Envelope::Value(queue_classes(rules.retention(), &queue_names))
    })
}

/// The class key of each queue, in the order of `queue_names` (the core's
/// `resolve_queue_retention(queue).class_key()`). Pure.
fn queue_classes(retention: &RetentionConfig, queue_names: &[String]) -> Vec<String> {
    queue_names
        .iter()
        .map(|queue| retention.resolve_queue_retention(queue).class_key())
        .collect()
}

/// The stored options text of a node of the task with the checked `options`
/// and the deadline `epochMicros` (the core's `stored_send_options_text`).
/// Microseconds outside the instant range are `CLIENT_DEFECT`: the package
/// checks the deadline first. Pure.
#[napi(ts_return_type = "NativeEnvelope<string>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn node_task_options_text(
    options: &TaskOptionsRecord,
    epoch_micros: BigInt,
) -> Envelope<String> {
    sync_call("node_task_options_text", || {
        if let Some(refusal) = pure_call_refusal() {
            return Envelope::Thrown(refusal.thrown());
        }
        match node_options_text(options.options(), &epoch_micros) {
            Ok(text) => Envelope::Value(text),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The options text of `options` with the deadline at `epoch_micros`, or the
/// refusal of microseconds outside the instant range. Pure.
fn node_options_text(
    options: &TaskOptions,
    epoch_micros: &BigInt,
) -> Result<String, BoundaryRefusal> {
    let deadline = instant_of_epoch_micros("epochMicros", epoch_micros)?;
    Ok(options.stored_send_options_text(deadline))
}

#[cfg(test)]
mod tests {
    use horsies_core::Execution;

    use super::*;
    use crate::values::boundary::bigint_of_i64;

    /// The text of the vector `name` of the core's task options vectors.
    fn options_vector_text(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../core/tests/fixtures/stored_forms/task_options.json");
        let file = std::fs::read_to_string(&path).expect("the task options vectors are readable");
        let document: serde_json::Value =
            serde_json::from_str(&file).expect("the task options vectors are JSON");
        let vectors = document["vectors"]
            .as_array()
            .expect("the file lists vectors");
        let vector = vectors
            .iter()
            .find(|vector| vector["name"] == name)
            .expect("the file holds the vector");
        vector["text"]
            .as_str()
            .expect("a vector has a text")
            .to_owned()
    }

    #[test]
    fn an_unmapped_queue_has_the_default_class_and_a_mapped_queue_its_own() {
        let mut retention = RetentionConfig::default();
        let _previous = retention.queue_retention.insert("audit".to_owned(), None);
        let _previous = retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));
        let names = ["default", "audit", "bulk", "default"].map(str::to_owned);
        assert_eq!(
            queue_classes(&retention, &names),
            ["standard_30d", "forever", "standard_7d", "standard_30d"].map(str::to_owned)
        );
    }

    #[test]
    fn a_node_with_a_deadline_stores_the_text_of_its_vector() {
        let options = TaskOptions {
            task_name: "report".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: Some(5_000),
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        // 2026-08-12T09:10:12.654321+00:00
        let deadline = bigint_of_i64(1_786_525_812_654_321);
        assert_eq!(
            node_options_text(&options, &deadline),
            Ok(options_vector_text("node_with_a_deadline"))
        );
    }

    #[test]
    fn a_deadline_outside_the_instant_range_is_a_boundary_refusal() {
        let options = TaskOptions {
            task_name: "report".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        assert!(matches!(
            node_options_text(&options, &bigint_of_i64(i64::MAX)),
            Err(refusal) if refusal.subject == "epochMicros"
        ));
    }
}
