//! The error data vector of a stale-crash `WORKER_CRASHED`
//! (`tests/fixtures/stored_forms/error_data.json`, `worker_crashed_stale`)
//! through `stale_attempt_command`: Python's `crash_result_json` text, with
//! its float and its two instants in Python's `isoformat()` form.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::*;
use crate::stored_form_vectors::error_data::vector;

fn instant(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap_or_else(|error| panic!("{text}: {error}"))
        .with_timezone(&Utc)
}

#[test]
fn the_stale_crash_result_is_python_s_text() {
    let vector = vector("worker_crashed_stale");
    let inputs = &vector.inputs;
    let text = |key: &str| -> String {
        inputs[key]
            .as_str()
            .unwrap_or_else(|| panic!("{key} is text"))
            .to_owned()
    };
    let candidate = StaleCandidate {
        task_id: Uuid::from_u128(1),
        task_name: "vector.task".to_owned(),
        queue_name: "default".to_owned(),
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
        retry_count: 0,
        max_retries: 0,
        task_options: None,
        good_until: None,
        claimed_by_worker_id: Some(text("claimed_by_worker_id")),
        worker_pid: Some(
            inputs["worker_pid"]
                .as_i64()
                .and_then(|pid| i32::try_from(pid).ok())
                .expect("a pid"),
        ),
        worker_hostname: Some(text("worker_hostname")),
        worker_process_name: Some("runner-0".to_owned()),
        started_at: instant(&text("started_at")),
        db_now: instant(&text("db_now")),
    };
    let stale_after = inputs["stale_after_ms"].as_u64().expect("a threshold");
    let thresholds = StaleRunningThresholds {
        stale_after: ThresholdMs::from_millis(stale_after).expect("within the integer range"),
        finalizing_stale_after: ThresholdMs::from_millis(60_000).expect("within the integer range"),
    };
    let command = stale_attempt_command(&candidate, thresholds).expect("the crash result encodes");
    assert_eq!(command.result_json, vector.text);
}
