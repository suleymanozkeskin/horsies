//! Port of Python `tests/e2e/test_layer1_tasks.py::test_task_cancelled` at
//! the read that turns a terminal row into the result of `get_result`.

use uuid::Uuid;

use super::{parse_task_result_row, TaskResultRow};
use crate::core::task::error::{OutcomeCode, TaskError};
use crate::core::task::result::TaskResult;

/// A CANCELLED task reads as `TASK_CANCELLED` with its task id in `data`
/// (Python: `result.err.data['task_id'] == task_id`).
#[test]
#[ignore = "waits on #701: the CANCELLED read leaves the task id out of TaskError.data"]
fn a_cancelled_task_reads_task_cancelled_with_its_id_in_data() {
    let task_id = Uuid::new_v4();
    let row = TaskResultRow {
        id: task_id,
        status: "CANCELLED".to_owned(),
        result: None,
        failed_reason: None,
    };

    let read: TaskResult<serde_json::Value> =
        parse_task_result_row(row).expect("a terminal row reads");

    let TaskResult::Err(error) = read else {
        panic!("a cancelled task must read as an error");
    };
    assert_eq!(
        error.error_code,
        TaskError::builtin(OutcomeCode::TaskCancelled, "cancelled").error_code
    );
    assert_eq!(
        error.data,
        Some(serde_json::json!({"task_id": task_id.to_string()}))
    );
}
