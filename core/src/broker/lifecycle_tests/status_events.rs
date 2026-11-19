//! Port of Python `clients/python/tests/integration/test_status_events.py`.
//!
//! `test_a_start_sends_no_status_event` and `test_a_completion_sends_the_status_event`
//! are `broker::monitoring_schema_tests::a_start_sends_no_status_event` and
//! `::a_terminal_move_sends_task_done_and_the_status_event` (#621).

use serial_test::serial;

use super::support::*;
use crate::broker::terminalization::on_test_pool::terminalize;
use crate::core::lifecycle::{BatchSize, TerminalizationCommand};

/// Python `test_a_batch_move_sends_one_status_event_per_task`.
#[tokio::test]
#[serial]
async fn a_batch_move_sends_one_status_event_per_task() {
    let pool = pool().await;
    drain_pending_expiry(&pool).await;
    let mut events = Wakes::listen(&pool, "horsies_task_status").await;
    let mut task_ids = Vec::new();
    for seconds_ago in [3.0, 2.0, 1.0] {
        let task_id = seed_task(&pool, &TaskSeed::pending()).await;
        set_deadline(&pool, task_id, Deadline::PassedSecondsAgo(seconds_ago)).await;
        task_ids.push(task_id);
    }
    // The seeds and their deadlines are not status changes of a terminal move.
    let _before_the_move: Vec<String> = events.payloads(&pool).await;

    let outcomes = terminalize(
        &pool,
        &TerminalizationCommand::ExpirePendingTasks {
            batch_size: BatchSize::new(10).expect("positive batch size"),
            result_json: r#"{"err": "expired"}"#.to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("expire pending");

    assert_eq!(outcomes.len(), 3, "{outcomes:?}");
    let payloads = events.payloads(&pool).await;
    for task_id in &task_ids {
        let raw_id = task_id.to_string();
        assert_eq!(
            payloads
                .iter()
                .filter(|payload| **payload == raw_id)
                .count(),
            1,
            "{payloads:?}"
        );
    }
    cleanup_tasks(&pool, &task_ids).await;
}
