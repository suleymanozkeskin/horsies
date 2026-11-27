//! Event handlers on a live worker: an app with a handler gets a handler
//! child (this binary in child mode), and the handler of `AttemptEnded`
//! gets the end of each attempt that the worker ran.
//!
//! ```text
//! HORSIES_TEST_DATABASE_URL=postgresql://... cargo test -p horsies-test-worker --test event_handlers
//! ```

use std::time::Duration;

use horsies_test_support::poll;
use horsies_test_worker::handlers::{attempt_ended_line, ATTEMPT_ENDED_MARKER};
use horsies_test_worker::harness::{succeeded, Suite};
use horsies_test_worker::instance::{Instance, Profile};
use horsies_test_worker::tasks::basic;
use serial_test::serial;

const BIN: &str = env!("CARGO_BIN_EXE_horsies-test-worker");

/// The longest wait for the outcome of a task that a running worker runs.
const RESULT_WAIT: Duration = Duration::from_secs(15);

/// The longest wait for the handler to record an event after the task
/// ended.
const HANDLER_WAIT: Duration = Duration::from_secs(20);

/// A task runs to its end; the handler of `AttemptEnded` in the handler
/// child records the event of its first attempt.
#[tokio::test]
#[serial]
async fn test_a_handler_receives_the_attempt_ended_event_of_a_task() {
    let suite = Suite::start(BIN, Instance::Handlers).await;
    let _worker = suite.worker(Profile::Isolated1).await;
    let handle = suite
        .function(basic::simple::function)
        .send(basic::simple::Input { x: 21 })
        .await
        .expect("send e2e_simple");
    assert_eq!(succeeded(&handle, RESULT_WAIT).await, 42);
    let line = attempt_ended_line(handle.task_id(), 1);
    poll::poll_until(
        HANDLER_WAIT,
        "the handler records the end of the attempt",
        async || {
            let recorded = suite.markers.read(ATTEMPT_ENDED_MARKER)?;
            recorded
                .lines()
                .any(|recorded_line| recorded_line == line)
                .then_some(())
        },
    )
    .await;
}
