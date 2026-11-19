//! The start log line of a worker profile (Python `clients/python/tests/unit/test_cli.py`,
//! `TestWorkerStartLog`): the resolved pool and task-send pool of each child
//! kind, the executor of the shared async children, and the pool of the
//! worker process with where its size comes from.

use std::num::NonZeroU32;

use serde_json::{json, Value};

use super::*;
use crate::core::config::child_sizing::{
    shared_async_executor_threads, shared_async_pool_connections,
};

fn parsed(document: Value) -> WorkerProfile {
    serde_json::from_value(document).expect("the profile is valid")
}

/// Python `TestWorkerStartLog::test_the_resolved_sizes_of_each_kind_are_logged`.
#[test]
fn the_log_line_states_the_resolved_sizes_of_each_kind() {
    let profile = parsed(json!({
        "isolated": {"kind": "isolated_children", "count": 2},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 25},
        "shared_thread": {
            "kind": "shared_thread_children",
            "count": 1,
            "seats": 8,
            "pool": {"max_size": {"kind": "pool_size", "connections": 5}, "check": true},
        },
    }));
    let async_seats = NonZeroU32::new(25).expect("25 seats");
    let line = describe_worker_profile(&profile);

    assert!(line.starts_with("isolated=2 ("), "{line}");
    assert!(
        line.contains("pool 0 to 1 connections, check off; task_send_pool 2 connections)"),
        "{line}"
    );
    let shared_async = format!(
        "pool 0 to {} connections, derived from seats, check off; executor {} threads, \
         derived from seats; task_send_pool 2 connections)",
        shared_async_pool_connections(async_seats).get(),
        shared_async_executor_threads(async_seats).get(),
    );
    assert!(line.contains(&shared_async), "{line}");
    assert!(line.contains("shared_thread=1x8 seats"), "{line}");
    assert!(
        line.contains("pool 0 to 5 connections, check on; task_send_pool 2 connections)"),
        "{line}"
    );
    assert!(
        line.contains(", pool=5 connections (derived from 35 seats), "),
        "{line}"
    );
}

/// Python `TestWorkerStartLog::test_a_set_parent_pool_is_logged_without_its_origin`.
#[test]
fn a_set_worker_pool_is_logged_without_its_origin() {
    let profile = parsed(json!({
        "isolated": {"kind": "isolated_children", "count": 2},
        "pool": {"max_size": {"kind": "pool_size", "connections": 12}},
    }));
    let line = describe_worker_profile(&profile);
    assert!(
        line.contains(", pool=12 connections, claim per queue per pass: "),
        "{line}"
    );
}
