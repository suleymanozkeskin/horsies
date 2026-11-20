//! The tasks of the end-to-end app, and their registration per instance
//! (Python `clients/python/tests/e2e/tasks/`).
//!
//! The test process and every process of the worker binary register the
//! same tasks for one instance, so the stored input of a send decodes in
//! the child that runs it.

use horsies::{Horsies, HorsiesError};

use crate::instance::Instance;

pub mod basic;
pub mod classes;
pub mod custom;
pub mod recovery;
pub mod retry;
pub mod scheduled;
pub mod subworkflow;
pub mod workflow;

/// Register the tasks of `instance` on `app`. Stops at the first refusal.
pub fn register(app: &mut Horsies, instance: Instance) -> Result<(), HorsiesError> {
    match instance {
        Instance::CustomQueues => register_custom(app),
        Instance::RecoveryQueues => register_recovery_queues(app),
        Instance::Scheduler => {
            register_default(app)?;
            register_scheduled(app)
        }
        Instance::Default
        | Instance::ClusterCap
        | Instance::Recovery
        | Instance::SoftCap
        | Instance::Handlers => register_default(app),
    }
}

/// The tasks of the default queue: basic, retry and the execution classes.
fn register_default(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _healthcheck = basic::healthcheck::register(app)?;
    let _simple = basic::simple::register(app)?;
    let _kwargs = basic::kwargs::register(app)?;
    let _error = basic::error::register(app)?;
    let _no_retry = basic::no_retry::register(app)?;
    let _error_code = basic::error_code::register(app)?;
    let _complex_result = basic::complex_result::register(app)?;
    let _slow = basic::slow::register(app)?;
    let _timeout_sleeper = basic::timeout_sleeper::register(app)?;
    let _async_timeout_sleeper = basic::async_timeout_sleeper::register(app)?;
    let _timeout_retry_sleeper = basic::timeout_retry_sleeper::register(app)?;
    let _pid = basic::pid::register(app)?;
    let _panics = basic::panics::register(app)?;
    let _abort = basic::abort::register(app)?;
    let _idempotent = basic::idempotent::register(app)?;
    let _slow_idempotent = basic::slow_idempotent::register(app)?;
    let _wait_inside_task = basic::wait_inside_task::register(app)?;
    let _async_sends = basic::async_sends::register(app)?;
    let _stamp = basic::stamp::register(app)?;

    let _retry_exhausted = retry::retry_exhausted::register(app)?;
    let _retry_success = retry::retry_success::register(app)?;
    let _exponential = retry::exponential::register(app)?;
    let _retry_error_code = retry::retry_error_code::register(app)?;
    let _retry_other_code = retry::retry_other_code::register(app)?;
    let _retry_long_interval = retry::retry_long_interval::register(app)?;

    let _shared_healthcheck = classes::shared_healthcheck::register(app)?;
    let _thread_healthcheck = classes::thread_healthcheck::register(app)?;
    let _shared_sleep = classes::shared_sleep::register(app)?;
    let _shared_hang = classes::shared_hang::register(app)?;
    let _shared_cooperative = classes::shared_cooperative::register(app)?;
    let _shared_blocks_once = classes::shared_blocks_once::register(app)?;
    let _shared_stuck_once = classes::shared_stuck_once::register(app)?;
    let _thread_sleep = classes::thread_sleep::register(app)?;
    let _thread_hang = classes::thread_hang::register(app)?;
    let _thread_overrun = classes::thread_overrun::register(app)?;
    let _outlives_drain = classes::outlives_drain::register(app)?;
    register_workflow(app)
}

/// The node tasks of the workflow layers, then the sub-workflow tasks and
/// definitions.
fn register_workflow(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _step = workflow::step::register(app)?;
    let _slow_step = workflow::slow_step::register(app)?;
    let _final_result = workflow::final_result::register(app)?;
    let _fail = workflow::fail::register(app)?;
    let _produce_int = workflow::produce_int::register(app)?;
    let _double = workflow::double::register(app)?;
    let _sum_two = workflow::sum_two::register(app)?;
    let _check_upstream_skipped = workflow::check_upstream_skipped::register(app)?;
    let _produce_dict = workflow::produce_dict::register(app)?;
    let _receive_dict = workflow::receive_dict::register(app)?;
    let _ctx_reader = workflow::ctx_reader::register(app)?;
    let _ctx_sum = workflow::ctx_sum::register(app)?;
    let _mixed_reader = workflow::mixed_reader::register(app)?;
    let _fail_with = workflow::fail_with::register(app)?;
    let _mark = workflow::mark::register(app)?;
    let _slow_mark = workflow::slow_mark::register(app)?;
    let _retry_then_ok = workflow::retry_then_ok::register(app)?;
    let _ctx_reader_str = workflow::ctx_reader_str::register(app)?;
    subworkflow::register(app)
}

/// The tasks of the custom queue instance.
fn register_custom(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _healthcheck = custom::custom_healthcheck::register(app)?;
    let _high = custom::high::register(app)?;
    let _normal = custom::normal::register(app)?;
    let _low = custom::low::register(app)?;
    let _slow_high = custom::slow_high::register(app)?;
    let _slow_normal = custom::slow_normal::register(app)?;
    let _slow_low = custom::slow_low::register(app)?;
    Ok(())
}

/// The tasks of the recovery queue instance.
fn register_recovery_queues(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _healthcheck = recovery::recovery_healthcheck::register(app)?;
    let _wf_step = recovery::recovery_wf_step::register(app)?;
    Ok(())
}

/// The tasks that the schedules of the scheduler instance send.
fn register_scheduled(app: &mut Horsies) -> Result<(), HorsiesError> {
    let _simple = scheduled::scheduled_simple::register(app)?;
    let _with_args = scheduled::scheduled_with_args::register(app)?;
    let _catch_up = scheduled::catch_up_task::register(app)?;
    let _cron = scheduled::cron_task::register(app)?;
    Ok(())
}
