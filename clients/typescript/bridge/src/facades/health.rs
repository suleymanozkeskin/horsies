//! The health reads on the handle: the worker ping, the worker state
//! snapshots, the stale and expired task lists, and the execution class
//! coverage and report.
//!
//! - Each read connects the handle's broker and installs the schema once
//!   per handle, as a send does (Rust `Horsies::ping_workers` and the reads
//!   of `reads/health.rs`). A failed connect or install has the code of the
//!   read (`WORKER_PING_FAILED`, else `MONITORING_QUERY_FAILED`) and the
//!   text `<read> failed: <cause>`.
//! - A value that the read refuses (a ping window or a response count that
//!   is not a whole number above 0, a ping window above
//!   `WORKER_PING_WAIT_BOUND`, a history limit above
//!   `WORKER_HISTORY_ROW_BOUND` or a stale threshold outside its rule) is
//!   `INVALID_INPUT`, answered at once with no connect. A history read of
//!   every retained snapshot of a worker that holds more than the bound is
//!   `INVALID_INPUT` too; the core refuses it after the read.
//! - The reads write nothing but the schema install. Failures are the raw
//!   record of the core's `DatabaseOperationError`, with the read as the
//!   subject of its exception.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use horsies_core::handle::HandleListener;
use horsies_core::{
    checked_history_limit, checked_ping_wait, ping_listener_disabled, BrokerError,
    BrokerOperationSource, DatabaseOperationError, DatabaseOperationFailedCode, PostgresBroker,
    StaleMinutes, StaleThresholdRefusal,
};
use napi::bindgen_prelude::{Object, ToNapiValue};
use napi::Env;
use napi_derive::napi;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::Envelope;
use crate::facades::answer::answered_at_once;
use crate::facades::health_records::{
    ExecutionClassCoverageRecord, ExecutionClassReportRecord, ExpiredTaskList, StaleTaskList,
    WorkerPongRecord, WorkerStateRecord,
};
use crate::facades::monitoring_query::whole_number;
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::values::records::Outcome;

/// Which workers a ping asks.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PingTargetArg {
    /// Every worker that listens.
    EveryWorker,
    /// The worker with this id; the ping returns as soon as it replies.
    Worker { worker_id: String },
}

/// When a ping returns before its window ends.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum MinResponsesArg {
    /// Wait the full window and list every worker that replied.
    FullWindow,
    /// Return as soon as `count` workers replied, 1 or more.
    AtLeast { count: f64 },
}

/// How many snapshots a worker history read returns.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum HistoryLimitArg {
    /// Every retained snapshot.
    AllRetained,
    /// The newest `count` snapshots, 1 or more.
    Newest { count: f64 },
}

/// The latest snapshot of one worker, or none.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the value becomes a JavaScript object when the call returns, and napi-rs has no conversion of a Box"
)]
pub enum WorkerStateRead {
    Found {
        snapshot: WorkerStateRecord,
    },
    /// The worker never wrote a snapshot, or retention removed its rows.
    NotFound,
}

/// A ping window: a whole number of milliseconds above 0, at most
/// `WORKER_PING_WAIT_BOUND`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PingRequest {
    target: PingTargetArg,
    window: Duration,
    min_responses: Option<usize>,
}

/// Ping the workers and collect the distinct replies by worker id.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<WorkerPongRecord[]>>>")]
pub fn ping_workers<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    target: PingTargetArg,
    wait_ms: f64,
    min_responses: MinResponsesArg,
) -> napi::Result<Object<'env>> {
    let subject = "ping_workers";
    let listener = handle.connection().lazy_handle().listener();
    let check = move || ping_request(target, wait_ms, &min_responses, listener);
    health_read(
        env,
        handle,
        subject,
        DatabaseOperationFailedCode::WorkerPingFailed,
        check,
        |broker, request| async move {
            let target_worker_id = match &request.target {
                PingTargetArg::EveryWorker => None,
                PingTargetArg::Worker { worker_id } => Some(worker_id.as_str()),
            };
            broker
                .ping_workers(target_worker_id, request.window, request.min_responses)
                .await
                .map(|pongs| {
                    pongs
                        .into_iter()
                        .map(WorkerPongRecord::from)
                        .collect::<Vec<_>>()
                })
        },
    )
}

/// The latest snapshot of every worker that wrote one.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<WorkerStateRecord[]>>>")]
pub fn list_worker_states<'env>(env: &'env Env, handle: &CoreHandle) -> napi::Result<Object<'env>> {
    monitoring_read(
        env,
        handle,
        "list_worker_states",
        || Ok(()),
        |broker, ()| async move {
            broker.list_worker_states().await.map(|states| {
                states
                    .into_iter()
                    .map(WorkerStateRecord::from)
                    .collect::<Vec<_>>()
            })
        },
    )
}

/// The latest snapshot of one worker, or `notFound`.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<WorkerStateRead>>>")]
pub fn get_worker_state<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    worker_id: String,
) -> napi::Result<Object<'env>> {
    monitoring_read(
        env,
        handle,
        "get_worker_state",
        move || Ok(worker_id),
        |broker, worker_id| async move {
            broker
                .get_worker_state(&worker_id)
                .await
                .map(|state| match state {
                    Some(snapshot) => WorkerStateRead::Found {
                        snapshot: WorkerStateRecord::from(snapshot),
                    },
                    None => WorkerStateRead::NotFound,
                })
        },
    )
}

/// The snapshots of one worker, newest first.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<WorkerStateRecord[]>>>")]
pub fn get_worker_state_history<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    worker_id: String,
    limit: HistoryLimitArg,
) -> napi::Result<Object<'env>> {
    let check = move || history_limit(&limit).map(|limit| (worker_id, limit));
    monitoring_read(
        env,
        handle,
        "get_worker_state_history",
        check,
        |broker, (worker_id, limit)| async move {
            broker
                .get_worker_state_history(&worker_id, limit)
                .await
                .map(|states| {
                    states
                        .into_iter()
                        .map(WorkerStateRecord::from)
                        .collect::<Vec<_>>()
                })
        },
    )
}

/// The `RUNNING` tasks whose latest beat, or their start, is older than
/// `threshold_minutes`; at most 10,000 rows.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<StaleTaskList>>>")]
pub fn get_stale_tasks<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    threshold_minutes: f64,
) -> napi::Result<Object<'env>> {
    let check = move || stale_threshold(threshold_minutes);
    monitoring_read(
        env,
        handle,
        "get_stale_tasks",
        check,
        |broker, threshold| async move {
            broker
                .get_stale_tasks(threshold.minutes())
                .await
                .map(StaleTaskList::from)
        },
    )
}

/// The `PENDING` tasks past their `good_until`, oldest deadline first; at
/// most 10,000 rows.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<ExpiredTaskList>>>")]
pub fn get_expired_tasks<'env>(env: &'env Env, handle: &CoreHandle) -> napi::Result<Object<'env>> {
    monitoring_read(
        env,
        handle,
        "get_expired_tasks",
        || Ok(()),
        |broker, ()| async move { broker.get_expired_tasks().await.map(ExpiredTaskList::from) },
    )
}

/// For each pending stream, whether a live worker can run it.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<ExecutionClassCoverageRecord>>>")]
pub fn get_execution_class_coverage<'env>(
    env: &'env Env,
    handle: &CoreHandle,
) -> napi::Result<Object<'env>> {
    monitoring_read(
        env,
        handle,
        "get_execution_class_coverage",
        || Ok(()),
        |broker, ()| async move {
            broker
                .get_execution_class_coverage()
                .await
                .map(ExecutionClassCoverageRecord::from)
        },
    )
}

/// The pending streams and the budget load of each live worker.
#[napi(ts_return_type = "Promise<NativeEnvelope<NativeOutcome<ExecutionClassReportRecord>>>")]
pub fn get_execution_class_report<'env>(
    env: &'env Env,
    handle: &CoreHandle,
) -> napi::Result<Object<'env>> {
    monitoring_read(
        env,
        handle,
        "get_execution_class_report",
        || Ok(()),
        |broker, ()| async move {
            broker
                .get_execution_class_report()
                .await
                .map(ExecutionClassReportRecord::from)
        },
    )
}

/// A health read whose failed connect is `MONITORING_QUERY_FAILED`.
fn monitoring_read<'env, Arg, Record, Check, Read, Answer>(
    env: &'env Env,
    handle: &CoreHandle,
    subject: &'static str,
    check: Check,
    read: Read,
) -> napi::Result<Object<'env>>
where
    Arg: Send + 'static,
    Record: ToNapiValue + Send + 'static,
    Check: FnOnce() -> Result<Arg, DatabaseOperationError>,
    Read: FnOnce(Arc<PostgresBroker>, Arg) -> Answer + Send + 'static,
    Answer: Future<Output = Result<Record, DatabaseOperationError>> + Send + 'static,
{
    let code = DatabaseOperationFailedCode::MonitoringQueryFailed;
    health_read(env, handle, subject, code, check, read)
}

/// Check the arguments of a health read on the JavaScript thread, then run
/// the read on the core runtime. A refused argument answers its
/// `INVALID_INPUT` at once.
fn health_read<'env, Arg, Record, Check, Read, Answer>(
    env: &'env Env,
    handle: &CoreHandle,
    subject: &'static str,
    code: DatabaseOperationFailedCode,
    check: Check,
    read: Read,
) -> napi::Result<Object<'env>>
where
    Arg: Send + 'static,
    Record: ToNapiValue + Send + 'static,
    Check: FnOnce() -> Result<Arg, DatabaseOperationError>,
    Read: FnOnce(Arc<PostgresBroker>, Arg) -> Answer + Send + 'static,
    Answer: Future<Output = Result<Record, DatabaseOperationError>> + Send + 'static,
{
    match guarded_start(subject, || Ok(check())) {
        Err(thrown) => answered_at_once(env, Envelope::<Outcome<Record>>::Thrown(thrown)),
        Ok(Err(refused)) => answered_at_once(
            env,
            Envelope::Value(Outcome::<Record>::failed(subject, &refused)),
        ),
        Ok(Ok(arg)) => {
            let connection = handle.connection();
            spawn_operation(env, subject, CallKind::Read, move |_core| {
                initialized_read(connection, subject, code, move |broker| read(broker, arg))
            })
        }
    }
}

/// Connect the handle's broker with the schema installed, run `read`, and
/// give its record or the raw record of its failure.
async fn initialized_read<Record, Read, Answer>(
    connection: BrokerConnection,
    subject: &'static str,
    code: DatabaseOperationFailedCode,
    read: Read,
) -> Outcome<Record>
where
    Read: FnOnce(Arc<PostgresBroker>) -> Answer,
    Answer: Future<Output = Result<Record, DatabaseOperationError>>,
{
    let broker = match connection.lazy_handle().initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => return Outcome::failed(subject, &connect_failure(code, subject, error)),
    };
    match read(broker).await {
        Ok(record) => Outcome::Done(record),
        Err(error) => Outcome::failed(subject, &error),
    }
}

/// The failure of a connect or an install before the read `read`, with the
/// text of the Python and Rust clients (`{read} failed: {error}`) and the
/// broker error as its cause. Pure.
fn connect_failure(
    code: DatabaseOperationFailedCode,
    read: &str,
    error: BrokerError,
) -> DatabaseOperationError {
    DatabaseOperationError::operation_failed(
        code,
        format!("{read} failed: {error}"),
        BrokerOperationSource::Broker(error),
    )
}

/// The checked request of a ping on a handle with `listener`, checked in
/// the order of the Python client: the window, the reply count, the
/// listener. A handle with no listener is `LISTENER_DISABLED`. Pure.
fn ping_request(
    target: PingTargetArg,
    wait_ms: f64,
    min_responses: &MinResponsesArg,
    listener: HandleListener,
) -> Result<PingRequest, DatabaseOperationError> {
    let window = match positive_count(wait_ms) {
        Some(milliseconds) => checked_ping_wait(Duration::from_millis(milliseconds))?,
        None => return Err(refused_count("waitMs", wait_ms)),
    };
    let min_responses = match min_responses {
        MinResponsesArg::FullWindow => None,
        MinResponsesArg::AtLeast { count } => {
            match positive_count(*count).and_then(|count| usize::try_from(count).ok()) {
                Some(count) => Some(count),
                None => return Err(refused_count("minResponses.count", *count)),
            }
        }
    };
    match listener {
        HandleListener::Process => {}
        HandleListener::Disabled => return Err(ping_listener_disabled()),
    }
    Ok(PingRequest {
        target,
        window,
        min_responses,
    })
}

/// The checked limit of a history read: `None` reads every retained
/// snapshot. A limit of 0 or less is refused with the core's text. Pure.
fn history_limit(limit: &HistoryLimitArg) -> Result<Option<i64>, DatabaseOperationError> {
    match limit {
        HistoryLimitArg::AllRetained => Ok(None),
        HistoryLimitArg::Newest { count } => match whole_number("limit.count", *count) {
            Ok(count) => checked_history_limit(Some(count)),
            Err(_) => Err(refused_count("limit.count", *count)),
        },
    }
}

/// The checked stale threshold, with the core's text for a refusal. Pure.
fn stale_threshold(minutes: f64) -> Result<StaleMinutes, DatabaseOperationError> {
    let refusal = match whole_number("thresholdMinutes", minutes) {
        Ok(whole) => match StaleMinutes::parse(whole) {
            Ok(threshold) => return Ok(threshold),
            Err(refusal) => refusal,
        },
        Err(_) => StaleThresholdRefusal::NotAnInteger {
            type_name: "number with a fraction".to_owned(),
        },
    };
    Err(refusal.error())
}

/// A whole number above 0 that a JavaScript number holds exactly, or
/// `None`. Pure.
fn positive_count(value: f64) -> Option<u64> {
    match whole_number("count", value) {
        Ok(whole) if whole > 0 => u64::try_from(whole).ok(),
        Ok(_) | Err(_) => None,
    }
}

/// `INVALID_INPUT` for a count of `field` that is not a whole number above
/// 0. Pure.
fn refused_count(field: &str, value: f64) -> DatabaseOperationError {
    DatabaseOperationError::invalid_input(format!(
        "{field} must be a whole number from 1 to 9007199254740991, got {value}"
    ))
}

#[cfg(test)]
mod tests {
    use horsies_core::{DatabaseErrorCode, WORKER_HISTORY_ROW_BOUND, WORKER_PING_WAIT_BOUND};

    use super::*;

    fn code_of<T: std::fmt::Debug>(
        refused: Result<T, DatabaseOperationError>,
    ) -> DatabaseErrorCode {
        match refused {
            Err(error) => error.code(),
            Ok(value) => panic!("a refusal, got {value:?}"),
        }
    }

    #[test]
    fn a_ping_of_every_worker_with_the_full_window_is_built() {
        let request = ping_request(
            PingTargetArg::EveryWorker,
            2_000.0,
            &MinResponsesArg::FullWindow,
            HandleListener::Process,
        );
        assert_eq!(
            request.ok(),
            Some(PingRequest {
                target: PingTargetArg::EveryWorker,
                window: Duration::from_millis(2_000),
                min_responses: None,
            })
        );
    }

    #[test]
    fn a_ping_window_that_is_not_a_whole_number_above_0_is_invalid_input() {
        for refused in [0.0, -1.0, 2.5, f64::NAN, f64::INFINITY] {
            assert_eq!(
                code_of(ping_request(
                    PingTargetArg::EveryWorker,
                    refused,
                    &MinResponsesArg::FullWindow,
                    HandleListener::Process
                )),
                DatabaseErrorCode::InvalidInput,
                "{refused}"
            );
        }
    }

    #[test]
    fn a_ping_window_above_the_bound_is_invalid_input_and_the_bound_is_kept() {
        let bound_ms = WORKER_PING_WAIT_BOUND.as_millis() as f64;
        let at_bound = ping_request(
            PingTargetArg::EveryWorker,
            bound_ms,
            &MinResponsesArg::FullWindow,
            HandleListener::Process,
        );
        assert_eq!(
            at_bound.ok().map(|request| request.window),
            Some(WORKER_PING_WAIT_BOUND)
        );
        assert_eq!(
            code_of(ping_request(
                PingTargetArg::EveryWorker,
                bound_ms + 1.0,
                &MinResponsesArg::FullWindow,
                HandleListener::Process
            )),
            DatabaseErrorCode::InvalidInput
        );
    }

    #[test]
    fn a_response_count_below_1_is_invalid_input() {
        for refused in [0.0, -3.0, 1.5] {
            let min = MinResponsesArg::AtLeast { count: refused };
            assert_eq!(
                code_of(ping_request(
                    PingTargetArg::EveryWorker,
                    100.0,
                    &min,
                    HandleListener::Process
                )),
                DatabaseErrorCode::InvalidInput,
                "{refused}"
            );
        }
        let min = MinResponsesArg::AtLeast { count: 2.0 };
        let request = ping_request(
            PingTargetArg::EveryWorker,
            100.0,
            &min,
            HandleListener::Process,
        );
        assert_eq!(
            request.ok().map(|request| request.min_responses),
            Some(Some(2))
        );
    }

    #[test]
    fn a_handle_with_no_listener_refuses_a_ping_with_listener_disabled() {
        assert_eq!(
            code_of(ping_request(
                PingTargetArg::EveryWorker,
                100.0,
                &MinResponsesArg::FullWindow,
                HandleListener::Disabled,
            )),
            DatabaseErrorCode::ListenerDisabled
        );
    }

    #[test]
    fn a_history_limit_of_0_is_refused_and_every_retained_reads_all() {
        assert_eq!(
            history_limit(&HistoryLimitArg::AllRetained).ok(),
            Some(None)
        );
        assert_eq!(
            history_limit(&HistoryLimitArg::Newest { count: 5.0 }).ok(),
            Some(Some(5))
        );
        let bound = WORKER_HISTORY_ROW_BOUND as f64;
        assert_eq!(
            history_limit(&HistoryLimitArg::Newest { count: bound }).ok(),
            Some(Some(WORKER_HISTORY_ROW_BOUND))
        );
        for refused in [0.0, -1.0, 2.5, bound + 1.0] {
            assert_eq!(
                code_of(history_limit(&HistoryLimitArg::Newest { count: refused })),
                DatabaseErrorCode::InvalidInput,
                "{refused}"
            );
        }
    }

    #[test]
    fn a_stale_threshold_outside_one_year_or_with_a_fraction_is_invalid_input() {
        assert_eq!(
            stale_threshold(0.0).ok().map(StaleMinutes::minutes),
            Some(0)
        );
        assert_eq!(
            stale_threshold(525_600.0).ok().map(StaleMinutes::minutes),
            Some(525_600)
        );
        for refused in [-1.0, 525_601.0, 0.5] {
            assert_eq!(
                code_of(stale_threshold(refused)),
                DatabaseErrorCode::InvalidInput,
                "{refused}"
            );
        }
    }

    #[test]
    fn a_connect_failure_has_the_code_of_the_read_and_the_retry_rule_of_the_cause() {
        let failure = connect_failure(
            DatabaseOperationFailedCode::MonitoringQueryFailed,
            "list_worker_states",
            BrokerError::ListenerClosed,
        );
        assert_eq!(failure.code(), DatabaseErrorCode::MonitoringQueryFailed);
        assert_eq!(
            failure.retryable(),
            BrokerError::ListenerClosed.is_retryable()
        );
        assert_eq!(
            failure.message(),
            "list_worker_states failed: the broker's listener stopped"
        );
    }
}
