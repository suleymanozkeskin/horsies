//! `TaskFunction<A, T, E>`: the typed function of a registered task, and its
//! sends (Python `TaskFunction.send`, `schedule`, `retry_send`,
//! `retry_schedule`, `core/task_decorator.py`). A delay is a send option
//! ([`TaskSendOptions::delay`]), so one send method covers a send now and a
//! send after a delay, and one retry method covers both.
//!
//! A send, in order: the suppression check, the retention of the send, the
//! read-back of the input (encode, decode with `A`, encode again, compare),
//! the payload size limit, a new task id and the sent instant, then one
//! enqueue through the core with `SendKind::First`. With
//! `AppConfig.resend_on_transient_err`, a retryable `ENQUEUE_FAILED` is
//! sent again up to 3 times with the same task id and payload as
//! `SendKind::Repeat`, after 200 ms, 400 ms and 800 ms (at most 2,000 ms):
//! a send whose commit reply was lost answers with its task id, and a task
//! that finished answers from history and does not run again.
//!
//! In a worker child the send goes through the child's producer handle
//! (`child_producer`), whose pool the worker sizes; anywhere else through
//! the app's handle.
//!
//! With the feature `otel`, the database calls of a send run inside one
//! span, and each call hands the core the trace carrier of that span
//! (`crate::telemetry`).
//!
//! A send in a process that was forked without `exec` after the core's
//! process runtime started panics (`process_state::refuse_in_a_fork`): a
//! fork is a state of the process, never a send result, as Python's sends
//! raise `ForkedAfterCoreStart`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use horsies_core::core::config::payload::{enforce_payload_policy, PayloadKind};
use horsies_core::core::task::send_types::send_error_from_enqueue;
use horsies_core::handle::CoreHandle;
use horsies_core::telemetry::trace_carrier::SendTrace;
use horsies_core::{
    child_producer, mint_task_id, CheckedSend, ChildProducer, Execution, FailureCause,
    PayloadPolicy, RetentionConfig, SendKind, TaskOptions, TaskSendError, TaskSendPayload,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use super::options::{send_retention, SendStart, TaskSendOptions};
use crate::app::SendSuppression;
use crate::process_state::refuse_in_a_fork;
use crate::task::body::encode_send_input;
use crate::telemetry::{CallEnd, TaskSendSpan, TaskSendSubject};
use crate::{ErrorCode, Horsies, HorsiesError, TaskHandle, UserErrorCode};

/// Resends of a retryable `ENQUEUE_FAILED` after the first send (Python
/// `_SEND_RETRY_COUNT`).
const RESEND_COUNT: u32 = 3;
/// The wait before the first resend (Python `_SEND_RETRY_INITIAL_MS`).
const RESEND_FIRST_WAIT: Duration = Duration::from_millis(200);
/// The longest wait before a resend (Python `_SEND_RETRY_MAX_MS`).
const RESEND_LONGEST_WAIT: Duration = Duration::from_millis(2_000);

/// What every function of one app sends with: the app's handle, the
/// suppression of the process and the config values of a send.
pub(crate) struct SendContext {
    handle: Arc<CoreHandle>,
    suppression: SendSuppression,
    resend_on_transient_err: bool,
    payload: PayloadPolicy,
    retention: RetentionConfig,
}

impl SendContext {
    /// The context of `app`'s sends. Copies the config values; no I/O.
    pub(crate) fn of(app: &Horsies) -> Self {
        Self {
            handle: Arc::clone(app.handle()),
            suppression: app.send_suppression(),
            resend_on_transient_err: app.config().resend_on_transient_err,
            payload: app.config().payload.clone(),
            retention: app.config().retention.clone(),
        }
    }

    /// The handle a send goes through: the child's producer handle in a
    /// worker child, else the app's.
    fn send_handle(&self) -> Arc<CoreHandle> {
        match child_producer() {
            ChildProducer::InChild(handle) => handle,
            ChildProducer::NotInAChild => Arc::clone(&self.handle),
        }
    }
}

/// The typed function of a registered task: input `A`, output `T`, code
/// type `E`. `#[horsies::task]` returns one from `register`; clones share
/// one app. Each send goes through the app of the registration.
///
/// # Panics
///
/// Every send and retry method, and their blocking forms, panic in a
/// process that was forked without `exec` after the core's process runtime
/// started in its parent. The fork is a state of the process: no send in it
/// can run, so it is never a [`TaskSendError`].
pub struct TaskFunction<A, T, E> {
    task_name: String,
    queue_name: String,
    priority: u32,
    task_options: Option<TaskOptions>,
    accepts_workflow_ctx: bool,
    context: Arc<SendContext>,
    types: PhantomData<fn(A) -> (T, E)>,
}

impl<A, T, E> Clone for TaskFunction<A, T, E> {
    fn clone(&self) -> Self {
        Self {
            task_name: self.task_name.clone(),
            queue_name: self.queue_name.clone(),
            priority: self.priority,
            task_options: self.task_options.clone(),
            accepts_workflow_ctx: self.accepts_workflow_ctx,
            context: Arc::clone(&self.context),
            types: PhantomData,
        }
    }
}

impl<A, T, E> std::fmt::Debug for TaskFunction<A, T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskFunction")
            .field("task_name", &self.task_name)
            .field("queue_name", &self.queue_name)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}

impl<A, T, E> TaskFunction<A, T, E>
where
    A: Serialize + DeserializeOwned + 'static,
    T: Serialize + DeserializeOwned + 'static,
    E: UserErrorCode,
{
    /// The function of the task registered on `app` as `task_name`, with
    /// the queue, priority and options of its registration. The task
    /// attribute calls it after `app.register`; not API.
    ///
    /// Errors: `TASK_NOT_REGISTERED` for a name the app does not hold; a
    /// refusal that names both types when the registered task decodes
    /// another input type than `A`.
    #[doc(hidden)]
    pub fn __registered(app: &Horsies, task_name: &str) -> Result<Self, HorsiesError> {
        let task = app.tasks().get(task_name)?;
        let asked = std::any::type_name::<A>();
        if let Some(registered) = task.input_type_name() {
            if registered != asked {
                return Err(HorsiesError::new(format!(
                    "task '{task_name}' decodes input of type {registered}, not {asked}"
                ))
                .with_help("build the typed function with the input type of the task"));
            }
        }
        let (Some(queue_name), Some(priority)) = (task.queue_name(), task.priority()) else {
            return Err(HorsiesError::new(format!(
                "task '{task_name}' has no resolved queue; register it with Horsies::register"
            ))
            .with_code(ErrorCode::TaskInvalidQueue));
        };
        let function = Self {
            task_name: task_name.to_owned(),
            queue_name: queue_name.to_owned(),
            priority,
            task_options: task.task_options().cloned(),
            accepts_workflow_ctx: task.accepts_workflow_ctx(),
            context: Arc::new(SendContext::of(app)),
            types: PhantomData,
        };
        app.functions().insert(task_name, function.clone());
        Ok(function)
    }

    /// The task name.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// The queue the task is sent to.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// The priority of the task's queue: 1 is claimed first, 100 last.
    pub fn priority(&self) -> u32 {
        self.priority
    }

    /// The options of the task's definition.
    pub fn task_options(&self) -> Option<&TaskOptions> {
        self.task_options.as_ref()
    }

    /// Whether the task's input takes the workflow context (the task
    /// attribute's `workflow_ctx`).
    pub fn accepts_workflow_ctx(&self) -> bool {
        self.accepts_workflow_ctx
    }

    /// Send the task now.
    ///
    /// Errors: [`TaskSendError`]; see the module docs for the order.
    pub async fn send(&self, input: A) -> Result<TaskHandle<T, E>, TaskSendError> {
        self.send_with_options(TaskSendOptions::new(), input).await
    }

    /// Send the task with `options`: now, or after the delay of
    /// [`TaskSendOptions::delay`].
    ///
    /// Errors: as [`Self::send`].
    pub async fn send_with_options(
        &self,
        options: TaskSendOptions,
        input: A,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        let (task_id, payload) = self.prepare(&options, &input)?;
        self.enqueue_with_resends(task_id, payload, SendKind::First)
            .await
    }

    /// The sends of this task with `options`.
    pub fn with_options(&self, options: TaskSendOptions) -> TaskFunctionSendOptions<'_, A, T, E> {
        TaskFunctionSendOptions {
            function: self,
            options,
        }
    }

    /// Send again the task of a failed send: the same task id and payload,
    /// the delay of the send included, each call `SendKind::Repeat`, so a
    /// task that an earlier call wrote answers with its id, also after it
    /// finished.
    ///
    /// Errors: `VALIDATION_FAILED` for an error the retry does not accept
    /// (only `ASYNC_CONTEXT`, `ENQUEUE_FAILED` and `IDEMPOTENCY_KEY_CONFLICT`
    /// carry a payload to send again) and for the error of another task;
    /// else the errors of a send.
    pub async fn retry_send(
        &self,
        error: &TaskSendError,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        let (task_id, payload) = self.retry_target(error)?;
        self.enqueue_with_resends(task_id, payload, SendKind::Repeat)
            .await
    }

    /// The task id and payload of a first send with `options`, or the
    /// refusal before any statement (Python `_send_with_options`,
    /// `_prepare_send`). No I/O.
    pub(crate) fn prepare(
        &self,
        options: &TaskSendOptions,
        input: &A,
    ) -> Result<(Uuid, TaskSendPayload), TaskSendError> {
        let context = &self.context;
        if context.suppression.is_active() {
            return Err(TaskSendError::SendSuppressed {
                message: format!(
                    "Task send suppressed for {} (import/check phase or \
                     HORSIES_SUPPRESS_SENDS=1)",
                    self.task_name
                ),
            });
        }
        let retention = send_retention(
            &context.retention,
            &self.queue_name,
            options.chosen_retention(),
        )
        .map_err(|refused| self.validation_failed(&format!("Invalid retention: {refused}")))?;
        let input_json = self.encoded_input(input)?;
        self.check_payload_size(&input_json)?;
        let priority = i32::try_from(self.priority).map_err(|_| {
            self.validation_failed(&format!(
                "the priority of queue '{}' does not fit a Postgres integer",
                self.queue_name
            ))
        })?;
        let task_id = mint_task_id().map_err(|refused| {
            self.validation_failed(&format!("task id mint failed: {refused}"))
        })?;
        let task_options = send_options_text(
            &self.task_name,
            self.task_options.as_ref(),
            options.deadline(),
        );
        let payload = TaskSendPayload {
            task_name: self.task_name.clone(),
            queue_name: self.queue_name.clone(),
            priority,
            input_json,
            sent_at: Utc::now(),
            good_until: options.deadline(),
            enqueue_delay_seconds: match options.start() {
                SendStart::Now => None,
                SendStart::AfterDelay(delay) => Some(i64::from(delay.get())),
            },
            task_options,
            idempotency_key: options.key().map(str::to_owned),
            retention,
        };
        Ok((task_id, payload))
    }

    /// The input as one JSON text that reads back as `A` to the same text
    /// (#569, #578); `{}` for a task with no parameters. A refusal names `A`
    /// and the failure, never the value.
    fn encoded_input(&self, input: &A) -> Result<String, TaskSendError> {
        let bytes = encode_send_input(input).map_err(|cause| {
            self.validation_failed(&format!(
                "Failed to encode the input of {} as {}: {cause}",
                self.task_name,
                std::any::type_name::<A>()
            ))
        })?;
        String::from_utf8(bytes).map_err(|_| {
            self.validation_failed(&format!(
                "the encoded input of {} is not UTF-8",
                self.task_name
            ))
        })
    }

    /// The warn and reject thresholds of `AppConfig.payload` on the input
    /// text (Python `warn_if_payload_large`, `check_payload_size`).
    fn check_payload_size(&self, input_json: &str) -> Result<(), TaskSendError> {
        let policy = &self.context.payload;
        match enforce_payload_policy(
            policy,
            &self.task_name,
            PayloadKind::Kwargs,
            input_json.len(),
        ) {
            None => Ok(()),
            Some(size) => {
                let size_bytes = u64::try_from(size).unwrap_or(u64::MAX);
                let limit_bytes = policy.reject_bytes.unwrap_or_else(|| {
                    unreachable!("the payload policy rejects only above its reject_bytes")
                });
                Err(TaskSendError::PayloadTooLarge {
                    message: format!(
                        "input payload for {} is {size_bytes} bytes, exceeding \
                         payload.reject_bytes={limit_bytes}; nothing was enqueued. Pass a \
                         reference to external storage or raise the limit.",
                        self.task_name
                    ),
                    size_bytes,
                    limit_bytes,
                })
            }
        }
    }

    /// The task id and payload of an error that `retry_send` accepts
    /// (Python `_validate_retry`): the payload of a send now or of a send
    /// after a delay, each sent again as it was.
    pub(crate) fn retry_target(
        &self,
        error: &TaskSendError,
    ) -> Result<(Uuid, TaskSendPayload), TaskSendError> {
        let Some(target) = error.resend_target() else {
            return Err(self.validation_failed(&format!(
                "cannot retry a send that failed with {}: only ASYNC_CONTEXT, \
                 ENQUEUE_FAILED and IDEMPOTENCY_KEY_CONFLICT carry a payload to send again",
                error.code()
            )));
        };
        if target.payload.task_name != self.task_name {
            return Err(self.validation_failed(&format!(
                "cross-task retry: error is for '{}' but this task is '{}'",
                target.payload.task_name, self.task_name
            )));
        }
        Ok((target.task_id, target.payload.clone()))
    }

    /// Enqueue `payload` as `task_id`, and resend a retryable
    /// `ENQUEUE_FAILED` when the app asks for it. Every call after the first
    /// is `SendKind::Repeat`. Every call runs inside the one span of the send
    /// and carries its trace carrier (feature `otel`; without it, no span
    /// and no carrier).
    ///
    /// # Panics
    ///
    /// In a fork, without `exec`, of the process that started the core's
    /// process runtime ([`refuse_in_a_fork`]). Nothing was sent.
    pub(crate) async fn enqueue_with_resends(
        &self,
        task_id: Uuid,
        payload: TaskSendPayload,
        first: SendKind,
    ) -> Result<TaskHandle<T, E>, TaskSendError> {
        refuse_in_a_fork(format_args!("send of task '{}'", self.task_name));
        let resends = match self.context.resend_on_transient_err {
            true => RESEND_COUNT,
            false => 0,
        };
        let handle = self.context.send_handle();
        let span = TaskSendSpan::open(&TaskSendSubject {
            task_id,
            task_name: &payload.task_name,
            queue_name: &payload.queue_name,
        });
        let trace = span.trace();
        let handle_ref: &CoreHandle = &handle;
        let payload_ref = &payload;
        let trace_ref = &trace;
        let sent = send_with_resends(resends, first, move |kind| {
            enqueue_once(handle_ref, task_id, payload_ref, trace_ref, kind)
        })
        .await;
        span.end(match &sent {
            Ok(_) => CallEnd::Succeeded,
            Err(error) => CallEnd::Failed(error.code()),
        });
        Ok(TaskHandle::new(sent?, Arc::clone(&handle)))
    }

    fn validation_failed(&self, message: &str) -> TaskSendError {
        TaskSendError::ValidationFailed {
            message: format!("{message} (task {})", self.task_name),
            cause: FailureCause::PreconditionFailed,
        }
    }
}

/// Run `send` with `first`; after a retryable `ENQUEUE_FAILED`, run it
/// again as `SendKind::Repeat`, at most `resends` more times, after
/// [`resend_wait`]. Returns the stored id of the first send that succeeds,
/// else the error of the last send. Any other error returns at once.
async fn send_with_resends<F, Fut>(
    resends: u32,
    first: SendKind,
    mut send: F,
) -> Result<Uuid, TaskSendError>
where
    F: FnMut(SendKind) -> Fut,
    Fut: std::future::Future<Output = Result<Uuid, TaskSendError>>,
{
    let mut kind = first;
    for resend in 0..=resends {
        if resend > 0 {
            tokio::time::sleep(resend_wait(resend)).await;
        }
        match send(kind).await {
            Ok(stored_id) => return Ok(stored_id),
            Err(TaskSendError::EnqueueFailed {
                retryable: true, ..
            }) if resend < resends => kind = SendKind::Repeat,
            Err(refused) => return Err(refused),
        }
    }
    unreachable!("the last send of the resend loop returns its outcome")
}

/// The wait before resend `resend` (1 is the first): 200 ms doubled per
/// resend, at most 2,000 ms.
fn resend_wait(resend: u32) -> Duration {
    let factor = 2u32.saturating_pow(resend.saturating_sub(1));
    RESEND_FIRST_WAIT
        .saturating_mul(factor)
        .min(RESEND_LONGEST_WAIT)
}

/// One enqueue through `handle`, which installs the schema once per handle
/// first: the core's check of the send (`CheckedSend::check`, with the
/// handle's default for keeping the rerun input), the trace carrier
/// `trace`, then the write. A failed connect or install, and every enqueue
/// error, maps to the send catalog by the core's one rule. A carrier that
/// breaks a bound of the core is not stored and never refuses the send.
async fn enqueue_once(
    handle: &CoreHandle,
    task_id: Uuid,
    payload: &TaskSendPayload,
    trace: &SendTrace,
    kind: SendKind,
) -> Result<Uuid, TaskSendError> {
    let refused = |error: horsies_core::BrokerError| {
        send_error_from_enqueue(task_id, payload.clone(), &error)
    };
    let broker = handle.initialized_broker().await.map_err(refused)?;
    let send = CheckedSend::check(
        &payload.task_name,
        &payload.input_json,
        &payload.queue_name,
        payload.priority,
        Some(payload.sent_at),
        None,
        payload.good_until,
        payload.task_options.as_deref(),
        Some(task_id),
        payload.enqueue_delay_seconds,
        payload.idempotency_key.as_deref(),
        payload.retention,
        handle.manifest().send.retain_rerun_input_default,
    )
    .map_err(refused)?
    .with_trace(trace.clone());
    broker.enqueue_checked(&send, kind).await.map_err(refused)
}

/// The stored options text of a send (Python `_serialize_options_for_send`,
/// `clients/python/horsies/core/task_decorator.py`). Pure.
///
/// - No options and no deadline: `None`, no text.
/// - Options and no deadline: [`TaskOptions::stored_options_text`].
/// - A deadline: [`TaskOptions::stored_send_options_text`], the deadline in
///   `good_until`, of the task's options; of an `ISOLATED` task with no
///   policy, timeout or grace when the task has none.
pub(crate) fn send_options_text(
    task_name: &str,
    task_options: Option<&TaskOptions>,
    deadline: Option<DateTime<Utc>>,
) -> Option<String> {
    match (task_options, deadline) {
        (None, None) => None,
        (Some(options), None) => Some(options.stored_options_text()),
        (Some(options), Some(at)) => Some(options.stored_send_options_text(at)),
        (None, Some(at)) => Some(isolated_options(task_name).stored_send_options_text(at)),
    }
}

/// The options of an `ISOLATED` task with no policy, timeout or grace: what
/// Python's send builds for a task with no options and a deadline.
fn isolated_options(task_name: &str) -> TaskOptions {
    TaskOptions {
        task_name: task_name.to_owned(),
        queue_name: None,
        good_until: None,
        auto_retry_for: None,
        retry_policy: None,
        timeout_ms: None,
        execution: Execution::Isolated,
        cancel_grace_ms: None,
    }
}

/// The sends of one task with per-send options.
pub struct TaskFunctionSendOptions<'a, A, T, E> {
    function: &'a TaskFunction<A, T, E>,
    options: TaskSendOptions,
}

impl<A, T, E> TaskFunctionSendOptions<'_, A, T, E>
where
    A: Serialize + DeserializeOwned + 'static,
    T: Serialize + DeserializeOwned + 'static,
    E: UserErrorCode,
{
    /// Send the task with these options: now, or after their delay.
    pub async fn send(&self, input: A) -> Result<TaskHandle<T, E>, TaskSendError> {
        self.function
            .send_with_options(self.options.clone(), input)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;

    use horsies_core::core::python_text::python_isoformat_utc;
    use horsies_core::EnqueueRetention;
    use serde::Deserialize;

    use super::*;
    use crate::{AppConfig, SendRetention, TaskError, WholeSeconds};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Invoice {
        id: u32,
        amount: f64,
    }

    /// Writes a field that it never reads back.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct LossyInput {
        kept: u32,
        #[serde(skip_deserializing)]
        dropped: u32,
    }

    async fn send_invoice(invoice: Invoice) -> Result<u32, TaskError<Infallible>> {
        Ok(invoice.id)
    }

    async fn lossy(input: LossyInput) -> Result<u32, TaskError<Infallible>> {
        Ok(input.kept)
    }

    fn app_with(config: AppConfig) -> Horsies {
        let mut app = Horsies::new(config).unwrap();
        app.register("send_invoice", crate::async_task_fn!(send_invoice, Invoice))
            .unwrap();
        app.register("lossy", crate::async_task_fn!(lossy, LossyInput))
            .unwrap();
        app
    }

    fn config() -> AppConfig {
        AppConfig::for_database_url("postgresql://localhost/horsies")
    }

    /// A config whose address has no database: a send that a defect lets
    /// past its checks fails to connect, and writes to no local database.
    fn config_without_database() -> AppConfig {
        AppConfig::for_database_url("postgresql://127.0.0.1:1/nothing")
    }

    fn function(app: &Horsies) -> TaskFunction<Invoice, u32, Infallible> {
        function_named(app, "send_invoice")
    }

    /// The typed function of the task `task_name`, which decodes `Invoice`.
    fn function_named(app: &Horsies, task_name: &str) -> TaskFunction<Invoice, u32, Infallible> {
        TaskFunction::__registered(app, task_name).unwrap()
    }

    fn invoice() -> Invoice {
        Invoice {
            id: 7,
            amount: 12.5,
        }
    }

    fn validation_message(error: TaskSendError) -> String {
        match error {
            TaskSendError::ValidationFailed { message, .. } => message,
            other => panic!("expected VALIDATION_FAILED, got {other}"),
        }
    }

    #[test]
    fn a_registered_function_has_the_queue_and_priority_of_its_registration() {
        let app = app_with(config());
        let function = function(&app);
        assert_eq!(function.task_name(), "send_invoice");
        assert_eq!(function.queue_name(), "default");
        assert_eq!(function.priority(), 100);
    }

    /// The queue and the stored options text of the one task node of a
    /// spec that a builder of `app` built.
    fn built_node_body(app: &Horsies) -> (String, Option<String>) {
        let mut builder = app.workflow_builder("invoices");
        let _node = builder
            .task(function(app).node().set_input(invoice()).unwrap())
            .unwrap();
        let spec = builder.build().unwrap();
        match &spec.nodes[0].body {
            crate::workflow::SpecBody::Task {
                queue,
                task_options,
                ..
            } => (queue.clone(), task_options.clone()),
            crate::workflow::SpecBody::SubWorkflow { .. } => panic!("a task node was built"),
        }
    }

    /// A workflow builder of an app gives a task node the queue and the
    /// stored options text of the task's registration on that app (Python
    /// `TaskNode(fn=task)`); a task registered without options gives its
    /// node none.
    #[test]
    fn a_builder_gives_a_node_the_queue_and_options_of_the_registration_on_its_app() {
        let options = TaskOptions {
            task_name: "send_invoice".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: Some(5_000),
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        let mut app = Horsies::new(config()).unwrap();
        app.register(
            "send_invoice",
            crate::async_task_fn!(send_invoice, Invoice).with_task_options(options.clone()),
        )
        .unwrap();
        let _function = function(&app);
        assert_eq!(
            built_node_body(&app),
            ("default".to_owned(), Some(options.stored_options_text()))
        );

        let plain = app_with(config());
        let _function = function(&plain);
        assert_eq!(built_node_body(&plain), ("default".to_owned(), None));
    }

    /// A builder of an app that does not register the task refuses its node
    /// with `HRS-302` and adds nothing, also when another app of the process
    /// registers the task.
    #[test]
    fn a_node_of_a_task_that_the_builder_app_does_not_register_is_hrs_302() {
        let registered = app_with(config());
        let node_of = || function(&registered).node().set_input(invoice()).unwrap();
        let mut accepted = registered.workflow_builder("invoices");
        assert!(accepted.task(node_of()).is_ok());

        let without = Horsies::new(config()).unwrap();
        let mut builder = without.workflow_builder("invoices");
        let refused = builder.task(node_of()).unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::WorkflowUnregisteredTask));
        assert!(refused.message.contains("task 'send_invoice'"), "{refused}");
        assert_eq!(
            builder.build().unwrap_err().code,
            Some(ErrorCode::WorkflowNoNodes)
        );
    }

    /// The app registers a task of the node's name with another input type:
    /// `HRS-302`, with both types named.
    #[test]
    fn a_node_with_other_types_than_the_registration_is_hrs_302() {
        let app = app_with(config());
        let _function = function(&app);
        let mut builder = app.workflow_builder("invoices");
        let refused = builder
            .task(crate::__private::task_node::<u32, String>("send_invoice"))
            .unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::WorkflowUnregisteredTask));
        assert!(refused.message.contains("Invoice"), "{refused}");
        assert!(refused.message.contains("String"), "{refused}");
    }

    #[test]
    fn an_unknown_task_name_is_refused() {
        let app = app_with(config());
        let refused =
            TaskFunction::<Invoice, u32, Infallible>::__registered(&app, "missing").unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskNotRegistered));
    }

    #[test]
    fn another_input_type_than_the_registered_one_is_refused_with_both_names() {
        let app = app_with(config());
        let refused = TaskFunction::<String, u32, Infallible>::__registered(&app, "send_invoice")
            .unwrap_err();
        assert!(refused.message.contains("Invoice"), "{refused}");
        assert!(refused.message.contains("String"), "{refused}");
    }

    #[test]
    fn the_payload_holds_the_encoded_input_and_the_send_options() {
        let app = app_with(config());
        let deadline = Utc::now() + chrono::Duration::hours(1);
        let options = TaskSendOptions::new()
            .delay(WholeSeconds::from_secs(30))
            .good_until(deadline)
            .idempotency_key("invoice-7")
            .retain_forever();
        let (task_id, payload) = function(&app).prepare(&options, &invoice()).unwrap();
        assert_eq!(task_id.get_version_num(), 7);
        assert_eq!(payload.task_name, "send_invoice");
        assert_eq!(payload.queue_name, "default");
        assert_eq!(payload.priority, 100);
        assert_eq!(payload.input_json, r#"{"id":7,"amount":12.5}"#);
        assert_eq!(payload.good_until, Some(deadline));
        assert_eq!(payload.enqueue_delay_seconds, Some(30));
        assert_eq!(payload.idempotency_key.as_deref(), Some("invoice-7"));
        assert_eq!(payload.retention, EnqueueRetention::Forever);
        // A task with no options and a send with a deadline: Python stores
        // the options of an ISOLATED task with the deadline.
        assert_eq!(
            payload.task_options,
            Some(format!(
                r#"{{"retry_policy":null,"good_until":"{}","timeout_ms":null,"execution":"ISOLATED","cancel_grace_ms":null}}"#,
                python_isoformat_utc(deadline)
            ))
        );
    }

    #[test]
    fn a_send_now_has_no_delay_and_the_default_retention() {
        let app = app_with(config());
        let (_task_id, payload) = function(&app)
            .prepare(&TaskSendOptions::new(), &invoice())
            .unwrap();
        assert_eq!(payload.enqueue_delay_seconds, None);
        assert_eq!(payload.retention, EnqueueRetention::Default);
        assert_eq!(payload.idempotency_key, None);
    }

    #[test]
    fn a_suppressed_send_writes_nothing_and_names_the_task() {
        let app = app_with(config());
        app.suppress_sends(true);
        let refused = function(&app)
            .prepare(&TaskSendOptions::new(), &invoice())
            .unwrap_err();
        match refused {
            TaskSendError::SendSuppressed { message } => {
                assert!(message.contains("Task send suppressed for send_invoice"));
            }
            other => panic!("expected SEND_SUPPRESSED, got {other}"),
        }
    }

    #[test]
    fn an_input_that_does_not_read_back_is_refused_before_any_statement() {
        let app = app_with(config());
        let function: TaskFunction<LossyInput, u32, Infallible> =
            TaskFunction::__registered(&app, "lossy").unwrap();
        let input = LossyInput {
            kept: 1,
            dropped: 2,
        };
        let refused = function
            .prepare(&TaskSendOptions::new(), &input)
            .unwrap_err();
        let message = validation_message(refused);
        assert!(message.contains("LossyInput"), "{message}");
        assert!(message.contains("encodes to other JSON"), "{message}");
    }

    #[test]
    fn a_non_finite_float_in_the_input_is_refused() {
        let app = app_with(config());
        let input = Invoice {
            id: 1,
            amount: f64::NAN,
        };
        let refused = function(&app)
            .prepare(&TaskSendOptions::new(), &input)
            .unwrap_err();
        assert!(validation_message(refused).contains("non-finite"));
    }

    #[test]
    fn an_input_over_reject_bytes_is_payload_too_large() {
        let mut config = config();
        config.payload.reject_bytes = Some(8);
        let app = app_with(config);
        match function(&app).prepare(&TaskSendOptions::new(), &invoice()) {
            Err(TaskSendError::PayloadTooLarge {
                size_bytes,
                limit_bytes,
                ..
            }) => {
                assert_eq!(limit_bytes, 8);
                assert_eq!(size_bytes, 22);
            }
            other => panic!("expected PAYLOAD_TOO_LARGE, got {other:?}"),
        }
    }

    #[test]
    fn a_retention_period_that_is_not_registered_is_refused() {
        let app = app_with(config());
        let ninety = horsies_core::RetentionDays::parse_period(chrono::Duration::days(90)).unwrap();
        let options = TaskSendOptions::new().retention(SendRetention::Period(ninety));
        let refused = function(&app).prepare(&options, &invoice()).unwrap_err();
        assert!(validation_message(refused).contains("retention of 90 days"));
    }

    fn enqueue_failed(task_name: &str, delay: Option<i64>) -> TaskSendError {
        let app = app_with(config());
        let (task_id, mut payload) = function(&app)
            .prepare(&TaskSendOptions::new(), &invoice())
            .unwrap();
        payload.task_name = task_name.to_owned();
        payload.enqueue_delay_seconds = delay;
        TaskSendError::EnqueueFailed {
            task_id,
            payload: Box::new(payload),
            message: "connection refused".to_owned(),
            retryable: true,
            cause: FailureCause::Connection,
        }
    }

    /// The retry of a send with a delay sends its payload again with the
    /// delay; the retry of a send now sends it with none.
    #[test]
    fn a_retry_takes_the_task_id_and_payload_of_the_failed_send() {
        let app = app_with(config());
        let error = enqueue_failed("send_invoice", None);
        let (task_id, payload) = function(&app).retry_target(&error).unwrap();
        assert_eq!(Some(task_id), error.task_id());
        assert_eq!(payload.input_json, r#"{"id":7,"amount":12.5}"#);
        assert_eq!(payload.enqueue_delay_seconds, None);
        let delayed = enqueue_failed("send_invoice", Some(30));
        let (delayed_id, delayed_payload) = function(&app).retry_target(&delayed).unwrap();
        assert_eq!(Some(delayed_id), delayed.task_id());
        assert_eq!(delayed_payload.enqueue_delay_seconds, Some(30));
    }

    /// Python `_validate_retry` accepts `TaskSendAsyncContext`: the
    /// prepared task id and payload of a blocking send on a runtime thread.
    #[test]
    fn a_retry_takes_the_task_id_and_payload_of_an_async_context() {
        let app = app_with(config());
        let (task_id, payload) = function(&app)
            .prepare(&TaskSendOptions::new(), &invoice())
            .unwrap();
        let error = TaskSendError::AsyncContext {
            task_id,
            payload: Box::new(payload),
            message: "blocking send on a runtime thread".to_owned(),
        };
        let (retried_id, retried) = function(&app).retry_target(&error).unwrap();
        assert_eq!(retried_id, task_id);
        assert_eq!(retried.input_json, r#"{"id":7,"amount":12.5}"#);
    }

    #[test]
    fn a_retry_refuses_errors_that_carry_no_payload_and_errors_of_another_task() {
        let app = app_with(config());
        let function = function(&app);
        let refusals = [
            (
                TaskSendError::ValidationFailed {
                    message: "bad".to_owned(),
                    cause: FailureCause::PreconditionFailed,
                },
                "VALIDATION_FAILED",
            ),
            (enqueue_failed("other_task", None), "cross-task retry"),
            (enqueue_failed("other_task", Some(30)), "cross-task retry"),
        ];
        for (error, expected) in refusals {
            let message = validation_message(function.retry_target(&error).unwrap_err());
            assert!(message.contains(expected), "{message}");
        }
    }

    #[test]
    fn the_resend_waits_double_from_200_ms_up_to_2_seconds() {
        let waits: Vec<u128> = (1..=5)
            .map(|resend| resend_wait(resend).as_millis())
            .collect();
        assert_eq!(waits, vec![200, 400, 800, 1_600, 2_000]);
    }

    #[test]
    fn the_runtime_of_the_app_finds_the_registered_function() {
        let app = app_with(config());
        let _function = function(&app);
        let runtime = app.task_runtime();
        let found = runtime
            .task_handle::<Invoice, u32, Infallible>("send_invoice")
            .unwrap();
        assert_eq!(found.queue_name(), "default");
        assert_eq!(
            runtime
                .task_handle::<Invoice, String, Infallible>("send_invoice")
                .unwrap_err(),
            crate::TaskFunctionNotFound::OtherTypes {
                task_name: "send_invoice".to_owned(),
                registered: std::any::type_name::<TaskFunction<Invoice, u32, Infallible>>(),
                asked: std::any::type_name::<TaskFunction<Invoice, String, Infallible>>(),
            }
        );
        assert!(matches!(
            runtime.task_handle::<Invoice, u32, Infallible>("lossy"),
            Err(crate::TaskFunctionNotFound::NotRegistered { .. })
        ));
    }

    #[test]
    fn a_function_is_send_and_sync_for_any_types() {
        fn assert_send_sync<V: Send + Sync>() {}
        assert_send_sync::<TaskFunction<std::rc::Rc<u8>, BTreeMap<String, u8>, Infallible>>();
    }

    /// An `ENQUEUE_FAILED` of the send of `invoice()`, retryable or not.
    fn failed_enqueue(retryable: bool) -> TaskSendError {
        match enqueue_failed("send_invoice", None) {
            TaskSendError::EnqueueFailed {
                task_id,
                payload,
                message,
                cause,
                ..
            } => TaskSendError::EnqueueFailed {
                task_id,
                payload,
                message,
                retryable,
                cause,
            },
            other => panic!("expected ENQUEUE_FAILED, got {other}"),
        }
    }

    const STORED_ID: Uuid = Uuid::from_u128(7);

    #[tokio::test(start_paused = true)]
    async fn a_first_send_that_succeeds_is_not_resent() {
        let mut kinds = Vec::new();
        let sent = send_with_resends(RESEND_COUNT, SendKind::First, |kind| {
            kinds.push(kind);
            std::future::ready(Ok(STORED_ID))
        })
        .await;
        assert_eq!(sent.unwrap(), STORED_ID);
        assert_eq!(kinds, vec![SendKind::First]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retryable_enqueue_failure_is_resent_until_a_send_succeeds() {
        let transient = failed_enqueue(true);
        let mut kinds = Vec::new();
        let sent = send_with_resends(RESEND_COUNT, SendKind::First, |kind| {
            kinds.push(kind);
            std::future::ready(match kinds.len() {
                1 | 2 => Err(transient.clone()),
                _ => Ok(STORED_ID),
            })
        })
        .await;
        assert_eq!(sent.unwrap(), STORED_ID);
        assert_eq!(
            kinds,
            vec![SendKind::First, SendKind::Repeat, SendKind::Repeat]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_that_is_not_retryable_is_not_resent() {
        for refusal in [
            failed_enqueue(false),
            TaskSendError::ValidationFailed {
                message: "bad input".to_owned(),
                cause: FailureCause::PreconditionFailed,
            },
        ] {
            let mut kinds = Vec::new();
            let sent = send_with_resends(RESEND_COUNT, SendKind::First, |kind| {
                kinds.push(kind);
                std::future::ready(Err::<Uuid, _>(refusal.clone()))
            })
            .await;
            let refused = sent.unwrap_err();
            assert_eq!(refused.code(), refusal.code(), "{refused}");
            assert_eq!(kinds, vec![SendKind::First], "{refusal}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_retryable_failure_is_resent_three_times_and_the_last_error_returns() {
        let transient = failed_enqueue(true);
        let mut kinds = Vec::new();
        let sent = send_with_resends(RESEND_COUNT, SendKind::First, |kind| {
            kinds.push(kind);
            std::future::ready(Err::<Uuid, _>(transient.clone()))
        })
        .await;
        assert!(
            matches!(
                sent,
                Err(TaskSendError::EnqueueFailed {
                    retryable: true,
                    ..
                })
            ),
            "{sent:?}"
        );
        assert_eq!(RESEND_COUNT, 3);
        assert_eq!(
            kinds,
            vec![
                SendKind::First,
                SendKind::Repeat,
                SendKind::Repeat,
                SendKind::Repeat
            ]
        );
    }

    /// The task of the tests of the generated sends.
    #[horsies::task("generated_send")]
    async fn generated_send(id: u32) -> Result<u32, TaskError<Infallible>> {
        Ok(id)
    }

    /// An app that registers `generated_send` with `config`.
    fn app_with_generated_send(config: AppConfig) -> Horsies {
        let mut app = Horsies::new(config).unwrap();
        let _function = generated_send::register(&mut app).unwrap();
        app
    }

    /// Two apps of one process, each with its own registration of the task:
    /// each generated send reads the app that it names. The first app
    /// suppresses its sends; the second refuses every input by its payload
    /// limit. Each refusal comes before any I/O.
    #[tokio::test]
    async fn each_generated_send_goes_through_the_app_that_it_names() {
        let first = app_with_generated_send(config_without_database());
        first.suppress_sends(true);
        let mut limited = config_without_database();
        limited.payload.reject_bytes = Some(1);
        let second = app_with_generated_send(limited);
        let refused = generated_send::send(&first, 7).await.unwrap_err();
        assert!(
            matches!(refused, TaskSendError::SendSuppressed { .. }),
            "{refused}"
        );
        let refused = generated_send::send(&second, 7).await.unwrap_err();
        assert!(
            matches!(refused, TaskSendError::PayloadTooLarge { .. }),
            "{refused}"
        );
        let delayed = TaskSendOptions::new().delay(WholeSeconds::from_secs(30));
        let refused = generated_send::with_options(delayed.clone())
            .send(&second, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(refused, TaskSendError::PayloadTooLarge { .. }),
            "{refused}"
        );
        let refused = generated_send::with_options(delayed)
            .send(&first, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(refused, TaskSendError::SendSuppressed { .. }),
            "{refused}"
        );
    }

    /// A generated send through an app that does not register the task is
    /// `VALIDATION_FAILED` with the cause `PreconditionFailed`, before any
    /// I/O, also when another app of the process registers it.
    #[tokio::test]
    async fn a_generated_send_through_an_app_without_the_task_is_validation_failed() {
        let _registered = app_with_generated_send(config_without_database());
        let without = Horsies::new(config_without_database()).unwrap();
        let refused = generated_send::send(&without, 7).await.unwrap_err();
        match &refused {
            TaskSendError::ValidationFailed { message, cause } => {
                assert_eq!(*cause, FailureCause::PreconditionFailed);
                assert!(message.contains("task 'generated_send'"), "{message}");
            }
            other => panic!("expected VALIDATION_FAILED, got {other}"),
        }
        let refused = generated_send::with_options(TaskSendOptions::new())
            .send(&without, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(
                refused,
                TaskSendError::ValidationFailed {
                    cause: FailureCause::PreconditionFailed,
                    ..
                }
            ),
            "{refused}"
        );
    }

    /// The app holds a task of this name with another input type: the
    /// generated send names both types and writes nothing.
    #[tokio::test]
    async fn a_generated_send_of_a_task_registered_with_other_types_is_validation_failed() {
        let mut app = Horsies::new(config_without_database()).unwrap();
        app.register(
            "generated_send",
            crate::async_task_fn!(send_invoice, Invoice),
        )
        .unwrap();
        let _function = function_named(&app, "generated_send");
        let refused = generated_send::send(&app, 7).await.unwrap_err();
        let message = validation_message(refused);
        assert!(message.contains("Invoice"), "{message}");
        assert!(message.contains("generated_send::Input"), "{message}");
    }
}
