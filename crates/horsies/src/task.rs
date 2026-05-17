use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::broker::{compute_enqueue_sha, TaskHandle};
use crate::core::{
    ErrorCode, HorsiesError, QueueMode, RegisteredTask, TaskNode, TaskOptions, TaskSendError,
    TaskSendErrorCode, TaskSendPayload, TaskSendResult, WorkerResilienceConfig,
};

use crate::lazy_broker::LazyBroker;

const SEND_RETRY_COUNT: u32 = 3;
const SEND_RETRY_INITIAL_MS: u64 = 200;
const SEND_RETRY_MAX_MS: u64 = 2000;

/// Per-send options for ad-hoc task sends.
///
/// For workflow tasks, prefer [`TaskNode::good_until`] so the deadline is
/// attached when the workflow spec is built.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskSendOptions {
    good_until: Option<DateTime<Utc>>,
}

impl TaskSendOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the task expiry deadline for this send.
    pub fn good_until(mut self, deadline: DateTime<Utc>) -> Self {
        self.good_until = Some(deadline);
        self
    }
}

pub struct TaskRegistrationBuilder<'a, A, T> {
    pub(crate) app: &'a mut crate::Horsies,
    pub(crate) name: String,
    pub(crate) task: RegisteredTask,
    pub(crate) queue: Option<String>,
    pub(crate) task_options: Option<TaskOptions>,
    pub(crate) _phantom: PhantomData<fn(A) -> T>,
}

impl<'a, A: Serialize + 'static, T: DeserializeOwned + Clone + 'static>
    TaskRegistrationBuilder<'a, A, T>
{
    pub(crate) fn new(app: &'a mut crate::Horsies, name: String, task: RegisteredTask) -> Self {
        Self {
            app,
            name,
            task,
            queue: None,
            task_options: None,
            _phantom: PhantomData,
        }
    }

    pub fn queue(mut self, queue: &str) -> Self {
        self.queue = Some(queue.to_owned());
        self
    }

    pub fn task_options(mut self, opts: TaskOptions) -> Self {
        self.task_options = Some(opts);
        self
    }

    pub fn register(mut self) -> Result<TaskFunction<A, T>, HorsiesError> {
        if self.app.runtime_catalog.is_frozen() {
            return Err(HorsiesError::new(format!(
                "runtime task handles are frozen; cannot register {} after task runtime use has begun",
                self.name
            )));
        }

        // Gap A: DEFAULT mode must not specify a queue at all.
        if self.queue.is_some() && matches!(self.app.core.config().queue_mode, QueueMode::Default) {
            return Err(HorsiesError::new(
                "queue cannot be specified in Default queue mode; \
                 remove .queue() or switch to Custom mode",
            )
            .with_code(ErrorCode::TaskInvalidOptions));
        }
        // Gap B: CUSTOM mode must specify a queue.
        if self.queue.is_none() && matches!(self.app.core.config().queue_mode, QueueMode::Custom) {
            return Err(HorsiesError::new(format!(
                "queue is required in Custom queue mode; \
                 call .queue() with one of: {:?}",
                self.app.core.get_valid_queue_names(),
            ))
            .with_code(ErrorCode::TaskInvalidOptions));
        }

        let queue_name = self.queue.as_deref().unwrap_or("default");
        self.app.core.validate_queue(queue_name).map_err(|err| {
            HorsiesError::new(format!(
                "queue '{}' is not in configured custom_queues; valid queues: {:?}",
                queue_name,
                self.app.core.get_valid_queue_names(),
            ))
            .with_code(ErrorCode::TaskInvalidOptions)
            .with_note(err.to_string())
        })?;
        let resolved_priority = self.app.core.effective_priority(queue_name, None);

        if let Some(ref opts) = self.task_options {
            if opts.good_until.is_some() {
                return Err(HorsiesError::new(
                    "definition-time good_until is not supported; use \
                     task.with_options(TaskSendOptions::new().good_until(deadline)).send(args) \
                     for ad-hoc sends, or task.node().good_until(deadline) for workflow tasks",
                )
                .with_code(ErrorCode::TaskInvalidOptions));
            }

            if let Some(ref rp) = opts.retry_policy {
                rp.validate()
                    .map_err(|e| HorsiesError::new(format!("invalid retry policy: {}", e)))?;
            }
        }

        if let Some(ref mut opts) = self.task_options {
            opts.task_name = self.name.clone();
            opts.queue_name = Some(queue_name.to_owned());
        }

        if let Some(ref opts) = self.task_options {
            self.task = self.task.with_task_options(opts.clone());
        }

        // Store input metadata for check() validation.
        self.task
            .set_expects_input(std::any::TypeId::of::<A>() != std::any::TypeId::of::<()>());
        self.task.set_input_type_name(std::any::type_name::<A>());

        match self.queue.as_deref() {
            Some(queue) => self
                .app
                .core
                .register_with_queue(&self.name, self.task, queue)?,
            None => self.app.core.register(&self.name, self.task)?,
        }

        // Refresh the workflow registry cache so WorkflowStarter sees
        // updated task defaults and queue priority maps.
        self.app.refresh_workflow_registry_cache();

        let handle = TaskFunction::new(
            self.name,
            Arc::clone(&self.app.broker),
            queue_name.to_owned(),
            resolved_priority,
            self.task_options,
            self.app.core.suppress_sends_handle(),
            self.app.core.config().resend_on_transient_err,
            self.app.core.config().resilience.clone(),
        );
        self.app.store_task_handle(&handle)?;
        Ok(handle)
    }

    pub fn finish(self) -> Result<TaskFunction<A, T>, HorsiesError> {
        self.register()
    }
}

pub struct TaskFunction<A, T> {
    task_name: String,
    broker: Arc<LazyBroker>,
    queue_name: String,
    priority: u32,
    task_options: Option<TaskOptions>,
    suppress_sends: Arc<AtomicBool>,
    resend_on_transient_err: bool,
    resilience: WorkerResilienceConfig,
    _phantom: PhantomData<fn(A) -> T>,
}

pub struct TaskFunctionSendOptions<'a, A, T> {
    task: &'a TaskFunction<A, T>,
    options: TaskSendOptions,
}

impl<A: Serialize, T: DeserializeOwned + Clone> TaskFunction<A, T> {
    pub(crate) fn new(
        task_name: String,
        broker: Arc<LazyBroker>,
        queue_name: String,
        priority: u32,
        task_options: Option<TaskOptions>,
        suppress_sends: Arc<AtomicBool>,
        resend_on_transient_err: bool,
        resilience: WorkerResilienceConfig,
    ) -> Self {
        Self {
            task_name,
            broker,
            queue_name,
            priority,
            task_options,
            suppress_sends,
            resend_on_transient_err,
            resilience,
            _phantom: PhantomData,
        }
    }

    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn priority(&self) -> u32 {
        self.priority
    }

    pub fn task_options(&self) -> Option<&TaskOptions> {
        self.task_options.as_ref()
    }

    pub fn with_options(&self, options: TaskSendOptions) -> TaskFunctionSendOptions<'_, A, T> {
        TaskFunctionSendOptions {
            task: self,
            options,
        }
    }

    pub async fn send(&self, args: A) -> TaskSendResult<TaskHandle<T>> {
        self.send_inner(args, GoodUntilMode::Inherit, None).await
    }

    pub async fn send_with_options(
        &self,
        options: TaskSendOptions,
        args: A,
    ) -> TaskSendResult<TaskHandle<T>> {
        self.send_inner(args, GoodUntilMode::Override(options.good_until), None)
            .await
    }

    pub async fn schedule(&self, delay: Duration, args: A) -> TaskSendResult<TaskHandle<T>> {
        self.send_inner(args, GoodUntilMode::Inherit, Some(delay))
            .await
    }

    pub async fn schedule_with_options(
        &self,
        options: TaskSendOptions,
        delay: Duration,
        args: A,
    ) -> TaskSendResult<TaskHandle<T>> {
        self.send_inner(
            args,
            GoodUntilMode::Override(options.good_until),
            Some(delay),
        )
        .await
    }

    async fn send_inner(
        &self,
        args: A,
        good_until_mode: GoodUntilMode,
        delay: Option<Duration>,
    ) -> TaskSendResult<TaskHandle<T>> {
        self.check_suppression()?;
        let (args_json, kwargs_json) = serialize_args::<A>(&self.task_name, &args)?;

        let (good_until, task_options_json) = self.resolve_send_options(good_until_mode)?;
        let sent_at = Utc::now();
        let delay_secs = delay.map(|d| d.as_secs() as i64);
        let enqueued_at = delay.map(|d| {
            sent_at + chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::zero())
        });
        let pre_task_id = Uuid::new_v4().to_string();

        let enqueue_sha = compute_enqueue_sha(
            &self.task_name,
            &self.queue_name,
            self.priority as i32,
            args_json.as_deref(),
            kwargs_json.as_deref(),
            sent_at,
            good_until,
            delay_secs,
            task_options_json.as_deref(),
        );

        let payload = TaskSendPayload {
            task_name: self.task_name.clone(),
            queue_name: self.queue_name.clone(),
            priority: self.priority as i32,
            args_json: args_json.clone(),
            kwargs_json: kwargs_json.clone(),
            sent_at,
            good_until,
            enqueue_delay_seconds: delay_secs,
            task_options: task_options_json.clone(),
            enqueue_sha: enqueue_sha.clone(),
        };

        self.enqueue_with_retry(
            args_json.as_deref(),
            kwargs_json.as_deref(),
            &pre_task_id,
            sent_at,
            enqueued_at,
            good_until,
            task_options_json.as_deref(),
            &enqueue_sha,
            &payload,
        )
        .await
    }

    pub async fn retry_send(&self, err: &TaskSendError) -> TaskSendResult<TaskHandle<T>> {
        let (task_id, payload) = validate_retry(err, &self.task_name, RetryKind::Send)?;
        let broker = self
            .broker
            .get_ready(&self.resilience)
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(task_id.to_owned()),
                payload: err.payload.clone(),
            })?;

        let handle = broker
            .retry_send(payload, Some(task_id))
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(task_id.to_owned()),
                payload: err.payload.clone(),
            })?;

        Ok(handle)
    }

    pub async fn retry_schedule(&self, err: &TaskSendError) -> TaskSendResult<TaskHandle<T>> {
        let (task_id, payload) = validate_retry(err, &self.task_name, RetryKind::Schedule)?;
        let broker = self
            .broker
            .get_ready(&self.resilience)
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(task_id.to_owned()),
                payload: err.payload.clone(),
            })?;

        let handle = broker
            .retry_send(payload, Some(task_id))
            .await
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::EnqueueFailed,
                message: format!("{}", e),
                retryable: e.is_retryable(),
                task_id: Some(task_id.to_owned()),
                payload: err.payload.clone(),
            })?;

        Ok(handle)
    }

    #[allow(clippy::too_many_arguments)]
    async fn enqueue_with_retry(
        &self,
        args_json: Option<&str>,
        kwargs_json: Option<&str>,
        pre_task_id: &str,
        sent_at: chrono::DateTime<Utc>,
        enqueued_at: Option<chrono::DateTime<Utc>>,
        good_until: Option<chrono::DateTime<Utc>>,
        task_options_json: Option<&str>,
        enqueue_sha: &str,
        payload: &TaskSendPayload,
    ) -> TaskSendResult<TaskHandle<T>> {
        let max_attempts = if self.resend_on_transient_err {
            1 + SEND_RETRY_COUNT
        } else {
            1
        };

        let mut last_err: Option<TaskSendError> = None;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                if let Some(ref err) = last_err {
                    if !err.retryable {
                        return Err(last_err.take().expect("stored retry error"));
                    }
                }

                let delay_ms =
                    (SEND_RETRY_INITIAL_MS * 2u64.pow(attempt - 1)).min(SEND_RETRY_MAX_MS);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            let broker =
                self.broker
                    .get_ready(&self.resilience)
                    .await
                    .map_err(|e| TaskSendError {
                        code: TaskSendErrorCode::EnqueueFailed,
                        message: format!("{}", e),
                        retryable: e.is_retryable(),
                        task_id: Some(pre_task_id.to_owned()),
                        payload: Some(payload.clone()),
                    })?;

            match broker
                .enqueue(
                    &self.task_name,
                    args_json,
                    kwargs_json,
                    &self.queue_name,
                    self.priority as i32,
                    Some(sent_at),
                    enqueued_at,
                    good_until,
                    task_options_json,
                    enqueue_sha,
                    Some(pre_task_id),
                )
                .await
            {
                Ok(task_id) => {
                    return Ok(TaskHandle::new(task_id, broker));
                }
                Err(broker_err) => {
                    let err = TaskSendError {
                        code: TaskSendErrorCode::EnqueueFailed,
                        message: format!("{}", broker_err),
                        retryable: broker_err.is_retryable(),
                        task_id: Some(pre_task_id.to_owned()),
                        payload: Some(payload.clone()),
                    };
                    if err.retryable && attempt < max_attempts - 1 {
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_err.expect("retry loop should preserve last error"))
    }

    #[allow(clippy::result_large_err)]
    fn serialize_task_options(&self) -> TaskSendResult<Option<String>> {
        self.task_options
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::ValidationFailed,
                message: format!("task_options serialization failed: {}", e),
                retryable: false,
                task_id: None,
                payload: None,
            })
    }

    #[allow(clippy::result_large_err)]
    fn resolve_send_options(
        &self,
        good_until_mode: GoodUntilMode,
    ) -> TaskSendResult<(Option<DateTime<Utc>>, Option<String>)> {
        match good_until_mode {
            GoodUntilMode::Inherit => {
                let good_until = self.task_options.as_ref().and_then(|o| o.good_until);
                let task_options_json = self.serialize_task_options()?;
                Ok((good_until, task_options_json))
            }
            GoodUntilMode::Override(good_until) => {
                let task_options_json = self.serialize_task_options_with_good_until(good_until)?;
                Ok((good_until, task_options_json))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn serialize_task_options_with_good_until(
        &self,
        good_until: Option<DateTime<Utc>>,
    ) -> TaskSendResult<Option<String>> {
        let mut opts = self.task_options.clone().unwrap_or_else(|| TaskOptions {
            task_name: self.task_name.clone(),
            queue_name: Some(self.queue_name.clone()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
        });

        opts.task_name = self.task_name.clone();
        opts.queue_name = Some(self.queue_name.clone());
        opts.good_until = good_until;

        if opts.good_until.is_none() && opts.auto_retry_for.is_none() && opts.retry_policy.is_none()
        {
            return Ok(None);
        }

        serde_json::to_string(&opts)
            .map(Some)
            .map_err(|e| TaskSendError {
                code: TaskSendErrorCode::ValidationFailed,
                message: format!("task_options serialization failed: {}", e),
                retryable: false,
                task_id: None,
                payload: None,
            })
    }

    pub fn node(&self) -> TaskNode<T, A> {
        let mut node = TaskNode::<T, A>::raw(&self.task_name)
            .queue(self.queue_name.clone())
            .priority(self.priority as i32);

        if let Some(ref opts) = self.task_options {
            if let Some(json) = opts.serialize_retry_options() {
                node = node.task_options(json);
            }
        }

        node
    }

    #[allow(clippy::result_large_err)]
    fn check_suppression(&self) -> TaskSendResult<()> {
        if self.suppress_sends.load(Ordering::Relaxed) {
            return Err(TaskSendError {
                code: TaskSendErrorCode::SendSuppressed,
                message: format!(
                    "task send suppressed for {} (import/check phase)",
                    self.task_name
                ),
                retryable: false,
                task_id: None,
                payload: None,
            });
        }
        Ok(())
    }
}

impl<A: Serialize, T: DeserializeOwned + Clone> TaskFunctionSendOptions<'_, A, T> {
    pub async fn send(&self, args: A) -> TaskSendResult<TaskHandle<T>> {
        self.task.send_with_options(self.options, args).await
    }

    pub async fn schedule(&self, delay: Duration, args: A) -> TaskSendResult<TaskHandle<T>> {
        self.task
            .schedule_with_options(self.options, delay, args)
            .await
    }
}

#[derive(Clone, Copy)]
enum GoodUntilMode {
    Inherit,
    Override(Option<DateTime<Utc>>),
}

impl<A, T> Clone for TaskFunction<A, T> {
    fn clone(&self) -> Self {
        Self {
            task_name: self.task_name.clone(),
            broker: Arc::clone(&self.broker),
            queue_name: self.queue_name.clone(),
            priority: self.priority,
            task_options: self.task_options.clone(),
            suppress_sends: Arc::clone(&self.suppress_sends),
            resend_on_transient_err: self.resend_on_transient_err,
            resilience: self.resilience.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<A, T> std::fmt::Debug for TaskFunction<A, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskFunction")
            .field("task_name", &self.task_name)
            .field("queue_name", &self.queue_name)
            .field("priority", &self.priority)
            .finish()
    }
}

enum RetryKind {
    Send,
    Schedule,
}

#[allow(clippy::needless_pass_by_value, clippy::result_large_err)]
fn validate_retry<'a>(
    err: &'a TaskSendError,
    task_name: &str,
    kind: RetryKind,
) -> TaskSendResult<(&'a str, &'a TaskSendPayload)> {
    if err.code != TaskSendErrorCode::EnqueueFailed {
        return Err(TaskSendError {
            code: TaskSendErrorCode::ValidationFailed,
            message: format!(
                "retry is only valid for ENQUEUE_FAILED errors, got {}",
                err.code
            ),
            retryable: false,
            task_id: None,
            payload: None,
        });
    }

    let task_id = err.task_id.as_deref().ok_or_else(|| TaskSendError {
        code: TaskSendErrorCode::ValidationFailed,
        message: "cannot retry: no task_id on error".to_owned(),
        retryable: false,
        task_id: None,
        payload: None,
    })?;

    let payload = err.payload.as_ref().ok_or_else(|| TaskSendError {
        code: TaskSendErrorCode::ValidationFailed,
        message: "cannot retry: no payload on error".to_owned(),
        retryable: false,
        task_id: None,
        payload: None,
    })?;

    if payload.task_name != task_name {
        return Err(TaskSendError {
            code: TaskSendErrorCode::ValidationFailed,
            message: format!(
                "cross-task retry rejected: error from '{}', task is '{}'",
                payload.task_name, task_name
            ),
            retryable: false,
            task_id: None,
            payload: None,
        });
    }

    match kind {
        RetryKind::Send if payload.enqueue_delay_seconds.is_some() => Err(TaskSendError {
            code: TaskSendErrorCode::ValidationFailed,
            message: "retry_send cannot replay a scheduled send; use retry_schedule".to_owned(),
            retryable: false,
            task_id: None,
            payload: None,
        }),
        RetryKind::Schedule if payload.enqueue_delay_seconds.is_none() => Err(TaskSendError {
            code: TaskSendErrorCode::ValidationFailed,
            message: "retry_schedule requires a delayed send error".to_owned(),
            retryable: false,
            task_id: None,
            payload: None,
        }),
        _ => Ok((task_id, payload)),
    }
}

#[allow(clippy::result_large_err)]
fn serialize_args<A: Serialize>(
    task_name: &str,
    args: &A,
) -> TaskSendResult<(Option<String>, Option<String>)> {
    let value = serde_json::to_value(args).map_err(|e| TaskSendError {
        code: TaskSendErrorCode::ValidationFailed,
        message: format!("failed to serialize args for '{}': {}", task_name, e),
        retryable: false,
        task_id: None,
        payload: None,
    })?;

    match value {
        serde_json::Value::Null => Ok((None, None)),
        serde_json::Value::Object(_) => Ok((None, Some(value.to_string()))),
        other => Ok((Some(other.to_string()), None)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        serialize_args, validate_retry, GoodUntilMode, RetryKind, TaskFunction, TaskSendOptions,
    };
    use crate::async_task_fn;
    use crate::core::{
        AppConfig, CustomQueueConfig, ErrorCode, Horsies as CoreHorsies, PostgresConfig, QueueMode,
        RecoveryConfig, TaskSendError, TaskSendErrorCode, TaskSendPayload, WorkerResilienceConfig,
    };
    use crate::core::{TaskErrorCode, TaskOptions};
    use crate::lazy_broker::LazyBroker;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn valid_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Args {
        a: i32,
        b: i32,
    }

    async fn add(args: Args) -> Result<i32, crate::core::TaskError> {
        Ok(args.a + args.b)
    }

    async fn double(value: i32) -> Result<i32, crate::core::TaskError> {
        Ok(value * 2)
    }

    fn task_function_with_options(opts: Option<TaskOptions>) -> TaskFunction<Args, i32> {
        let config = valid_config();
        TaskFunction::new(
            "deadline_task".to_owned(),
            Arc::new(LazyBroker::new(config.broker)),
            "default".to_owned(),
            100,
            opts,
            Arc::new(AtomicBool::new(false)),
            false,
            config.resilience,
        )
    }

    #[test]
    fn register_returns_task_function() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let task = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .register()
            .unwrap();

        assert_eq!(task.task_name(), "add");
        assert_eq!(task.queue_name(), "default");
    }

    #[test]
    fn registration_rejects_definition_time_good_until() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let deadline = chrono::Utc::now() + chrono::Duration::minutes(5);

        let err = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .task_options(TaskOptions {
                task_name: String::new(),
                queue_name: None,
                good_until: Some(deadline),
                auto_retry_for: None,
                retry_policy: None,
            })
            .register()
            .unwrap_err();

        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.message.contains("definition-time good_until"));
    }

    #[test]
    fn send_options_good_until_is_serialized_for_this_send() {
        let task = task_function_with_options(None);
        let deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
        let options = TaskSendOptions::new().good_until(deadline);

        let (good_until, json) = task
            .resolve_send_options(GoodUntilMode::Override(options.good_until))
            .unwrap();

        assert_eq!(good_until, Some(deadline));
        let parsed: serde_json::Value = serde_json::from_str(json.as_deref().unwrap()).unwrap();
        let stored = parsed["good_until"].as_str().unwrap();
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(stored)
                .unwrap()
                .with_timezone(&chrono::Utc),
            deadline,
        );
    }

    #[test]
    fn send_options_none_clears_existing_definition_time_good_until() {
        let stale = chrono::Utc::now() + chrono::Duration::minutes(5);
        let task = task_function_with_options(Some(TaskOptions {
            task_name: "deadline_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: Some(stale),
            auto_retry_for: None,
            retry_policy: None,
        }));

        let (good_until, json) = task
            .resolve_send_options(GoodUntilMode::Override(None))
            .unwrap();

        assert!(good_until.is_none());
        assert!(json.is_none());
    }

    #[test]
    fn send_options_preserves_retry_options_while_overriding_good_until() {
        let task = task_function_with_options(Some(TaskOptions {
            task_name: "deadline_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("TRANSIENT".to_owned())]),
            retry_policy: None,
        }));
        let deadline = chrono::Utc::now() + chrono::Duration::minutes(5);

        let (good_until, json) = task
            .resolve_send_options(GoodUntilMode::Override(Some(deadline)))
            .unwrap();

        assert_eq!(good_until, Some(deadline));
        let parsed: serde_json::Value = serde_json::from_str(json.as_deref().unwrap()).unwrap();
        let stored = parsed["good_until"].as_str().unwrap();
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(stored)
                .unwrap()
                .with_timezone(&chrono::Utc),
            deadline,
        );
        assert_eq!(parsed["auto_retry_for"][0], "TRANSIENT");
    }

    #[test]
    fn serialize_struct_as_kwargs() {
        let (args, kwargs) = serialize_args::<Args>("add", &Args { a: 1, b: 2 }).unwrap();
        assert!(args.is_none());
        assert!(kwargs.is_some());
        let parsed: serde_json::Value = serde_json::from_str(kwargs.as_ref().unwrap()).unwrap();
        assert_eq!(parsed["a"], 1);
        assert_eq!(parsed["b"], 2);
    }

    #[test]
    fn serialize_unit_as_nothing() {
        let (args, kwargs) = serialize_args::<()>("noop", &()).unwrap();
        assert!(args.is_none());
        assert!(kwargs.is_none());
    }

    #[test]
    fn serialize_scalar_as_args() {
        let (args, kwargs) = serialize_args::<i32>("scalar", &42).unwrap();
        assert_eq!(args.as_deref(), Some("42"));
        assert!(kwargs.is_none());
    }

    #[test]
    fn serialize_string_as_args() {
        let (args, kwargs) = serialize_args::<&str>("scalar", &"hello").unwrap();
        assert_eq!(args.as_deref(), Some("\"hello\""));
        assert!(kwargs.is_none());
    }

    #[test]
    fn serialize_vec_as_args() {
        let (args, kwargs) = serialize_args::<Vec<i32>>("scalar", &vec![1, 2, 3]).unwrap();
        assert_eq!(args.as_deref(), Some("[1,2,3]"));
        assert!(kwargs.is_none());
    }

    #[test]
    fn node_with_struct_populates_kwargs_and_defaults() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let task = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .register()
            .unwrap();

        let node = task.node().set_input(Args { a: 1, b: 2 }).unwrap();

        assert_eq!(node.task_name(), "add");
        let any = node.into_any_node(0);
        assert_eq!(any.queue.as_deref(), Some("default"));
        assert_eq!(any.priority, Some(100));
        assert!(any.args_json.is_none());
        let kwargs: serde_json::Value =
            serde_json::from_str(any.kwargs_json.as_deref().unwrap()).unwrap();
        assert_eq!(kwargs["a"], 1);
        assert_eq!(kwargs["b"], 2);
    }

    #[test]
    fn node_with_scalar_populates_args() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let task = app
            .task::<i32, i32>("double", async_task_fn!(double, i32))
            .unwrap()
            .register()
            .unwrap();

        let node = task.node().set_input(21).unwrap();
        let any = node.into_any_node(0);

        assert_eq!(any.args_json.as_deref(), Some("21"));
        assert!(any.kwargs_json.is_none());
    }

    fn make_payload(task_name: &str, delay: Option<i64>) -> TaskSendPayload {
        TaskSendPayload {
            task_name: task_name.to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            args_json: None,
            kwargs_json: None,
            sent_at: chrono::Utc::now(),
            good_until: None,
            enqueue_delay_seconds: delay,
            task_options: None,
            enqueue_sha: "sha".to_owned(),
        }
    }

    fn make_error(task_name: &str, delay: Option<i64>) -> TaskSendError {
        TaskSendError {
            code: TaskSendErrorCode::EnqueueFailed,
            message: "test error".to_owned(),
            retryable: true,
            task_id: Some("tid-1".to_owned()),
            payload: Some(make_payload(task_name, delay)),
        }
    }

    #[test]
    fn retry_send_rejects_non_enqueue_error() {
        let err = TaskSendError {
            code: TaskSendErrorCode::ValidationFailed,
            message: "bad".to_owned(),
            retryable: false,
            task_id: None,
            payload: None,
        };
        let result = validate_retry(&err, "my_task", RetryKind::Send);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code,
            TaskSendErrorCode::ValidationFailed
        );
    }

    #[test]
    fn retry_send_rejects_missing_payload() {
        let err = TaskSendError {
            code: TaskSendErrorCode::EnqueueFailed,
            message: "test".to_owned(),
            retryable: true,
            task_id: Some("tid-1".to_owned()),
            payload: None,
        };
        assert!(validate_retry(&err, "my_task", RetryKind::Send).is_err());
    }

    #[test]
    fn retry_send_rejects_missing_task_id() {
        let err = TaskSendError {
            code: TaskSendErrorCode::EnqueueFailed,
            message: "test".to_owned(),
            retryable: true,
            task_id: None,
            payload: Some(make_payload("my_task", None)),
        };
        assert!(validate_retry(&err, "my_task", RetryKind::Send).is_err());
    }

    #[test]
    fn retry_send_rejects_cross_task() {
        let err = make_error("other_task", None);
        let result = validate_retry(&err, "my_task", RetryKind::Send);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("cross-task"));
    }

    #[test]
    fn retry_send_rejects_scheduled_error() {
        let err = make_error("my_task", Some(60));
        let result = validate_retry(&err, "my_task", RetryKind::Send);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("retry_schedule"));
    }

    #[test]
    fn retry_send_accepts_immediate_error() {
        let err = make_error("my_task", None);
        let (task_id, payload) = validate_retry(&err, "my_task", RetryKind::Send).unwrap();
        assert_eq!(task_id, "tid-1");
        assert_eq!(payload.task_name, "my_task");
    }

    #[test]
    fn retry_schedule_rejects_non_scheduled_error() {
        let err = make_error("my_task", None);
        let result = validate_retry(&err, "my_task", RetryKind::Schedule);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("delayed send"));
    }

    #[test]
    fn retry_schedule_accepts_scheduled_error() {
        let err = make_error("my_task", Some(60));
        let (task_id, payload) = validate_retry(&err, "my_task", RetryKind::Schedule).unwrap();
        assert_eq!(task_id, "tid-1");
        assert_eq!(payload.enqueue_delay_seconds, Some(60));
    }

    // --- Builder-path queue mismatch guard (decorator equivalent) ---

    fn custom_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![
                CustomQueueConfig {
                    name: "fast".to_owned(),
                    priority: 1,
                    max_concurrency: 10,
                },
                CustomQueueConfig {
                    name: "slow".to_owned(),
                    priority: 50,
                    max_concurrency: 5,
                },
            ]),
            ..valid_config()
        }
    }

    // 1. DEFAULT + .queue("fast") → hard fail
    #[test]
    fn builder_rejects_queue_in_default_mode() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let err = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .queue("fast")
            .register()
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("Default queue mode"));
    }

    // 1b. DEFAULT + .queue("default") → also hard fail (matches Python)
    #[test]
    fn builder_rejects_explicit_default_queue_in_default_mode() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let err = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .queue("default")
            .register()
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("Default queue mode"));
    }

    // 2. CUSTOM + no .queue() → hard fail
    #[test]
    fn builder_rejects_missing_queue_in_custom_mode() {
        let core = CoreHorsies::new(custom_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let err = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .register()
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("Custom queue mode"));
    }

    // 3. CUSTOM + .queue("analytics") (not configured) → hard fail
    #[test]
    fn builder_rejects_unknown_queue_in_custom_mode() {
        let core = CoreHorsies::new(custom_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let err = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .queue("analytics")
            .register()
            .unwrap_err();
        // Falls through to validate_queue which rejects unknown queues.
        assert!(err.to_string().contains("analytics"));
    }

    // 4. CUSTOM + .queue("fast") → OK
    #[test]
    fn builder_accepts_valid_queue_in_custom_mode() {
        let core = CoreHorsies::new(custom_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let task = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .queue("fast")
            .register()
            .unwrap();
        assert_eq!(task.queue_name(), "fast");
    }

    // 5. DEFAULT + no .queue() → OK
    #[test]
    fn builder_accepts_no_queue_in_default_mode() {
        let core = CoreHorsies::new(valid_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);
        let task = app
            .task::<Args, i32>("add", async_task_fn!(add, Args))
            .unwrap()
            .register()
            .unwrap();
        assert_eq!(task.queue_name(), "default");
    }

    // 6. CUSTOM + one valid + one invalid → second registration fails
    #[test]
    fn builder_one_bad_task_poisons_startup() {
        let core = CoreHorsies::new(custom_config()).unwrap();
        let mut app = crate::Horsies::from_core(core);

        // First: valid
        app.task::<Args, i32>("good", async_task_fn!(add, Args))
            .unwrap()
            .queue("fast")
            .register()
            .unwrap();

        // Second: invalid queue → whole startup fails
        let err = app
            .task::<Args, i32>("bad", async_task_fn!(add, Args))
            .unwrap()
            .queue("analytics")
            .register()
            .unwrap_err();
        assert!(err.to_string().contains("analytics"));
    }
}
