//! The workflow start of a client spec on a client handle: the tree of a
//! spec that the client sent as data ([`ClientSpec`]), written by
//! `horsies_workflow_start`, with the children of its sub-workflow nodes
//! from the client's [`ChildSpecSource`].
//!
//! The entry point of the Python client's `start_workflow_async`
//! (`clients/python/horsies/core/workflows/lifecycle.py`), which calls it through
//! `horsies._core` in place of its own statements.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::db::CheckoutClass;
use crate::workflow_engine::child_spec::{ChildSpecSource, ClientSpec};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start_statement::{
    client_start_arguments, start_tree, StartArguments, StartOutcome,
};
use crate::workflow_engine::steps::advance_parents;

/// Retries of a start with `resend_on_transient_err` after a retryable
/// failure (Python `_START_RETRY_COUNT`).
pub const START_RETRY_COUNT: u32 = 3;

/// The wait before the first retry; each later wait doubles (Python
/// `_START_RETRY_INITIAL_MS`).
pub const START_RETRY_INITIAL: Duration = Duration::from_millis(200);

/// The longest wait between two attempts (Python `_START_RETRY_MAX_MS`).
pub const START_RETRY_MAX: Duration = Duration::from_millis(2_000);

/// What a start of a client spec found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientStartOutcome {
    /// The tree was written.
    Started,
    /// A workflow with this id and this name exists, or the row with the id
    /// was gone by the read of its name. Nothing was written.
    Exists,
    /// A workflow with this id exists under `existing_name`. Nothing was
    /// written.
    IdTaken { existing_name: String },
}

/// Why a start of a client spec failed.
#[derive(Debug, thiserror::Error)]
pub enum ClientStartError {
    /// The install of the schema failed; nothing of the start was sent.
    #[error("Schema initialization failed: {0}")]
    SchemaInstall(BrokerError),
    /// The tree build, the client family check, the statement or an owed
    /// child start failed. Nothing committed, or the commit outcome is
    /// unknown (a resend with the same id then finds the workflow).
    #[error("{0}")]
    Start(WorkflowError),
}

impl ClientStartError {
    /// Whether a resend of the same start can succeed. Pure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::SchemaInstall(error) => error.is_retryable(),
            Self::Start(error) => error.is_retryable(),
        }
    }

    /// The error that the start met, as a workflow engine error: the broker
    /// error of a failed install, or the start's own error. The cause of
    /// `WorkflowStartError::EnqueueFailed`. Pure.
    pub fn into_workflow_error(self) -> WorkflowError {
        match self {
            Self::SchemaInstall(error) => WorkflowError::Broker(error),
            Self::Start(error) => error,
        }
    }
}

impl PostgresBroker {
    /// Start the workflow `workflow_id` of a client spec (Python
    /// `start_workflow_async`).
    ///
    /// Steps:
    /// 1. The tree, once: the workflow row, every node row, a pre-built
    ///    child for every sub-workflow root from `source` (recursively), and a
    ///    failed-node record for each root whose child cannot start. A build
    ///    runs once per start, also when the id exists and on a retry.
    /// 2. Each attempt: the schema install (once per handle), the client
    ///    family check (once per handle), then `horsies_workflow_start` with
    ///    `sent_at` as the start call time: one statement in autocommit, or a
    ///    transaction with the child starts it owes when the tree has a
    ///    failed-node record.
    /// 3. After the commit, the parent advances that those child starts owe,
    ///    one transaction per level. A failed advance is logged and left to
    ///    workflow recovery; the start returns its outcome.
    ///
    /// `resend_on_transient_err`: a retryable failure of step 2 runs the
    /// step again, at most [`START_RETRY_COUNT`] times, after a wait of
    /// [`START_RETRY_INITIAL`] that doubles up to [`START_RETRY_MAX`].
    ///
    /// Errors: [`ClientStartError`]. A failure of the tree build is not
    /// retried: nothing was sent.
    pub async fn start_client_workflow(
        &self,
        spec: &ClientSpec,
        workflow_id: Uuid,
        sent_at: DateTime<Utc>,
        resend_on_transient_err: bool,
        source: &dyn ChildSpecSource,
    ) -> Result<ClientStartOutcome, ClientStartError> {
        let arguments = client_start_arguments(spec, workflow_id, source)
            .await
            .map_err(ClientStartError::Start)?;
        resend_while_transient(
            spec.name(),
            workflow_id,
            resend_on_transient_err,
            || self.client_start_attempt(spec, workflow_id, &arguments, sent_at, source),
            tokio::time::sleep,
        )
        .await
    }

    /// One attempt of step 2 and step 3 of [`Self::start_client_workflow`].
    async fn client_start_attempt(
        &self,
        spec: &ClientSpec,
        workflow_id: Uuid,
        arguments: &StartArguments,
        sent_at: DateTime<Utc>,
        source: &dyn ChildSpecSource,
    ) -> Result<ClientStartOutcome, ClientStartError> {
        self.ensure_schema_initialized()
            .await
            .map_err(ClientStartError::SchemaInstall)?;
        self.ensure_client_family_for_write()
            .await
            .map_err(|error| ClientStartError::Start(WorkflowError::Broker(error)))?;
        let started = start_tree(self, arguments, sent_at, source)
            .await
            .map_err(ClientStartError::Start)?;
        let outcome = match started.outcome {
            StartOutcome::Started => {
                tracing::debug!(workflow_id = %workflow_id, name = %spec.name(), "workflow created");
                ClientStartOutcome::Started
            }
            StartOutcome::Exists { stored_name } => match stored_name {
                Some(existing_name) if existing_name != spec.name() => {
                    ClientStartOutcome::IdTaken { existing_name }
                }
                Some(_) | None => ClientStartOutcome::Exists,
            },
        };
        if let Err(failure) =
            advance_parents(self, CheckoutClass::Batch, started.parent_advances, source).await
        {
            tracing::error!(
                workflow_id = %workflow_id,
                child_workflow_id = %failure.failed_child_workflow_id,
                error = %failure.error,
                "Parent advance after the start of workflow {workflow_id} failed at child \
                 workflow {}: {}; workflow recovery advances it",
                failure.failed_child_workflow_id,
                failure.error,
            );
        }
        Ok(outcome)
    }
}

/// Runs `attempt` once. With `resend_on_transient_err`, runs it again after
/// each retryable failure, at most [`START_RETRY_COUNT`] more times, and
/// waits through `wait` for [`retry_wait`] of that retry before each run.
/// Returns the first success, or the failure of the last run. A failure that
/// is not retryable is never run again. Logs each retry at WARNING.
async fn resend_while_transient<T, Attempt, AttemptFuture, Wait, WaitFuture>(
    workflow_name: &str,
    workflow_id: Uuid,
    resend_on_transient_err: bool,
    mut attempt: Attempt,
    mut wait: Wait,
) -> Result<T, ClientStartError>
where
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = Result<T, ClientStartError>>,
    Wait: FnMut(Duration) -> WaitFuture,
    WaitFuture: Future<Output = ()>,
{
    let attempts = match resend_on_transient_err {
        true => 1 + START_RETRY_COUNT,
        false => 1,
    };
    let mut retry: u32 = 0;
    loop {
        match attempt().await {
            Ok(outcome) => return Ok(outcome),
            Err(error) if error.is_retryable() && retry + 1 < attempts => {
                let pause = retry_wait(retry);
                tracing::warn!(
                    workflow_name = %workflow_name,
                    workflow_id = %workflow_id,
                    attempt = retry + 2,
                    wait_ms = pause.as_millis() as u64,
                    error = %error,
                    "workflow start failed (retryable), retrying: {error}",
                );
                wait(pause).await;
                retry += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// The wait before retry `attempt + 1` (0 is the first retry). Pure.
fn retry_wait(attempt: u32) -> Duration {
    // At most `START_RETRY_COUNT` retries, so the doubling stays small.
    let doubled = START_RETRY_INITIAL.saturating_mul(2_u32.saturating_pow(attempt));
    doubled.min(START_RETRY_MAX)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;

    use super::*;

    /// A failure whose cause may clear: no connection.
    fn transient() -> ClientStartError {
        ClientStartError::SchemaInstall(BrokerError::ConnectionFailed("refused".to_owned()))
    }

    /// The runs of [`resend_while_transient`] over `answers` (one per run, in
    /// order), with the waits it asked for and the number of runs.
    fn resend_over(
        resend_on_transient_err: bool,
        answers: Vec<Result<&'static str, ClientStartError>>,
    ) -> (Result<&'static str, ClientStartError>, Vec<Duration>, usize) {
        let answers = RefCell::new(VecDeque::from(answers));
        let runs = Cell::new(0_usize);
        let waits = RefCell::new(Vec::new());
        let result = futures::executor::block_on(resend_while_transient(
            "resend_wf",
            Uuid::nil(),
            resend_on_transient_err,
            || {
                runs.set(runs.get() + 1);
                let answer = answers
                    .borrow_mut()
                    .pop_front()
                    .expect("the test gives an answer for each run");
                std::future::ready(answer)
            },
            |pause| {
                waits.borrow_mut().push(pause);
                std::future::ready(())
            },
        ));
        (result, waits.into_inner(), runs.get())
    }

    #[test]
    fn a_start_without_resend_runs_once_on_a_transient_failure() {
        let (result, waits, runs) = resend_over(false, vec![Err(transient())]);

        assert!(
            matches!(result, Err(ref error) if error.is_retryable()),
            "{result:?}"
        );
        assert_eq!(runs, 1);
        assert!(waits.is_empty());
    }

    /// A start that fails with a transient failure every time runs
    /// `1 + START_RETRY_COUNT` times. The first wait is `START_RETRY_INITIAL`,
    /// each later one doubles, and none passes `START_RETRY_MAX`.
    #[test]
    fn a_transient_failure_is_resent_the_retry_count_with_doubling_waits() {
        let answers = (0..=START_RETRY_COUNT).map(|_| Err(transient())).collect();

        let (result, waits, runs) = resend_over(true, answers);

        assert!(result.is_err(), "{result:?}");
        assert_eq!(runs, 1 + START_RETRY_COUNT as usize);
        assert_eq!(waits.len(), START_RETRY_COUNT as usize);
        assert_eq!(waits[0], START_RETRY_INITIAL);
        for pair in waits.windows(2) {
            assert_eq!(pair[1], (pair[0] * 2).min(START_RETRY_MAX));
        }
        assert!(waits.iter().all(|&pause| pause <= START_RETRY_MAX));
    }

    #[test]
    fn a_resent_start_returns_the_first_success() {
        let (result, waits, runs) = resend_over(
            true,
            vec![Err(transient()), Err(transient()), Ok("started")],
        );

        assert_eq!(result.expect("the third run succeeds"), "started");
        assert_eq!(runs, 3);
        assert_eq!(waits, vec![retry_wait(0), retry_wait(1)]);
    }

    /// A failure that cannot clear is not resent: a schema install refused
    /// for its data, and a start refused for its spec.
    #[test]
    fn a_failure_that_cannot_clear_is_not_resent() {
        for failure in [
            ClientStartError::SchemaInstall(BrokerError::InvalidWorkflowNodeStatus),
            ClientStartError::Start(WorkflowError::Validation("refused".to_owned())),
        ] {
            assert!(!failure.is_retryable(), "{failure:?}");

            let (result, waits, runs) = resend_over(true, vec![Err(failure)]);

            assert!(result.is_err(), "{result:?}");
            assert_eq!(runs, 1);
            assert!(waits.is_empty());
        }
    }

    #[test]
    fn the_retry_wait_doubles_up_to_its_bound() {
        assert_eq!(retry_wait(0), Duration::from_millis(200));
        assert_eq!(retry_wait(1), Duration::from_millis(400));
        assert_eq!(retry_wait(2), Duration::from_millis(800));
        assert_eq!(retry_wait(4), Duration::from_millis(2_000));
    }

    #[test]
    fn a_schema_install_failure_names_the_install() {
        let error =
            ClientStartError::SchemaInstall(BrokerError::ConnectionFailed("bad".to_owned()));
        assert!(error
            .to_string()
            .starts_with("Schema initialization failed: "));
    }
}
