//! The workflow start of a client spec on a client handle: the tree of a
//! spec that the client sent as data ([`ClientSpec`]), written by
//! `horsies_workflow_start`, with the children of its sub-workflow nodes
//! from the client's [`ChildSpecSource`].
//!
//! The entry point of the Python client's `start_workflow_async`
//! (`python/horsies/core/workflows/lifecycle.py`), which calls it through
//! `horsies._core` in place of its own statements.

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
use crate::workflow_engine::steps::{advance_parents, ChildBuild};

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
        let attempts = match resend_on_transient_err {
            true => 1 + START_RETRY_COUNT,
            false => 1,
        };
        let mut attempt: u32 = 0;
        loop {
            match self
                .client_start_attempt(spec, workflow_id, &arguments, sent_at, source)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if error.is_retryable() && attempt + 1 < attempts => {
                    let wait = retry_wait(attempt);
                    tracing::warn!(
                        workflow_name = %spec.name(),
                        workflow_id = %workflow_id,
                        attempt = attempt + 2,
                        wait_ms = wait.as_millis() as u64,
                        error = %error,
                        "workflow start failed (retryable), retrying",
                    );
                    tokio::time::sleep(wait).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
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
        let build = ChildBuild::Client(source);
        let started = start_tree(self, arguments, sent_at, build)
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
            advance_parents(self, CheckoutClass::Batch, started.parent_advances, build).await
        {
            tracing::error!(
                workflow_id = %workflow_id,
                child_workflow_id = %failure.failed_child_workflow_id,
                error = %failure.error,
                "parent advance after the workflow start failed; workflow recovery advances it",
            );
        }
        Ok(outcome)
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
    use super::*;

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
