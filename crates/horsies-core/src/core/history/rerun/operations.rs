//! Post-terminal rerun as a fresh, lineage-bearing enqueue.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::core::history::ddl::classes::DEFAULT_RETENTION_CLASS_KEY;
use crate::core::history::enqueue::{prepare_enqueue_facts_with_lineage, EnqueuePreparationError};
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::fingerprint::FingerprintError;
use crate::core::history::identity::keys::{
    validate_reservation_window, IdempotencyKeyError, IDEMPOTENCY_SCOPE_VERSION,
};
use crate::core::history::identity::reservations::{claim_key_reservation, ReservationClaim};
use crate::core::history::identity::uuid7::{mint_task_id, Uuid7Error};
use crate::core::history::names::{LIVE_TASKS, RETENTION_CLASSES};
use crate::core::history::reads::detail::{read_task_detail, TaskAbsenceReason, TaskDetailResult};
use crate::core::history::rerun::input_envelope::{
    decode_framed_rerun_input, InputEnvelopeDecodeError,
};
use crate::core::types::status::TaskStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunTask {
    source_task_id: Uuid,
    deadline: Option<DateTime<Utc>>,
    caller_key: Option<String>,
}

impl RerunTask {
    pub fn new(
        source_task_id: Uuid,
        deadline: Option<DateTime<Utc>>,
        caller_key: Option<String>,
    ) -> Self {
        Self {
            source_task_id,
            deadline,
            caller_key,
        }
    }

    pub fn source_task_id(&self) -> Uuid {
        self.source_task_id
    }

    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }

    pub fn caller_key(&self) -> Option<&str> {
        self.caller_key.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunEnqueuePolicy {
    retention_class_key: String,
    retain_rerun_input: bool,
    reservation_window: Duration,
}

impl RerunEnqueuePolicy {
    pub fn new(
        retention_class_key: impl Into<String>,
        retain_rerun_input: bool,
        reservation_window: Duration,
    ) -> Result<Self, RerunError> {
        let retention_class_key = retention_class_key.into();
        if retention_class_key.is_empty() {
            return Err(RerunError::InvalidPolicy(
                "retention class key must be non-empty".to_owned(),
            ));
        }
        validate_reservation_window(reservation_window)?;
        Ok(Self {
            retention_class_key,
            retain_rerun_input,
            reservation_window,
        })
    }

    pub fn standard(retain_rerun_input: bool) -> Self {
        Self {
            retention_class_key: DEFAULT_RETENTION_CLASS_KEY.to_owned(),
            retain_rerun_input,
            reservation_window: Duration::hours(24),
        }
    }

    pub fn retention_class_key(&self) -> &str {
        &self.retention_class_key
    }

    pub fn retain_rerun_input(&self) -> bool {
        self.retain_rerun_input
    }

    pub fn reservation_window(&self) -> Duration {
        self.reservation_window
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotEligibleReason {
    CompletedSource,
    WorkflowTask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerunOutcome {
    Enqueued {
        new_task_id: Uuid,
        source_task_id: Uuid,
        rerun_root_task_id: Uuid,
    },
    SourceLive {
        task_id: Uuid,
    },
    SourceAbsent {
        task_id: Uuid,
        reason: TaskAbsenceReason,
    },
    NotEligible {
        task_id: Uuid,
        reason: NotEligibleReason,
    },
    InputUnavailable {
        task_id: Uuid,
        disposition: String,
    },
    InputCorrupt {
        task_id: Uuid,
        detail: String,
    },
    KeyConflict {
        task_id: Uuid,
        reserved_by_task_id: Uuid,
    },
    KeyReplay {
        existing_task_id: Uuid,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RerunError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("history read failed: {0}")]
    History(#[from] HistoryError),
    #[error("unknown retention class {0:?}: register the class before rerunning into it")]
    UnknownRetentionClass(String),
    #[error("invalid rerun policy: {0}")]
    InvalidPolicy(String),
    #[error("rerun enqueue preparation failed: {0}")]
    EnqueuePreparation(#[from] EnqueuePreparationError),
    #[error("rerun fingerprint failed: {0}")]
    Fingerprint(#[from] FingerprintError),
    #[error("rerun idempotency key failed: {0}")]
    Idempotency(#[from] IdempotencyKeyError),
    #[error("rerun identity mint failed: {0}")]
    Identity(#[from] Uuid7Error),
    #[error("rerun JSON serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub async fn rerun_task_in_tx(
    connection: &mut PgConnection,
    command: &RerunTask,
    policy: &RerunEnqueuePolicy,
) -> Result<RerunOutcome, RerunError> {
    let registered_sql =
        format!("SELECT EXISTS (SELECT 1 FROM {RETENTION_CLASSES} WHERE class_key = $1)");
    let registered: bool = sqlx::query_scalar(&registered_sql)
        .bind(policy.retention_class_key())
        .fetch_one(&mut *connection)
        .await?;
    if !registered {
        return Err(RerunError::UnknownRetentionClass(
            policy.retention_class_key.clone(),
        ));
    }

    let detail = match read_task_detail(connection, command.source_task_id).await? {
        TaskDetailResult::Live { task_id } => return Ok(RerunOutcome::SourceLive { task_id }),
        TaskDetailResult::Absent { task_id, reason } => {
            return Ok(RerunOutcome::SourceAbsent { task_id, reason });
        }
        TaskDetailResult::History(detail) => detail,
    };

    if detail.is_workflow_task {
        return Ok(RerunOutcome::NotEligible {
            task_id: command.source_task_id,
            reason: NotEligibleReason::WorkflowTask,
        });
    }
    if detail.status == TaskStatus::Completed {
        return Ok(RerunOutcome::NotEligible {
            task_id: command.source_task_id,
            reason: NotEligibleReason::CompletedSource,
        });
    }
    if detail.rerun_input_disposition != "INLINE" {
        return Ok(RerunOutcome::InputUnavailable {
            task_id: command.source_task_id,
            disposition: detail.rerun_input_disposition,
        });
    }
    let (Some(version), Some(payload), Some(digest)) = (
        detail.rerun_input_version,
        detail.rerun_input_inline.as_deref(),
        detail.rerun_input_digest.as_deref(),
    ) else {
        return Ok(RerunOutcome::InputCorrupt {
            task_id: command.source_task_id,
            detail: "inline disposition with an incomplete envelope".to_owned(),
        });
    };
    let framed = match decode_framed_rerun_input(version, payload, digest) {
        Ok(decoded) => decoded,
        Err(InputEnvelopeDecodeError::VersionUnknown(version)) => {
            return Ok(RerunOutcome::InputCorrupt {
                task_id: command.source_task_id,
                detail: format!("unknown input-envelope version {version}"),
            });
        }
        Err(InputEnvelopeDecodeError::Corrupt(detail)) => {
            return Ok(RerunOutcome::InputCorrupt {
                task_id: command.source_task_id,
                detail,
            });
        }
    };

    let new_task_id = mint_task_id()?;
    let root_task_id = detail.rerun_root_task_id.unwrap_or(detail.task_id);
    let facts = prepare_enqueue_facts_with_lineage(
        &detail.task_name,
        &detail.queue_name,
        detail.priority,
        &framed.input,
        command.deadline,
        None,
        framed.options.as_deref(),
        Some(policy.retention_class_key()),
        policy.retain_rerun_input,
        command.caller_key(),
        Some(detail.task_id),
        Some(root_task_id),
    )?;

    if let Some(key_digest) = facts.idempotency_key_digest.as_ref() {
        match claim_key_reservation(
            connection,
            key_digest,
            IDEMPOTENCY_SCOPE_VERSION,
            policy.reservation_window.num_seconds(),
            facts.command_fingerprint_version,
            &facts.command_fingerprint,
            new_task_id,
        )
        .await?
        {
            ReservationClaim::Replay { task_id } => {
                return Ok(RerunOutcome::KeyReplay {
                    existing_task_id: task_id,
                });
            }
            ReservationClaim::Conflict { task_id, .. } => {
                return Ok(RerunOutcome::KeyConflict {
                    task_id: command.source_task_id,
                    reserved_by_task_id: task_id,
                });
            }
            ReservationClaim::Applied { .. } => {}
        }
    }

    let sent_at = Utc::now();
    // Every NOT NULL column is named: the baseline schema has no column
    // defaults. The rerun carries the source's exact input and options texts.
    let insert_sql = format!(
        "INSERT INTO {LIVE_TASKS} (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at,
             command_fingerprint_version, command_fingerprint, idempotency_key_digest,
             status, enqueued_at, retry_count, max_retries, good_until,
             is_workflow_task, updated_at,
             retain_rerun_input, rerun_of_task_id, rerun_root_task_id
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, statement_timestamp(), $10, $11, $12,
             'PENDING', statement_timestamp(), 0, $13, $14, FALSE, statement_timestamp(),
             $15, $16, $17
         )"
    );
    sqlx::query(&insert_sql)
        .bind(new_task_id)
        .bind(&detail.task_name)
        .bind(&detail.queue_name)
        .bind(detail.priority)
        .bind(&framed.input)
        .bind(framed.options.as_deref())
        .bind(facts.execution.as_str())
        .bind(&facts.retention_class_key)
        .bind(sent_at)
        .bind(facts.command_fingerprint_version)
        .bind(facts.command_fingerprint.as_slice())
        .bind(
            facts
                .idempotency_key_digest
                .as_ref()
                .map(<[u8; 32]>::as_slice),
        )
        .bind(detail.max_retries)
        .bind(command.deadline)
        .bind(facts.retain_rerun_input)
        .bind(detail.task_id)
        .bind(root_task_id)
        .execute(&mut *connection)
        .await?;

    Ok(RerunOutcome::Enqueued {
        new_task_id,
        source_task_id: detail.task_id,
        rerun_root_task_id: root_task_id,
    })
}
