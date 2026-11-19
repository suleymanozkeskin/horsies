//! The send of one task in one statement: `horsies_enqueue_task`
//! (Python `clients/python/horsies/core/brokers/enqueue_task.py`).
//!
//! The function writes one PENDING row, or answers what the row that holds
//! the task id or the key holds:
//! - an unkeyed send inserts the row; a task id that a live row holds
//!   answers `SAME_COMMAND` when the stored command fingerprint and `sent_at`
//!   are the sent ones, else `PAYLOAD_MISMATCH`;
//! - a keyed send claims the key reservation first: a key that another send
//!   holds answers `KEY_REPLAY` (same command) or `KEY_CONFLICT`, with the id
//!   that holds the key; a keyed send on a live row of the same command binds
//!   its key to the row, and a row bound to another key is
//!   `PAYLOAD_MISMATCH`;
//! - a [`SendKind::Repeat`] send reads history first, under the task advisory
//!   lock: a task that finished answers `SAME_COMMAND` or `PAYLOAD_MISMATCH`
//!   by the same comparison, and never runs a second time.
//!
//! The statement runs alone in autocommit; a refusal writes nothing and
//! leaves no key reservation of this send.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::error::{BrokerError, EnqueueRefusal};
use crate::core::history::enqueue::SendPart;
use crate::core::history::identity::keys::IDEMPOTENCY_SCOPE_VERSION;
use crate::db::{ParameterType, TypedStatement};

/// `horsies_enqueue_task` for a first run (no rerun lineage). Parameters in
/// order: task id, task name, queue name, priority, input, task options,
/// execution, retention class key, retain rerun input, max retries, good
/// until, sent at, start kind, start time, start delay in seconds,
/// fingerprint version, fingerprint, key kind, key digest, key scope
/// version, key window in microseconds, send kind, trace kind, trace
/// carrier text, trace id (`StoredTrace::columns`). The delay is cast to
/// `integer` by the server, as Python's statement does, so a value outside
/// the integer range is the server's refusal. The window is made into the
/// exact `interval` (`$21::bigint * interval '1 microsecond'`); NULL for an
/// unkeyed send.
pub(crate) static ENQUEUE_TASK: TypedStatement = TypedStatement::new(
    "enqueue_task",
    "SELECT outcome, task_id FROM horsies_enqueue_task(\
     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::integer, $16, $17, \
     'FIRST_RUN', NULL, NULL, $18, $19, $20, $21::bigint * interval '1 microsecond', $22, \
     $23, $24, $25)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Bool,
        ParameterType::Int4,
        ParameterType::Timestamptz,
        ParameterType::Timestamptz,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Int8,
        ParameterType::Int2,
        ParameterType::Bytea,
        ParameterType::Text,
        ParameterType::Bytea,
        ParameterType::Int2,
        ParameterType::Int8,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Bytea,
    ],
);

/// Whether a send is the first send of its task id or a repeat of it
/// (Python `SendKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    /// The first send of a task id: the function does not read history.
    First,
    /// A send retry: the same task id, command and `sent_at` again. The
    /// function reads history first, so a task that finished answers with
    /// its id and does not run again.
    Repeat,
}

impl SendKind {
    pub(crate) const fn as_sql(self) -> &'static str {
        match self {
            Self::First => "FIRST",
            Self::Repeat => "REPEAT",
        }
    }
}

/// When the row is due (Python `EnqueueStart`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueStart {
    /// At the database clock's now.
    Now,
    /// At the given instant.
    At(DateTime<Utc>),
    /// The given whole seconds after the database clock's now.
    AfterDelay { seconds: i64 },
}

impl EnqueueStart {
    /// The start of a send from its two optional arguments.
    ///
    /// Errors: [`BrokerError::EnqueueContract`] when both are given (Python
    /// `_check_enqueue_timing`); nothing is sent.
    pub(crate) fn of(
        enqueued_at: Option<DateTime<Utc>>,
        enqueue_delay_seconds: Option<i64>,
    ) -> Result<EnqueueStart, BrokerError> {
        match (enqueued_at, enqueue_delay_seconds) {
            (None, None) => Ok(Self::Now),
            (Some(at), None) => Ok(Self::At(at)),
            (None, Some(seconds)) => Ok(Self::AfterDelay { seconds }),
            (Some(_), Some(_)) => Err(BrokerError::EnqueueContract(EnqueueRefusal::by_rule(
                SendPart::Input,
                "cannot specify both enqueued_at and enqueue_delay_seconds".to_owned(),
            ))),
        }
    }

    /// `p_start_kind`, `p_start_at` and `p_start_delay_s`.
    pub(crate) fn columns(self) -> (&'static str, Option<DateTime<Utc>>, Option<i64>) {
        match self {
            Self::Now => ("NOW", None, None),
            Self::At(at) => ("AT", Some(at), None),
            Self::AfterDelay { seconds } => ("AFTER_DELAY", None, Some(seconds)),
        }
    }
}

/// The key of a send (Python `EnqueueKey`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueKey {
    Unkeyed,
    /// The scoped key digest, reserved for `window_micros` microseconds
    /// after its task finishes.
    Keyed {
        digest: [u8; 32],
        window_micros: i64,
    },
}

impl EnqueueKey {
    /// The key of a send with the optional digest of its scoped key.
    pub(crate) fn of(digest: Option<[u8; 32]>, window_micros: i64) -> EnqueueKey {
        match digest {
            Some(digest) => Self::Keyed {
                digest,
                window_micros,
            },
            None => Self::Unkeyed,
        }
    }

    /// `p_key_kind`, `p_key_scope_version` and the microseconds of
    /// `p_key_window`; the digest is [`EnqueueKey::digest`].
    pub(crate) fn columns(&self) -> (&'static str, Option<i16>, Option<i64>) {
        match self {
            Self::Unkeyed => ("UNKEYED", None, None),
            Self::Keyed { window_micros, .. } => (
                "KEYED",
                Some(IDEMPOTENCY_SCOPE_VERSION),
                Some(*window_micros),
            ),
        }
    }

    /// `p_key_digest`.
    pub(crate) fn digest(&self) -> Option<&[u8]> {
        match self {
            Self::Unkeyed => None,
            Self::Keyed { digest, .. } => Some(digest.as_slice()),
        }
    }
}

/// What `horsies_enqueue_task` answered (Python `EnqueueOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueTaskOutcome {
    /// The send wrote its row.
    Inserted,
    /// The task id holds this send: a live row, or a task that finished (a
    /// repeat send). A keyed send bound its key to the live row.
    SameCommand,
    /// The key is held by `holder` with the same command.
    KeyReplay { holder: Uuid },
    /// The key is held by `owner` with another command.
    KeyConflict { owner: Uuid },
    /// The task id holds another command or another `sent_at`, or its live
    /// row is bound to another key.
    PayloadMismatch,
    /// The task id was taken, and its row moved to history before the
    /// comparison read it.
    ConflictUnverifiable,
}

/// The outcome of the function's one row: its `outcome` text and its
/// `task_id` (the id that holds the key for the two key outcomes). Pure.
///
/// Errors: the unknown outcome text, which is a defect of the function.
pub(crate) fn decode_enqueue_outcome(
    outcome: &str,
    task_id: Uuid,
) -> Result<EnqueueTaskOutcome, String> {
    match outcome {
        "INSERTED" => Ok(EnqueueTaskOutcome::Inserted),
        "SAME_COMMAND" => Ok(EnqueueTaskOutcome::SameCommand),
        "KEY_REPLAY" => Ok(EnqueueTaskOutcome::KeyReplay { holder: task_id }),
        "KEY_CONFLICT" => Ok(EnqueueTaskOutcome::KeyConflict { owner: task_id }),
        "PAYLOAD_MISMATCH" => Ok(EnqueueTaskOutcome::PayloadMismatch),
        "CONFLICT_UNVERIFIABLE" => Ok(EnqueueTaskOutcome::ConflictUnverifiable),
        other => Err(format!("horsies_enqueue_task returned outcome {other:?}")),
    }
}

/// The result of an enqueue for `outcome` (Python `enqueue_result`). `Ok`
/// names the task that holds the send: the sent id, or the id that holds
/// the key for a replay. Pure.
pub(crate) fn enqueue_result(
    outcome: EnqueueTaskOutcome,
    task_id: Uuid,
    task_name: &str,
) -> Result<Uuid, BrokerError> {
    match outcome {
        EnqueueTaskOutcome::Inserted | EnqueueTaskOutcome::SameCommand => Ok(task_id),
        EnqueueTaskOutcome::KeyReplay { holder } => Ok(holder),
        EnqueueTaskOutcome::KeyConflict { owner } => Err(BrokerError::IdempotencyKeyConflict {
            task_name: task_name.to_owned(),
            task_id: owner,
        }),
        EnqueueTaskOutcome::PayloadMismatch => Err(BrokerError::PayloadMismatch { task_id }),
        EnqueueTaskOutcome::ConflictUnverifiable => Err(BrokerError::EnqueueConflictUnverifiable {
            task_id,
            task_name: task_name.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn each_outcome_text_has_its_own_outcome_and_an_unknown_one_is_refused() {
        let cases = [
            ("INSERTED", EnqueueTaskOutcome::Inserted),
            ("SAME_COMMAND", EnqueueTaskOutcome::SameCommand),
            (
                "KEY_REPLAY",
                EnqueueTaskOutcome::KeyReplay { holder: task(9) },
            ),
            (
                "KEY_CONFLICT",
                EnqueueTaskOutcome::KeyConflict { owner: task(9) },
            ),
            ("PAYLOAD_MISMATCH", EnqueueTaskOutcome::PayloadMismatch),
            (
                "CONFLICT_UNVERIFIABLE",
                EnqueueTaskOutcome::ConflictUnverifiable,
            ),
        ];
        for (text, outcome) in cases {
            assert_eq!(decode_enqueue_outcome(text, task(9)), Ok(outcome), "{text}");
        }
        let refused = decode_enqueue_outcome("APPLIED", task(9)).expect_err("not an outcome");
        assert!(refused.contains("\"APPLIED\""), "{refused}");
    }

    /// Python `enqueue_task.enqueue_result`.
    #[test]
    fn the_result_names_the_task_that_holds_the_send() {
        let sent = task(1);
        assert_eq!(
            enqueue_result(EnqueueTaskOutcome::Inserted, sent, "t").ok(),
            Some(sent)
        );
        assert_eq!(
            enqueue_result(EnqueueTaskOutcome::SameCommand, sent, "t").ok(),
            Some(sent)
        );
        assert_eq!(
            enqueue_result(EnqueueTaskOutcome::KeyReplay { holder: task(2) }, sent, "t").ok(),
            Some(task(2))
        );
        assert!(matches!(
            enqueue_result(EnqueueTaskOutcome::KeyConflict { owner: task(3) }, sent, "t"),
            Err(BrokerError::IdempotencyKeyConflict { task_id, .. }) if task_id == task(3)
        ));
        assert!(matches!(
            enqueue_result(EnqueueTaskOutcome::PayloadMismatch, sent, "t"),
            Err(BrokerError::PayloadMismatch { task_id }) if task_id == sent
        ));
        let unverifiable = enqueue_result(EnqueueTaskOutcome::ConflictUnverifiable, sent, "t")
            .expect_err("an unverifiable conflict is an error");
        assert!(matches!(
            unverifiable,
            BrokerError::EnqueueConflictUnverifiable { task_id, .. } if task_id == sent
        ));
        assert!(!unverifiable.is_retryable());
        // The texts that a Python send gets.
        let mismatch = enqueue_result(EnqueueTaskOutcome::PayloadMismatch, sent, "t")
            .expect_err("a payload mismatch is an error");
        assert!(
            mismatch.to_string().contains(&sent.to_string()),
            "{mismatch}"
        );
        assert!(
            unverifiable
                .to_string()
                .contains("cannot verify payload identity"),
            "{unverifiable}"
        );
    }

    #[test]
    fn a_start_takes_one_of_its_two_arguments_or_none() {
        let at = Utc::now();
        assert_eq!(EnqueueStart::of(None, None).ok(), Some(EnqueueStart::Now));
        assert_eq!(
            EnqueueStart::of(Some(at), None).ok(),
            Some(EnqueueStart::At(at))
        );
        assert_eq!(
            EnqueueStart::of(None, Some(60)).ok(),
            Some(EnqueueStart::AfterDelay { seconds: 60 })
        );
        assert!(matches!(
            EnqueueStart::of(Some(at), Some(60)),
            Err(BrokerError::EnqueueContract(_))
        ));
        assert_eq!(EnqueueStart::Now.columns(), ("NOW", None, None));
        assert_eq!(EnqueueStart::At(at).columns(), ("AT", Some(at), None));
        assert_eq!(
            EnqueueStart::AfterDelay { seconds: 60 }.columns(),
            ("AFTER_DELAY", None, Some(60))
        );
    }

    #[test]
    fn a_key_names_its_digest_scope_and_window_and_no_key_names_none() {
        let keyed = EnqueueKey::of(Some([7; 32]), 1_500_000);
        assert_eq!(
            keyed.columns(),
            ("KEYED", Some(IDEMPOTENCY_SCOPE_VERSION), Some(1_500_000))
        );
        assert_eq!(keyed.digest(), Some([7_u8; 32].as_slice()));
        let unkeyed = EnqueueKey::of(None, 1_500_000);
        assert_eq!(unkeyed.columns(), ("UNKEYED", None, None));
        assert_eq!(unkeyed.digest(), None);
    }

    #[test]
    fn the_send_kinds_are_their_function_texts() {
        assert_eq!(SendKind::First.as_sql(), "FIRST");
        assert_eq!(SendKind::Repeat.as_sql(), "REPEAT");
    }
}
