//! The outcome of a rerun and its expected failures, as values.
//!
//! - [`RerunOutcomeRecord`]: the core's `RerunOutcome`. Only `enqueued`
//!   writes.
//! - [`AppRerunFailure`]: a failure of the rerun in the app's transaction
//!   (`PostgresBroker::rerun_task`). Each record carries the core's text of
//!   the error as `message`.
//! - [`CallerStepRefusal`]: a refusal of the rerun steps in the caller's
//!   transaction. Nothing is written; the caller does not commit after it.
//!
//! The grouping `CONTRACT_VIOLATION`: a row of the source read or of the
//! write that does not fit its statement (`RowDecode`, `CallerRow`), and the
//! three preparation variants that the core keeps in `RerunError`
//! (`EnqueuePreparation`, `Fingerprint`, `Serialization`). The rerun steps
//! turn a preparation refusal into `inputCorrupt` or `CALLER_KEY_REFUSED`
//! (`horsies_core::prepare_rerun_write`), so the three reach no rerun of
//! this core; the map keeps them total.

use horsies_core::db::{FailedTransactionOutcome, StatementFailure, Transience};
use horsies_core::{
    BrokerError, ClientFamily, FamilyCheckFailure, NotEligibleReason, RecordedFamily, RerunError,
    RerunOutcome, TaskAbsenceReason,
};
use napi_derive::napi;

use crate::values::boundary::text_of_uuid;
use crate::values::core_failures::CauseRecord;

/// The subject of the cause of a failed rerun.
pub const RERUN_SUBJECT: &str = "rerun_task";

/// Why no live or history record has the source id.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbsenceReasonName {
    PredatesRetainedHistory,
    WithinRetainedHistory,
    NotClassified,
}

/// Why a history source is not eligible for a rerun.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotEligibleName {
    CompletedSource,
    WorkflowTask,
}

/// The outcome of one rerun. Task ids are lower-case hyphenated text.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RerunOutcomeRecord {
    /// The new task exists; the source record is not changed.
    Enqueued {
        new_task_id: String,
        source_task_id: String,
        rerun_root_task_id: String,
    },
    /// The source is live; its own lifecycle owns its retry.
    SourceLive { task_id: String },
    SourceAbsent {
        task_id: String,
        reason: AbsenceReasonName,
    },
    /// The source history row does not decode; `detail` names why.
    SourceCorrupt { task_id: String, detail: String },
    NotEligible {
        task_id: String,
        reason: NotEligibleName,
    },
    /// The source stores no input; `disposition` is its stored reason, for
    /// example `DECLINED_BY_POLICY`.
    InputUnavailable {
        task_id: String,
        disposition: String,
    },
    /// The stored input or its options fail their checks.
    InputCorrupt { task_id: String, detail: String },
    /// The caller key holds another command (the source's own included).
    KeyConflict {
        task_id: String,
        reserved_by_task_id: String,
    },
    /// The caller key holds this rerun command already.
    KeyReplay { existing_task_id: String },
}

impl From<RerunOutcome> for RerunOutcomeRecord {
    fn from(outcome: RerunOutcome) -> Self {
        match outcome {
            RerunOutcome::Enqueued {
                new_task_id,
                source_task_id,
                rerun_root_task_id,
            } => Self::Enqueued {
                new_task_id: text_of_uuid(new_task_id),
                source_task_id: text_of_uuid(source_task_id),
                rerun_root_task_id: text_of_uuid(rerun_root_task_id),
            },
            RerunOutcome::SourceLive { task_id } => Self::SourceLive {
                task_id: text_of_uuid(task_id),
            },
            RerunOutcome::SourceAbsent { task_id, reason } => Self::SourceAbsent {
                task_id: text_of_uuid(task_id),
                reason: absence_reason_name(reason),
            },
            RerunOutcome::SourceCorrupt { task_id, detail } => Self::SourceCorrupt {
                task_id: text_of_uuid(task_id),
                detail,
            },
            RerunOutcome::NotEligible { task_id, reason } => Self::NotEligible {
                task_id: text_of_uuid(task_id),
                reason: not_eligible_name(reason),
            },
            RerunOutcome::InputUnavailable {
                task_id,
                disposition,
            } => Self::InputUnavailable {
                task_id: text_of_uuid(task_id),
                disposition,
            },
            RerunOutcome::InputCorrupt { task_id, detail } => Self::InputCorrupt {
                task_id: text_of_uuid(task_id),
                detail,
            },
            RerunOutcome::KeyConflict {
                task_id,
                reserved_by_task_id,
            } => Self::KeyConflict {
                task_id: text_of_uuid(task_id),
                reserved_by_task_id: text_of_uuid(reserved_by_task_id),
            },
            RerunOutcome::KeyReplay { existing_task_id } => Self::KeyReplay {
                existing_task_id: text_of_uuid(existing_task_id),
            },
        }
    }
}

/// The name of `reason`. Pure.
fn absence_reason_name(reason: TaskAbsenceReason) -> AbsenceReasonName {
    match reason {
        TaskAbsenceReason::PredatesRetainedHistory => AbsenceReasonName::PredatesRetainedHistory,
        TaskAbsenceReason::WithinRetainedHistory => AbsenceReasonName::WithinRetainedHistory,
        TaskAbsenceReason::NotClassified => AbsenceReasonName::NotClassified,
    }
}

/// The name of `reason`. Pure.
fn not_eligible_name(reason: NotEligibleReason) -> NotEligibleName {
    match reason {
        NotEligibleReason::CompletedSource => NotEligibleName::CompletedSource,
        NotEligibleReason::WorkflowTask => NotEligibleName::WorkflowTask,
    }
}

/// A client family, by its stored value.
#[napi(string_enum)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FamilyName {
    #[napi(value = "PYTHON")]
    Python,
    #[napi(value = "RUST")]
    Rust,
    #[napi(value = "TYPESCRIPT")]
    TypeScript,
}

/// What a database records about the client family that installed it.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredFamilyRecord {
    Recorded {
        family: FamilyName,
    },
    /// The family table has no row.
    NotRecorded,
}

/// The record of `stored`. Pure.
pub fn stored_family(stored: RecordedFamily) -> StoredFamilyRecord {
    match stored {
        RecordedFamily::Recorded(family) => StoredFamilyRecord::Recorded {
            family: match family {
                ClientFamily::Python => FamilyName::Python,
                ClientFamily::Rust => FamilyName::Rust,
                ClientFamily::TypeScript => FamilyName::TypeScript,
            },
        },
        RecordedFamily::NotRecorded => StoredFamilyRecord::NotRecorded,
    }
}

/// Whether a failed statement of the rerun's own transaction may have
/// committed it.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementOutcome {
    /// Nothing committed.
    NotCommitted,
    /// The reply to `COMMIT` was lost: the rerun may have committed. The
    /// same keyed rerun sent again answers `keyReplay` for it.
    Unknown,
}

/// A failure of the rerun in the app's transaction.
#[napi(
    discriminant = "code",
    discriminant_case = "UPPER_SNAKE",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppRerunFailure {
    /// The handle could not connect, could not install the schema, or could
    /// not open its closed pools again. Nothing ran.
    ConnectFailed { message: String, cause: CauseRecord },
    /// A statement of the rerun's transaction failed.
    StatementFailed {
        message: String,
        cause: CauseRecord,
        outcome: StatementOutcome,
    },
    /// The retention class of the new task has no class row. Nothing is
    /// written.
    ClassNotRegistered { message: String, class_key: String },
    /// The core refused the policy. Nothing is written.
    InvalidPolicy { message: String },
    /// The caller key is not a valid idempotency key. Nothing is written.
    CallerKeyRefused { message: String },
    /// The task id source refused to mint the new task's id. Nothing is
    /// written.
    IdMintRefused { message: String },
    /// The database records another client family, or none. Nothing is read
    /// or written.
    ClientFamilyMismatch {
        message: String,
        stored: StoredFamilyRecord,
    },
    /// The family row holds a value that is not a client family: corrupt
    /// stored data. Nothing is written.
    UnknownStoredFamily { message: String, stored: String },
    /// The family check could not read the family row. Nothing is read or
    /// written.
    FamilyCheckFailed { message: String, cause: CauseRecord },
    /// The stored retry list of the source names a custom code that the
    /// task does not declare. Nothing is written.
    RetryCodeNotDeclared {
        message: String,
        task_name: String,
        retry_code: String,
    },
    /// The stored retry list of the source names a custom code, and the app
    /// does not list the task. Nothing is written.
    TaskNotRegistered { message: String, task_name: String },
    /// A row or stored data broke the rerun's contract. Nothing committed.
    ContractViolation { message: String },
}

/// The failure record of `error`, a failure of `PostgresBroker::rerun_task`.
/// Pure.
pub fn app_failure(error: RerunError) -> AppRerunFailure {
    let message = error.to_string();
    match error {
        RerunError::Statement(failure) => statement_failed(message, &failure),
        RerunError::PoolSettings(settings) => AppRerunFailure::ConnectFailed {
            cause: CauseRecord::of(RERUN_SUBJECT, &BrokerError::PoolSettings(settings)),
            message,
        },
        RerunError::UnknownRetentionClass(class_key) => {
            AppRerunFailure::ClassNotRegistered { message, class_key }
        }
        RerunError::InvalidPolicy(_) => AppRerunFailure::InvalidPolicy { message },
        RerunError::Idempotency(_) => AppRerunFailure::CallerKeyRefused { message },
        RerunError::Identity(_) => AppRerunFailure::IdMintRefused { message },
        RerunError::ClientFamilyMismatch { stored, own: _ } => {
            AppRerunFailure::ClientFamilyMismatch {
                message,
                stored: stored_family(stored),
            }
        }
        RerunError::UnknownStoredFamily(unknown) => AppRerunFailure::UnknownStoredFamily {
            message,
            stored: unknown.stored,
        },
        RerunError::FamilyCheckFailed(failure) => family_check_failed(message, failure),
        RerunError::RetryCodeNotDeclared {
            task_name,
            retry_code,
        } => AppRerunFailure::RetryCodeNotDeclared {
            message,
            task_name,
            retry_code,
        },
        RerunError::TaskNotRegistered { task_name } => {
            AppRerunFailure::TaskNotRegistered { message, task_name }
        }
        RerunError::RowDecode(_)
        | RerunError::CallerRow(_)
        | RerunError::EnqueuePreparation(_)
        | RerunError::Fingerprint(_)
        | RerunError::Serialization(_) => AppRerunFailure::ContractViolation { message },
    }
}

/// The failure record of a connect or a schema install that failed before
/// the rerun's transaction. Pure.
pub fn connect_failed(error: &BrokerError) -> AppRerunFailure {
    AppRerunFailure::ConnectFailed {
        message: format!("the rerun could not connect: {error}"),
        cause: CauseRecord::of(RERUN_SUBJECT, error),
    }
}

/// The record of a failed statement of the rerun's transaction. Pure.
fn statement_failed(message: String, failure: &StatementFailure) -> AppRerunFailure {
    AppRerunFailure::StatementFailed {
        cause: CauseRecord::new(
            RERUN_SUBJECT,
            failure.to_string(),
            transient(failure.transience()),
            failure.failure_cause(),
        ),
        outcome: statement_outcome(failure),
        message,
    }
}

/// Whether `failure` may have committed the rerun's transaction, by the
/// core's rule ([`FailedTransactionOutcome::of`]). Pure.
fn statement_outcome(failure: &StatementFailure) -> StatementOutcome {
    match FailedTransactionOutcome::of(failure) {
        FailedTransactionOutcome::NotCommitted => StatementOutcome::NotCommitted,
        FailedTransactionOutcome::Unknown => StatementOutcome::Unknown,
    }
}

/// The record of a family check that could not read the family row: the
/// check's text, its retry rule, and the class of its cause. Pure.
fn family_check_failed(message: String, failure: FamilyCheckFailure) -> AppRerunFailure {
    let text = failure.to_string();
    let retryable = transient(failure.transience());
    let cause = BrokerError::from(failure).failure_cause();
    AppRerunFailure::FamilyCheckFailed {
        cause: CauseRecord::new(RERUN_SUBJECT, text, retryable, cause),
        message,
    }
}

/// Whether the cause may clear. Pure.
fn transient(transience: Transience) -> bool {
    match transience {
        Transience::Transient => true,
        Transience::Permanent => false,
    }
}

/// A refusal of the rerun steps in the caller's transaction. Nothing is
/// written.
#[napi(
    discriminant = "code",
    discriminant_case = "UPPER_SNAKE",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallerStepRefusal {
    ClassNotRegistered {
        message: String,
        class_key: String,
    },
    CallerKeyRefused {
        message: String,
    },
    IdMintRefused {
        message: String,
    },
    ClientFamilyMismatch {
        message: String,
        stored: StoredFamilyRecord,
    },
    UnknownStoredFamily {
        message: String,
        stored: String,
    },
    /// The stored retry list of the source names a custom code that the
    /// task does not declare.
    RetryCodeNotDeclared {
        message: String,
        task_name: String,
        retry_code: String,
    },
    /// The stored retry list of the source names a custom code, and the app
    /// does not list the task.
    TaskNotRegistered {
        message: String,
        task_name: String,
    },
    /// A row that the caller's driver read does not fit its statement.
    ContractViolation {
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use horsies_core::db::{
        CancelRequestOutcome, NotSentCause, UnknownOutcomeCause, COMMIT_STATEMENT_NAME,
    };
    use horsies_core::{IdempotencyKeyError, RowDecodeError, UnknownStoredFamily, Uuid7Error};
    use uuid::Uuid;

    use super::*;

    const SOURCE: Uuid = Uuid::from_u128(0x0189_0a5d_ac96_774b_bcce_b302_099a_8057);
    const OTHER: Uuid = Uuid::from_u128(0x0189_0a5d_ac96_774b_bcce_b302_099a_8058);
    const SOURCE_TEXT: &str = "01890a5d-ac96-774b-bcce-b302099a8057";
    const OTHER_TEXT: &str = "01890a5d-ac96-774b-bcce-b302099a8058";

    fn lost_reply(statement: &'static str) -> StatementFailure {
        StatementFailure::OutcomeUnknown {
            statement,
            cause: UnknownOutcomeCause::NoReplyWithinBound {
                bound: Duration::from_secs(30),
                grace: Duration::from_secs(1),
                cancel: CancelRequestOutcome::Sent,
            },
        }
    }

    #[test]
    fn each_outcome_has_its_record_with_task_ids_as_text() {
        let cases = [
            (
                RerunOutcome::Enqueued {
                    new_task_id: OTHER,
                    source_task_id: SOURCE,
                    rerun_root_task_id: SOURCE,
                },
                RerunOutcomeRecord::Enqueued {
                    new_task_id: OTHER_TEXT.to_owned(),
                    source_task_id: SOURCE_TEXT.to_owned(),
                    rerun_root_task_id: SOURCE_TEXT.to_owned(),
                },
            ),
            (
                RerunOutcome::SourceLive { task_id: SOURCE },
                RerunOutcomeRecord::SourceLive {
                    task_id: SOURCE_TEXT.to_owned(),
                },
            ),
            (
                RerunOutcome::SourceAbsent {
                    task_id: SOURCE,
                    reason: TaskAbsenceReason::PredatesRetainedHistory,
                },
                RerunOutcomeRecord::SourceAbsent {
                    task_id: SOURCE_TEXT.to_owned(),
                    reason: AbsenceReasonName::PredatesRetainedHistory,
                },
            ),
            (
                RerunOutcome::SourceCorrupt {
                    task_id: SOURCE,
                    detail: "digest".to_owned(),
                },
                RerunOutcomeRecord::SourceCorrupt {
                    task_id: SOURCE_TEXT.to_owned(),
                    detail: "digest".to_owned(),
                },
            ),
            (
                RerunOutcome::NotEligible {
                    task_id: SOURCE,
                    reason: NotEligibleReason::WorkflowTask,
                },
                RerunOutcomeRecord::NotEligible {
                    task_id: SOURCE_TEXT.to_owned(),
                    reason: NotEligibleName::WorkflowTask,
                },
            ),
            (
                RerunOutcome::InputUnavailable {
                    task_id: SOURCE,
                    disposition: "DECLINED_BY_POLICY".to_owned(),
                },
                RerunOutcomeRecord::InputUnavailable {
                    task_id: SOURCE_TEXT.to_owned(),
                    disposition: "DECLINED_BY_POLICY".to_owned(),
                },
            ),
            (
                RerunOutcome::InputCorrupt {
                    task_id: SOURCE,
                    detail: "envelope".to_owned(),
                },
                RerunOutcomeRecord::InputCorrupt {
                    task_id: SOURCE_TEXT.to_owned(),
                    detail: "envelope".to_owned(),
                },
            ),
            (
                RerunOutcome::KeyConflict {
                    task_id: SOURCE,
                    reserved_by_task_id: OTHER,
                },
                RerunOutcomeRecord::KeyConflict {
                    task_id: SOURCE_TEXT.to_owned(),
                    reserved_by_task_id: OTHER_TEXT.to_owned(),
                },
            ),
            (
                RerunOutcome::KeyReplay {
                    existing_task_id: OTHER,
                },
                RerunOutcomeRecord::KeyReplay {
                    existing_task_id: OTHER_TEXT.to_owned(),
                },
            ),
        ];
        for (outcome, expected) in cases {
            assert_eq!(RerunOutcomeRecord::from(outcome), expected);
        }
    }

    #[test]
    fn each_absence_reason_and_not_eligible_reason_has_its_name() {
        assert_eq!(
            absence_reason_name(TaskAbsenceReason::WithinRetainedHistory),
            AbsenceReasonName::WithinRetainedHistory
        );
        assert_eq!(
            absence_reason_name(TaskAbsenceReason::NotClassified),
            AbsenceReasonName::NotClassified
        );
        assert_eq!(
            not_eligible_name(NotEligibleReason::CompletedSource),
            NotEligibleName::CompletedSource
        );
    }

    #[test]
    fn each_stored_family_has_its_record() {
        assert_eq!(
            stored_family(RecordedFamily::Recorded(ClientFamily::Python)),
            StoredFamilyRecord::Recorded {
                family: FamilyName::Python
            }
        );
        assert_eq!(
            stored_family(RecordedFamily::Recorded(ClientFamily::Rust)),
            StoredFamilyRecord::Recorded {
                family: FamilyName::Rust
            }
        );
        assert_eq!(
            stored_family(RecordedFamily::Recorded(ClientFamily::TypeScript)),
            StoredFamilyRecord::Recorded {
                family: FamilyName::TypeScript
            }
        );
        assert_eq!(
            stored_family(RecordedFamily::NotRecorded),
            StoredFamilyRecord::NotRecorded
        );
    }

    #[test]
    fn a_statement_not_sent_did_not_commit_and_keeps_its_retry_rule() {
        let failure = StatementFailure::NotSent {
            statement: "rerun_source_read",
            cause: NotSentCause::ConnectionClosed,
        };
        let text = failure.to_string();
        match app_failure(RerunError::Statement(failure)) {
            AppRerunFailure::StatementFailed {
                message,
                cause,
                outcome,
            } => {
                assert_eq!(message, format!("database error: {text}"));
                assert_eq!(outcome, StatementOutcome::NotCommitted);
                assert_eq!(
                    cause,
                    CauseRecord::Connection {
                        subject: RERUN_SUBJECT.to_owned(),
                        message: text,
                        retryable: true,
                    }
                );
            }
            other => panic!("a statement failure is STATEMENT_FAILED, got {other:?}"),
        }
    }

    #[test]
    fn a_lost_reply_to_commit_has_an_unknown_outcome() {
        assert_eq!(
            statement_outcome(&lost_reply(COMMIT_STATEMENT_NAME)),
            StatementOutcome::Unknown
        );
    }

    #[test]
    fn a_lost_reply_before_commit_did_not_commit() {
        for statement in ["rerun_source_read", "rerun_enqueue", "begin"] {
            assert_eq!(
                statement_outcome(&lost_reply(statement)),
                StatementOutcome::NotCommitted,
                "{statement}"
            );
        }
    }

    #[test]
    fn the_preconditions_have_their_codes_and_the_core_text() {
        let unknown = UnknownStoredFamily {
            stored: "GO".to_owned(),
        };
        let cases = [
            (
                RerunError::UnknownRetentionClass("standard_8d".to_owned()),
                AppRerunFailure::ClassNotRegistered {
                    message: RerunError::UnknownRetentionClass("standard_8d".to_owned())
                        .to_string(),
                    class_key: "standard_8d".to_owned(),
                },
            ),
            (
                RerunError::InvalidPolicy("retention class key must be non-empty".to_owned()),
                AppRerunFailure::InvalidPolicy {
                    message: "invalid rerun policy: retention class key must be non-empty"
                        .to_owned(),
                },
            ),
            (
                RerunError::Idempotency(IdempotencyKeyError::Empty {
                    label: "idempotency key",
                }),
                AppRerunFailure::CallerKeyRefused {
                    message: "rerun idempotency key failed: idempotency key must be non-empty"
                        .to_owned(),
                },
            ),
            (
                RerunError::Identity(Uuid7Error::ClockOutOfRange),
                AppRerunFailure::IdMintRefused {
                    message: "rerun identity mint failed: millisecond clock is outside the \
                              48-bit range"
                        .to_owned(),
                },
            ),
            (
                RerunError::UnknownStoredFamily(unknown.clone()),
                AppRerunFailure::UnknownStoredFamily {
                    message: RerunError::UnknownStoredFamily(unknown).to_string(),
                    stored: "GO".to_owned(),
                },
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(app_failure(error), expected);
        }
    }

    #[test]
    fn a_family_mismatch_names_the_stored_family() {
        let error = RerunError::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Rust),
            own: ClientFamily::TypeScript,
        };
        match app_failure(error) {
            AppRerunFailure::ClientFamilyMismatch { message, stored } => {
                assert!(message.contains("the RUST client"), "{message}");
                assert!(message.contains("TYPESCRIPT"), "{message}");
                assert_eq!(
                    stored,
                    StoredFamilyRecord::Recorded {
                        family: FamilyName::Rust
                    }
                );
            }
            other => panic!("a family mismatch is CLIENT_FAMILY_MISMATCH, got {other:?}"),
        }
    }

    #[test]
    fn a_family_check_that_could_not_read_keeps_the_rule_of_its_statement() {
        let failure = FamilyCheckFailure::Statement(StatementFailure::NotSent {
            statement: "client_family_read",
            cause: NotSentCause::ConnectionClosed,
        });
        match app_failure(RerunError::FamilyCheckFailed(failure)) {
            AppRerunFailure::FamilyCheckFailed { cause, .. } => {
                assert!(
                    matches!(
                        cause,
                        CauseRecord::Connection {
                            ref subject,
                            retryable: true,
                            ..
                        } if subject == RERUN_SUBJECT
                    ),
                    "{cause:?}"
                );
            }
            other => panic!("a failed family read is FAMILY_CHECK_FAILED, got {other:?}"),
        }
    }

    #[test]
    fn a_row_outside_the_contract_is_a_contract_violation() {
        let no_row = RerunError::RowDecode(RowDecodeError::NoRow {
            statement: "rerun_enqueue",
        });
        let text = no_row.to_string();
        assert_eq!(
            app_failure(no_row),
            AppRerunFailure::ContractViolation { message: text }
        );
        let serialization = serde_json::from_str::<u8>("not json")
            .map_err(RerunError::Serialization)
            .expect_err("the text is not JSON");
        assert!(matches!(
            app_failure(serialization),
            AppRerunFailure::ContractViolation { .. }
        ));
    }

    #[test]
    fn an_undeclared_retry_code_names_the_task_and_the_code() {
        let error = RerunError::RetryCodeNotDeclared {
            task_name: "charge".to_owned(),
            retry_code: "CARD_DECLINED".to_owned(),
        };
        let text = error.to_string();
        assert_eq!(
            app_failure(error),
            AppRerunFailure::RetryCodeNotDeclared {
                message: text,
                task_name: "charge".to_owned(),
                retry_code: "CARD_DECLINED".to_owned(),
            }
        );
    }

    #[test]
    fn an_unlisted_task_names_the_task() {
        let error = RerunError::TaskNotRegistered {
            task_name: "charge".to_owned(),
        };
        let text = error.to_string();
        assert_eq!(
            app_failure(error),
            AppRerunFailure::TaskNotRegistered {
                message: text,
                task_name: "charge".to_owned(),
            }
        );
    }

    #[test]
    fn a_connect_failure_carries_the_cause_of_the_broker_error() {
        let error = BrokerError::ListenerClosed;
        match connect_failed(&error) {
            AppRerunFailure::ConnectFailed { message, cause } => {
                assert!(
                    message.starts_with("the rerun could not connect: "),
                    "{message}"
                );
                assert_eq!(cause, CauseRecord::of(RERUN_SUBJECT, &error));
            }
            other => panic!("a connect failure is CONNECT_FAILED, got {other:?}"),
        }
    }
}
