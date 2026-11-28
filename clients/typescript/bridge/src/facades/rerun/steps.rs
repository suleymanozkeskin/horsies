//! The rerun in a transaction that the caller owns: the core's three pure
//! rerun steps as sync functions (Python `rerun_read`,
//! `prepare_rerun_write`, `decode_rerun_write`).
//!
//! 1. `rerunReadStatement`: the source read (`horsies_rerun_source_read`).
//! 2. `prepareRerunWrite`: from the read's row, the write
//!    ([`PreparedRerunWrite`]), the outcome that ends the rerun with nothing
//!    written, or a refusal ([`CallerStepRefusal`]).
//! 3. `PreparedRerunWrite.decodeRow`: the outcome of the write's row.
//!
//! The package runs each statement with the caller's driver; nothing here
//! commits. The functions do no I/O and start no core; the fork and stop
//! rules apply, as for every core call. The client family of every rerun
//! here is `TYPESCRIPT`, and the new task is sent at the core's clock when
//! `prepareRerunWrite` runs. The stored retry list of the source is checked
//! against the declared custom codes of the calling app's tasks, which the
//! caller passes to `prepareRerunWrite`.

use chrono::{DateTime, Utc};
use horsies_core::{
    decode_rerun_write, prepare_rerun_write as prepare_core_write, rerun_read, ClientFamily,
    RegisteredTaskCodes, RerunCommand, RerunEnqueuePolicy, RerunError, RerunOutcome,
    RerunPreparation, RerunWrite,
};
use napi_derive::napi;

use crate::call::guard::sync_call;
use crate::call::thrown::{Envelope, NativeThrown};
use crate::facades::rerun::arguments::{
    registered_task_codes, rerun_command, rerun_policy, RegisteredTaskCodesArg, RerunCommandArg,
    RerunPolicyArg,
};
use crate::facades::rerun::caller_values::{
    caller_row, statement_record, CallerRowArg, CallerStatementRecord,
};
use crate::facades::rerun::records::{stored_family, CallerStepRefusal, RerunOutcomeRecord};
use crate::process_core::runtime::pure_call_refusal;
use crate::values::trace_carrier::TraceArgument;

/// The client family of every rerun in the caller's transaction.
const OWN_FAMILY: ClientFamily = ClientFamily::TypeScript;

/// The write of one rerun, prepared from the source read's row. JavaScript
/// gets it from `prepareRerunWrite`; it has no constructor.
#[napi]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedRerunWrite {
    write: RerunWrite,
}

#[napi]
impl PreparedRerunWrite {
    /// The write: `horsies_enqueue_task` with the lineage `RERUN`, which
    /// holds the idempotency key of a keyed rerun first.
    #[napi(ts_return_type = "NativeEnvelope<CallerStatementRecord>")]
    pub fn statement(&self) -> Envelope<CallerStatementRecord> {
        sync_call("rerun_write_statement", || {
            Envelope::Value(statement_record(self.write.statement()))
        })
    }

    /// The outcome of the write's row, or the refusal of a row that does not
    /// fit the write's columns. An answer of the write that contradicts the
    /// minted task id is a defect of the core: `CORE_PANIC`.
    #[napi(ts_return_type = "NativeEnvelope<WriteDecodeRecord>")]
    pub fn decode_row(&self, row: CallerRowArg) -> Envelope<WriteDecodeRecord> {
        sync_call("decode_rerun_write", || match pure_call_refusal() {
            Some(refusal) => Envelope::Thrown(refusal.thrown()),
            None => Envelope::Value(decode_write_row(&self.write, row)),
        })
    }
}

/// What the read's row decides.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RerunPreparationRecord {
    /// Run the write, then decode its row.
    Write { write: PreparedRerunWrite },
    /// The rerun ends with this outcome; nothing is written.
    Answered { outcome: RerunOutcomeRecord },
    /// The rerun is refused; nothing is written.
    Refused { error: CallerStepRefusal },
}

/// The outcome of the write's row.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteDecodeRecord {
    Decoded {
        outcome: RerunOutcomeRecord,
    },
    /// The row does not fit the write's columns.
    RowRefused {
        message: String,
    },
}

/// The command and the policy of one rerun, as the core takes them.
struct StepRequest {
    command: RerunCommand,
    policy: RerunEnqueuePolicy,
}

/// The source read of a rerun as a caller statement: `$1` the source task
/// id, `$2` the retention class key of the new task. Reads only.
#[napi(ts_return_type = "NativeEnvelope<CallerStatementRecord>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn rerun_read_statement(
    command: RerunCommandArg,
    policy: RerunPolicyArg,
) -> Envelope<CallerStatementRecord> {
    sync_call("rerun_read_statement", || {
        match step_request(command, &policy) {
            Ok(request) => Envelope::Value(statement_record(&rerun_read(
                &request.command,
                &request.policy,
            ))),
            Err(thrown) => Envelope::Thrown(thrown),
        }
    })
}

/// Decide the rerun from `read_row`, the row of the source read of
/// `command` and `policy`. The new task stores the carrier of `trace`.
/// `codes` are the declared custom codes of the calling app's tasks: the
/// stored retry list of the source is checked against them. Pure apart from
/// the id mint and the clock.
#[napi(ts_return_type = "NativeEnvelope<RerunPreparationRecord>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn prepare_rerun_write(
    command: RerunCommandArg,
    policy: RerunPolicyArg,
    trace: TraceArgument,
    read_row: CallerRowArg,
    codes: RegisteredTaskCodesArg,
) -> Envelope<RerunPreparationRecord> {
    sync_call("prepare_rerun_write", || {
        let request = match step_request(command, &policy) {
            Ok(request) => request,
            Err(thrown) => return Envelope::Thrown(thrown),
        };
        let codes = match registered_task_codes(&codes) {
            Ok(codes) => codes,
            Err(refusal) => return Envelope::Thrown(refusal.into()),
        };
        match trace.send_trace() {
            Ok(trace) => Envelope::Value(preparation_of_row(
                &request.command.with_trace(trace),
                &request.policy,
                &codes,
                read_row,
                Utc::now(),
            )),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The core's request of `command` and `policy`, after the fork and stop
/// rules.
///
/// Errors: the thrown code of a forked or stopped core process, or
/// `CLIENT_DEFECT` for a value that breaks a boundary rule.
fn step_request(
    command: RerunCommandArg,
    policy: &RerunPolicyArg,
) -> Result<StepRequest, NativeThrown> {
    if let Some(refusal) = pure_call_refusal() {
        return Err(refusal.thrown());
    }
    Ok(StepRequest {
        command: rerun_command(command)?,
        policy: rerun_policy(policy)?,
    })
}

/// What the source read's row decides for `command`, with the new task sent
/// at `sent_at`. Pure apart from the id mint.
fn preparation_of_row(
    command: &RerunCommand,
    policy: &RerunEnqueuePolicy,
    codes: &RegisteredTaskCodes,
    read_row: CallerRowArg,
    sent_at: DateTime<Utc>,
) -> RerunPreparationRecord {
    let read = rerun_read(command, policy);
    let row = match caller_row(&read, read_row) {
        Ok(row) => row,
        Err(refused) => return row_refused(refused.to_string()),
    };
    match prepare_core_write(&read, &row, command, policy, codes, OWN_FAMILY, sent_at) {
        Ok(preparation) => preparation_record(preparation),
        Err(refused) => row_refused(RerunError::CallerRow(refused).to_string()),
    }
}

/// The refusal of a read row that does not fit its statement. Pure.
fn row_refused(message: String) -> RerunPreparationRecord {
    RerunPreparationRecord::Refused {
        error: CallerStepRefusal::ContractViolation { message },
    }
}

/// The record of what the read's row decides. Each refusal carries the
/// core's text of the same error (`RerunError`). Pure.
fn preparation_record(preparation: RerunPreparation) -> RerunPreparationRecord {
    let refusal = match preparation {
        RerunPreparation::Write(write) => {
            return RerunPreparationRecord::Write {
                write: PreparedRerunWrite { write },
            }
        }
        RerunPreparation::Refused(refusal) => {
            return RerunPreparationRecord::Answered {
                outcome: RerunOutcome::from(refusal).into(),
            }
        }
        RerunPreparation::ClassNotRegistered { class_key } => {
            CallerStepRefusal::ClassNotRegistered {
                message: RerunError::UnknownRetentionClass(class_key.clone()).to_string(),
                class_key,
            }
        }
        RerunPreparation::ClientFamilyMismatch { stored, own } => {
            CallerStepRefusal::ClientFamilyMismatch {
                message: RerunError::ClientFamilyMismatch { stored, own }.to_string(),
                stored: stored_family(stored),
            }
        }
        RerunPreparation::UnknownStoredFamily(unknown) => CallerStepRefusal::UnknownStoredFamily {
            message: RerunError::UnknownStoredFamily(unknown.clone()).to_string(),
            stored: unknown.stored,
        },
        RerunPreparation::IdempotencyKeyRefused(refused) => {
            CallerStepRefusal::IdempotencyKeyRefused {
                message: RerunError::Idempotency(refused).to_string(),
            }
        }
        RerunPreparation::RetryCodeNotDeclared {
            task_name,
            retry_code,
        } => CallerStepRefusal::RetryCodeNotDeclared {
            message: RerunError::RetryCodeNotDeclared {
                task_name: task_name.clone(),
                retry_code: retry_code.clone(),
            }
            .to_string(),
            task_name,
            retry_code,
        },
        RerunPreparation::TaskNotRegistered { task_name } => CallerStepRefusal::TaskNotRegistered {
            message: RerunError::TaskNotRegistered {
                task_name: task_name.clone(),
            }
            .to_string(),
            task_name,
        },
        RerunPreparation::IdMintRefused(refused) => CallerStepRefusal::IdMintRefused {
            message: RerunError::Identity(refused).to_string(),
        },
    };
    RerunPreparationRecord::Refused { error: refusal }
}

/// The outcome of the row of `write`, or the refusal of a row that does not
/// fit the write's columns. Panics, as the core does, on an answer that
/// contradicts the minted task id.
fn decode_write_row(write: &RerunWrite, row: CallerRowArg) -> WriteDecodeRecord {
    let row = match caller_row(write.statement(), row) {
        Ok(row) => row,
        Err(refused) => {
            return WriteDecodeRecord::RowRefused {
                message: refused.to_string(),
            }
        }
    };
    match decode_rerun_write(write, &row) {
        Ok(written) => WriteDecodeRecord::Decoded {
            outcome: RerunOutcome::from(written).into(),
        },
        Err(refused) => WriteDecodeRecord::RowRefused {
            message: RerunError::CallerRow(refused).to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use horsies_core::{
        CallerStatement, EnqueueRetention, IdempotencyKeyError, RecordedFamily, RetentionDays,
        UnknownStoredFamily, Uuid7Error,
    };
    use uuid::Uuid;

    use super::*;
    use crate::facades::rerun::caller_values::SqlTextValue;
    use crate::facades::rerun::records::{AbsenceReasonName, FamilyName, StoredFamilyRecord};

    const SOURCE: Uuid = Uuid::from_u128(0x0189_0a5d_ac96_774b_bcce_b302_099a_8057);

    fn command() -> RerunCommand {
        RerunCommand::new(SOURCE, None, None)
    }

    fn policy(retention: EnqueueRetention) -> RerunEnqueuePolicy {
        RerunEnqueuePolicy::new(retention, true, Duration::hours(24)).expect("a valid policy")
    }

    /// The retention of 8 days, whose class `standard_8d` has no class row.
    fn eight_days() -> EnqueueRetention {
        EnqueueRetention::Period(
            RetentionDays::parse_period(Duration::days(8)).expect("8 days is a retention period"),
        )
    }

    /// A row of `statement` with the text of each named column; every other
    /// cell is SQL NULL.
    fn row_of(statement: &CallerStatement, values: &[(&str, &str)]) -> CallerRowArg {
        CallerRowArg {
            cells: statement
                .columns()
                .iter()
                .map(|column| {
                    values.iter().find(|(name, _)| *name == column.name).map_or(
                        SqlTextValue::Null,
                        |(_, text)| SqlTextValue::Text {
                            text: (*text).to_owned(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn prepared(retention: EnqueueRetention, values: &[(&str, &str)]) -> RerunPreparationRecord {
        let policy = policy(retention);
        let read = rerun_read(&command(), &policy);
        preparation_of_row(
            &command(),
            &policy,
            &RegisteredTaskCodes::NONE,
            row_of(&read, values),
            Utc::now(),
        )
    }

    #[test]
    fn an_absent_source_answers_source_absent_and_writes_nothing() {
        assert_eq!(
            prepared(
                EnqueueRetention::Forever,
                &[
                    ("class_registered", "t"),
                    ("client_family", "TYPESCRIPT"),
                    ("location", "ABSENT"),
                    ("absence_reason", "WITHIN_RETAINED_HISTORY"),
                ],
            ),
            RerunPreparationRecord::Answered {
                outcome: RerunOutcomeRecord::SourceAbsent {
                    task_id: "01890a5d-ac96-774b-bcce-b302099a8057".to_owned(),
                    reason: AbsenceReasonName::WithinRetainedHistory,
                }
            }
        );
    }

    #[test]
    fn a_live_source_answers_source_live() {
        assert!(matches!(
            prepared(
                EnqueueRetention::Forever,
                &[
                    ("class_registered", "t"),
                    ("client_family", "TYPESCRIPT"),
                    ("location", "LIVE"),
                ],
            ),
            RerunPreparationRecord::Answered {
                outcome: RerunOutcomeRecord::SourceLive { .. }
            }
        ));
    }

    #[test]
    fn a_database_of_another_family_is_refused_before_the_write() {
        match prepared(
            EnqueueRetention::Forever,
            &[("class_registered", "t"), ("client_family", "PYTHON")],
        ) {
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::ClientFamilyMismatch { message, stored },
            } => {
                assert_eq!(
                    stored,
                    StoredFamilyRecord::Recorded {
                        family: FamilyName::Python
                    }
                );
                assert!(message.contains("the PYTHON client"), "{message}");
                assert!(message.contains("TYPESCRIPT"), "{message}");
            }
            other => panic!("another family is CLIENT_FAMILY_MISMATCH, got {other:?}"),
        }
    }

    #[test]
    fn a_family_value_that_is_no_client_family_is_refused() {
        assert_eq!(
            prepared(
                EnqueueRetention::Forever,
                &[("class_registered", "t"), ("client_family", "GO")]
            ),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::UnknownStoredFamily {
                    message: RerunError::UnknownStoredFamily(UnknownStoredFamily {
                        stored: "GO".to_owned()
                    })
                    .to_string(),
                    stored: "GO".to_owned(),
                }
            }
        );
    }

    #[test]
    fn an_unregistered_class_is_refused_with_its_key() {
        assert_eq!(
            prepared(
                eight_days(),
                &[("class_registered", "f"), ("client_family", "TYPESCRIPT")],
            ),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::ClassNotRegistered {
                    message: RerunError::UnknownRetentionClass("standard_8d".to_owned())
                        .to_string(),
                    class_key: "standard_8d".to_owned(),
                }
            }
        );
    }

    #[test]
    fn a_row_with_null_where_the_function_sets_a_value_is_a_contract_violation() {
        assert!(matches!(
            prepared(EnqueueRetention::Forever, &[("client_family", "TYPESCRIPT")]),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::ContractViolation { ref message }
            } if message.contains("class_registered")
        ));
    }

    #[test]
    fn a_row_of_another_cell_count_is_a_contract_violation() {
        let policy = policy(EnqueueRetention::Forever);
        let refused = preparation_of_row(
            &command(),
            &policy,
            &RegisteredTaskCodes::NONE,
            CallerRowArg {
                cells: vec![SqlTextValue::Null],
            },
            Utc::now(),
        );
        assert!(matches!(
            refused,
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::ContractViolation { ref message }
            } if message.contains("declares 19 result columns")
        ));
    }

    #[test]
    fn a_refused_key_and_a_refused_mint_carry_the_core_text() {
        assert_eq!(
            preparation_record(RerunPreparation::IdempotencyKeyRefused(
                IdempotencyKeyError::OverBound {
                    label: "idempotency key"
                }
            )),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::IdempotencyKeyRefused {
                    message: "rerun idempotency key failed: idempotency key must be at most \
                              255 UTF-8 bytes"
                        .to_owned()
                }
            }
        );
        assert_eq!(
            preparation_record(RerunPreparation::IdMintRefused(
                Uuid7Error::EntropyOutOfRange
            )),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::IdMintRefused {
                    message: "rerun identity mint failed: entropy source exceeded 62 bits"
                        .to_owned()
                }
            }
        );
    }

    #[test]
    fn an_undeclared_retry_code_is_refused_with_the_task_and_the_code() {
        assert_eq!(
            preparation_record(RerunPreparation::RetryCodeNotDeclared {
                task_name: "charge".to_owned(),
                retry_code: "CARD_DECLINED".to_owned(),
            }),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::RetryCodeNotDeclared {
                    message: RerunError::RetryCodeNotDeclared {
                        task_name: "charge".to_owned(),
                        retry_code: "CARD_DECLINED".to_owned(),
                    }
                    .to_string(),
                    task_name: "charge".to_owned(),
                    retry_code: "CARD_DECLINED".to_owned(),
                }
            }
        );
    }

    #[test]
    fn an_unlisted_task_is_refused_with_its_name() {
        assert_eq!(
            preparation_record(RerunPreparation::TaskNotRegistered {
                task_name: "charge".to_owned(),
            }),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::TaskNotRegistered {
                    message: RerunError::TaskNotRegistered {
                        task_name: "charge".to_owned(),
                    }
                    .to_string(),
                    task_name: "charge".to_owned(),
                }
            }
        );
    }

    #[test]
    fn a_database_with_no_family_row_is_refused_as_not_recorded() {
        assert!(matches!(
            preparation_record(RerunPreparation::ClientFamilyMismatch {
                stored: RecordedFamily::NotRecorded,
                own: OWN_FAMILY,
            }),
            RerunPreparationRecord::Refused {
                error: CallerStepRefusal::ClientFamilyMismatch {
                    stored: StoredFamilyRecord::NotRecorded,
                    ..
                }
            }
        ));
    }
}
