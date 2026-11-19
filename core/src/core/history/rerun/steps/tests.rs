//! The three rerun steps without a database: the read statement, each
//! answer of the read's row, the write statement and each write outcome.
//!
//! Python specification: the outcome catalog of `rerun_task`
//! (`clients/python/horsies/core/history/rerun/operations.py`) and
//! `_rerun_write_outcome`.

use chrono::Duration;
use sha2::{Digest, Sha256};

use super::*;
use crate::core::history::archive::attempts::encode_attempt_snapshot;
use crate::core::history::identity::keys::IDEMPOTENCY_SCOPE_VERSION;

const SOURCE: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00e0);
const ROOT: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00d0);
const HOLDER: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00f0);
const CLASS: &str = "standard_14d";
const INPUT: &str = "{\"k\":\"v\"}";

fn command(caller_key: Option<&str>) -> RerunTask {
    RerunTask::new(SOURCE, None, caller_key.map(str::to_owned))
}

fn policy() -> RerunEnqueuePolicy {
    RerunEnqueuePolicy::new(CLASS, true, Duration::hours(24)).expect("a valid policy")
}

fn sent_at() -> DateTime<Utc> {
    DateTime::from_timestamp(1_790_000_000, 0).expect("an instant")
}

/// A version-1 framed envelope of `input` and no options, and its digest.
fn envelope(input: &str) -> (Vec<u8>, Vec<u8>) {
    let payload = format!("{{\"input\":{input},\"options\":null}}").into_bytes();
    let digest = Sha256::digest(&payload).to_vec();
    (payload, digest)
}

/// The read row of an eligible history source of the `RUST` family: a
/// failed task with an inline input, a decodable attempt snapshot and the
/// root `ROOT`.
fn history_values() -> Vec<StatementValue> {
    let snapshot = encode_attempt_snapshot(&[]).expect("an empty attempt snapshot");
    let (payload, digest) = envelope(INPUT);
    vec![
        StatementValue::Bool(true),
        StatementValue::Text("RUST".to_owned()),
        StatementValue::Text("HISTORY".to_owned()),
        StatementValue::Null(CallerType::Text),
        StatementValue::Uuid(SOURCE),
        StatementValue::Text("p8.rerun".to_owned()),
        StatementValue::Text("default".to_owned()),
        StatementValue::Int4(50),
        StatementValue::Int4(3),
        StatementValue::Text("FAILED".to_owned()),
        StatementValue::Bool(false),
        StatementValue::Uuid(ROOT),
        StatementValue::Int2(snapshot.version),
        StatementValue::Bytea(snapshot.payload),
        StatementValue::Bytea(snapshot.digest.to_vec()),
        StatementValue::Text("INLINE".to_owned()),
        StatementValue::Int2(1),
        StatementValue::Bytea(digest),
        StatementValue::Bytea(payload),
    ]
}

/// `values` with the value of `column` replaced.
fn with(
    mut values: Vec<StatementValue>,
    column: &str,
    value: StatementValue,
) -> Vec<StatementValue> {
    let index = RERUN_READ_COLUMNS
        .iter()
        .position(|declared| declared.name == column)
        .expect("a column of the read");
    values[index] = value;
    values
}

fn prepare_values(values: Vec<StatementValue>, caller_key: Option<&str>) -> RerunPreparation {
    let command = command(caller_key);
    let read = rerun_read(&command, &policy());
    prepare_rerun_write(
        &read,
        &CallerRow::new(values),
        &command,
        &policy(),
        ClientFamily::Rust,
        sent_at(),
    )
    .expect("a row of the read's columns")
}

fn refusal(values: Vec<StatementValue>) -> RerunRefusal {
    match prepare_values(values, None) {
        RerunPreparation::Refused(refusal) => refusal,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn write(caller_key: Option<&str>) -> RerunWrite {
    match prepare_values(history_values(), caller_key) {
        RerunPreparation::Write(write) => write,
        other => panic!("expected a write, got {other:?}"),
    }
}

#[test]
fn the_read_names_the_source_and_the_class_of_the_new_task() {
    let read = rerun_read(&command(None), &policy());
    assert_eq!(read.name(), "rerun_source_read");
    assert_eq!(
        read.parameters(),
        [
            StatementValue::Uuid(SOURCE),
            StatementValue::Text(CLASS.to_owned())
        ]
    );
    assert_eq!(read.columns().len(), 19);
    assert!(
        read.named_sql()
            .ends_with("FROM horsies_rerun_source_read(CAST(:p1 AS uuid), CAST(:p2 AS text))"),
        "{}",
        read.named_sql()
    );
}

#[test]
fn a_database_of_another_family_or_of_none_is_refused_first() {
    let python = with(
        with(
            history_values(),
            "class_registered",
            StatementValue::Bool(false),
        ),
        "client_family",
        StatementValue::Text("PYTHON".to_owned()),
    );
    assert_eq!(
        prepare_values(python, None),
        RerunPreparation::ClientFamilyMismatch {
            stored: RecordedFamily::Recorded(ClientFamily::Python),
            own: ClientFamily::Rust,
        }
    );
    let none = with(
        history_values(),
        "client_family",
        StatementValue::Null(CallerType::Text),
    );
    assert_eq!(
        prepare_values(none, None),
        RerunPreparation::ClientFamilyMismatch {
            stored: RecordedFamily::NotRecorded,
            own: ClientFamily::Rust,
        }
    );
}

/// A family value that is not a client family is corrupt stored data: the
/// rerun ends with the stored value before the class check, and nothing is
/// written. No panic.
#[test]
fn a_family_value_that_is_no_client_family_is_refused_as_corrupt_first() {
    let corrupt = with(
        with(
            history_values(),
            "class_registered",
            StatementValue::Bool(false),
        ),
        "client_family",
        StatementValue::Text("GO".to_owned()),
    );
    assert_eq!(
        prepare_values(corrupt, None),
        RerunPreparation::UnknownStoredFamily(UnknownStoredFamily {
            stored: "GO".to_owned(),
        })
    );
}

#[test]
fn an_unregistered_class_is_refused_before_the_source() {
    let unregistered = with(
        history_values(),
        "class_registered",
        StatementValue::Bool(false),
    );
    assert_eq!(
        prepare_values(unregistered, None),
        RerunPreparation::ClassNotRegistered {
            class_key: CLASS.to_owned()
        }
    );
}

#[test]
fn each_location_answer_of_the_read_has_its_refusal() {
    assert_eq!(
        refusal(with(
            history_values(),
            "location",
            StatementValue::Text("LIVE".to_owned())
        )),
        RerunRefusal::SourceLive { task_id: SOURCE }
    );
    let absent = with(
        with(
            history_values(),
            "location",
            StatementValue::Text("ABSENT".to_owned()),
        ),
        "absence_reason",
        StatementValue::Text("NOT_CLASSIFIED".to_owned()),
    );
    assert_eq!(
        refusal(absent),
        RerunRefusal::SourceAbsent {
            task_id: SOURCE,
            reason: TaskAbsenceReason::NotClassified,
        }
    );
    let no_reason = with(
        history_values(),
        "location",
        StatementValue::Text("ABSENT".to_owned()),
    );
    assert_eq!(
        refusal(no_reason),
        RerunRefusal::SourceCorrupt {
            task_id: SOURCE,
            detail: format!(
                "absence reason of {SOURCE} is None, which is not a task absence reason"
            ),
        }
    );
    let purged = with(
        with(
            history_values(),
            "location",
            StatementValue::Text("ABSENT".to_owned()),
        ),
        "absence_reason",
        StatementValue::Text("PURGED".to_owned()),
    );
    assert_eq!(
        refusal(purged),
        RerunRefusal::SourceCorrupt {
            task_id: SOURCE,
            detail: format!(
                "absence reason of {SOURCE} is 'PURGED', which is not a task absence reason"
            ),
        }
    );
    let unknown = with(
        history_values(),
        "location",
        StatementValue::Text("ELSEWHERE".to_owned()),
    );
    assert_eq!(
        refusal(unknown),
        RerunRefusal::SourceCorrupt {
            task_id: SOURCE,
            detail: "staged detail returned unknown location 'ELSEWHERE'".to_owned(),
        }
    );
}

#[test]
fn a_history_row_that_does_not_decode_is_a_corrupt_source() {
    let status = with(
        history_values(),
        "status",
        StatementValue::Text("RETIRED".to_owned()),
    );
    assert_eq!(
        refusal(status),
        RerunRefusal::SourceCorrupt {
            task_id: SOURCE,
            detail: format!(
                "history row {SOURCE} has status 'RETIRED', which is not a task status"
            ),
        }
    );
    let snapshot = with(
        history_values(),
        "attempt_snapshot_digest",
        StatementValue::Bytea(vec![0; 32]),
    );
    assert!(matches!(
        refusal(snapshot),
        RerunRefusal::SourceCorrupt { .. }
    ));
}

#[test]
fn the_eligibility_ladder_refuses_in_python_order() {
    let workflow = with(
        with(
            history_values(),
            "is_workflow_task",
            StatementValue::Bool(true),
        ),
        "status",
        StatementValue::Text("COMPLETED".to_owned()),
    );
    assert_eq!(
        refusal(workflow),
        RerunRefusal::NotEligible {
            task_id: SOURCE,
            reason: NotEligibleReason::WorkflowTask,
        }
    );
    let completed = with(
        history_values(),
        "status",
        StatementValue::Text("COMPLETED".to_owned()),
    );
    assert_eq!(
        refusal(completed),
        RerunRefusal::NotEligible {
            task_id: SOURCE,
            reason: NotEligibleReason::CompletedSource,
        }
    );
    let declined = with(
        history_values(),
        "rerun_input_disposition",
        StatementValue::Text("DECLINED_BY_POLICY".to_owned()),
    );
    assert_eq!(
        refusal(declined),
        RerunRefusal::InputUnavailable {
            task_id: SOURCE,
            disposition: "DECLINED_BY_POLICY".to_owned(),
        }
    );
    let incomplete = with(
        history_values(),
        "rerun_input_inline",
        StatementValue::Null(CallerType::Bytea),
    );
    assert_eq!(
        refusal(incomplete),
        RerunRefusal::InputCorrupt {
            task_id: SOURCE,
            detail: "inline disposition with an incomplete envelope".to_owned(),
        }
    );
    let tampered = with(
        history_values(),
        "rerun_input_digest",
        StatementValue::Bytea(vec![0; 32]),
    );
    assert!(matches!(
        refusal(tampered),
        RerunRefusal::InputCorrupt { ref detail, .. } if detail.contains("digest")
    ));
    let unknown_version = with(
        history_values(),
        "rerun_input_version",
        StatementValue::Int2(9),
    );
    assert_eq!(
        refusal(unknown_version),
        RerunRefusal::InputCorrupt {
            task_id: SOURCE,
            detail: "unknown input-envelope version 9".to_owned(),
        }
    );
}

#[test]
fn an_invalid_caller_key_is_refused_before_the_write() {
    assert!(matches!(
        prepare_values(history_values(), Some("")),
        RerunPreparation::CallerKeyRefused(IdempotencyKeyError::Empty { .. })
    ));
}

#[test]
fn an_unkeyed_write_carries_the_source_lineage_and_no_key() {
    let write = write(None);
    let statement = write.statement();
    assert_eq!(statement.name(), "rerun_enqueue");
    assert_eq!(write.source_task_id(), SOURCE);
    assert_eq!(write.rerun_root_task_id(), ROOT);
    let parameters = statement.parameters();
    assert_eq!(parameters[0], StatementValue::Uuid(write.new_task_id()));
    assert_eq!(parameters[4], StatementValue::Text(INPUT.to_owned()));
    assert_eq!(parameters[5], StatementValue::Null(CallerType::Text));
    assert_eq!(parameters[7], StatementValue::Text(CLASS.to_owned()));
    assert_eq!(
        parameters[10],
        StatementValue::Null(CallerType::Timestamptz)
    );
    assert_eq!(parameters[11], StatementValue::Timestamptz(sent_at()));
    assert_eq!(parameters[14], StatementValue::Uuid(SOURCE));
    assert_eq!(parameters[15], StatementValue::Uuid(ROOT));
    assert_eq!(parameters[16], StatementValue::Text("UNKEYED".to_owned()));
    assert_eq!(parameters[17], StatementValue::Null(CallerType::Bytea));
    assert_eq!(parameters[18], StatementValue::Null(CallerType::Int2));
    assert_eq!(parameters[19], StatementValue::Null(CallerType::Int8));
    assert!(statement
        .named_sql()
        .contains("CAST(:p20 AS int8)::bigint * interval '1 microsecond'"));
}

#[test]
fn a_keyed_write_carries_the_scoped_digest_and_the_exact_window() {
    let write = write(Some("rerun-key-1"));
    let parameters = write.statement().parameters();
    assert_eq!(parameters[16], StatementValue::Text("KEYED".to_owned()));
    assert!(matches!(parameters[17], StatementValue::Bytea(ref digest) if digest.len() == 32));
    assert_eq!(
        parameters[18],
        StatementValue::Int2(IDEMPOTENCY_SCOPE_VERSION)
    );
    assert_eq!(parameters[19], StatementValue::Int8(86_400_000_000));
}

#[test]
fn a_source_without_a_root_is_its_own_root() {
    let rootless = with(
        history_values(),
        "rerun_root_task_id",
        StatementValue::Null(CallerType::Uuid),
    );
    match prepare_values(rootless, None) {
        RerunPreparation::Write(write) => assert_eq!(write.rerun_root_task_id(), SOURCE),
        other => panic!("expected a write, got {other:?}"),
    }
}

#[test]
fn a_row_that_does_not_fit_the_read_is_a_row_error() {
    let command = command(None);
    let read = rerun_read(&command, &policy());
    let null_location = CallerRow::new(with(
        history_values(),
        "location",
        StatementValue::Null(CallerType::Text),
    ));
    assert_eq!(
        prepare_rerun_write(
            &read,
            &null_location,
            &command,
            &policy(),
            ClientFamily::Rust,
            sent_at()
        ),
        Err(RerunRowError::NullColumn {
            statement: "rerun_source_read",
            column: "location",
        })
    );
    let wrong_type = CallerRow::new(with(history_values(), "priority", StatementValue::Int8(50)));
    assert!(matches!(
        prepare_rerun_write(
            &read,
            &wrong_type,
            &command,
            &policy(),
            ClientFamily::Rust,
            sent_at()
        ),
        Err(RerunRowError::Row(CallerRowRefused::TypeMismatch {
            column: "priority",
            ..
        }))
    ));
}

fn written(outcome: &str, task_id: Uuid) -> CallerRow {
    CallerRow::new(vec![
        StatementValue::Text(outcome.to_owned()),
        StatementValue::Uuid(task_id),
    ])
}

/// Python `_rerun_write_outcome`: an insert and a repeat of this write are
/// the enqueued rerun; the key answers name the task that holds the key.
#[test]
fn each_write_answer_maps_onto_its_rerun_outcome() {
    let write = write(None);
    let enqueued = RerunWriteOutcome::Enqueued {
        new_task_id: write.new_task_id(),
        source_task_id: SOURCE,
        rerun_root_task_id: ROOT,
    };
    for outcome in ["INSERTED", "SAME_COMMAND"] {
        assert_eq!(
            decode_rerun_write(&write, &written(outcome, write.new_task_id())),
            Ok(enqueued.clone())
        );
    }
    assert_eq!(
        decode_rerun_write(&write, &written("KEY_REPLAY", HOLDER)),
        Ok(RerunWriteOutcome::KeyReplay {
            existing_task_id: HOLDER
        })
    );
    assert_eq!(
        decode_rerun_write(&write, &written("KEY_CONFLICT", HOLDER)),
        Ok(RerunWriteOutcome::KeyConflict {
            task_id: SOURCE,
            reserved_by_task_id: HOLDER,
        })
    );
    assert_eq!(
        RerunOutcome::from(enqueued.clone()),
        RerunOutcome::Enqueued {
            new_task_id: write.new_task_id(),
            source_task_id: SOURCE,
            rerun_root_task_id: ROOT,
        }
    );
}

/// A minted id that another row holds is a defect, not an outcome.
#[test]
#[should_panic(expected = "no other row can hold a minted id")]
fn a_payload_mismatch_on_a_minted_id_panics() {
    let write = write(None);
    let _outcome = decode_rerun_write(&write, &written("PAYLOAD_MISMATCH", write.new_task_id()));
}

#[test]
#[should_panic(expected = "no other row can hold a minted id")]
fn a_conflict_that_cannot_be_verified_on_a_minted_id_panics() {
    let write = write(None);
    let _outcome = decode_rerun_write(
        &write,
        &written("CONFLICT_UNVERIFIABLE", write.new_task_id()),
    );
}

#[test]
#[should_panic(expected = "outside the enqueue contract")]
fn an_outcome_outside_the_function_set_panics() {
    let write = write(None);
    let _outcome = decode_rerun_write(&write, &written("RETRIED", write.new_task_id()));
}
