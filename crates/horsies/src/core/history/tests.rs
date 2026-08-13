use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::archive::attempts::{decode_attempt_snapshot, encode_attempt_snapshot, AttemptRecord};
use super::archive::rerun_input::{
    decode_rerun_input, disposition_of, store_inline_rerun_input, store_referenced_rerun_input,
    store_unavailable_rerun_input, DecodedRerunInput, RerunInputDisposition,
    RerunInputUnavailability, RerunInputUnavailableReason, RERUN_INPUT_INLINE_MAX_BYTES,
};
use super::archive::results::{decode_result_envelope, encode_result_envelope};
use super::archive::versions::{
    ArchiveDecodeError, ArchiveDomain, ARCHIVE_VERSION_1, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
};
use super::commands::is_safe_identifier;
use super::ddl::classes::finite_class_parent_name;
use super::ddl::runtime_names::{daily_leaf_name, leaf_enqueued_index_name, leaf_id_index_name};
use super::identity::fingerprint::EnqueueCommandV1;
use super::identity::keys::{
    validate_reservation_window, ScopedIdempotencyKey, IDEMPOTENCY_WINDOW_MAX_DAYS,
};
use super::identity::uuid7::{uuid7_birth_at, MonotonicUuid7Generator};
use super::names::{MAX_RETENTION_CLASS_KEY_LENGTH, POSTGRES_IDENTIFIER_LIMIT};
use super::rerun::input_envelope::{decode_input_envelope, encode_input_envelope_v1};

fn vectors() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/task_history/python-v052-codec-vectors.json"
    )))
    .unwrap()
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn bytes_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn string(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap().to_owned()
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value[key].as_str().map(ToOwned::to_owned)
}

#[test]
fn result_vectors_match_python_bytes() {
    for vector in vectors()["result_envelopes"].as_array().unwrap() {
        let result_json = vector["input"]["result_json"].as_str().unwrap();
        let expected = &vector["envelope"];
        let encoded = encode_result_envelope(result_json).unwrap();
        assert_eq!(
            encoded.version,
            expected["version"].as_i64().unwrap() as i16
        );
        assert_eq!(encoded.codec, expected["codec"].as_str().unwrap());
        assert_eq!(
            encoded.content_type,
            expected["content_type"].as_str().unwrap()
        );
        assert_eq!(
            encoded.payload,
            decode_hex(expected["payload"]["hex"].as_str().unwrap())
        );
        assert_eq!(
            bytes_hex(&encoded.digest),
            expected["digest_hex"].as_str().unwrap()
        );
        decode_result_envelope(
            encoded.version,
            encoded.codec,
            encoded.content_type,
            &encoded.payload,
            &encoded.digest,
        )
        .unwrap();
    }
}

#[test]
fn attempt_vectors_match_python_bytes_and_round_trip() {
    for vector in vectors()["attempt_snapshots"].as_array().unwrap() {
        let records: Vec<AttemptRecord> = vector["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                AttemptRecord::new(
                    item["attempt"].as_i64().unwrap() as i32,
                    string(item, "outcome"),
                    item["will_retry"].as_bool().unwrap(),
                    DateTime::parse_from_rfc3339(item["started_at"].as_str().unwrap())
                        .unwrap()
                        .with_timezone(&Utc),
                    DateTime::parse_from_rfc3339(item["finished_at"].as_str().unwrap())
                        .unwrap()
                        .with_timezone(&Utc),
                    optional_string(item, "error_code"),
                    optional_string(item, "error_message"),
                    optional_string(item, "failed_reason"),
                    optional_string(item, "worker_id"),
                    optional_string(item, "worker_hostname"),
                    item["worker_pid"].as_i64().map(|value| value as i32),
                    optional_string(item, "worker_process_name"),
                )
                .unwrap()
            })
            .collect();
        let encoded = encode_attempt_snapshot(&records).unwrap();
        let expected = &vector["envelope"];
        assert_eq!(
            encoded.payload,
            decode_hex(expected["payload"]["hex"].as_str().unwrap())
        );
        assert_eq!(
            bytes_hex(&encoded.digest),
            expected["digest_hex"].as_str().unwrap()
        );
        assert_eq!(
            decode_attempt_snapshot(
                encoded.version,
                encoded.codec,
                encoded.content_type,
                &encoded.payload,
                &encoded.digest,
            )
            .unwrap(),
            records
        );
    }
}

#[test]
fn fingerprint_vectors_match_python_canonical_bytes() {
    for vector in vectors()["command_fingerprints"].as_array().unwrap() {
        let input = &vector["input"];
        let command = EnqueueCommandV1::new(
            string(input, "task_name"),
            string(input, "queue_name"),
            input["priority"].as_i64().unwrap() as i32,
            optional_string(input, "args_json"),
            optional_string(input, "kwargs_json"),
            input["good_until"].as_str().map(|value| {
                DateTime::parse_from_rfc3339(value)
                    .unwrap()
                    .with_timezone(&Utc)
            }),
            input["enqueue_delay_seconds"].as_i64(),
            optional_string(input, "task_options_json"),
            string(input, "retention_class_key"),
            input["retain_rerun_input"].as_bool().unwrap(),
            input["rerun_of_task_id"]
                .as_str()
                .map(|value| Uuid::parse_str(value).unwrap()),
            input["rerun_root_task_id"]
                .as_str()
                .map(|value| Uuid::parse_str(value).unwrap()),
        )
        .unwrap();
        let expected = &vector["canonical"];
        assert_eq!(
            command.canonical_bytes().unwrap(),
            decode_hex(expected["hex"].as_str().unwrap())
        );
        assert_eq!(
            bytes_hex(&command.fingerprint().unwrap()),
            vector["fingerprint_sha256_hex"].as_str().unwrap()
        );
    }
}

#[test]
fn input_and_stored_rerun_vectors_match_python() {
    let rerun = &vectors()["rerun_input"];
    for vector in rerun["content_envelopes"].as_array().unwrap() {
        let input = &vector["input"];
        let args = input["args"].as_array().unwrap();
        let kwargs = input["kwargs"].as_object().unwrap();
        let options = input["options"].as_object();
        let content = encode_input_envelope_v1(args, kwargs, options).unwrap();
        assert_eq!(
            content,
            decode_hex(vector["content"]["hex"].as_str().unwrap())
        );
        let stored = store_inline_rerun_input(&content).unwrap();
        assert_eq!(disposition_of(&stored), RerunInputDisposition::Inline);
        let digest = Sha256::digest(&content);
        assert_eq!(
            bytes_hex(&digest),
            vector["stored_inline"]["digest_hex"].as_str().unwrap()
        );
        let reconstructed = decode_input_envelope(ARCHIVE_VERSION_1, &content, &digest).unwrap();
        assert_eq!(reconstructed.args, *args);
        assert_eq!(reconstructed.kwargs, *kwargs);
        assert_eq!(reconstructed.options.as_ref(), options);
    }

    let reference = &rerun["reference"];
    let digest = decode_hex(reference["stored"]["digest_hex"].as_str().unwrap());
    let stored =
        store_referenced_rerun_input(reference["input"]["reference"].as_str().unwrap(), &digest)
            .unwrap();
    assert_eq!(disposition_of(&stored), RerunInputDisposition::Reference);

    for unavailable in rerun["unavailable"].as_array().unwrap() {
        let unavailability = match unavailable["disposition"].as_str().unwrap() {
            "DECLINED_BY_POLICY" => RerunInputUnavailability::DeclinedByPolicy,
            "OVER_BOUND" => RerunInputUnavailability::OverBound,
            "NEVER_ELIGIBLE" => RerunInputUnavailability::NeverEligible,
            other => panic!("unrecognized fixture unavailability {other}"),
        };
        let stored = store_unavailable_rerun_input(unavailability);
        assert_eq!(
            disposition_of(&stored).as_str(),
            unavailable["disposition"].as_str().unwrap()
        );
        let decoded = decode_rerun_input(
            unavailable["disposition"].as_str().unwrap(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(matches!(
            decoded,
            DecodedRerunInput::Unavailable {
                reason: RerunInputUnavailableReason::DeclinedByPolicy
                    | RerunInputUnavailableReason::OverBound
                    | RerunInputUnavailableReason::NeverEligible
            }
        ));
    }
}

#[test]
fn partition_commands_reject_empty_health_class_and_invalid_timeouts() {
    use super::commands::{CollectPartitionHealth, FinalizeInterruptedLeafDetach};

    assert!(CollectPartitionHealth::new("", true).is_err());
    let bounds = super::commands::LeafBounds::new(
        DateTime::from_timestamp(0, 0).unwrap(),
        DateTime::from_timestamp(86_400, 0).unwrap(),
    )
    .unwrap();
    let leaf = super::commands::LeafRef::new("history_leaf", "finite", bounds).unwrap();
    assert!(FinalizeInterruptedLeafDetach::new(leaf, Some(0)).is_err());
}

#[test]
fn scoped_key_vectors_match_python_framing() {
    for vector in vectors()["scoped_idempotency_keys"].as_array().unwrap() {
        let input = &vector["input"];
        let scoped = ScopedIdempotencyKey::new(
            input["task_name"].as_str().unwrap(),
            input["key"].as_str().unwrap(),
        )
        .unwrap();
        assert_eq!(
            bytes_hex(&scoped.digest()),
            vector["digest_sha256_hex"].as_str().unwrap()
        );
    }
}

#[test]
fn archive_decoders_fail_closed_and_bounds_are_inclusive() {
    let payload = b"null";
    let digest = Sha256::digest(payload);
    assert!(matches!(
        decode_result_envelope(2, JSON_UTF8_CODEC, JSON_CONTENT_TYPE, payload, &digest),
        Err(ArchiveDecodeError::UnknownVersion {
            domain: ArchiveDomain::Result,
            version: 2
        })
    ));
    assert!(matches!(
        decode_result_envelope(1, JSON_UTF8_CODEC, JSON_CONTENT_TYPE, payload, &[0; 32]),
        Err(ArchiveDecodeError::DigestMismatch {
            domain: ArchiveDomain::Result
        })
    ));
    let invalid_json = b"not json";
    let invalid_json_digest = Sha256::digest(invalid_json);
    assert!(matches!(
        decode_result_envelope(
            1,
            JSON_UTF8_CODEC,
            JSON_CONTENT_TYPE,
            invalid_json,
            &invalid_json_digest,
        ),
        Err(ArchiveDecodeError::Corrupt {
            domain: ArchiveDomain::Result,
            detail,
        }) if detail == "JSONDecodeError"
    ));
    assert!(matches!(
        decode_rerun_input("EXPIRED", None, None, None, None, None, None),
        Err(ArchiveDecodeError::Corrupt {
            domain: ArchiveDomain::RerunInput,
            detail,
        }) if detail == "unknown_disposition"
    ));
    assert!(store_inline_rerun_input(&vec![0; RERUN_INPUT_INLINE_MAX_BYTES]).is_ok());
    assert!(store_inline_rerun_input(&vec![0; RERUN_INPUT_INLINE_MAX_BYTES + 1]).is_err());
}

#[test]
fn identifier_budget_pins_all_four_failure_bands() {
    assert_eq!(MAX_RETENTION_CLASS_KEY_LENGTH, 18);
    let lower = DateTime::parse_from_rfc3339("2026-08-11T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let names = |length: usize| {
        let key = "a".repeat(length);
        let parent = finite_class_parent_name(&key).unwrap();
        let leaf = daily_leaf_name(&parent, lower).unwrap();
        let task_index = leaf_id_index_name(&leaf);
        let ordering_index = leaf_enqueued_index_name(&leaf);
        (parent, leaf, task_index, ordering_index)
    };
    let (_, _, _, ordering_18) = names(18);
    assert_eq!(ordering_18.len(), POSTGRES_IDENTIFIER_LIMIT);
    let (_, leaf_19, _, ordering_19) = names(19);
    assert!(leaf_19.len() <= POSTGRES_IDENTIFIER_LIMIT);
    assert!(ordering_19.len() > POSTGRES_IDENTIFIER_LIMIT);
    let (_, leaf_30, task_30, ordering_30) = names(30);
    assert!(leaf_30.len() <= POSTGRES_IDENTIFIER_LIMIT);
    assert_eq!(
        &task_30.as_bytes()[..POSTGRES_IDENTIFIER_LIMIT],
        &ordering_30.as_bytes()[..POSTGRES_IDENTIFIER_LIMIT]
    );
    let parent_32 = finite_class_parent_name(&"a".repeat(32)).unwrap();
    assert!(daily_leaf_name(&parent_32, lower).is_err());

    let standard_parent = finite_class_parent_name("standard_30d").unwrap();
    let standard_leaf = daily_leaf_name(&standard_parent, lower).unwrap();
    assert_eq!(
        standard_leaf,
        "horsies_task_history_standard_30d_2026_08_11"
    );
    assert_eq!(
        leaf_id_index_name(&standard_leaf),
        "horsies_task_history_standard_30d_2026_08_11_task_idx"
    );

    assert!(is_safe_identifier("history_leaf_1"));
    assert!(!is_safe_identifier("HistoryLeaf"));
    assert!(!is_safe_identifier("1history_leaf"));
    assert!(!is_safe_identifier(&"a".repeat(64)));
}

#[test]
fn uuid7_is_monotonic_across_same_and_backward_milliseconds() {
    let clock = Arc::new(Mutex::new(VecDeque::from([1_000, 1_000, 999, 1_001])));
    let clock_for_generator = Arc::clone(&clock);
    let mut generator = MonotonicUuid7Generator::new(
        move || clock_for_generator.lock().unwrap().pop_front().unwrap(),
        || 7,
    );
    let ids: Vec<Uuid> = (0..4).map(|_| generator.mint().unwrap()).collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        uuid7_birth_at(ids[0]).unwrap(),
        DateTime::from_timestamp_millis(1_000).unwrap()
    );
    assert!(uuid7_birth_at(Uuid::new_v4()).is_none());
}

#[test]
fn uuid7_counter_exhaustion_waits_for_clock_advance() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_for_generator = Arc::clone(&calls);
    let mut generator = MonotonicUuid7Generator::new(
        move || {
            let call = calls_for_generator.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call < 4_096 {
                1_000
            } else {
                1_001
            }
        },
        || 0,
    );
    let mut previous = generator.mint().unwrap();
    for _ in 1..=4_096 {
        let current = generator.mint().unwrap();
        assert!(previous < current);
        previous = current;
    }
    assert_eq!(uuid7_birth_at(previous).unwrap().timestamp_millis(), 1_001);
}

#[test]
fn reservation_window_accepts_only_positive_through_thirty_days() {
    assert!(validate_reservation_window(Duration::microseconds(1)).is_ok());
    assert!(validate_reservation_window(Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS)).is_ok());
    assert!(validate_reservation_window(Duration::zero()).is_err());
    assert!(validate_reservation_window(
        Duration::days(IDEMPOTENCY_WINDOW_MAX_DAYS) + Duration::microseconds(1)
    )
    .is_err());
}
