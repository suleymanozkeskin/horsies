//! The config sections of an app config: each section from the config, and
//! each value outside the bound of its manifest field.

use std::collections::HashMap;

use serde_json::json;

use super::*;
use crate::worker_manifest::tests::{full_profile, manifest, named, parsed};

/// The bound of the manifest count `$count`, as a refusal gives it.
macro_rules! bound_of {
    ($count:path) => {
        (
            u64::from($count(BoundedU32::SMALLEST).0.get()),
            u64::from($count(BoundedU32::LARGEST).0.get()),
        )
    };
}

/// The manifest of the core's test fixture: the profile `batch` with every
/// kind of setting, and a value of its own in every section.
fn fixture() -> wire::WorkerManifest {
    parsed(&manifest(
        &json!({"batch": full_profile()}),
        &named("batch"),
    ))
}

fn config() -> AppConfig {
    AppConfig::for_database_url("postgresql://app@db.example/horsies")
}

fn sections(config: &AppConfig) -> ConfigSections {
    config_sections(config)
        .unwrap_or_else(|refused| panic!("every value is inside its bound: {refused:?}"))
}

fn queue(name: &str, priority: u32, max_concurrency: Option<u32>) -> CustomQueueConfig {
    CustomQueueConfig {
        name: name.to_owned(),
        priority,
        max_concurrency,
    }
}

/// The path of `field` with its members joined by `.`, a position as `[n]`
/// and a key as a member, so that a case names its field in one line.
fn dotted(field: &ConfigField) -> String {
    let mut path = String::new();
    for segment in field.segments() {
        match segment {
            FieldSegment::Member(name) => push_member(&mut path, name),
            FieldSegment::Key(key) => push_member(&mut path, key),
            FieldSegment::Position(position) => path.push_str(&format!("[{position}]")),
        }
    }
    path
}

fn push_member(path: &mut String, name: &str) {
    if !path.is_empty() {
        path.push('.');
    }
    path.push_str(name);
}

/// Each profile of the config is its manifest profile: a profile set read
/// from manifest profiles builds the same manifest profiles. The three
/// profiles hold each variant of each choice.
#[test]
fn each_profile_of_the_config_is_its_manifest_profile() {
    let mut other = full_profile();
    other["isolated"] = json!({"kind": "NO_CHILDREN"});
    other["shared_async"]["executor"] = json!({"kind": "FROM_SEATS"});
    other["shared_async"]["pool"]["max_size"] = json!({"kind": "CONNECTIONS", "connections": 4});
    other["shared_async"]["recycle"] = json!({
        "after_per_seat": {"kind": "TASKS", "tasks": 10},
        "above": {"kind": "MEGABYTES", "mb": 512}
    });
    other["shared_thread"] = json!({
        "kind": "CHILDREN",
        "count": 1,
        "seats": 4,
        "recycle": {"after_per_seat": {"kind": "NO_LIMIT"}, "above": {"kind": "NO_LIMIT"}},
        "pool": {"min_size": 0, "max_size": {"kind": "FROM_SEATS"}, "pre_ping": true},
        "task_send_pool": 1
    });
    other["pool"] = json!({"max_size": {"kind": "CONNECTIONS", "connections": 5}});
    other["claim"] = json!({
        "per_queue_per_pass": {"kind": "NO_LIMIT"},
        "claimed_per_worker": {"kind": "TASKS", "tasks": 30}
    });
    other["preload"] = json!("APP");
    other["parent_cpu"] = json!("OWN_CORE");
    let mut library = full_profile();
    library["preload"] = json!("LIBRARY");
    let manifest = parsed(&manifest(
        &json!({"batch": full_profile(), "other": other, "library": library}),
        &named("batch"),
    ));
    let mut config = config();
    config.worker_profiles = manifest.config_profiles();
    assert_eq!(sections(&config).profiles, manifest.profiles);
}

/// A client builds the sections only of a config that `AppConfig::validate`
/// accepted. A profile value outside its bound is a defect of the client:
/// the builder panics with its path and its bounds.
#[test]
#[should_panic(
    expected = "worker_profiles['web'].isolated.count: 0 is outside the bounds 1 to 1024"
)]
fn a_profile_value_outside_its_bound_is_a_defect_of_the_client() {
    let mut config = config();
    let profile = crate::core::config::worker_profile::input::WorkerProfile {
        isolated: crate::core::config::worker_profile::input::IsolatedChildren::new(0).into(),
        ..crate::core::config::worker_profile::input::WorkerProfile::default()
    };
    let _replaced = config.worker_profiles.insert("web".to_owned(), profile);
    let _sections = config_sections(&config);
}

/// The custom queues in config order, with their priority and limit, and
/// the claim values of the config.
#[test]
fn the_claim_section_has_the_queues_and_the_claim_values_of_the_config() {
    let mut config = config();
    config.queue_mode = QueueMode::Custom;
    config.custom_queues = Some(vec![queue("high", 1, Some(0)), queue("low", 100, None)]);
    config.prefetch_buffer = 0;
    config.claim_lease_ms = None;
    config.max_claim_renew_age_ms = 180_000;
    config.cluster_wide_cap = crate::core::config::ClusterCapSetting::ClusterCap { tasks: 20 };
    assert_eq!(sections(&config).claim, fixture().claim);
}

/// The default queue mode is the one queue `default`; no lease is the
/// worker's default lease and a lease is its milliseconds; no cap is no
/// limit.
#[test]
fn the_default_queue_and_no_lease_or_cap_are_variants_of_their_own() {
    let mut config = config();
    let claim = sections(&config).claim;
    assert_eq!(claim.queues, wire::Queues::Default);
    assert_eq!(claim.claim_lease, wire::ClaimLease::WorkerDefault);
    assert_eq!(claim.cluster_wide_cap, wire::ClusterWideCap::NoLimit);
    config.claim_lease_ms = Some(30_000);
    let wire::ClaimLease::Milliseconds { ms } = sections(&config).claim.claim_lease else {
        panic!("a lease of the config is a lease of milliseconds");
    };
    assert_eq!(ms.0.get(), 30_000);
}

/// Every field of the recovery section comes from its config field.
#[test]
fn the_recovery_section_is_the_config_field_by_field() {
    let fixture = fixture();
    let mut config = config();
    config.recovery = fixture.recovery.to_config();
    assert_eq!(sections(&config).recovery, fixture.recovery);
}

/// Every field of the retention section comes from its config field: a
/// period is whole days, a mapped queue is a period or forever, and the
/// paused age is exact to the microsecond.
#[test]
fn the_retention_section_is_the_config_field_by_field() {
    let fixture = fixture();
    let mut config = config();
    config.retention = fixture.retention.to_config();
    assert_eq!(sections(&config).retention, fixture.retention);
}

/// The other variant of each retention choice.
#[test]
fn a_retention_rule_that_is_off_is_a_variant_of_its_own() {
    let mut config = config();
    config.retention.worker_state_retention_hours = None;
    config.retention.terminal_record_retention_hours = Some(43_800);
    config.retention.paused_workflow_cancel_after = None;
    let retention = sections(&config).retention;
    assert_eq!(
        retention.worker_state_retention,
        wire::WorkerStateRetention::NoPruning
    );
    let wire::TerminalRecordRetention::Hours { hours } = retention.terminal_record_retention else {
        panic!("a period of the config is a period of hours");
    };
    assert_eq!(hours.0.get(), 43_800);
    assert_eq!(
        retention.paused_workflow_cancel_after,
        wire::PausedWorkflowExpiry::NoExpiry
    );
}

/// Every field of the resilience section comes from its config field; 0
/// attempts is no limit.
#[test]
fn the_resilience_section_is_the_config_and_zero_attempts_is_no_limit() {
    let fixture = fixture();
    let mut config = config();
    config.resilience = fixture.resilience.to_config();
    assert_eq!(sections(&config).resilience, fixture.resilience);
    config.resilience.database_retry_max_attempts = 0;
    assert_eq!(
        sections(&config).resilience.database_retry_max_attempts,
        wire::RetryAttemptLimit::NoLimit
    );
}

/// A threshold is its bytes and no threshold is no limit.
#[test]
fn the_payload_section_is_the_config_and_no_threshold_is_no_limit() {
    let fixture = fixture();
    let mut config = config();
    config.payload = fixture.payload.to_config();
    assert_eq!(sections(&config).payload, fixture.payload);
}

#[test]
fn a_drain_timeout_is_rounded_up_to_whole_milliseconds() {
    let timeout = DrainTimeout::try_from(std::time::Duration::from_micros(1_500))
        .expect("1.5 ms is a drain timeout");
    assert_eq!(drain_timeout_ms(timeout).get(), 2);
}

/// The change of one case of the bound test below.
type ConfigChange = Box<dyn Fn(&mut AppConfig)>;

/// One case of the bound test below: the change, the field that it puts
/// outside its bound, the path of the field and the bound.
type BoundCase = (ConfigChange, ConfigField, &'static str, (u64, u64));

fn recovery_case(
    change: ConfigChange,
    field: RecoveryField,
    path: &'static str,
    bound: (u64, u64),
) -> BoundCase {
    (change, ConfigField::Recovery(field), path, bound)
}

fn retention_case(
    change: ConfigChange,
    field: RetentionField,
    path: &'static str,
    bound: (u64, u64),
) -> BoundCase {
    (change, ConfigField::Retention(field), path, bound)
}

/// The cases of the claim values and the custom queues.
fn claim_cases() -> Vec<BoundCase> {
    vec![
        (
            Box::new(|config: &mut AppConfig| config.prefetch_buffer = u32::MAX),
            ConfigField::PrefetchBuffer,
            "prefetch_buffer",
            bound_of!(wire::ClaimCount),
        ),
        (
            Box::new(|config: &mut AppConfig| config.claim_lease_ms = Some(0)),
            ConfigField::ClaimLeaseMs,
            "claim_lease_ms",
            bound_of!(wire::Milliseconds),
        ),
        (
            Box::new(|config: &mut AppConfig| config.max_claim_renew_age_ms = 0),
            ConfigField::MaxClaimRenewAgeMs,
            "max_claim_renew_age_ms",
            bound_of!(wire::Milliseconds),
        ),
        (
            Box::new(|config: &mut AppConfig| {
                config.cluster_wide_cap =
                    crate::core::config::ClusterCapSetting::ClusterCap { tasks: u32::MAX }
            }),
            ConfigField::ClusterWideCap,
            "cluster_wide_cap",
            bound_of!(wire::LimitTasks),
        ),
        (
            Box::new(|config: &mut AppConfig| {
                config.queue_mode = QueueMode::Custom;
                config.custom_queues = Some(vec![queue("critical", 0, None)]);
            }),
            ConfigField::CustomQueue {
                position: 0,
                field: CustomQueueField::Priority,
            },
            "custom_queues[0].priority",
            bound_of!(wire::QueuePriority),
        ),
        (
            Box::new(|config: &mut AppConfig| {
                config.queue_mode = QueueMode::Custom;
                config.custom_queues = Some(vec![
                    queue("critical", 1, None),
                    queue("bulk", 90, Some(u32::MAX)),
                ]);
            }),
            ConfigField::CustomQueue {
                position: 1,
                field: CustomQueueField::MaxConcurrency,
            },
            "custom_queues[1].max_concurrency",
            bound_of!(wire::QueueConcurrencyLimit),
        ),
    ]
}

/// The cases of the thresholds and limits of the recovery section.
fn recovery_limit_cases() -> Vec<BoundCase> {
    vec![
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.claimed_stale_threshold_ms = 0),
            RecoveryField::ClaimedStaleThresholdMs,
            "recovery.claimed_stale_threshold_ms",
            bound_of!(wire::ClaimedStaleThresholdMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.running_stale_threshold_ms = 0),
            RecoveryField::RunningStaleThresholdMs,
            "recovery.running_stale_threshold_ms",
            bound_of!(wire::StaleThresholdMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.finalizing_stale_threshold_ms = 0),
            RecoveryField::FinalizingStaleThresholdMs,
            "recovery.finalizing_stale_threshold_ms",
            bound_of!(wire::StaleThresholdMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| {
                config.recovery.crashed_worker_recovery_grace_ms = u64::MAX;
            }),
            RecoveryField::CrashedWorkerRecoveryGraceMs,
            "recovery.crashed_worker_recovery_grace_ms",
            bound_of!(wire::RecoveryGraceMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| {
                config.recovery.finalization_quarantine_after_attempts = 0;
            }),
            RecoveryField::FinalizationQuarantineAfterAttempts,
            "recovery.finalization_quarantine_after_attempts",
            bound_of!(wire::RecoveryPassLimit),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.workflow_recovery_failure_limit = 0),
            RecoveryField::WorkflowRecoveryFailureLimit,
            "recovery.workflow_recovery_failure_limit",
            bound_of!(wire::RecoveryPassLimit),
        ),
    ]
}

/// The cases of the intervals of the recovery section.
fn recovery_interval_cases() -> Vec<BoundCase> {
    vec![
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.check_interval_ms = 0),
            RecoveryField::CheckIntervalMs,
            "recovery.check_interval_ms",
            bound_of!(wire::CheckIntervalMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.orphan_task_audit_interval_ms = 0),
            RecoveryField::OrphanTaskAuditIntervalMs,
            "recovery.orphan_task_audit_interval_ms",
            bound_of!(wire::AuditIntervalMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.runner_heartbeat_interval_ms = 0),
            RecoveryField::RunnerHeartbeatIntervalMs,
            "recovery.runner_heartbeat_interval_ms",
            bound_of!(wire::HeartbeatIntervalMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| config.recovery.claimer_heartbeat_interval_ms = 0),
            RecoveryField::ClaimerHeartbeatIntervalMs,
            "recovery.claimer_heartbeat_interval_ms",
            bound_of!(wire::HeartbeatIntervalMs),
        ),
        recovery_case(
            Box::new(|config: &mut AppConfig| {
                config.recovery.worker_state_snapshot_interval_ms = 0;
            }),
            RecoveryField::WorkerStateSnapshotIntervalMs,
            "recovery.worker_state_snapshot_interval_ms",
            bound_of!(wire::SnapshotIntervalMs),
        ),
    ]
}

/// The cases of the hours and the leaves of the retention section.
fn retention_hour_cases() -> Vec<BoundCase> {
    vec![
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.worker_state_retention_hours = Some(0);
            }),
            RetentionField::WorkerStateRetentionHours,
            "retention.worker_state_retention_hours",
            bound_of!(wire::WorkerStateRetentionHours),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.terminal_record_retention_hours = Some(0);
            }),
            RetentionField::TerminalRecordRetentionHours,
            "retention.terminal_record_retention_hours",
            bound_of!(wire::TerminalRecordRetentionHours),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| config.retention.history_leaf_horizon_days = 0),
            RetentionField::HistoryLeafHorizonDays,
            "retention.history_leaf_horizon_days",
            bound_of!(wire::HistoryLeafHorizonDays),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| config.retention.heartbeat_leaf_horizon_hours = 0),
            RetentionField::HeartbeatLeafHorizonHours,
            "retention.heartbeat_leaf_horizon_hours",
            bound_of!(wire::HeartbeatLeafHorizonHours),
        ),
    ]
}

/// The cases of the periods and the intervals of the retention section. A
/// period of 36 hours is not whole days.
fn retention_period_cases() -> Vec<BoundCase> {
    vec![
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.retention_periods =
                    vec![chrono::Duration::days(7), chrono::Duration::hours(36)];
            }),
            RetentionField::RetentionPeriod { position: 1 },
            "retention.retention_periods[1]",
            bound_of!(wire::RetentionPeriodDays),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.queue_retention =
                    HashMap::from([("bulk".to_owned(), Some(chrono::Duration::hours(36)))]);
            }),
            RetentionField::QueueRetention {
                queue: "bulk".to_owned(),
            },
            "retention.queue_retention.bulk",
            bound_of!(wire::RetentionPeriodDays),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.partition_maintenance_interval_seconds = 0;
            }),
            RetentionField::PartitionMaintenanceIntervalSeconds,
            "retention.partition_maintenance_interval_seconds",
            bound_of!(wire::MaintenanceIntervalSeconds),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| {
                config.retention.retention_sweep_interval_seconds = 0
            }),
            RetentionField::RetentionSweepIntervalSeconds,
            "retention.retention_sweep_interval_seconds",
            bound_of!(wire::SweepIntervalSeconds),
        ),
        retention_case(
            Box::new(|config: &mut AppConfig| config.retention.retention_delete_batch_size = 0),
            RetentionField::RetentionDeleteBatchSize,
            "retention.retention_delete_batch_size",
            bound_of!(wire::DeleteBatchSize),
        ),
    ]
}

/// The cases of the resilience and payload sections. 0 attempts is no
/// limit, so the attempt case has no `u32` form.
fn resilience_and_payload_cases() -> Vec<BoundCase> {
    vec![
        (
            Box::new(|config: &mut AppConfig| config.resilience.database_retry_initial_ms = 0),
            ConfigField::Resilience(ResilienceField::DatabaseRetryInitialMs),
            "resilience.database_retry_initial_ms",
            bound_of!(wire::RetryInitialMs),
        ),
        (
            Box::new(|config: &mut AppConfig| config.resilience.database_retry_max_ms = 0),
            ConfigField::Resilience(ResilienceField::DatabaseRetryMaxMs),
            "resilience.database_retry_max_ms",
            bound_of!(wire::RetryMaxMs),
        ),
        (
            Box::new(|config: &mut AppConfig| {
                config.resilience.database_retry_max_attempts = u64::MAX
            }),
            ConfigField::Resilience(ResilienceField::DatabaseRetryMaxAttempts),
            "resilience.database_retry_max_attempts",
            bound_of!(wire::RetryAttemptCount),
        ),
        (
            Box::new(|config: &mut AppConfig| config.resilience.notify_poll_interval_ms = 0),
            ConfigField::Resilience(ResilienceField::NotifyPollIntervalMs),
            "resilience.notify_poll_interval_ms",
            bound_of!(wire::PollIntervalMs),
        ),
        (
            Box::new(|config: &mut AppConfig| config.payload.warn_bytes = Some(0)),
            ConfigField::Payload(PayloadField::WarnBytes),
            "payload.warn_bytes",
            (MIN_PAYLOAD_BYTES, wire::MAX_PAYLOAD_BYTES),
        ),
        (
            Box::new(|config: &mut AppConfig| config.payload.reject_bytes = Some(0)),
            ConfigField::Payload(PayloadField::RejectBytes),
            "payload.reject_bytes",
            (MIN_PAYLOAD_BYTES, wire::MAX_PAYLOAD_BYTES),
        ),
    ]
}

/// Each value outside its manifest bound is refused with its field and the
/// bound of its manifest field; the field's path is the path of the config
/// text. One case for each field of [`ConfigField`].
#[test]
fn a_value_outside_its_manifest_bound_is_refused_with_its_field_and_bound() {
    let cases = claim_cases()
        .into_iter()
        .chain(recovery_limit_cases())
        .chain(recovery_interval_cases())
        .chain(retention_hour_cases())
        .chain(retention_period_cases())
        .chain(resilience_and_payload_cases());
    for (change, field, path, (min, max)) in cases {
        let mut config = config();
        change(&mut config);
        let refused = config_sections(&config).expect_err(path);
        assert_eq!(dotted(&refused.field), path);
        assert_eq!(refused, ManifestBoundRefused { field, min, max }, "{path}");
    }
}

/// The first value outside its bound in the order of the builder is the
/// refusal: a claim value before a queue, a queue before a later queue and
/// before the sections, and a mapped queue before another in name order.
#[test]
fn the_first_value_outside_its_bound_is_refused() {
    let first = |config: &AppConfig| {
        config_sections(config)
            .expect_err("a value is outside its bound")
            .field
    };
    let mut config = config();
    config.payload.warn_bytes = Some(0);
    config.recovery.check_interval_ms = 0;
    config.queue_mode = QueueMode::Custom;
    config.custom_queues = Some(vec![queue("a", 0, None), queue("b", 0, None)]);
    config.prefetch_buffer = u32::MAX;
    assert_eq!(first(&config), ConfigField::PrefetchBuffer);
    config.prefetch_buffer = 0;
    assert_eq!(
        first(&config),
        ConfigField::CustomQueue {
            position: 0,
            field: CustomQueueField::Priority,
        }
    );
    config.custom_queues = None;
    assert_eq!(
        first(&config),
        ConfigField::Recovery(RecoveryField::CheckIntervalMs)
    );
    config.recovery = RecoveryConfig::default();
    let not_whole_days = Some(chrono::Duration::hours(36));
    config.retention.queue_retention = HashMap::from([
        ("zeta".to_owned(), not_whole_days),
        ("alpha".to_owned(), not_whole_days),
    ]);
    assert_eq!(
        first(&config),
        ConfigField::Retention(RetentionField::QueueRetention {
            queue: "alpha".to_owned(),
        })
    );
}

/// A position and a map key are segments of their own, so a client renders
/// each in its own form; a key holds its text as it is.
#[test]
fn a_position_and_a_map_key_are_segments_of_their_own() {
    let field = ConfigField::CustomQueue {
        position: 2,
        field: CustomQueueField::MaxConcurrency,
    };
    assert_eq!(
        field.segments(),
        [
            FieldSegment::Member("custom_queues"),
            FieldSegment::Position(2),
            FieldSegment::Member("max_concurrency"),
        ]
    );
    let field = ConfigField::Retention(RetentionField::QueueRetention {
        queue: "a.b".to_owned(),
    });
    assert_eq!(
        field.segments(),
        [
            FieldSegment::Member("retention"),
            FieldSegment::Member("queue_retention"),
            FieldSegment::Key("a.b"),
        ]
    );
}
