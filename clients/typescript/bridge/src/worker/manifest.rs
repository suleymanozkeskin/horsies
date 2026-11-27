//! The sections of a worker manifest that the app config gives, for the
//! TypeScript addon.
//!
//! - The config sections (profiles, claim, recovery, retention, resilience,
//!   payload) come from the core's builder (`wire::config_sections`), from
//!   the core's `AppConfig` read from the config text that `Horsies.create`
//!   accepted. Every field that the TypeScript
//!   config left out has the core's default: each default has one source.
//! - The database and send sections come from the producer manifest of the
//!   same config (the `sends` area's `checkAppConfig`), so the URL form and
//!   the reservation window have one source in this addon. The pool of the
//!   worker process is the resolved pool of its profile with no overflow, so
//!   the app's `poolSize` and `maxOverflow` do not apply.
//! - Every profile takes `preload` from the core's `WorkerProfile`. The
//!   TypeScript config text has no `preload` key, so it is `NO_PRELOAD`.
//! - A config value that the config rules accept and that is outside the
//!   tighter bound of its manifest field is refused with its field path as
//!   the TypeScript config writes it and the bound, never the value
//!   ([`ManifestBoundRefused`]). The renderer of the path matches every
//!   `ConfigField`: a new field fails the build here.

use horsies_core::manifest::ProducerManifest;
use horsies_core::worker_manifest::{
    self as wire, ConfigField, CustomQueueField, PayloadField, RecoveryField, ResilienceField,
    RetentionField,
};
use horsies_core::{AppConfig, PostgresConfig};

/// A config value that the config check accepted and that is outside the
/// bound of its worker manifest field. `path` names the field as the
/// TypeScript config writes it (`customQueues[0].maxConcurrency.tasks`);
/// the value is not kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestBoundRefused {
    pub path: String,
    pub min: u64,
    pub max: u64,
}

/// The sections of a worker manifest that the app config gives.
#[derive(Debug, Clone)]
pub struct ConfigSections {
    pub database: wire::WorkerDatabaseManifest,
    pub send: horsies_core::manifest::SendManifest,
    pub profiles: std::collections::BTreeMap<String, wire::WorkerProfileManifest>,
    pub claim: wire::WorkerClaimManifest,
    pub recovery: wire::RecoveryManifest,
    pub retention: wire::RetentionManifest,
    pub resilience: wire::ResilienceManifest,
    pub payload: wire::PayloadManifest,
}

/// The config sections of `config`, whose producer manifest is `producer`.
/// Pure: no I/O.
///
/// Errors: [`ManifestBoundRefused`] for the first value outside its
/// manifest bound, in the core's order: the claim values
/// (`claim.prefetchBuffer`, `claim.leaseMs`, `maxClaimRenewAgeMs`,
/// `clusterWideCap.tasks`, each custom queue), then the recovery,
/// retention, resilience and payload sections.
pub fn config_sections(
    config: &AppConfig,
    producer: &ProducerManifest,
) -> Result<ConfigSections, ManifestBoundRefused> {
    let wire::ConfigSections {
        profiles,
        claim,
        recovery,
        retention,
        resilience,
        payload,
    } = wire::config_sections(config).map_err(|refused| ManifestBoundRefused {
        path: typescript_path(&refused.field),
        min: refused.min,
        max: refused.max,
    })?;
    Ok(ConfigSections {
        database: database_manifest(producer, &config.database),
        send: producer.send,
        profiles,
        claim,
        recovery,
        retention,
        resilience,
        payload,
    })
}

/// The database section of a worker: the URL forms and the connection
/// settings of the producer manifest, and the pre-ping rule of the worker
/// process from the config. Pure.
fn database_manifest(
    producer: &ProducerManifest,
    database_config: &PostgresConfig,
) -> wire::WorkerDatabaseManifest {
    let database = &producer.database;
    wire::WorkerDatabaseManifest {
        database_url: database.database_url.clone(),
        session_database: database.session_database.clone(),
        pgbouncer_transaction_mode: database.pgbouncer_transaction_mode,
        pool_pre_ping: database.pool_pre_ping,
        worker_pool_pre_ping: database_config.worker_pool_pre_ping,
        pool_timeout_seconds: database.pool_timeout_seconds,
        pool_recycle: database.pool_recycle,
        echo: database.echo,
        tcp_keepalives: database.tcp_keepalives,
        tcp_user_timeout: database.tcp_user_timeout,
    }
}

/// The path of `field` as the TypeScript config writes it: camelCase
/// members, the member of a variant object (`clusterWideCap.tasks`), the
/// members of the claim mode under `claim` (`claim.prefetchBuffer`,
/// `claim.leaseMs`), a position as `[n]` and a queue name as a JSON string.
/// Pure.
fn typescript_path(field: &ConfigField) -> String {
    match field {
        ConfigField::PrefetchBuffer => "claim.prefetchBuffer".to_owned(),
        ConfigField::ClaimLeaseMs => "claim.leaseMs".to_owned(),
        ConfigField::MaxClaimRenewAgeMs => "maxClaimRenewAgeMs".to_owned(),
        ConfigField::ClusterWideCap => "clusterWideCap.tasks".to_owned(),
        ConfigField::CustomQueue { position, field } => {
            let member = match field {
                CustomQueueField::Priority => "priority",
                CustomQueueField::MaxConcurrency => "maxConcurrency.tasks",
            };
            format!("customQueues[{position}].{member}")
        }
        ConfigField::Recovery(field) => format!("recovery.{}", recovery_member(*field)),
        ConfigField::Retention(field) => format!("retention.{}", retention_member(field)),
        ConfigField::Resilience(field) => format!("resilience.{}", resilience_member(*field)),
        ConfigField::Payload(field) => format!("payload.{}", payload_member(*field)),
    }
}

fn recovery_member(field: RecoveryField) -> &'static str {
    match field {
        RecoveryField::ClaimedStaleThresholdMs => "claimedStaleThresholdMs",
        RecoveryField::RunningStaleThresholdMs => "runningStaleThresholdMs",
        RecoveryField::FinalizingStaleThresholdMs => "finalizingStaleThresholdMs",
        RecoveryField::CrashedWorkerRecoveryGraceMs => "crashedWorkerRecoveryGraceMs",
        RecoveryField::FinalizationQuarantineAfterAttempts => "finalizationQuarantineAfterAttempts",
        RecoveryField::WorkflowRecoveryFailureLimit => "workflowRecoveryFailureLimit",
        RecoveryField::CheckIntervalMs => "checkIntervalMs",
        RecoveryField::OrphanTaskAuditIntervalMs => "orphanTaskAuditIntervalMs",
        RecoveryField::RunnerHeartbeatIntervalMs => "runnerHeartbeatIntervalMs",
        RecoveryField::ClaimerHeartbeatIntervalMs => "claimerHeartbeatIntervalMs",
        RecoveryField::WorkerStateSnapshotIntervalMs => "workerStateSnapshotIntervalMs",
    }
}

fn retention_member(field: &RetentionField) -> String {
    match field {
        RetentionField::WorkerStateRetentionHours => "workerStateRetention.hours".to_owned(),
        RetentionField::TerminalRecordRetentionHours => "terminalRecordRetention.hours".to_owned(),
        RetentionField::HistoryLeafHorizonDays => "historyLeafHorizonDays".to_owned(),
        RetentionField::HeartbeatLeafHorizonHours => "heartbeatLeafHorizonHours".to_owned(),
        RetentionField::RetentionPeriod { position } => {
            format!("retentionPeriods[{position}].days")
        }
        RetentionField::QueueRetention { queue } => format!(
            "queueRetention[{}].days",
            serde_json::Value::String(queue.clone())
        ),
        RetentionField::PartitionMaintenanceIntervalSeconds => {
            "partitionMaintenanceIntervalSeconds".to_owned()
        }
        RetentionField::RetentionSweepIntervalSeconds => "retentionSweepIntervalSeconds".to_owned(),
        RetentionField::RetentionDeleteBatchSize => "retentionDeleteBatchSize".to_owned(),
    }
}

fn resilience_member(field: ResilienceField) -> &'static str {
    match field {
        ResilienceField::DatabaseRetryInitialMs => "databaseRetryInitialMs",
        ResilienceField::DatabaseRetryMaxMs => "databaseRetryMaxMs",
        ResilienceField::DatabaseRetryMaxAttempts => "databaseRetryMaxAttempts.attempts",
        ResilienceField::NotifyPollIntervalMs => "notifyPollIntervalMs",
    }
}

fn payload_member(field: PayloadField) -> &'static str {
    match field {
        PayloadField::WarnBytes => "warnBytes.bytes",
        PayloadField::RejectBytes => "rejectBytes.bytes",
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::manifest::parse_producer_manifest;
    use horsies_core::QueueMode;

    use super::*;
    use crate::call::thrown::Envelope;
    use crate::sends::checks::{check_app_config, AppConfigCheck};

    /// The JSON text of an app config with `rest` after its broker section.
    fn config_text(rest: &str) -> String {
        format!(r#"{{"database": {{"database_url": "postgres://127.0.0.1:55432/horsies"}}{rest}}}"#)
    }

    /// The config and the producer manifest of `text`, as `prepareWorker`
    /// reads them.
    fn read(text: &str) -> (AppConfig, ProducerManifest) {
        let config: AppConfig = serde_json::from_str(text).expect("the config text reads");
        let Envelope::Value(AppConfigCheck::Accepted { manifest_json, .. }) =
            check_app_config(text.to_owned())
        else {
            panic!("the config is accepted");
        };
        let producer = parse_producer_manifest(&manifest_json).expect("the manifest reads");
        (config, producer)
    }

    fn sections(rest: &str) -> Result<ConfigSections, ManifestBoundRefused> {
        let (config, producer) = read(&config_text(rest));
        config_sections(&config, &producer)
    }

    fn profile(rest: &str, name: &str) -> wire::WorkerProfileManifest {
        sections(rest)
            .expect("the sections are inside their bounds")
            .profiles
            .remove(name)
            .expect("the profile is in the manifest")
    }

    #[test]
    fn the_database_section_takes_the_url_form_of_the_producer_manifest() {
        let built = sections("").expect("the default sections");
        assert_eq!(
            built.database.database_url.expose(),
            "postgresql://127.0.0.1:55432/horsies"
        );
        assert!(!built.database.worker_pool_pre_ping);
    }

    #[test]
    fn an_isolated_profile_has_its_children_and_no_preload() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 2}}}"#,
            "io",
        );
        assert!(matches!(
            built.isolated,
            wire::Isolated::Children {
                count: wire::IsolatedCount(_),
                ..
            }
        ));
        assert_eq!(built.shared_async, wire::SharedAsync::NoChildren);
        assert_eq!(built.shared_thread, wire::SharedThread::NoChildren);
        assert_eq!(built.preload, wire::Preload::NoPreload);
    }

    #[test]
    fn a_shared_async_profile_with_no_isolated_children_has_seats_and_the_executor_from_seats() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "no_isolated_children"}, "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 50}}}"#,
            "io",
        );
        assert_eq!(built.isolated, wire::Isolated::NoChildren);
        let wire::SharedAsync::Children { executor, .. } = built.shared_async else {
            panic!("the profile has shared async children");
        };
        assert_eq!(executor, wire::DefaultExecutor::FromSeats);
    }

    #[test]
    fn a_shared_thread_profile_has_its_children() {
        let built = profile(
            r#", "worker_profiles": {"cpu": {"isolated": {"kind": "no_isolated_children"}, "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4}}}"#,
            "cpu",
        );
        assert!(matches!(
            built.shared_thread,
            wire::SharedThread::Children { .. }
        ));
    }

    #[test]
    fn each_recycle_variant_maps_to_its_manifest_limit() {
        let built = profile(
            r#", "worker_profiles": {"io": {"isolated": {"kind": "isolated_children", "count": 1, "recycle": {"after": {"kind": "no_task_limit"}, "above": {"kind": "memory_limit", "mb": 512}}}, "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 2, "recycle": {"after_per_seat": {"kind": "task_limit", "tasks": 7}, "above": {"kind": "no_memory_limit"}}}}}"#,
            "io",
        );
        let wire::Isolated::Children { recycle, .. } = built.isolated else {
            panic!("the profile has isolated children");
        };
        assert_eq!(recycle.after, wire::TaskLimit::NoLimit);
        assert!(matches!(recycle.above, wire::MemoryLimit::Megabytes { .. }));
        let wire::SharedAsync::Children { recycle, .. } = built.shared_async else {
            panic!("the profile has shared async children");
        };
        assert!(matches!(
            recycle.after_per_seat,
            wire::TaskLimit::Tasks { .. }
        ));
        assert_eq!(recycle.above, wire::MemoryLimit::NoLimit);
    }

    /// The change of one case of the bound test below.
    type ConfigChange = Box<dyn Fn(&mut AppConfig)>;

    /// The bound cases of the fields that the first list of the bound test
    /// leaves out: together they refuse every kind of `ConfigField`.
    fn more_bound_cases() -> Vec<(ConfigChange, &'static str)> {
        let recovery = |path, change: fn(&mut horsies_core::RecoveryConfig)| {
            let change: ConfigChange =
                Box::new(move |config: &mut AppConfig| change(&mut config.recovery));
            (change, path)
        };
        let retention = |path, change: fn(&mut horsies_core::RetentionConfig)| {
            let change: ConfigChange =
                Box::new(move |config: &mut AppConfig| change(&mut config.retention));
            (change, path)
        };
        let resilience = |path, change: fn(&mut horsies_core::WorkerResilienceConfig)| {
            let change: ConfigChange =
                Box::new(move |config: &mut AppConfig| change(&mut config.resilience));
            (change, path)
        };
        vec![
            (
                Box::new(|config: &mut AppConfig| config.max_claim_renew_age_ms = 0),
                "maxClaimRenewAgeMs",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.queue_mode = QueueMode::Custom;
                    config.custom_queues = Some(vec![horsies_core::CustomQueueConfig {
                        name: "critical".to_owned(),
                        priority: 1,
                        max_concurrency: Some(u32::MAX),
                    }]);
                }),
                "customQueues[0].maxConcurrency.tasks",
            ),
            recovery("recovery.claimedStaleThresholdMs", |c| {
                c.claimed_stale_threshold_ms = u64::MAX
            }),
            recovery("recovery.runningStaleThresholdMs", |c| {
                c.running_stale_threshold_ms = u64::MAX
            }),
            recovery("recovery.finalizingStaleThresholdMs", |c| {
                c.finalizing_stale_threshold_ms = u64::MAX
            }),
            recovery("recovery.crashedWorkerRecoveryGraceMs", |c| {
                c.crashed_worker_recovery_grace_ms = u64::MAX
            }),
            recovery("recovery.finalizationQuarantineAfterAttempts", |c| {
                c.finalization_quarantine_after_attempts = u32::MAX;
            }),
            recovery("recovery.workflowRecoveryFailureLimit", |c| {
                c.workflow_recovery_failure_limit = u32::MAX
            }),
            recovery("recovery.orphanTaskAuditIntervalMs", |c| {
                c.orphan_task_audit_interval_ms = u64::MAX
            }),
            recovery("recovery.runnerHeartbeatIntervalMs", |c| {
                c.runner_heartbeat_interval_ms = u64::MAX
            }),
            recovery("recovery.claimerHeartbeatIntervalMs", |c| {
                c.claimer_heartbeat_interval_ms = u64::MAX
            }),
            recovery("recovery.workerStateSnapshotIntervalMs", |c| {
                c.worker_state_snapshot_interval_ms = u64::MAX
            }),
            retention("retention.workerStateRetention.hours", |c| {
                c.worker_state_retention_hours = Some(u32::MAX)
            }),
            retention("retention.terminalRecordRetention.hours", |c| {
                c.terminal_record_retention_hours = Some(u32::MAX)
            }),
            retention("retention.historyLeafHorizonDays", |c| {
                c.history_leaf_horizon_days = u32::MAX
            }),
            retention("retention.heartbeatLeafHorizonHours", |c| {
                c.heartbeat_leaf_horizon_hours = u32::MAX
            }),
            retention(r#"retention.queueRetention["bulk"].days"#, |c| {
                c.queue_retention
                    .insert("bulk".to_owned(), Some(chrono::Duration::hours(36)));
            }),
            retention("retention.partitionMaintenanceIntervalSeconds", |c| {
                c.partition_maintenance_interval_seconds = u64::MAX;
            }),
            retention("retention.retentionSweepIntervalSeconds", |c| {
                c.retention_sweep_interval_seconds = u64::MAX
            }),
            retention("retention.retentionDeleteBatchSize", |c| {
                c.retention_delete_batch_size = u32::MAX
            }),
            resilience("resilience.databaseRetryInitialMs", |c| {
                c.database_retry_initial_ms = u64::MAX
            }),
            resilience("resilience.databaseRetryMaxMs", |c| {
                c.database_retry_max_ms = u64::MAX
            }),
            resilience("resilience.databaseRetryMaxAttempts.attempts", |c| {
                c.database_retry_max_attempts = u64::MAX
            }),
            (
                Box::new(|config: &mut AppConfig| config.payload.reject_bytes = Some(0)),
                "payload.rejectBytes.bytes",
            ),
        ]
    }

    /// Each value outside its manifest bound is refused with its path as the
    /// TypeScript config writes it and the bound of its field, never the
    /// value. The config is changed after its check, as a value that the
    /// config rules accept and the manifest bound refuses would be.
    #[test]
    fn a_value_outside_its_manifest_bound_is_refused_with_its_path_and_no_value() {
        let cases: Vec<(ConfigChange, &str)> = vec![
            (
                Box::new(|config: &mut AppConfig| config.prefetch_buffer = u32::MAX),
                "claim.prefetchBuffer",
            ),
            (
                Box::new(|config: &mut AppConfig| config.claim_lease_ms = Some(0)),
                "claim.leaseMs",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.cluster_wide_cap =
                        horsies_core::core::ClusterCapSetting::ClusterCap { tasks: u32::MAX }
                }),
                "clusterWideCap.tasks",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.queue_mode = QueueMode::Custom;
                    config.custom_queues = Some(vec![horsies_core::CustomQueueConfig {
                        name: "critical".to_owned(),
                        priority: 4_000_000,
                        max_concurrency: None,
                    }]);
                }),
                "customQueues[0].priority",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.recovery.check_interval_ms = 4_000_000_000_000;
                }),
                "recovery.checkIntervalMs",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.retention.retention_periods = vec![chrono::Duration::hours(36)];
                }),
                "retention.retentionPeriods[0].days",
            ),
            (
                Box::new(|config: &mut AppConfig| {
                    config.resilience.notify_poll_interval_ms = 4_000_000_000_000;
                }),
                "resilience.notifyPollIntervalMs",
            ),
            (
                Box::new(|config: &mut AppConfig| config.payload.warn_bytes = Some(0)),
                "payload.warnBytes.bytes",
            ),
        ];
        let cases = cases
            .into_iter()
            .chain(more_bound_cases())
            .collect::<Vec<_>>();
        for (change, path) in cases {
            let (mut config, producer) = read(&config_text(""));
            change(&mut config);
            let refused = config_sections(&config, &producer).expect_err(path);
            assert_eq!(refused.path, path);
            assert!(refused.min <= refused.max, "{refused:?}");
        }
    }

    /// The profile JSON of a recycle of `after` and `above`, in the config
    /// text's forms.
    fn recycle_text(after_key: &str, after: &str, above: &str) -> String {
        format!(r#"{{"{after_key}": {after}, "above": {above}}}"#)
    }

    /// The four recycle forms: each task limit with each memory limit.
    fn recycle_forms() -> [(&'static str, &'static str); 4] {
        [
            (
                r#"{"kind": "no_task_limit"}"#,
                r#"{"kind": "no_memory_limit"}"#,
            ),
            (
                r#"{"kind": "no_task_limit"}"#,
                r#"{"kind": "memory_limit", "mb": 512}"#,
            ),
            (
                r#"{"kind": "task_limit", "tasks": 7}"#,
                r#"{"kind": "no_memory_limit"}"#,
            ),
            (
                r#"{"kind": "task_limit", "tasks": 7}"#,
                r#"{"kind": "memory_limit", "mb": 512}"#,
            ),
        ]
    }

    /// The task and memory limit that `form` of `recycle_forms` gives, as
    /// whether each one is set.
    fn is_limited(after: &wire::TaskLimit, above: &wire::MemoryLimit) -> (bool, bool) {
        (
            matches!(after, wire::TaskLimit::Tasks { .. }),
            matches!(above, wire::MemoryLimit::Megabytes { .. }),
        )
    }

    #[test]
    fn each_recycle_form_of_an_isolated_child_is_kept() {
        for (after, above) in recycle_forms() {
            let recycle = recycle_text("after", after, above);
            let built = profile(
                &format!(
                    r#", "worker_profiles": {{"io": {{"isolated": {{"kind": "isolated_children", "count": 1, "recycle": {recycle}}}}}}}"#
                ),
                "io",
            );
            let wire::Isolated::Children { recycle, .. } = built.isolated else {
                panic!("the profile has isolated children");
            };
            assert_eq!(
                is_limited(&recycle.after, &recycle.above),
                (
                    after.contains("\"task_limit\""),
                    above.contains("\"memory_limit\"")
                ),
                "{after} {above}"
            );
        }
    }

    #[test]
    fn each_recycle_form_of_a_shared_async_child_is_kept() {
        for (after, above) in recycle_forms() {
            let recycle = recycle_text("after_per_seat", after, above);
            let built = profile(
                &format!(
                    r#", "worker_profiles": {{"io": {{"isolated": {{"kind": "no_isolated_children"}}, "shared_async": {{"kind": "shared_async_children", "count": 1, "seats": 2, "recycle": {recycle}}}}}}}"#
                ),
                "io",
            );
            let wire::SharedAsync::Children { recycle, .. } = built.shared_async else {
                panic!("the profile has shared async children");
            };
            assert_eq!(
                is_limited(&recycle.after_per_seat, &recycle.above),
                (
                    after.contains("\"task_limit\""),
                    above.contains("\"memory_limit\"")
                ),
                "{after} {above}"
            );
        }
    }

    #[test]
    fn each_recycle_form_of_a_shared_thread_child_is_kept() {
        for (after, above) in recycle_forms() {
            let recycle = recycle_text("after_per_seat", after, above);
            let built = profile(
                &format!(
                    r#", "worker_profiles": {{"cpu": {{"isolated": {{"kind": "no_isolated_children"}}, "shared_thread": {{"kind": "shared_thread_children", "count": 1, "seats": 2, "recycle": {recycle}}}}}}}"#
                ),
                "cpu",
            );
            let wire::SharedThread::Children { recycle, .. } = built.shared_thread else {
                panic!("the profile has shared thread children");
            };
            assert_eq!(
                is_limited(&recycle.after_per_seat, &recycle.above),
                (
                    after.contains("\"task_limit\""),
                    above.contains("\"memory_limit\"")
                ),
                "{after} {above}"
            );
        }
    }

    #[test]
    fn each_profile_kind_is_with_children_where_it_is_given_and_without_where_it_is_not() {
        let built = sections(
            r#", "worker_profiles": {
                "only_isolated": {"isolated": {"kind": "isolated_children", "count": 1}},
                "only_async": {"isolated": {"kind": "no_isolated_children"}, "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 2}},
                "only_thread": {"isolated": {"kind": "no_isolated_children"}, "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 2}}
            }"#,
        )
        .expect("the sections are inside their bounds")
        .profiles;
        let kinds = |name: &str| {
            let profile = built.get(name).expect("the profile is in the manifest");
            (
                matches!(profile.isolated, wire::Isolated::Children { .. }),
                matches!(profile.shared_async, wire::SharedAsync::Children { .. }),
                matches!(profile.shared_thread, wire::SharedThread::Children { .. }),
            )
        };
        assert_eq!(kinds("only_isolated"), (true, false, false));
        assert_eq!(kinds("only_async"), (false, true, false));
        assert_eq!(kinds("only_thread"), (false, false, true));
    }

    #[test]
    fn no_retry_attempt_limit_is_its_own_variant() {
        let (mut config, producer) = read(&config_text(""));
        config.resilience.database_retry_max_attempts = 0;
        let built = config_sections(&config, &producer).expect("inside every bound");
        assert_eq!(
            built.resilience.database_retry_max_attempts,
            wire::RetryAttemptLimit::NoLimit
        );
    }

    #[test]
    fn each_kind_of_field_has_its_typescript_path() {
        let cases = [
            (ConfigField::PrefetchBuffer, "claim.prefetchBuffer"),
            (ConfigField::ClaimLeaseMs, "claim.leaseMs"),
            (ConfigField::MaxClaimRenewAgeMs, "maxClaimRenewAgeMs"),
            (ConfigField::ClusterWideCap, "clusterWideCap.tasks"),
            (
                ConfigField::CustomQueue {
                    position: 2,
                    field: CustomQueueField::MaxConcurrency,
                },
                "customQueues[2].maxConcurrency.tasks",
            ),
            (
                ConfigField::Recovery(RecoveryField::FinalizationQuarantineAfterAttempts),
                "recovery.finalizationQuarantineAfterAttempts",
            ),
            (
                ConfigField::Retention(RetentionField::QueueRetention {
                    queue: "bulk \"low\"".to_owned(),
                }),
                r#"retention.queueRetention["bulk \"low\""].days"#,
            ),
            (
                ConfigField::Retention(RetentionField::PartitionMaintenanceIntervalSeconds),
                "retention.partitionMaintenanceIntervalSeconds",
            ),
            (
                ConfigField::Resilience(ResilienceField::DatabaseRetryMaxAttempts),
                "resilience.databaseRetryMaxAttempts.attempts",
            ),
            (
                ConfigField::Payload(PayloadField::RejectBytes),
                "payload.rejectBytes.bytes",
            ),
        ];
        for (field, path) in cases {
            assert_eq!(typescript_path(&field), path);
        }
    }

    #[test]
    fn the_default_sections_are_inside_every_bound() {
        let built = sections("").expect("the core defaults fit the manifest");
        assert_eq!(built.claim.queues, wire::Queues::Default);
        assert_eq!(built.claim.cluster_wide_cap, wire::ClusterWideCap::NoLimit);
    }
}
