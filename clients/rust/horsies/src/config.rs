//! The app's configuration, and its resolution into the core's manifests.
//!
//! `horsies-core` holds the one serde schema of the configuration, which the
//! Python client reads too, and its validation rules. This module names the
//! types a Rust app builds an [`AppConfig`] from, adds the rules that only a
//! Rust app has, and resolves a validated config into the manifest that the
//! core reads, as the Python client does (`clients/python/horsies/core/manifest.py`).

use std::collections::BTreeMap;

use crate::{HorsiesError, HorsiesErrorCode};

mod scheduler_manifest;
mod worker_manifest;

pub(crate) use scheduler_manifest::{schedule_input_text, scheduler_manifest};
pub(crate) use worker_manifest::{declared_codes, rust_child_launch, worker_manifest};

pub use horsies_core::core::config::worker_profile::input::{
    ClaimLimits, ExecutorThreadsSetting, IsolatedChildren, IsolatedPool, IsolatedRecycle,
    IsolatedSetting, MemoryLimitSetting, ParentPool, PoolSize, PoolSizeSetting,
    ProfileValueRefused, SharedAsyncChildren, SharedAsyncSetting, SharedPool, SharedRecycle,
    SharedThreadChildren, SharedThreadSetting, TaskLimitSetting, WorkerProfile,
};
pub use horsies_core::core::config::{
    ChildPreload, ClusterCapSetting, ParentCpu, SessionDatabase, SocketValueRefused,
    TcpKeepalivesConfig, TcpUserTimeoutConfig,
};
pub use horsies_core::manifest::PoolRecycle;
pub use horsies_core::{
    mask_database_url, AppConfig, AppConfigError, CronEnumTerm, CronNumericTerm, CronOrdinal,
    CronSchedule, CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector,
    HourlySchedule, IntervalSchedule, JsonTextRefused, Month, MonthlySchedule, PausedAgeRefused,
    PausedWorkflowAge, PayloadPolicy, PostgresConfig, PostgresConfigError, QueueMode,
    RecoveryConfig, RecoveryConfigError, ResilienceConfigError, RetentionConfig,
    RetentionConfigError, ScheduleConfig, SchedulePattern, TaskSchedule, Weekday, WeeklySchedule,
    WorkerResilienceConfig,
};

/// The refusals of the rules that only a Rust app has, in profile name
/// order. Pure.
///
/// - A worker profile asks for no preload parent. A preload parent imports
///   Python code once before it forks children; a Rust worker starts each
///   child with `exec` of its own binary, which holds every task. A profile
///   that does not set `preload` has the core's default,
///   `ChildPreload::NoPreload`; an explicit `Library` or `App` is refused.
pub(crate) fn rust_config_refusals(config: &AppConfig) -> Vec<HorsiesError> {
    preload_refusals(&config.worker_profiles)
}

fn preload_refusals(profiles: &BTreeMap<String, WorkerProfile>) -> Vec<HorsiesError> {
    profiles
        .iter()
        .filter_map(|(name, profile)| match profile.preload {
            ChildPreload::NoPreload => None,
            ChildPreload::Library | ChildPreload::App => {
                Some(preload_refusal(name, profile.preload))
            }
        })
        .collect()
}

fn preload_refusal(profile_name: &str, preload: ChildPreload) -> HorsiesError {
    HorsiesError::new(format!(
        "worker_profiles.{profile_name}.preload: '{}' is not available to a Rust worker",
        preload.as_str()
    ))
    .with_code(HorsiesErrorCode::ConfigInvalidWorkerProfile)
    .with_note(
        "a Rust worker starts each child with exec of its own binary, which holds every \
         task, so there is nothing to preload",
    )
    .with_help(
        "remove preload from this profile: the default is ChildPreload::NoPreload, the only \
         value that a Rust worker takes",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicit `Library` or `App` is refused with its path; an explicit
    /// `NoPreload` is not.
    #[test]
    fn a_profile_with_a_preload_is_refused_with_its_path() {
        let mut config = AppConfig::from_database_url("postgresql://db/horsies");
        for (name, preload) in [
            ("app", ChildPreload::App),
            ("library", ChildPreload::Library),
            ("plain", ChildPreload::NoPreload),
        ] {
            let profile = WorkerProfile {
                preload,
                ..WorkerProfile::default()
            };
            config.worker_profiles.insert(name.to_owned(), profile);
        }
        let refusals = rust_config_refusals(&config);
        let messages: Vec<&str> = refusals
            .iter()
            .map(|error| error.message.as_str())
            .collect();
        assert_eq!(refusals.len(), 2);
        assert!(messages[0].starts_with("worker_profiles.app.preload"));
        assert!(messages[1].starts_with("worker_profiles.library.preload"));
        assert!(refusals
            .iter()
            .all(|error| error.code == Some(HorsiesErrorCode::ConfigInvalidWorkerProfile)));
    }

    /// `Horsies::new` refuses a config with two profile values outside
    /// their bounds in one report that holds both refusals, each with its
    /// path and its bounds.
    #[test]
    fn horsies_new_refuses_every_profile_value_outside_its_bound_at_once() {
        let mut config = AppConfig::from_database_url("postgresql://db/horsies");
        let _previous = config.worker_profiles.insert(
            "web".to_owned(),
            WorkerProfile {
                isolated: IsolatedChildren::new(0).into(),
                shared_async: SharedAsyncChildren::new(1, 1001).into(),
                ..WorkerProfile::default()
            },
        );

        let refused = crate::Horsies::new(config).expect_err("two values are outside their bounds");

        let refusals = refused.refusals();
        assert_eq!(refusals.len(), 2, "{refused}");
        assert!(refusals
            .iter()
            .all(|refusal| refusal.code == Some(HorsiesErrorCode::ConfigInvalidWorkerProfile)));
        assert!(
            refusals[0].to_string().contains(
                "worker_profiles['web'].isolated.count: 0 is outside the bounds 1 to 1024"
            ),
            "{refused}",
        );
        assert!(
            refusals[1].to_string().contains(
                "worker_profiles['web'].shared_async.seats: 1001 is outside the bounds 1 to 1000"
            ),
            "{refused}",
        );
    }

    #[test]
    fn a_config_without_profiles_has_no_rust_refusal() {
        let config = AppConfig::from_database_url("postgresql://db/horsies");
        assert!(rust_config_refusals(&config).is_empty());
    }
}
