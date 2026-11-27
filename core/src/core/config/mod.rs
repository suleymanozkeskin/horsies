pub mod app;
pub mod bounded;
pub mod broker;
pub mod child_sizing;
pub mod payload;
pub mod queue;
pub mod recovery;
pub mod resilience;
pub mod retention;
pub mod schedule;
pub mod socket;
pub mod worker_profile;

pub use app::{mask_database_url, AppConfig, AppConfigError, ClusterCapSetting};
pub use broker::{PostgresConfig, PostgresConfigError, SessionDatabase};
pub use payload::PayloadPolicy;
pub use queue::{CustomQueueConfig, CustomQueueConfigError, QueueMode};
pub use recovery::{RecoveryConfig, RecoveryConfigError};
pub use resilience::{ResilienceConfigError, WorkerResilienceConfig};
pub use retention::{
    EnqueueRetention, PausedAgeRefused, PausedWorkflowAge, RetentionChoice,
    RetentionClassKeyRefused, RetentionConfig, RetentionConfigError,
};
pub use schedule::{
    CronEnumTerm, CronNumericTerm, CronOrdinal, CronSchedule, DailySchedule, DaySelector,
    HourlySchedule, IntervalSchedule, Month, MonthlySchedule, ScheduleConfig, SchedulePattern,
    TaskSchedule, Weekday, WeeklySchedule,
};
pub use socket::input::{SocketValueRefused, TcpKeepalivesConfig, TcpUserTimeoutConfig};
pub use worker_profile::{describe_worker_profile, ChildPreload, ParentCpu, WorkerProfile};

/// The body of a unit variant of a tagged config enum: the members after
/// the tag, which must be none. serde reads a unit variant of an internally
/// tagged enum and skips its other members, also under
/// `deny_unknown_fields`. Each such variant takes this function as
/// `deserialize_with`, and its serialized form does not change:
///
/// - [`CronNumericTerm::Every`], [`CronEnumTerm::Every`] and
///   [`DaySelector::EveryDay`]: Python's `CronEvery` and `EveryDay` refuse a
///   member (`extra='forbid'`).
/// - [`TcpKeepalivesConfig::Off`], [`TcpUserTimeoutConfig::SystemDefault`],
///   [`SessionDatabase::SameAsDatabaseUrl`],
///   [`crate::manifest::PoolRecycle::Never`] and
///   [`ClusterCapSetting::NoClusterCap`]: Rust forms of Python's
///   `PostgresConfig` and `AppConfig` unions, which refuse an unknown key.
///
/// Errors: serde's unknown-field error, which names the first member.
pub(crate) fn no_member_after_the_tag<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    /// A body with no members.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NoMembers {}

    NoMembers::deserialize(deserializer).map(|NoMembers {}| ())
}
