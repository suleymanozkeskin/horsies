//! The app's configuration.
//!
//! `horsies-core` holds the one serde schema of the configuration, which the
//! Python client reads too, and its validation rules. This module names the
//! types a Rust app builds an [`AppConfig`] from.

pub use horsies_core::core::config::bounded::{BoundedU32, OutOfBounds};
pub use horsies_core::core::config::worker_profile::{
    ClaimLimits, DrainTimeout, DrainTimeoutRefused, ExecutorThreadCount, ExecutorThreadsSetting,
    IsolatedChildCount, IsolatedChildren, IsolatedPool, IsolatedRecycle, IsolatedSetting,
    MemoryLimitSetting, MemoryMb, ParentPool, PoolConnectionCount, PoolMinSize, PoolSize,
    PoolSizeSetting, SharedAsyncChildCount, SharedAsyncChildren, SharedAsyncSeats,
    SharedAsyncSetting, SharedPool, SharedRecycle, SharedThreadChildCount, SharedThreadChildren,
    SharedThreadSeats, SharedThreadSetting, TaskCount, TaskLimitSetting,
};
pub use horsies_core::core::config::{
    ChildPreload, KeepaliveProbes, KeepaliveSeconds, ParentCpu, TcpKeepalivesConfig,
    TcpUserTimeoutConfig, TcpUserTimeoutMs, WorkerProfile,
};
pub use horsies_core::manifest::PoolRecycle;
pub use horsies_core::{
    mask_database_url, AppConfig, AppConfigError, CronEnumTerm, CronNumericTerm, CronOrdinal,
    CronSchedule, CustomQueueConfig, CustomQueueConfigError, DailySchedule, DaySelector,
    HourlySchedule, IntervalSchedule, Month, MonthlySchedule, PausedAgeRefused, PausedWorkflowAge,
    PayloadPolicy, PostgresConfig, PostgresConfigError, QueueMode, RecoveryConfig,
    RecoveryConfigError, ResilienceConfigError, RetentionConfig, RetentionConfigError,
    ScheduleConfig, SchedulePattern, TaskSchedule, Weekday, WeeklySchedule, WorkerResilienceConfig,
};
