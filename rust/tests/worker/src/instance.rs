//! The app instances of the end-to-end tests and their worker profiles
//! (Python `tests/e2e/tasks/instance*.py`, `profiles.py`).
//!
//! A test builds the app of one instance in its own process, as a producer,
//! and starts the test binary with the same instance as its worker or
//! scheduler. The binary reads the instance and the database from
//! [`INSTANCE_ENV`] and [`DATABASE_URL_ENV`]; its children inherit both.

use std::collections::BTreeMap;
use std::time::Duration;

use horsies::{
    AppConfig, ChildPreload, ClaimLimits, CronEnumTerm, CronNumericTerm, CronSchedule,
    CustomQueueConfig, DaySelector, DrainTimeout, IntervalSchedule, IsolatedChildCount,
    IsolatedChildren, IsolatedRecycle, IsolatedSetting, QueueMode, RecoveryConfig, ScheduleConfig,
    SchedulePattern, SharedAsyncChildCount, SharedAsyncChildren, SharedAsyncSeats,
    SharedAsyncSetting, SharedThreadChildCount, SharedThreadChildren, SharedThreadSeats,
    SharedThreadSetting, TaskCount, TaskLimitSetting, TaskSchedule, WorkerProfile,
};

/// The database URL of the worker binary.
pub const DATABASE_URL_ENV: &str = "HORSIES_RUST_E2E_DATABASE_URL";

/// The session database URL of the worker binary; set only through a
/// PgBouncer transaction pool.
pub const SESSION_DATABASE_URL_ENV: &str = "HORSIES_RUST_E2E_SESSION_DATABASE_URL";

/// The instance of the worker binary: [`Instance::name`].
pub const INSTANCE_ENV: &str = "HORSIES_RUST_E2E_INSTANCE";

/// The broker pool of every instance (Python `pool_size=5, max_overflow=5`).
const POOL_SIZE: u32 = 5;
const POOL_OVERFLOW: u32 = 5;

/// The cluster-wide cap of [`Instance::ClusterCap`].
pub const CLUSTER_WIDE_CAP: u32 = 2;

/// The prefetch buffer and the claim lease of [`Instance::SoftCap`].
pub const SOFT_CAP_PREFETCH: u32 = 2;
pub const SOFT_CAP_CLAIM_LEASE_MS: u32 = 2_000;

/// The custom queues of [`Instance::CustomQueues`]: name, priority, cap.
pub const CUSTOM_QUEUES: [(&str, u32, u32); 3] =
    [("high", 1, 5), ("normal", 50, 10), ("low", 100, 20)];

/// How long a stopping worker of a test profile waits for its tasks.
pub const TEST_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// The interval of the schedules of [`Instance::Scheduler`].
pub const SCHEDULE_INTERVAL_SECONDS: u32 = 2;

/// The `input` of the schedule with input.
pub const SCHEDULED_VALUE: i64 = 42;

/// The schedule names of [`Instance::Scheduler`] (Python
/// `instance_scheduler.py`).
pub const SCHEDULE_SIMPLE_INTERVAL: &str = "e2e_scheduled_simple_interval";
pub const SCHEDULE_DISABLED: &str = "e2e_disabled_schedule";
pub const SCHEDULE_WITH_ARGS: &str = "e2e_schedule_with_args";
pub const SCHEDULE_CATCH_UP: &str = "e2e_schedule_catch_up";
pub const SCHEDULE_CRON_EVERY_MINUTE: &str = "e2e_schedule_cron_every_minute";

/// One app instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Instance {
    /// Default queue mode (Python `instance.py`).
    Default,
    /// Custom queues `high`, `normal`, `low` with caps (Python
    /// `instance_custom.py`).
    CustomQueues,
    /// A cluster-wide cap of [`CLUSTER_WIDE_CAP`] (Python
    /// `instance_cluster_cap.py`).
    ClusterCap,
    /// Short heartbeats and stale thresholds, for crash recovery (Python
    /// `instance_recovery.py`).
    Recovery,
    /// A prefetch buffer with a short claim lease (Python
    /// `instance_softcap.py`).
    SoftCap,
    /// The schedules of the scheduler tests (Python
    /// `instance_scheduler.py`).
    Scheduler,
}

impl Instance {
    /// Every instance.
    pub const ALL: [Instance; 6] = [
        Instance::Default,
        Instance::CustomQueues,
        Instance::ClusterCap,
        Instance::Recovery,
        Instance::SoftCap,
        Instance::Scheduler,
    ];

    /// The value of [`INSTANCE_ENV`].
    pub const fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::CustomQueues => "custom_queues",
            Self::ClusterCap => "cluster_cap",
            Self::Recovery => "recovery",
            Self::SoftCap => "soft_cap",
            Self::Scheduler => "scheduler",
        }
    }

    /// The instance of `name`.
    pub fn parse(name: &str) -> Result<Instance, String> {
        Self::ALL
            .into_iter()
            .find(|instance| instance.name() == name)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.iter().map(|instance| instance.name()).collect();
                format!("{INSTANCE_ENV}={name:?} is not one of {names:?}")
            })
    }

    /// The instance of [`INSTANCE_ENV`].
    pub fn from_env() -> Result<Instance, String> {
        let name = std::env::var(INSTANCE_ENV).map_err(|_| format!("{INSTANCE_ENV} is not set"))?;
        Self::parse(&name)
    }

    /// The queue that the readiness tasks and the tasks without a queue of
    /// their own use.
    pub const fn ready_queue(self) -> &'static str {
        match self {
            Self::CustomQueues => "high",
            Self::Default | Self::ClusterCap | Self::Recovery | Self::SoftCap | Self::Scheduler => {
                "default"
            }
        }
    }

    /// The configuration of the instance on `connection`.
    pub fn config(self, connection: &Connection) -> AppConfig {
        let mut config = AppConfig::for_database_url(connection.database_url.clone());
        config.broker.pool_size = POOL_SIZE;
        config.broker.max_overflow = POOL_OVERFLOW;
        if let Some(session) = &connection.session_database_url {
            config.broker.session_database_url = Some(session.clone());
            config.broker.pgbouncer_transaction_mode = true;
        }
        config.worker_profiles = Profile::ALL
            .into_iter()
            .map(|profile| (profile.name().to_owned(), profile.worker_profile()))
            .collect::<BTreeMap<_, _>>();
        match self {
            Self::Default => {}
            Self::CustomQueues => {
                config.queue_mode = QueueMode::Custom;
                config.custom_queues = Some(
                    CUSTOM_QUEUES
                        .into_iter()
                        .map(|(name, priority, cap)| CustomQueueConfig {
                            name: name.to_owned(),
                            priority,
                            max_concurrency: Some(cap),
                        })
                        .collect(),
                );
            }
            Self::ClusterCap => config.cluster_wide_cap = Some(CLUSTER_WIDE_CAP),
            Self::Recovery => config.recovery = fast_recovery(),
            Self::SoftCap => {
                config.prefetch_buffer = SOFT_CAP_PREFETCH;
                config.claim_lease_ms = Some(SOFT_CAP_CLAIM_LEASE_MS);
                config.recovery = RecoveryConfig {
                    claimer_heartbeat_interval_ms: 1_000,
                    ..RecoveryConfig::default()
                };
            }
            Self::Scheduler => config.schedule = Some(schedules()),
        }
        config
    }
}

/// Python `instance_recovery.py`: 1 s heartbeats and checks, the smallest
/// stale thresholds that the validation admits, and a worker state
/// snapshot each second.
pub fn fast_recovery() -> RecoveryConfig {
    RecoveryConfig {
        runner_heartbeat_interval_ms: 1_000,
        claimer_heartbeat_interval_ms: 1_000,
        running_stale_threshold_ms: 6_000,
        claimed_stale_threshold_ms: 2_000,
        crashed_worker_recovery_grace_ms: 2_000,
        check_interval_ms: 1_000,
        worker_state_snapshot_interval_ms: 1_000,
        ..RecoveryConfig::default()
    }
}

/// The schedules of [`Instance::Scheduler`] (Python `instance_scheduler.py`).
fn schedules() -> ScheduleConfig {
    let no_input = || serde_json::json!({});
    let interval = |seconds: u32| {
        SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(seconds),
            ..IntervalSchedule::default()
        })
    };
    ScheduleConfig::new(vec![
        TaskSchedule::new(
            SCHEDULE_SIMPLE_INTERVAL,
            "e2e_scheduled_simple",
            interval(SCHEDULE_INTERVAL_SECONDS),
        )
        .input(no_input())
        .timezone("UTC")
        .catch_up_missed(false),
        TaskSchedule::new(SCHEDULE_DISABLED, "e2e_scheduled_simple", interval(1))
            .input(no_input())
            .enabled(false),
        TaskSchedule::new(
            SCHEDULE_WITH_ARGS,
            "e2e_scheduled_with_args",
            interval(SCHEDULE_INTERVAL_SECONDS),
        )
        .input(serde_json::json!({ "value": SCHEDULED_VALUE })),
        TaskSchedule::new(SCHEDULE_CATCH_UP, "e2e_catch_up_task", interval(1))
            .input(no_input())
            .catch_up_missed(true),
        TaskSchedule::new(
            SCHEDULE_CRON_EVERY_MINUTE,
            "e2e_cron_task",
            SchedulePattern::Cron(every_minute()),
        )
        .input(no_input())
        .timezone("UTC")
        .catch_up_missed(false),
    ])
    .check_interval_seconds(1)
}

/// A cron schedule that fires at second 0 of every minute.
fn every_minute() -> CronSchedule {
    CronSchedule {
        minute: vec![CronNumericTerm::Every],
        hour: vec![CronNumericTerm::Every],
        month: vec![CronEnumTerm::Every],
        day: DaySelector::EveryDay,
    }
}

/// Where the apps of a test connect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    /// The URL of every pooled connection.
    pub database_url: String,
    /// The direct URL of the session statements when `database_url` is a
    /// PgBouncer transaction pool.
    pub session_database_url: Option<String>,
}

impl Connection {
    /// The connection that the test harness gave the worker binary.
    pub fn from_env() -> Result<Connection, String> {
        let database_url = std::env::var(DATABASE_URL_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("{DATABASE_URL_ENV} is not set"))?;
        let session_database_url = std::env::var(SESSION_DATABASE_URL_ENV)
            .ok()
            .filter(|value| !value.is_empty());
        Ok(Connection {
            database_url,
            session_database_url,
        })
    }

    /// The variables that give this connection to the worker binary.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![(DATABASE_URL_ENV, self.database_url.clone())];
        if let Some(session) = &self.session_database_url {
            env.push((SESSION_DATABASE_URL_ENV, session.clone()));
        }
        env
    }
}

/// Which task proves that a worker of a profile is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyClass {
    Isolated,
    SharedAsync,
    SharedThread,
}

/// The worker profiles that every instance defines (Python `profiles.py`).
/// Every profile starts its children with `exec`, the start path of a Rust
/// child, and drains in [`TEST_DRAIN_TIMEOUT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// `isolated-1`: one isolated child.
    Isolated1,
    /// `isolated-2`: two isolated children.
    Isolated2,
    /// `isolated-4`: four isolated children.
    Isolated4,
    /// `isolated-8`: eight isolated children, more than the cap of the
    /// `high` queue.
    Isolated8,
    /// `claim-batch-1`: one isolated child; one claim pass takes at most one
    /// task per queue.
    ClaimBatch1,
    /// `recycle-after-2-tasks`: two isolated children, each replaced after
    /// two tasks.
    RecycleAfter2,
    /// `shared-only-1x4`: one shared async child with 4 seats.
    SharedOnly1x4,
    /// `thread-only-1x4`: one shared thread child with 4 seats.
    ThreadOnly1x4,
    /// `split-isolated-1-shared-1x4`: one isolated child and one shared
    /// async child with 4 seats.
    SplitShared1x4,
    /// `split-isolated-1-thread-1x4`: one isolated child and one shared
    /// thread child with 4 seats.
    SplitThread1x4,
}

impl Profile {
    /// Every profile.
    pub const ALL: [Profile; 10] = [
        Profile::Isolated1,
        Profile::Isolated2,
        Profile::Isolated4,
        Profile::Isolated8,
        Profile::ClaimBatch1,
        Profile::RecycleAfter2,
        Profile::SharedOnly1x4,
        Profile::ThreadOnly1x4,
        Profile::SplitShared1x4,
        Profile::SplitThread1x4,
    ];

    /// The profile name of `worker --profile`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Isolated1 => "isolated-1",
            Self::Isolated2 => "isolated-2",
            Self::Isolated4 => "isolated-4",
            Self::Isolated8 => "isolated-8",
            Self::ClaimBatch1 => "claim-batch-1",
            Self::RecycleAfter2 => "recycle-after-2-tasks",
            Self::SharedOnly1x4 => "shared-only-1x4",
            Self::ThreadOnly1x4 => "thread-only-1x4",
            Self::SplitShared1x4 => "split-isolated-1-shared-1x4",
            Self::SplitThread1x4 => "split-isolated-1-thread-1x4",
        }
    }

    /// The class of the readiness task of a worker of this profile.
    pub const fn ready_class(self) -> ReadyClass {
        match self {
            Self::SharedOnly1x4 => ReadyClass::SharedAsync,
            Self::ThreadOnly1x4 => ReadyClass::SharedThread,
            Self::Isolated1
            | Self::Isolated2
            | Self::Isolated4
            | Self::Isolated8
            | Self::ClaimBatch1
            | Self::RecycleAfter2
            | Self::SplitShared1x4
            | Self::SplitThread1x4 => ReadyClass::Isolated,
        }
    }

    /// The worker settings of the profile.
    pub fn worker_profile(self) -> WorkerProfile {
        let base = WorkerProfile {
            isolated: IsolatedSetting::NoIsolatedChildren {},
            shared_async: SharedAsyncSetting::NoSharedAsyncChildren {},
            shared_thread: SharedThreadSetting::NoSharedThreadChildren {},
            preload: ChildPreload::NoPreload,
            drain_timeout: DrainTimeout::try_from(TEST_DRAIN_TIMEOUT)
                .expect("the test drain timeout is above 0 and below 24 hours"),
            ..WorkerProfile::default()
        };
        match self {
            Self::Isolated1 => with_isolated(base, 1),
            Self::Isolated2 => with_isolated(base, 2),
            Self::Isolated4 => with_isolated(base, 4),
            Self::Isolated8 => with_isolated(base, 8),
            Self::ClaimBatch1 => WorkerProfile {
                claim: ClaimLimits {
                    per_queue_per_pass: TaskLimitSetting::TaskLimit {
                        tasks: TaskCount::from_constant(1),
                    },
                    ..ClaimLimits::default()
                },
                ..with_isolated(base, 1)
            },
            Self::RecycleAfter2 => WorkerProfile {
                isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren {
                    recycle: IsolatedRecycle {
                        after: TaskLimitSetting::TaskLimit {
                            tasks: TaskCount::from_constant(2),
                        },
                        ..IsolatedRecycle::default()
                    },
                    ..IsolatedChildren::new(IsolatedChildCount::from_constant(2))
                }),
                ..base
            },
            Self::SharedOnly1x4 => with_shared_async(base),
            Self::ThreadOnly1x4 => with_shared_thread(base),
            Self::SplitShared1x4 => with_shared_async(with_isolated(base, 1)),
            Self::SplitThread1x4 => with_shared_thread(with_isolated(base, 1)),
        }
    }
}

fn with_isolated(profile: WorkerProfile, count: u32) -> WorkerProfile {
    WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(
            IsolatedChildCount::from_constant(count),
        )),
        ..profile
    }
}

/// One shared async child with 4 seats.
fn with_shared_async(profile: WorkerProfile) -> WorkerProfile {
    WorkerProfile {
        shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
            SharedAsyncChildCount::from_constant(1),
            SharedAsyncSeats::from_constant(4),
        )),
        ..profile
    }
}

/// One shared thread child with 4 seats.
fn with_shared_thread(profile: WorkerProfile) -> WorkerProfile {
    WorkerProfile {
        shared_thread: SharedThreadSetting::SharedThreadChildren(SharedThreadChildren::new(
            SharedThreadChildCount::from_constant(1),
            SharedThreadSeats::from_constant(4),
        )),
        ..profile
    }
}
