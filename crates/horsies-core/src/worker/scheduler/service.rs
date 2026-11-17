//! The scheduler service (Python `python/horsies/core/scheduler/service.py`,
//! `Scheduler`): the start, the tick, and the fire of each due schedule
//! through `horsies_fire_schedule` on the main core pool of the broker.
//!
//! A service is built from one of two sources: the schedule section of an
//! app config with the send facts that `Horsies::prepare_schedules`
//! resolved ([`SchedulerService::new`]), or a client's scheduler manifest,
//! whose send facts the client resolved
//! ([`SchedulerService::from_manifest`]). Both give the same started
//! scheduler: each enabled schedule with its send command.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broker::core_rows::StatementLog;
use crate::broker::postgres::SendCommand;
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::app::PreparedSchedules;
use crate::core::config::payload::{enforce_payload_policy, PayloadKind};
use crate::core::config::{AppConfig, ScheduleConfig, SchedulePattern, TaskSchedule};
use crate::db::CorePool;
use crate::scheduler_manifest::{ScheduleActivation, ScheduleSendManifest, SchedulerManifest};

use super::calculator::next_run_at;
use super::config_digest::config_digest;
use super::state::{
    self, DueSchedule, FiredSlot, ScheduleFire, ScheduleFireAnswer, ScheduleFireError, SlotOutcome,
};

/// Wall-clock target between missing-row existence checks while every state
/// row is present and initialized. The check guards rare conditions (startup
/// init failure, external row deletion); running it every tick reads the whole
/// schedule-state table each tick for an answer that changes only on those
/// rare events. Denominated in seconds, not ticks, so the worst-case dormancy
/// bound for an externally deleted row does not scale with
/// `check_interval_seconds`. While any row is missing or a re-init failed, the
/// check reruns every tick until a fully healthy pass.
const EXISTENCE_CHECK_INTERVAL_S: u32 = 60;

/// Cadence gate for the missing-row existence check.
///
/// Reproduces Python's countdown: a check runs when the countdown hits zero;
/// a healthy pass re-arms the full interval, an unhealthy pass re-checks on
/// the very next tick.
struct ExistenceCheckCadence {
    /// Ticks between checks while healthy, derived once from the tick length.
    interval_ticks: u32,
    /// Zero means the next tick runs the check.
    ticks_until_check: u32,
}

impl ExistenceCheckCadence {
    fn new(check_interval_seconds: u32) -> Self {
        let seconds = check_interval_seconds.max(1);
        let interval_ticks =
            ((f64::from(EXISTENCE_CHECK_INTERVAL_S) / f64::from(seconds)).round() as u32).max(1);
        Self {
            interval_ticks,
            ticks_until_check: 0,
        }
    }

    /// True when this tick must run the existence check.
    fn should_check(&self) -> bool {
        self.ticks_until_check == 0
    }

    /// Record a completed check's health and start the next countdown.
    fn record(&mut self, healthy: bool) {
        self.ticks_until_check = if healthy { self.interval_ticks } else { 1 };
    }

    /// Advance one tick.
    fn tick(&mut self) {
        self.ticks_until_check = self.ticks_until_check.saturating_sub(1);
    }
}

/// Resolve the effective priority for a queue, mirroring [`Horsies::effective_priority()`].
///
/// Priority resolution:
/// 1. Queue priority from [`CustomQueueConfig`] (if Custom mode and the queue has a config entry)
/// 2. Default priority (100)
fn resolve_queue_priority(app_config: &AppConfig, queue_name: &str) -> i32 {
    if let Some(ref queues) = app_config.custom_queues {
        if let Some(q) = queues.iter().find(|q| q.name == queue_name) {
            return q.priority as i32;
        }
    }
    100
}

/// One enabled schedule as the start and the tick read it: its name, its
/// pattern in its timezone, and its catch-up rule. Its send facts are in its
/// send command.
#[derive(Debug, Clone)]
struct EnabledSchedule {
    name: String,
    pattern: SchedulePattern,
    /// An IANA zone name that the core knows: the start checks it.
    timezone: String,
    catch_up_missed: bool,
    max_catch_up_runs: u32,
}

impl EnabledSchedule {
    /// The enabled schedule of a config entry, or `None` for a disabled
    /// one. Pure.
    fn of(schedule: &TaskSchedule) -> Option<Self> {
        match schedule.enabled {
            true => Some(Self {
                name: schedule.name.clone(),
                pattern: schedule.pattern.clone(),
                timezone: schedule.timezone.clone(),
                catch_up_missed: schedule.catch_up_missed,
                max_catch_up_runs: schedule.max_catch_up_runs,
            }),
            false => None,
        }
    }
}

/// The enabled schedules of a config, in config order. Pure.
fn enabled_schedules(schedules: &[TaskSchedule]) -> Vec<EnabledSchedule> {
    schedules.iter().filter_map(EnabledSchedule::of).collect()
}

/// Why the scheduler refused to start. Nothing is written before a refusal.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerStartError {
    /// The schedule section is invalid (pattern bounds, timezone, names).
    #[error("invalid schedule config: {0}")]
    InvalidScheduleConfig(String),
    /// An enabled schedule's pattern has no slot after the start time, so its
    /// `next_run_at` (NOT NULL) cannot be stored.
    #[error("schedule '{schedule}' has no run after {after}: its pattern never fires again")]
    NoNextSlot {
        schedule: String,
        after: DateTime<Utc>,
    },
    /// An enabled schedule has no send facts: the host did not build the
    /// prepared schedules with `Horsies::prepare_schedules` for this config.
    #[error(
        "schedule '{schedule}' has no prepared send facts; build them with \
         Horsies::prepare_schedules from the same config"
    )]
    NotPrepared { schedule: String },
    /// An enabled schedule's input is over `payload.reject_bytes`.
    #[error(
        "the input of schedule '{schedule}' is {size} bytes, over \
         payload.reject_bytes={limit}"
    )]
    PayloadTooLarge {
        schedule: String,
        size: usize,
        limit: u64,
    },
    /// The broker refused the send command of an enabled schedule (Python
    /// `ConfigurationError` with `CONFIG_INVALID_SCHEDULE`): its input or the
    /// task's options. Fix the schedule's input or the task's options.
    #[error("schedule '{schedule}' cannot be sent: {cause}")]
    CommandRefused {
        schedule: String,
        #[source]
        cause: BrokerError,
    },
    /// An enabled schedule of a manifest names a timezone that the core
    /// does not know. The client checked the name with its own zone
    /// database; give a name that the core's IANA database also has.
    #[error(
        "schedule '{schedule}' has the timezone '{timezone}', which is not an IANA zone name \
         that the core knows"
    )]
    UnknownTimezone { schedule: String, timezone: String },
    /// The client family check before the first schedule-state write
    /// failed: `BrokerError::ClientFamilyMismatch` when the database records
    /// another family or none (not retryable), or the error of the family
    /// read (`BrokerError::is_retryable` tells whether a later start may
    /// pass).
    #[error("the client family check before the first schedule write failed: {0}")]
    ClientFamily(#[source] BrokerError),
}

/// Checks that run before the scheduler reads or writes anything: the
/// schedule section is valid, and every enabled schedule has its prepared
/// send facts. Pure.
fn check_schedules_at_start(
    schedule_config: &ScheduleConfig,
    prepared: &PreparedSchedules,
) -> Result<(), SchedulerStartError> {
    schedule_config
        .validate()
        .map_err(SchedulerStartError::InvalidScheduleConfig)?;
    match schedule_config
        .schedules
        .iter()
        .filter(|s| s.enabled)
        .find(|s| !prepared.contains_key(&s.name))
    {
        Some(schedule) => Err(SchedulerStartError::NotPrepared {
            schedule: schedule.name.clone(),
        }),
        None => Ok(()),
    }
}

/// Refuse an enabled schedule whose pattern has no slot after
/// `database_now`, the database time of the start read: its `next_run_at`
/// (NOT NULL) cannot be stored. Pure.
fn check_next_slots(
    schedules: &[EnabledSchedule],
    database_now: DateTime<Utc>,
) -> Result<(), SchedulerStartError> {
    match schedules
        .iter()
        .find(|s| next_run_at(&s.pattern, database_now, &s.timezone).is_none())
    {
        Some(schedule) => Err(SchedulerStartError::NoNextSlot {
            schedule: schedule.name.clone(),
            after: database_now,
        }),
        None => Ok(()),
    }
}

/// Asks a scheduler to stop at its next tick boundary. Idempotent, returns
/// at once, and may be called from any thread. A fire in flight runs to its
/// end. A request before the start stops the scheduler after its start.
#[derive(Clone, Debug, Default)]
pub struct SchedulerStop {
    token: CancellationToken,
}

impl SchedulerStop {
    /// A stop request that nothing has asked for yet, to give to
    /// [`SchedulerService::from_manifest`].
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request_stop(&self) {
        self.token.cancel();
    }

    /// Whether a stop was asked for.
    pub fn is_requested(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Wait until a stop is asked for. Ends at once when it was asked for
    /// before.
    pub async fn requested(&self) {
        self.token.cancelled().await;
    }
}

/// The scheduler of one schedule config, before its start.
///
/// - [`Self::start`]: the start checks, the client family check and the
///   state start. A refusal writes nothing.
/// - [`ActiveScheduler::tick`]: one tick: the due read, the existence heal
///   on its cadence, one fire per due schedule.
/// - [`Self::run`]: the start, then one tick at once and one per
///   `check_interval_seconds` until a stop request ([`SchedulerStop`]).
///
/// Every statement runs on the main core pool of the broker.
pub struct SchedulerService {
    broker: Arc<PostgresBroker>,
    source: ScheduleSource,
    stop: CancellationToken,
}

/// Where the schedules of a service and their send facts come from.
enum ScheduleSource {
    /// The schedule section of an app config. `prepared` comes from
    /// `Horsies::prepare_schedules`, which checks the schedules against the
    /// task registry (task registered, queue, input type, input size) and
    /// resolves each fired slot's queue and task options. The `app_config`
    /// gives each slot's queue priority, retention and payload policy.
    AppConfig {
        schedule_config: ScheduleConfig,
        app_config: AppConfig,
        prepared: PreparedSchedules,
    },
    /// A client's scheduler manifest: every send fact is resolved.
    Manifest(SchedulerManifest),
}

/// What the start checks leave before any statement.
enum StartPlan {
    /// The master switch is off or no schedule is configured.
    Idle,
    Active(ActivePlan),
}

/// The schedules to start.
struct ActivePlan {
    /// The name of every configured schedule, disabled ones included: a
    /// state row of another name is logged as not in this config.
    configured_names: HashSet<String>,
    /// The enabled schedules, in config order.
    schedules: Vec<EnabledSchedule>,
    /// The send command of each enabled schedule, by schedule name.
    commands: HashMap<String, SendCommand>,
    /// The wait between two ticks, 1 to 60 seconds.
    check_interval_seconds: u32,
}

/// What a scheduler start leaves.
pub enum SchedulerStarted {
    /// The master switch is off or no schedule is configured: there is
    /// nothing to tick, and the start wrote nothing.
    Idle,
    /// The state rows are started; the scheduler ticks.
    Active(ActiveScheduler),
}

/// A started scheduler.
pub struct ActiveScheduler {
    broker: Arc<PostgresBroker>,
    /// The enabled schedules, in config order.
    schedules: Vec<EnabledSchedule>,
    /// The send command of each enabled schedule, built at the start.
    commands: HashMap<String, SendCommand>,
    /// The wait between two ticks, 1 to 60 seconds.
    check_interval_seconds: u32,
    existence_cadence: ExistenceCheckCadence,
    stop: CancellationToken,
}

/// Why one tick fired nothing. The failure of one due schedule is logged
/// and does not end the tick.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerTickError {
    /// The core pools of the broker could not open again after a close, so
    /// no statement ran in this tick. The next tick tries again.
    #[error("the scheduler pools could not open: {0}")]
    PoolsUnavailable(#[source] BrokerError),
    /// The due read failed, so no schedule fired in this tick. The next tick
    /// reads again.
    #[error("the due schedule read failed: {0}")]
    DueRead(#[source] BrokerError),
}

impl SchedulerService {
    /// A scheduler of `schedule_config`. Starts nothing and connects to
    /// nothing.
    pub fn new(
        broker: Arc<PostgresBroker>,
        schedule_config: ScheduleConfig,
        app_config: AppConfig,
        prepared: PreparedSchedules,
    ) -> Self {
        Self {
            broker,
            source: ScheduleSource::AppConfig {
                schedule_config,
                app_config,
                prepared,
            },
            stop: CancellationToken::new(),
        }
    }

    /// A scheduler of a client's manifest
    /// ([`crate::scheduler_manifest::parse_scheduler_manifest`]) that
    /// `stop` stops. Starts nothing and connects to nothing.
    pub fn from_manifest(
        broker: Arc<PostgresBroker>,
        manifest: SchedulerManifest,
        stop: &SchedulerStop,
    ) -> Self {
        Self {
            broker,
            source: ScheduleSource::Manifest(manifest),
            stop: stop.token.clone(),
        }
    }

    /// The stop request of this scheduler, for any later stage of it.
    pub fn stop_handle(&self) -> SchedulerStop {
        SchedulerStop {
            token: self.stop.clone(),
        }
    }

    /// Start the scheduler:
    /// 1. With the master switch off or no schedule: [`SchedulerStarted::Idle`].
    /// 2. The start checks of the source, with no database call: an app
    ///    config's schedule section is valid and every enabled schedule has
    ///    its send facts; a manifest's enabled schedules name zones that the
    ///    core knows. Then the send command of each enabled schedule is
    ///    built.
    /// 3. The client family check of the broker.
    /// 4. The state start: one read with the database time; a refusal of an
    ///    enabled pattern with no slot after that time; one write of the new
    ///    and changed rows. A failed read or write is logged and the start
    ///    goes on: the tick's existence heal creates the missing rows.
    ///
    /// Failure exits: [`SchedulerStartError`]. A refusal writes nothing.
    pub async fn start(self) -> Result<SchedulerStarted, SchedulerStartError> {
        let Self {
            broker,
            source,
            stop,
        } = self;
        let plan = match source {
            ScheduleSource::AppConfig {
                schedule_config,
                app_config,
                prepared,
            } => app_config_plan(&broker, &schedule_config, &app_config, &prepared)?,
            ScheduleSource::Manifest(manifest) => manifest_plan(manifest)?,
        };
        let plan = match plan {
            StartPlan::Idle => {
                tracing::info!("scheduler disabled or no schedules configured");
                return Ok(SchedulerStarted::Idle);
            }
            StartPlan::Active(plan) => plan,
        };
        // The state start is the first schedule-state write; the fire writes
        // the slot tasks without a second family check.
        broker
            .ensure_client_family_for_write()
            .await
            .map_err(SchedulerStartError::ClientFamily)?;
        match broker.core_pools_for_write() {
            Ok((open, _write)) => {
                initialize_schedules(
                    open.main(),
                    broker.statement_log(),
                    &plan.configured_names,
                    &plan.schedules,
                )
                .await?;
            }
            Err(error) => tracing::error!(
                %error,
                "the scheduler pools could not open at start: {error}; the existence heal \
                 creates missing rows",
            ),
        }
        let existence_cadence = ExistenceCheckCadence::new(plan.check_interval_seconds);
        tracing::info!("scheduler started");
        Ok(SchedulerStarted::Active(ActiveScheduler {
            broker,
            schedules: plan.schedules,
            commands: plan.commands,
            check_interval_seconds: plan.check_interval_seconds,
            existence_cadence,
            stop,
        }))
    }

    /// The start, then the tick loop until a stop request. Failure exits:
    /// those of [`Self::start`]; the loop logs each tick's failure and goes
    /// on.
    pub async fn run(self) -> Result<(), SchedulerStartError> {
        match self.start().await? {
            SchedulerStarted::Idle => Ok(()),
            SchedulerStarted::Active(active) => {
                active.run().await;
                Ok(())
            }
        }
    }
}

impl ActiveScheduler {
    /// The stop request of this scheduler.
    pub fn stop_handle(&self) -> SchedulerStop {
        SchedulerStop {
            token: self.stop.clone(),
        }
    }

    /// One tick: the due read, the existence heal on its cadence, then one
    /// fire per due schedule. A panic inside one schedule's fire is caught
    /// at that fire, logged with the schedule and the cause, and the tick
    /// goes on with the next schedule.
    ///
    /// Every statement of the tick runs on the main core pool of the broker;
    /// a close of the broker waits for a tick in flight.
    ///
    /// Failure exits: [`SchedulerTickError::PoolsUnavailable`],
    /// [`SchedulerTickError::DueRead`].
    pub async fn tick(&mut self) -> Result<(), SchedulerTickError> {
        let (open, _write) = self
            .broker
            .core_pools_for_write()
            .map_err(SchedulerTickError::PoolsUnavailable)?;
        check_and_enqueue(
            open.main(),
            self.broker.statement_log(),
            &self.schedules,
            &self.commands,
            &mut self.existence_cadence,
        )
        .await
        .map_err(SchedulerTickError::DueRead)
    }

    /// One tick at once, then one per `check_interval_seconds`, until a stop
    /// request. A stop request ends the wait between two ticks; a tick in
    /// flight runs to its end. A failed tick is logged and the next one
    /// runs.
    pub async fn run(mut self) {
        let check_interval = Duration::from_secs(u64::from(self.check_interval_seconds));
        while !self.stop.is_cancelled() {
            if let Err(error) = self.tick().await {
                tracing::error!(error = %error, "Error in scheduler loop: {error}");
            }
            tokio::select! {
                _ = self.stop.cancelled() => break,
                _ = tokio::time::sleep(check_interval) => {}
            }
        }
        tracing::info!("scheduler stopped");
    }
}

/// Spawn [`SchedulerService::run`] with `cancel` as its stop request.
///
/// The join handle yields the [`SchedulerStartError`] of a refused start;
/// the host exits non-zero. Multiple schedulers can coexist: each fire locks
/// its schedule's state row and compares its `next_run_at` with the one the
/// slots were computed from, so a second scheduler finds the row moved and
/// writes nothing. No advisory lock and no second connection.
pub fn spawn_scheduler(
    broker: Arc<PostgresBroker>,
    schedule_config: ScheduleConfig,
    app_config: AppConfig,
    prepared: PreparedSchedules,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), SchedulerStartError>> {
    let service = SchedulerService {
        broker,
        source: ScheduleSource::AppConfig {
            schedule_config,
            app_config,
            prepared,
        },
        stop: cancel,
    };
    tokio::spawn(service.run())
}

/// The start plan of an app config's schedule section: the start checks
/// ([`check_schedules_at_start`]) and the send commands
/// ([`build_commands`]). Does no I/O.
///
/// Errors: those of the two steps.
fn app_config_plan(
    broker: &PostgresBroker,
    schedule_config: &ScheduleConfig,
    app_config: &AppConfig,
    prepared: &PreparedSchedules,
) -> Result<StartPlan, SchedulerStartError> {
    if !schedule_config.enabled || schedule_config.schedules.is_empty() {
        return Ok(StartPlan::Idle);
    }
    check_schedules_at_start(schedule_config, prepared)?;
    let commands = build_commands(broker, schedule_config, app_config, prepared)?;
    Ok(StartPlan::Active(ActivePlan {
        configured_names: schedule_config
            .schedules
            .iter()
            .map(|schedule| schedule.name.clone())
            .collect(),
        schedules: enabled_schedules(&schedule_config.schedules),
        commands,
        check_interval_seconds: schedule_config.check_interval_seconds,
    }))
}

/// The start plan of a manifest, in Python's order (`Scheduler.start`): the
/// zone of every enabled schedule first, then the send command of each
/// (Python `_build_commands`). The manifest parser checked the rest.
/// Does no I/O.
///
/// Errors: [`SchedulerStartError::UnknownTimezone`] for the first enabled
/// schedule whose zone the core does not know;
/// [`SchedulerStartError::CommandRefused`] for the first send that the
/// command rules refuse.
fn manifest_plan(manifest: SchedulerManifest) -> Result<StartPlan, SchedulerStartError> {
    if !manifest.enabled || manifest.schedules.is_empty() {
        return Ok(StartPlan::Idle);
    }
    let configured_names: HashSet<String> = manifest
        .schedules
        .iter()
        .map(|schedule| schedule.name.clone())
        .collect();
    let mut enabled: Vec<(EnabledSchedule, ScheduleSendManifest)> = Vec::new();
    for schedule in manifest.schedules {
        match schedule.activation {
            ScheduleActivation::Disabled => {}
            ScheduleActivation::Enabled {
                pattern_json,
                timezone,
                catch_up_missed,
                max_catch_up_runs,
                send,
            } => {
                if timezone.parse::<Tz>().is_err() {
                    return Err(SchedulerStartError::UnknownTimezone {
                        schedule: schedule.name,
                        timezone,
                    });
                }
                let enabled_schedule = EnabledSchedule {
                    name: schedule.name,
                    pattern: pattern_json.into_pattern(),
                    timezone,
                    catch_up_missed,
                    max_catch_up_runs: max_catch_up_runs.0.get(),
                };
                enabled.push((enabled_schedule, send));
            }
        }
    }
    let mut commands = HashMap::with_capacity(enabled.len());
    for (schedule, send) in &enabled {
        let command =
            manifest_command(send).map_err(|cause| SchedulerStartError::CommandRefused {
                schedule: schedule.name.clone(),
                cause,
            })?;
        let _first_command = commands.insert(schedule.name.clone(), command);
    }
    Ok(StartPlan::Active(ActivePlan {
        configured_names,
        schedules: enabled
            .into_iter()
            .map(|(schedule, _send)| schedule)
            .collect(),
        commands,
        check_interval_seconds: manifest.check_interval_seconds.0.get(),
    }))
}

/// The send command of a manifest schedule's send facts: the values that a
/// send of the same arguments writes, with the manifest's retention class
/// and rerun-input posture. Does no I/O.
///
/// Errors: the refusals of [`SendCommand::prepare`].
fn manifest_command(send: &ScheduleSendManifest) -> Result<SendCommand, BrokerError> {
    let priority =
        i32::try_from(send.priority.0.get()).expect("a queue priority of at most 100 fits an i32");
    SendCommand::prepare(
        &send.task_name,
        &send.queue_name,
        priority,
        &send.input_json,
        send.task_options.text(),
        send.retention_class_key.retention(),
        send.retain_rerun_input,
    )
}

/// The send command of each enabled schedule, built once from its prepared
/// facts (Python `Scheduler._build_commands`): the queue of the facts, that
/// queue's priority and retention class, the schedule's input as JSON text
/// and the task's options text. Every fire of a schedule sends its command.
/// Does no I/O. Logs the payload warning of an input over
/// `payload.warn_bytes`.
///
/// Errors: [`SchedulerStartError::NotPrepared`] for an enabled schedule with
/// no facts; [`SchedulerStartError::PayloadTooLarge`] for an input over
/// `payload.reject_bytes`; [`SchedulerStartError::CommandRefused`] when the
/// broker refuses the command.
fn build_commands(
    broker: &PostgresBroker,
    schedule_config: &ScheduleConfig,
    app_config: &AppConfig,
    prepared: &PreparedSchedules,
) -> Result<HashMap<String, SendCommand>, SchedulerStartError> {
    let mut commands = HashMap::new();
    for schedule in schedule_config.schedules.iter().filter(|s| s.enabled) {
        let facts =
            prepared
                .get(&schedule.name)
                .ok_or_else(|| SchedulerStartError::NotPrepared {
                    schedule: schedule.name.clone(),
                })?;
        // The schedule's input is one JSON value; absent is `null`. A JSON
        // value always encodes.
        let input_json = serde_json::to_string(&schedule.input)
            .expect("a serde_json value encodes to JSON text");
        let oversize = enforce_payload_policy(
            &app_config.payload,
            &schedule.task_name,
            PayloadKind::Kwargs,
            input_json.len(),
        );
        if let (Some(size), Some(limit)) = (oversize, app_config.payload.reject_bytes) {
            return Err(SchedulerStartError::PayloadTooLarge {
                schedule: schedule.name.clone(),
                size,
                limit,
            });
        }
        let queue = facts.queue_name.as_str();
        let command = broker
            .prepare_send_command(
                &schedule.task_name,
                queue,
                resolve_queue_priority(app_config, queue),
                &input_json,
                facts.task_options.as_deref(),
                app_config.retention.resolve_queue_retention(queue),
            )
            .map_err(|cause| SchedulerStartError::CommandRefused {
                schedule: schedule.name.clone(),
                cause,
            })?;
        commands.insert(schedule.name.clone(), command);
    }
    Ok(commands)
}

/// Store the state row of each enabled schedule that is new or whose pattern
/// or timezone changed, in two statements on `pool` (Python `_initialize_schedules`,
/// `python/horsies/core/scheduler/service.py`). `configured_names` holds the
/// name of every configured schedule, disabled ones included:
///
/// 1. One read of every row's name and digest, with the database time.
/// 2. One write of the new and changed rows, each with its first run computed
///    from that database time. A changed row keeps its run record.
///
/// Rows of names that are not in this config are kept and logged. An
/// enabled schedule whose pattern has no run after the database time is
/// refused before the write ([`SchedulerStartError::NoNextSlot`]). A
/// database error of either statement is logged and start goes on: the
/// tick's existence heal creates the missing rows.
async fn initialize_schedules(
    pool: &CorePool,
    log: StatementLog,
    configured_names: &HashSet<String>,
    schedules: &[EnabledSchedule],
) -> Result<(), SchedulerStartError> {
    let read = match state::read_start_states(pool, log).await {
        Ok(read) => read,
        Err(e) => {
            tracing::error!(
                error = %e,
                "Failed to read schedule state rows at start: {e}; the tick loop creates missing \
                 rows",
            );
            return Ok(());
        }
    };
    check_next_slots(schedules, read.database_now)?;

    // Schedule-state rows whose name is absent from this process's config are
    // left intact, not deleted: another scheduler (rolling deploy, shared DB)
    // may own them, and deleting would silently stop its schedule. Such rows are
    // inert here — the due read only considers configured, enabled names.
    // Warn for visibility. Parity with horsies PR #101 e26a0f55.
    for row in &read.rows {
        if !configured_names.contains(row.schedule_name.as_str()) {
            tracing::warn!(
                schedule = %row.schedule_name,
                "schedule state present in DB but not in this config; \
                 leaving intact (may belong to another scheduler)",
            );
        }
    }
    let stored: std::collections::HashMap<&str, &[u8]> = read
        .rows
        .iter()
        .map(|row| (row.schedule_name.as_str(), row.config_hash.as_slice()))
        .collect();

    let writes = start_writes(schedules, &stored, read.database_now);
    if writes.is_empty() {
        return Ok(());
    }
    match state::store_start_states(pool, log, &writes).await {
        Ok(written) => log_start_writes(&writes, &written, &stored),
        Err(e) => tracing::error!(
            error = %e,
            "Failed to store schedule state rows at start: {e}; the tick loop creates missing \
             rows",
        ),
    }
    Ok(())
}

/// The start write's rows: each enabled schedule whose stored digest is
/// missing or differs, with its first run after `database_now`. A schedule
/// whose pattern has no run after that time is left out and logged. Does no
/// I/O.
fn start_writes(
    schedules: &[EnabledSchedule],
    stored: &std::collections::HashMap<&str, &[u8]>,
    database_now: DateTime<Utc>,
) -> Vec<state::ScheduleStateWrite> {
    let mut writes: Vec<state::ScheduleStateWrite> = Vec::new();
    let mut failed: Vec<&str> = Vec::new();
    for schedule in schedules {
        let config_hash = compute_config_hash(schedule);
        if stored
            .get(schedule.name.as_str())
            .is_some_and(|hash| *hash == config_hash.as_slice())
        {
            tracing::debug!(schedule = %schedule.name, "schedule state unchanged");
            continue;
        }
        match next_run_at(&schedule.pattern, database_now, &schedule.timezone) {
            Some(next) => writes.push(state::ScheduleStateWrite {
                schedule_name: schedule.name.clone(),
                next_run_at: next,
                config_hash: config_hash.to_vec(),
            }),
            None => {
                tracing::error!(
                    schedule = %schedule.name,
                    after = %database_now,
                    "failed to initialize schedule state: the pattern has no run after the database time",
                );
                failed.push(schedule.name.as_str());
            }
        }
    }
    if !failed.is_empty() {
        tracing::error!(
            failed = ?failed,
            "{} schedule(s) failed to initialize; remaining schedules will proceed",
            failed.len(),
        );
    }
    writes
}

/// Log each row of the start write: created, moved to a changed pattern, or
/// left to the row another scheduler wrote first.
fn log_start_writes(
    writes: &[state::ScheduleStateWrite],
    written: &[String],
    stored: &std::collections::HashMap<&str, &[u8]>,
) {
    for write in writes {
        let was_written = written.contains(&write.schedule_name);
        let was_stored = stored.contains_key(write.schedule_name.as_str());
        match (was_written, was_stored) {
            (true, false) => tracing::info!(
                schedule = %write.schedule_name,
                next_run_at = %write.next_run_at,
                "schedule initialized",
            ),
            (true, true) => tracing::warn!(
                schedule = %write.schedule_name,
                next_run_at = %write.next_run_at,
                "schedule pattern or timezone changed; next_run_at recalculated, run record kept",
            ),
            (false, _) => tracing::debug!(
                schedule = %write.schedule_name,
                "schedule state written by another scheduler first",
            ),
        }
    }
}

/// Create the state rows missing for enabled schedules, in one statement on
/// `pool` with no read before it (Python `_ensure_states_exist`): `INSERT ...
/// ON CONFLICT DO NOTHING`. A row that exists is not written; a concurrent
/// winner's row stands. Each missing row gets the first run after
/// `database_now`, the database time of this tick's due read.
///
/// Runs on a cadence, not every tick: while healthy the caller re-checks
/// roughly every [`EXISTENCE_CHECK_INTERVAL_S`] seconds; an unhealthy pass
/// makes the caller re-check every tick until a fully healthy one.
///
/// Returns `true` when the first run of every enabled schedule was computed
/// and the statement ran.
async fn ensure_states_exist(
    pool: &CorePool,
    log: StatementLog,
    schedules: &[EnabledSchedule],
    database_now: DateTime<Utc>,
) -> bool {
    let mut writes: Vec<state::ScheduleStateWrite> = Vec::new();
    let mut healthy = true;
    for schedule in schedules {
        match next_run_at(&schedule.pattern, database_now, &schedule.timezone) {
            Some(next) => writes.push(state::ScheduleStateWrite {
                schedule_name: schedule.name.clone(),
                next_run_at: next,
                config_hash: compute_config_hash(schedule).to_vec(),
            }),
            None => {
                healthy = false;
                tracing::error!(
                    schedule = %schedule.name,
                    after = %database_now,
                    "schedule self-heal: the pattern has no next run; row not created",
                );
            }
        }
    }

    match state::heal_missing_states(pool, log, &writes).await {
        Ok(healed) => {
            for name in healed {
                tracing::warn!(
                    schedule = %name,
                    "schedule had no state row (failed startup init or external delete); created it",
                );
            }
            healthy
        }
        Err(e) => {
            // The unhealthy verdict makes the next tick run the heal again.
            tracing::warn!(error = %e, "schedule self-heal failed, will retry next tick: {e}");
            false
        }
    }
}

/// Check for due schedules and fire their slots (Python
/// `_check_and_run_schedules`).
///
/// The due read returns the database time with the due rows; that time is
/// the tick's only clock. No tick-level lock: each fire locks its schedule's
/// state row. Each due schedule's fire runs inside its own boundary: an error
/// is logged, a panic is caught and logged with the schedule and its cause,
/// and the other due schedules still fire.
///
/// Errors: the error of the due read; nothing was written.
async fn check_and_enqueue(
    pool: &CorePool,
    log: StatementLog,
    schedules: &[EnabledSchedule],
    commands: &HashMap<String, SendCommand>,
    existence_cadence: &mut ExistenceCheckCadence,
) -> Result<(), BrokerError> {
    let enabled_names: Vec<String> = schedules.iter().map(|s| s.name.clone()).collect();
    if enabled_names.is_empty() {
        return Ok(());
    }

    let due_read = state::read_due_schedules(pool, log, &enabled_names).await?;
    let database_now = due_read.database_now;

    // Self-heal: recreate state rows for enabled schedules missing one (init
    // failure or external delete) so they are not silently dormant until the
    // next restart. A recreated row was not in the due read, and its
    // next_run_at is after the read's database time.
    if existence_cadence.should_check() {
        let healthy = ensure_states_exist(pool, log, schedules, database_now).await;
        existence_cadence.record(healthy);
    }
    existence_cadence.tick();

    for due in &due_read.due {
        let Some(schedule) = schedules.iter().find(|s| s.name == due.schedule_name) else {
            tracing::warn!(schedule = %due.schedule_name, "due schedule not in config");
            continue;
        };
        // The start builds a command for each enabled schedule.
        let command = commands.get(&schedule.name).unwrap_or_else(|| {
            panic!(
                "schedule '{}' has no send command after the start",
                schedule.name
            )
        });
        let fired = FireBoundary::new(fire_due_schedule(
            pool,
            log,
            schedule,
            command,
            due,
            database_now,
        ))
        .await;
        match fired {
            FireEnded::Returned(Ok(())) => {}
            FireEnded::Returned(Err(error)) => {
                tracing::error!(
                    schedule = %schedule.name,
                    %error,
                    "Error checking schedule '{}': {error}",
                    schedule.name,
                );
            }
            FireEnded::Panicked { cause } => tracing::error!(
                schedule = %schedule.name,
                cause = %cause,
                "panic in the fire of a schedule; the scheduler goes on with the next schedule",
            ),
        }
    }

    Ok(())
}

/// How the fire of one due schedule ended.
enum FireEnded<T> {
    Returned(T),
    /// The fire panicked; `cause` is the panic text.
    Panicked {
        cause: String,
    },
}

/// The panic boundary of one schedule's fire. A panic is a defect, but one
/// broken schedule does not stop the others: the boundary turns a panic in
/// a poll of the fire into [`FireEnded::Panicked`], and the tick goes on with
/// the next schedule (Python's per-schedule seam catches every exception).
/// The panicked fire is not polled again.
struct FireBoundary<F> {
    fire: Pin<Box<F>>,
}

impl<F: Future> FireBoundary<F> {
    fn new(fire: F) -> Self {
        Self {
            fire: Box::pin(fire),
        }
    }
}

impl<F: Future> Future for FireBoundary<F> {
    type Output = FireEnded<F::Output>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let fire = self.fire.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| fire.poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(FireEnded::Returned(output)),
            Err(payload) => Poll::Ready(FireEnded::Panicked {
                cause: panic_text(payload.as_ref()),
            }),
        }
    }
}

/// The text of a panic payload: `panic!` with a literal gives `&str`, with
/// format arguments `String`.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "the panic payload is not text".to_owned(),
    }
}

/// The slots one fire sends and where it moves the row, computed from the
/// due read (Python `FirePlan`).
///
/// `slots` strictly increase from the stored `next_run_at` and are at or
/// before the database time of the due read; empty when the latest-slot
/// scan reached its bound (the fire then only moves the row). `next_run_at`
/// is after the last slot. `skip_to` is where a refused fire moves the row:
/// past the first slot (catch-up), or to `next_run_at` (latest slot).
#[derive(Debug, Clone, PartialEq, Eq)]
struct FirePlan {
    slots: Vec<DateTime<Utc>>,
    next_run_at: DateTime<Utc>,
    skip_to: DateTime<Utc>,
}

/// Why one due schedule was not checked to its end. The next tick checks it
/// again.
#[derive(Debug, thiserror::Error)]
enum ScheduleCheckError {
    /// The pattern has no run after this time, so the row has no next run
    /// to move to; nothing was written.
    #[error("the pattern has no run after {after}; the state row is not moved")]
    NoRunAfter { after: DateTime<Utc> },
    /// The heal of a row that the fire did not find failed.
    #[error("the heal of the missing state row failed: {0}")]
    HealFailed(BrokerError),
}

/// Fire the due slots of one schedule in one statement (Python
/// `_check_schedule`).
///
/// `due` is the schedule's row in this tick's due read and `database_now`
/// the database time of that read. The slots and the next run come from
/// them ([`fire_plan`]); `horsies_fire_schedule` then inserts the slot tasks
/// and moves the row in one commit, under a lock of the row that serializes
/// concurrent schedulers.
///
/// Outcomes:
/// - Advanced: the slots are logged by outcome.
/// - State missing (the row was deleted after the due read): the heal
///   creates it with the first run after `database_now`.
/// - State moved (another scheduler fired first): nothing to do.
/// - Outcome unknown (a failure that may clear): logged; the next tick's due
///   read shows whether the row moved.
/// - Refused (nothing committed): logged; a second fire with no slots moves
///   the row to `skip_to` and records no run, so a refusal that repeats does
///   not stop the schedule.
///
/// Errors: [`ScheduleCheckError`].
///
/// Panics: an argument that the fire function refuses as a caller defect
/// (SQLSTATE 22023, 22004), or an answer outside its contract
/// ([`state::fire_schedule`]); the caller's boundary catches it.
async fn fire_due_schedule(
    pool: &CorePool,
    log: StatementLog,
    schedule: &EnabledSchedule,
    command: &SendCommand,
    due: &DueSchedule,
    database_now: DateTime<Utc>,
) -> Result<(), ScheduleCheckError> {
    let plan = fire_plan(schedule, due.next_run_at, database_now)?;
    let fire = ScheduleFire {
        schedule_name: &schedule.name,
        expected_next_run_at: due.next_run_at,
        next_run_at: plan.next_run_at,
        slot_ats: &plan.slots,
        command,
    };
    match state::fire_schedule(pool, log, &fire).await {
        Ok(ScheduleFireAnswer::Advanced { slots }) => {
            log_fired(schedule, command, &slots);
            Ok(())
        }
        Ok(ScheduleFireAnswer::StateMissing) => heal_one(pool, log, schedule, database_now).await,
        Ok(ScheduleFireAnswer::StateMoved) => {
            tracing::debug!(
                schedule = %schedule.name,
                "schedule was moved by another scheduler; nothing fired",
            );
            Ok(())
        }
        Err(ScheduleFireError::OutcomeUnknown { cause, .. }) => {
            tracing::warn!(
                schedule = %schedule.name,
                error = %cause,
                "Schedule '{}' fire failed (transient), will retry next tick: {cause}",
                schedule.name,
            );
            Ok(())
        }
        Err(ScheduleFireError::Refused { cause, .. }) => {
            tracing::error!(
                schedule = %schedule.name,
                skip_to = %plan.skip_to,
                error = %cause,
                "Schedule '{}' fire refused (permanent), moving to {} with no run recorded: \
                 {cause}",
                schedule.name,
                plan.skip_to,
            );
            move_past_refused(pool, log, schedule, command, due, &plan).await;
            Ok(())
        }
    }
}

/// The slots of one fire and the next run after them (Python
/// `_fire_plan`). Does no I/O.
///
/// `catch_up_missed`: the due slots in order, at most `max_catch_up_runs`;
/// the rest fire on later ticks. Else the latest due slot only: older missed
/// slots are dropped and the schedule resumes in the future. When that scan
/// reaches its bound, no slot fires and the row moves to where the scan
/// stopped.
///
/// Errors: [`ScheduleCheckError::NoRunAfter`] when the pattern has no run
/// after a slot it needs one after.
///
/// Panics: when `first_due` is after `database_now` (the due read returns
/// only rows due at its database time).
fn fire_plan(
    schedule: &EnabledSchedule,
    first_due: DateTime<Utc>,
    database_now: DateTime<Utc>,
) -> Result<FirePlan, ScheduleCheckError> {
    assert!(
        first_due <= database_now,
        "schedule '{}' is not due: next run {first_due}, database time {database_now}",
        schedule.name
    );
    let next_after = |slot: DateTime<Utc>| {
        next_run_at(&schedule.pattern, slot, &schedule.timezone)
            .ok_or(ScheduleCheckError::NoRunAfter { after: slot })
    };
    match schedule.catch_up_missed {
        true => {
            let due_runs = calculate_missed_runs(
                schedule,
                first_due,
                database_now,
                schedule.max_catch_up_runs,
            );
            let (Some(first), Some(last)) = (due_runs.first(), due_runs.last()) else {
                panic!("schedule '{}' is due and has no slot", schedule.name);
            };
            if due_runs.len() > 1 {
                tracing::warn!(
                    schedule = %schedule.name,
                    due = due_runs.len(),
                    max_catch_up = schedule.max_catch_up_runs,
                    "catching up missed schedule runs",
                );
            }
            Ok(FirePlan {
                next_run_at: next_after(*last)?,
                skip_to: next_after(*first)?,
                slots: due_runs,
            })
        }
        false => {
            let (slot_time, next, skipped) =
                advance_to_latest_due_slot(schedule, first_due, database_now);
            if skipped > 0 {
                tracing::warn!(
                    schedule = %schedule.name,
                    skipped,
                    slot = %slot_time,
                    "missed run(s); skipping to latest due slot (catch_up_missed=false)",
                );
            }
            let next = next.ok_or(ScheduleCheckError::NoRunAfter { after: slot_time })?;
            // Scan bound reached on a very deep backlog: move the row without
            // firing a stale slot; the next tick continues from here.
            let slots = match next <= database_now {
                true => Vec::new(),
                false => vec![slot_time],
            };
            Ok(FirePlan {
                slots,
                next_run_at: next,
                skip_to: next,
            })
        }
    }
}

/// Log each slot of an advanced fire by its outcome (Python `_log_fired`).
fn log_fired(schedule: &EnabledSchedule, command: &SendCommand, slots: &[FiredSlot]) {
    for slot in slots {
        match slot.outcome {
            SlotOutcome::Inserted => tracing::info!(
                schedule = %schedule.name,
                task_name = %command.task_name,
                task_id = %slot.task_id,
                slot = %slot.slot_at,
                "scheduled task enqueued",
            ),
            SlotOutcome::SameCommand => tracing::info!(
                schedule = %schedule.name,
                task_id = %slot.task_id,
                slot = %slot.slot_at,
                "the slot already has its task; recorded as fired",
            ),
            SlotOutcome::PayloadMismatch => tracing::warn!(
                schedule = %schedule.name,
                task_id = %slot.task_id,
                slot = %slot.slot_at,
                "the slot's task id already holds a task with a different command (an \
                 earlier version, or a change to the schedule's input or the task's \
                 options); the slot counts as fired and is not enqueued again",
            ),
            SlotOutcome::ConflictUnverifiable => tracing::error!(
                schedule = %schedule.name,
                task_id = %slot.task_id,
                slot = %slot.slot_at,
                "the slot's task finished while the fire read it; the slot is not \
                 enqueued again and not recorded as a run",
            ),
        }
    }
}

/// Create the state row of a schedule whose row the fire did not find
/// (Python `_heal_one`), with the first run after `database_now`.
///
/// Errors: [`ScheduleCheckError::NoRunAfter`]; [`ScheduleCheckError::HealFailed`].
async fn heal_one(
    pool: &CorePool,
    log: StatementLog,
    schedule: &EnabledSchedule,
    database_now: DateTime<Utc>,
) -> Result<(), ScheduleCheckError> {
    let next = next_run_at(&schedule.pattern, database_now, &schedule.timezone).ok_or(
        ScheduleCheckError::NoRunAfter {
            after: database_now,
        },
    )?;
    let write = state::ScheduleStateWrite {
        schedule_name: schedule.name.clone(),
        next_run_at: next,
        config_hash: compute_config_hash(schedule).to_vec(),
    };
    let healed = state::heal_missing_states(pool, log, std::slice::from_ref(&write))
        .await
        .map_err(ScheduleCheckError::HealFailed)?;
    let created = match healed.is_empty() {
        true => "another scheduler created it",
        false => "created it",
    };
    tracing::warn!(
        schedule = %schedule.name,
        next_run_at = %next,
        "schedule state missing at its fire; {created}",
    );
    Ok(())
}

/// Fire with no slots: move the row to `plan.skip_to` and record no run
/// (Python `_move_past_refused`). Every outcome is logged.
async fn move_past_refused(
    pool: &CorePool,
    log: StatementLog,
    schedule: &EnabledSchedule,
    command: &SendCommand,
    due: &DueSchedule,
    plan: &FirePlan,
) {
    let skip = ScheduleFire {
        schedule_name: &schedule.name,
        expected_next_run_at: due.next_run_at,
        next_run_at: plan.skip_to,
        slot_ats: &[],
        command,
    };
    match state::fire_schedule(pool, log, &skip).await {
        Ok(ScheduleFireAnswer::Advanced { .. }) => tracing::warn!(
            schedule = %schedule.name,
            skip_to = %plan.skip_to,
            "schedule moved past the refused slot",
        ),
        Ok(ScheduleFireAnswer::StateMissing | ScheduleFireAnswer::StateMoved) => tracing::debug!(
            schedule = %schedule.name,
            "schedule row is gone or moved; nothing to skip",
        ),
        Err(error) => tracing::error!(
            schedule = %schedule.name,
            %error,
            "Schedule '{}' could not move past the refused slot, will retry next tick: {error}",
            schedule.name,
        ),
    }
}

/// Bound the per-tick slot scan so a pathological backlog (tiny period, very
/// long downtime) cannot stall a tick; progress is persisted and the next tick
/// continues from where this one stopped. Parity with Python
/// `_MAX_SKIP_SCAN_PER_TICK`.
const MAX_SKIP_SCAN_PER_TICK: u32 = 100_000;

/// Advance through due slots to the latest one (catch_up_missed=false).
///
/// Returns `(latest_due_slot, next_run_after_slot, skipped)`. `next_run_after_slot`
/// is `None` when the pattern is unsatisfiable from the latest slot, and may still
/// be `Some(t)` with `t <= now` when the scan cap was reached — callers must not
/// fire a slot in that case. Mirrors Python `_advance_to_latest_due_slot`.
fn advance_to_latest_due_slot(
    schedule: &EnabledSchedule,
    first_due: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (DateTime<Utc>, Option<DateTime<Utc>>, u32) {
    let mut slot = first_due;
    let mut skipped: u32 = 0;
    let mut next_run = next_run_at(&schedule.pattern, slot, &schedule.timezone);
    while let Some(candidate) = next_run {
        if candidate > now || skipped >= MAX_SKIP_SCAN_PER_TICK {
            break;
        }
        // Non-monotonic guard: a non-advancing calculator would loop forever.
        if candidate <= slot {
            tracing::error!(
                schedule = %schedule.name,
                current = %slot,
                next = %candidate,
                "non-monotonic next_run_at — stopping skip-to-latest",
            );
            break;
        }
        slot = candidate;
        skipped += 1;
        next_run = next_run_at(&schedule.pattern, slot, &schedule.timezone);
    }
    (slot, next_run, skipped)
}

fn calculate_missed_runs(
    schedule: &EnabledSchedule,
    first_due_run: DateTime<Utc>,
    now: DateTime<Utc>,
    max_runs: u32,
) -> Vec<DateTime<Utc>> {
    if first_due_run > now {
        return Vec::new();
    }

    let mut due_runs = Vec::new();
    let mut cursor = first_due_run;

    while cursor <= now && due_runs.len() < max_runs as usize {
        due_runs.push(cursor);

        let Some(next) = next_run_at(&schedule.pattern, cursor, &schedule.timezone) else {
            break;
        };

        // Non-monotonic guard: if the calculator returns a non-advancing time,
        // break to prevent an infinite loop.
        if next <= cursor {
            tracing::error!(
                schedule = %schedule.name,
                current = %cursor,
                next = %next,
                "non-monotonic next_run_at — stopping catch-up",
            );
            break;
        }

        cursor = next;
    }

    if cursor <= now && due_runs.len() >= max_runs as usize {
        tracing::warn!(
            schedule = %schedule.name,
            cap = max_runs,
            "catch-up cap reached, backlog remains",
        );
    }

    due_runs
}

/// SHA-256 of the schedule's pattern and timezone in Python's digest text
/// (`config_digest`), for change detection. A change of input, queue or
/// catch-up settings does not move `next_run_at`.
fn compute_config_hash(schedule: &EnabledSchedule) -> [u8; 32] {
    config_digest(&schedule.pattern, &schedule.timezone)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod slot_fire_spec_tests;

#[cfg(test)]
mod start_and_boundary_tests {
    use super::*;
    use crate::core::config::{IntervalSchedule, SchedulePattern};

    async fn panicking_fire() -> u8 {
        panic!("planned fire panic")
    }

    #[tokio::test]
    async fn a_panic_in_one_fire_is_caught_at_its_boundary() {
        match FireBoundary::new(panicking_fire()).await {
            FireEnded::Panicked { cause } => assert_eq!(cause, "planned fire panic"),
            FireEnded::Returned(value) => panic!("the fire panics, but it returned {value}"),
        }
    }

    #[tokio::test]
    async fn a_fire_that_returns_passes_its_outcome_through_the_boundary() {
        assert!(matches!(
            FireBoundary::new(async { 7_u8 }).await,
            FireEnded::Returned(7)
        ));
    }

    /// An interval with no unit has no next slot: a pattern that never fires
    /// again.
    fn schedule_without_next_slot(name: &str, enabled: bool) -> TaskSchedule {
        TaskSchedule::new(
            name,
            "my_task",
            SchedulePattern::Interval(IntervalSchedule::default()),
        )
        .enabled(enabled)
    }

    #[test]
    fn an_enabled_pattern_without_a_slot_after_the_database_time_is_refused() {
        let database_now = Utc::now();
        let schedules = vec![
            TaskSchedule::new(
                "fires",
                "my_task",
                SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    ..IntervalSchedule::default()
                }),
            ),
            schedule_without_next_slot("never", true),
        ];
        assert!(matches!(
            check_next_slots(&enabled_schedules(&schedules), database_now),
            Err(SchedulerStartError::NoNextSlot { ref schedule, after })
                if schedule == "never" && after == database_now
        ));
    }

    /// A disabled schedule is not an enabled schedule of its config, so the
    /// start does not check its pattern.
    #[test]
    fn a_disabled_pattern_without_a_next_slot_is_not_checked() {
        let schedules = vec![schedule_without_next_slot("off", false)];
        let enabled = enabled_schedules(&schedules);
        assert!(enabled.is_empty());
        assert!(check_next_slots(&enabled, Utc::now()).is_ok());
    }
}
