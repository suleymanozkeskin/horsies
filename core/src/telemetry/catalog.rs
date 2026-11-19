//! The metric catalog: every metric of horsies, once, as data.
//!
//! Each metric has its OpenTelemetry name, its kind, its unit, the process
//! that records it, its help text and its labels. The Prometheus name is
//! derived from the OpenTelemetry name ([`MetricSpec::prometheus_name`]), so
//! a name is written once. Each label has its OpenTelemetry attribute key,
//! its Prometheus label name and its closed value set (`labels`).
//!
//! `catalog_snapshot.txt` holds the text of the catalog. A test compares the
//! catalog with it, so a rename, a new metric or a new label value is a
//! deliberate edit of that file.

use std::time::Duration;

use crate::telemetry::labels::{
    ClaimPassResult, DeclaredSource, EventDropCause, FleetQuery, FleetWorkerState, LabelName,
    LabelValue, LabelValues, LiveTaskStatus, LiveWorkflowStatus, MemoryProcess, ProcessFact,
    RecoveryAction, RecoveryStep, ReleaseReason, ReplaceReason, TelemetryPart, ERROR_CODE_VALUES,
    SEND_RESULT_VALUES,
};
use crate::worker::executor::protocol::StopReason;
use crate::worker::executor::slots::outcome::WriteStage;
use crate::{Execution, TaskAttemptOutcome};

/// The upper bounds of the buckets of every duration histogram. A duration
/// above the last bound counts only in the `+Inf` bucket. The bounds reach
/// one day; the longest task timeout is about 49.7 days.
pub const DURATION_BUCKET_BOUNDS: [Duration; 21] = [
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2_500),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(300),
    Duration::from_secs(600),
    Duration::from_secs(1_800),
    Duration::from_secs(3_600),
    Duration::from_secs(7_200),
    Duration::from_secs(21_600),
    Duration::from_secs(86_400),
];

/// The kind of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// A count that only goes up.
    Counter,
    /// A value that is set.
    Gauge,
    /// A distribution of durations over the given bucket upper bounds, in
    /// increasing order.
    Histogram(&'static [Duration]),
}

impl MetricKind {
    /// The name of the kind in the Prometheus `# TYPE` line.
    pub fn prometheus_type(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram(_) => "histogram",
        }
    }
}

/// The unit of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricUnit {
    /// A count of things.
    Count,
    /// Seconds.
    Seconds,
    /// Bytes.
    Bytes,
}

impl MetricUnit {
    /// The OpenTelemetry unit (UCUM).
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Count => "1",
            Self::Seconds => "s",
            Self::Bytes => "By",
        }
    }

    /// The suffix that the unit adds to the Prometheus name.
    pub fn prometheus_suffix(self) -> UnitSuffix {
        match self {
            Self::Count => UnitSuffix::NoSuffix,
            Self::Seconds => UnitSuffix::Suffix("seconds"),
            Self::Bytes => UnitSuffix::Suffix("bytes"),
        }
    }
}

/// The suffix of a unit in a Prometheus name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitSuffix {
    /// The name gets `_<suffix>`.
    Suffix(&'static str),
    /// The unit adds nothing to the name.
    NoSuffix,
}

/// The process that records a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorder {
    /// The core worker, natively.
    Worker,
    /// The client in the producer process, with the user's OpenTelemetry
    /// meter.
    Producer,
    /// The fleet collector, from Postgres.
    FleetCollector,
}

impl Recorder {
    /// The name of the recorder, for the catalog text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Producer => "producer",
            Self::FleetCollector => "fleet collector",
        }
    }
}

/// One label of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LabelSpec {
    pub name: LabelName,
    pub values: LabelValues,
}

/// One metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSpec {
    /// The OpenTelemetry name.
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: MetricUnit,
    pub recorder: Recorder,
    /// One line, in plain words.
    pub help: &'static str,
    /// The labels, in the order of the label values of a series.
    pub labels: &'static [LabelSpec],
}

impl MetricSpec {
    /// The Prometheus name: the OpenTelemetry name with each `.` as `_`,
    /// then the unit suffix, then `_total` for a counter.
    pub fn prometheus_name(&self) -> String {
        let mut name = self.name.replace('.', "_");
        match self.unit.prometheus_suffix() {
            UnitSuffix::Suffix(suffix) => {
                name.push('_');
                name.push_str(suffix);
            }
            UnitSuffix::NoSuffix => {}
        }
        match self.kind {
            MetricKind::Counter => name.push_str("_total"),
            MetricKind::Gauge | MetricKind::Histogram(_) => {}
        }
        name
    }
}

/// The text of a duration in seconds, as a bucket bound (`le`) holds it:
/// the whole seconds, then the fraction with no trailing zeros (`0.005`,
/// `2.5`, `60`).
pub fn seconds_text(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let nanos = duration.subsec_nanos();
    if nanos == 0 {
        return seconds.to_string();
    }
    let fraction = format!("{nanos:09}");
    format!("{seconds}.{}", fraction.trim_end_matches('0'))
}

/// One metric of the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricId {
    TaskAttempts,
    TaskAttemptsEndNotKnown,
    TaskRunDuration,
    TaskQueueDuration,
    TaskSlotWaitDuration,
    TaskRecordDuration,
    TaskRetries,
    TaskClaimsReleased,
    TaskStartsRefused,
    FinalizeRetries,
    RecoveryTasks,
    RecoveryPassFailures,
    ChildrenStarted,
    ChildrenReplaced,
    ChildBootDuration,
    Seats,
    SeatsInUse,
    SlotsDown,
    ClaimPasses,
    MemoryRss,
    EventsDropped,
    SeriesDropped,
    HandlerErrors,
    HandlerDropped,
    WorkerInfo,
    TaskSent,
    TaskSendDuration,
    QueueTasks,
    QueueOldestPendingAge,
    FleetWorkers,
    FleetWorkersTelemetryDown,
    FleetSeats,
    FleetSeatsInUse,
    FleetUnservedClasses,
    Workflows,
    ScheduleLastRunAge,
    SchemaVersion,
    CollectDuration,
    CollectErrors,
}

impl MetricId {
    /// Every metric, in declaration order.
    pub const ALL: [Self; 39] = [
        Self::TaskAttempts,
        Self::TaskAttemptsEndNotKnown,
        Self::TaskRunDuration,
        Self::TaskQueueDuration,
        Self::TaskSlotWaitDuration,
        Self::TaskRecordDuration,
        Self::TaskRetries,
        Self::TaskClaimsReleased,
        Self::TaskStartsRefused,
        Self::FinalizeRetries,
        Self::RecoveryTasks,
        Self::RecoveryPassFailures,
        Self::ChildrenStarted,
        Self::ChildrenReplaced,
        Self::ChildBootDuration,
        Self::Seats,
        Self::SeatsInUse,
        Self::SlotsDown,
        Self::ClaimPasses,
        Self::MemoryRss,
        Self::EventsDropped,
        Self::SeriesDropped,
        Self::HandlerErrors,
        Self::HandlerDropped,
        Self::WorkerInfo,
        Self::TaskSent,
        Self::TaskSendDuration,
        Self::QueueTasks,
        Self::QueueOldestPendingAge,
        Self::FleetWorkers,
        Self::FleetWorkersTelemetryDown,
        Self::FleetSeats,
        Self::FleetSeatsInUse,
        Self::FleetUnservedClasses,
        Self::Workflows,
        Self::ScheduleLastRunAge,
        Self::SchemaVersion,
        Self::CollectDuration,
        Self::CollectErrors,
    ];

    /// The catalog entry of the metric.
    pub fn spec(self) -> &'static MetricSpec {
        match self {
            Self::TaskAttempts => &TASK_ATTEMPTS,
            Self::TaskAttemptsEndNotKnown => &TASK_ATTEMPTS_END_NOT_KNOWN,
            Self::TaskRunDuration => &TASK_RUN_DURATION,
            Self::TaskQueueDuration => &TASK_QUEUE_DURATION,
            Self::TaskSlotWaitDuration => &TASK_SLOT_WAIT_DURATION,
            Self::TaskRecordDuration => &TASK_RECORD_DURATION,
            Self::TaskRetries => &TASK_RETRIES,
            Self::TaskClaimsReleased => &TASK_CLAIMS_RELEASED,
            Self::TaskStartsRefused => &TASK_STARTS_REFUSED,
            Self::FinalizeRetries => &FINALIZE_RETRIES,
            Self::RecoveryTasks => &RECOVERY_TASKS,
            Self::RecoveryPassFailures => &RECOVERY_PASS_FAILURES,
            Self::ChildrenStarted => &CHILDREN_STARTED,
            Self::ChildrenReplaced => &CHILDREN_REPLACED,
            Self::ChildBootDuration => &CHILD_BOOT_DURATION,
            Self::Seats => &SEATS,
            Self::SeatsInUse => &SEATS_IN_USE,
            Self::SlotsDown => &SLOTS_DOWN,
            Self::ClaimPasses => &CLAIM_PASSES,
            Self::MemoryRss => &MEMORY_RSS,
            Self::EventsDropped => &EVENTS_DROPPED,
            Self::SeriesDropped => &SERIES_DROPPED,
            Self::HandlerErrors => &HANDLER_ERRORS,
            Self::HandlerDropped => &HANDLER_DROPPED,
            Self::WorkerInfo => &WORKER_INFO,
            Self::TaskSent => &TASK_SENT,
            Self::TaskSendDuration => &TASK_SEND_DURATION,
            Self::QueueTasks => &QUEUE_TASKS,
            Self::QueueOldestPendingAge => &QUEUE_OLDEST_PENDING_AGE,
            Self::FleetWorkers => &FLEET_WORKERS,
            Self::FleetWorkersTelemetryDown => &FLEET_WORKERS_TELEMETRY_DOWN,
            Self::FleetSeats => &FLEET_SEATS,
            Self::FleetSeatsInUse => &FLEET_SEATS_IN_USE,
            Self::FleetUnservedClasses => &FLEET_UNSERVED_CLASSES,
            Self::Workflows => &WORKFLOWS,
            Self::ScheduleLastRunAge => &SCHEDULE_LAST_RUN_AGE,
            Self::SchemaVersion => &SCHEMA_VERSION,
            Self::CollectDuration => &COLLECT_DURATION,
            Self::CollectErrors => &COLLECT_ERRORS,
        }
    }
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

const TASK: LabelSpec = LabelSpec {
    name: LabelName::Task,
    values: LabelValues::Declared(DeclaredSource::TaskNames),
};
const QUEUE: LabelSpec = LabelSpec {
    name: LabelName::Queue,
    values: LabelValues::Declared(DeclaredSource::QueueNames),
};
const EXECUTION: LabelSpec = LabelSpec {
    name: LabelName::Execution,
    values: <Execution as LabelValue>::VALUES,
};
const KIND: LabelSpec = LabelSpec {
    name: LabelName::Kind,
    values: <Execution as LabelValue>::VALUES,
};
const OUTCOME: LabelSpec = LabelSpec {
    name: LabelName::Outcome,
    values: <TaskAttemptOutcome as LabelValue>::VALUES,
};
const ERROR_CODE: LabelSpec = LabelSpec {
    name: LabelName::ErrorCode,
    values: ERROR_CODE_VALUES,
};
const HANDLER: LabelSpec = LabelSpec {
    name: LabelName::Handler,
    values: LabelValues::Declared(DeclaredSource::HandlerNames),
};
const QUERY: LabelSpec = LabelSpec {
    name: LabelName::Query,
    values: <FleetQuery as LabelValue>::VALUES,
};

// ---------------------------------------------------------------------------
// Worker metrics
// ---------------------------------------------------------------------------

static TASK_ATTEMPTS: MetricSpec = MetricSpec {
    name: "horsies.task.attempts",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Attempts that ended with a known outcome, by outcome and error code.",
    labels: &[TASK, QUEUE, EXECUTION, OUTCOME, ERROR_CODE],
};

static TASK_ATTEMPTS_END_NOT_KNOWN: MetricSpec = MetricSpec {
    name: "horsies.task.attempts.end_not_known",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Attempts whose child was killed before its exit was seen; recovery decides the row.",
    labels: &[TASK, QUEUE, EXECUTION],
};

static TASK_RUN_DURATION: MetricSpec = MetricSpec {
    name: "horsies.task.run.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Worker,
    help: "Time from the placement of an attempt on a child to the end that the worker saw, with the frame transit and the start write.",
    labels: &[TASK, QUEUE, EXECUTION, OUTCOME],
};

static TASK_QUEUE_DURATION: MetricSpec = MetricSpec {
    name: "horsies.task.queue.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Worker,
    help: "Time from the due time of a task to its claim, both from the database.",
    labels: &[QUEUE, EXECUTION],
};

static TASK_SLOT_WAIT_DURATION: MetricSpec = MetricSpec {
    name: "horsies.task.slot_wait.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Worker,
    help: "Time from the claim result in the worker to the placement of the attempt on a child.",
    labels: &[EXECUTION],
};

static TASK_RECORD_DURATION: MetricSpec = MetricSpec {
    name: "horsies.task.record.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Worker,
    help: "Time from the end that the worker saw to the end of the outcome write.",
    labels: &[EXECUTION],
};

static TASK_RETRIES: MetricSpec = MetricSpec {
    name: "horsies.task.retries",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Attempts whose outcome write scheduled a retry, by error code.",
    labels: &[TASK, QUEUE, ERROR_CODE],
};

static TASK_CLAIMS_RELEASED: MetricSpec = MetricSpec {
    name: "horsies.task.claims.released",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Claimed tasks that the worker released before they ran, by reason.",
    labels: &[LabelSpec {
        name: LabelName::Reason,
        values: <ReleaseReason as LabelValue>::VALUES,
    }],
};

static TASK_STARTS_REFUSED: MetricSpec = MetricSpec {
    name: "horsies.task.starts.refused",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Attempts that stopped before the child set the task RUNNING, by reason.",
    labels: &[LabelSpec {
        name: LabelName::Reason,
        values: <StopReason as LabelValue>::VALUES,
    }],
};

static FINALIZE_RETRIES: MetricSpec = MetricSpec {
    name: "horsies.finalize.retries",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Outcome writes that were sent again after a failure, by write stage.",
    labels: &[LabelSpec {
        name: LabelName::Stage,
        values: <WriteStage as LabelValue>::VALUES,
    }],
};

static RECOVERY_TASKS: MetricSpec = MetricSpec {
    name: "horsies.recovery.tasks",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Task rows that the reaper recovered, by action.",
    labels: &[LabelSpec {
        name: LabelName::Action,
        values: <RecoveryAction as LabelValue>::VALUES,
    }],
};

static RECOVERY_PASS_FAILURES: MetricSpec = MetricSpec {
    name: "horsies.recovery.pass.failures",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Reaper passes in which a recovery step failed, by step.",
    labels: &[LabelSpec {
        name: LabelName::Step,
        values: <RecoveryStep as LabelValue>::VALUES,
    }],
};

static CHILDREN_STARTED: MetricSpec = MetricSpec {
    name: "horsies.worker.children.started",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Child processes that sent their ready frame, by kind.",
    labels: &[KIND],
};

static CHILDREN_REPLACED: MetricSpec = MetricSpec {
    name: "horsies.worker.children.replaced",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Child processes that ended and were replaced, by kind and reason.",
    labels: &[
        KIND,
        LabelSpec {
            name: LabelName::Reason,
            values: <ReplaceReason as LabelValue>::VALUES,
        },
    ],
};

static CHILD_BOOT_DURATION: MetricSpec = MetricSpec {
    name: "horsies.worker.child.boot.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Worker,
    help: "Time from the start of a child process to its ready frame, by kind.",
    labels: &[KIND],
};

static SEATS: MetricSpec = MetricSpec {
    name: "horsies.worker.seats",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Seats that the worker runs, by execution class.",
    labels: &[EXECUTION],
};

static SEATS_IN_USE: MetricSpec = MetricSpec {
    name: "horsies.worker.seats.in_use",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Seats that hold a task, by execution class.",
    labels: &[EXECUTION],
};

static SLOTS_DOWN: MetricSpec = MetricSpec {
    name: "horsies.worker.slots.down",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Child slots that are down, by kind.",
    labels: &[KIND],
};

static CLAIM_PASSES: MetricSpec = MetricSpec {
    name: "horsies.worker.claim.passes",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Passes of the claim loop, by result.",
    labels: &[LabelSpec {
        name: LabelName::Result,
        values: <ClaimPassResult as LabelValue>::VALUES,
    }],
};

static MEMORY_RSS: MetricSpec = MetricSpec {
    name: "horsies.worker.memory.rss",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Bytes,
    recorder: Recorder::Worker,
    help: "Resident memory of the worker parent, of the task children together, and of the handler child.",
    labels: &[LabelSpec {
        name: LabelName::Process,
        values: <MemoryProcess as LabelValue>::VALUES,
    }],
};

static EVENTS_DROPPED: MetricSpec = MetricSpec {
    name: "horsies.telemetry.events.dropped",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Lifecycle events that did not reach the handler child, by cause.",
    labels: &[LabelSpec {
        name: LabelName::Cause,
        values: <EventDropCause as LabelValue>::VALUES,
    }],
};

static SERIES_DROPPED: MetricSpec = MetricSpec {
    name: "horsies.telemetry.series.dropped",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Samples that were dropped because their metric holds its greatest number of series, by metric.",
    labels: &[LabelSpec {
        name: LabelName::Metric,
        values: <MetricId as LabelValue>::VALUES,
    }],
};

static HANDLER_ERRORS: MetricSpec = MetricSpec {
    name: "horsies.telemetry.handler.errors",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Events for which an event handler failed, by handler.",
    labels: &[HANDLER],
};

static HANDLER_DROPPED: MetricSpec = MetricSpec {
    name: "horsies.telemetry.handler.dropped",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Events that the full queue of an event handler dropped, by handler.",
    labels: &[HANDLER],
};

static WORKER_INFO: MetricSpec = MetricSpec {
    name: "horsies.worker.info",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::Worker,
    help: "Always 1. The labels hold the version, the profile and the id of the worker.",
    labels: &[
        LabelSpec {
            name: LabelName::Version,
            values: LabelValues::OneProcessValue(ProcessFact::Version),
        },
        LabelSpec {
            name: LabelName::Profile,
            values: LabelValues::OneProcessValue(ProcessFact::Profile),
        },
        LabelSpec {
            name: LabelName::WorkerId,
            values: LabelValues::OneProcessValue(ProcessFact::WorkerId),
        },
    ],
};

// ---------------------------------------------------------------------------
// Producer metrics
// ---------------------------------------------------------------------------

static TASK_SENT: MetricSpec = MetricSpec {
    name: "horsies.task.sent",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::Producer,
    help: "Sends of tasks from this producer, by result.",
    labels: &[
        TASK,
        QUEUE,
        LabelSpec {
            name: LabelName::Result,
            values: SEND_RESULT_VALUES,
        },
    ],
};

static TASK_SEND_DURATION: MetricSpec = MetricSpec {
    name: "horsies.task.send.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::Producer,
    help: "Time of one send from this producer.",
    labels: &[QUEUE],
};

// ---------------------------------------------------------------------------
// Fleet metrics
// ---------------------------------------------------------------------------

static QUEUE_TASKS: MetricSpec = MetricSpec {
    name: "horsies.queue.tasks",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Live task rows, by queue, execution class and status.",
    labels: &[
        QUEUE,
        EXECUTION,
        LabelSpec {
            name: LabelName::Status,
            values: <LiveTaskStatus as LabelValue>::VALUES,
        },
    ],
};

static QUEUE_OLDEST_PENDING_AGE: MetricSpec = MetricSpec {
    name: "horsies.queue.oldest_pending.age",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Seconds,
    recorder: Recorder::FleetCollector,
    help: "Age of the oldest due PENDING task, by queue and execution class.",
    labels: &[QUEUE, EXECUTION],
};

static FLEET_WORKERS: MetricSpec = MetricSpec {
    name: "horsies.fleet.workers",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Workers with a snapshot inside the live window, by state.",
    labels: &[LabelSpec {
        name: LabelName::State,
        values: <FleetWorkerState as LabelValue>::VALUES,
    }],
};

static FLEET_WORKERS_TELEMETRY_DOWN: MetricSpec = MetricSpec {
    name: "horsies.fleet.workers.telemetry_down",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Live workers whose telemetry part is down, by part.",
    labels: &[LabelSpec {
        name: LabelName::Part,
        values: <TelemetryPart as LabelValue>::VALUES,
    }],
};

static FLEET_SEATS: MetricSpec = MetricSpec {
    name: "horsies.fleet.seats",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Seats of the live workers, by execution class.",
    labels: &[EXECUTION],
};

static FLEET_SEATS_IN_USE: MetricSpec = MetricSpec {
    name: "horsies.fleet.seats.in_use",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Seats of the live workers that hold a task, by execution class.",
    labels: &[EXECUTION],
};

static FLEET_UNSERVED_CLASSES: MetricSpec = MetricSpec {
    name: "horsies.fleet.unserved_classes",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help:
        "1 for each queue and execution class with PENDING tasks and no live worker that serves it.",
    labels: &[QUEUE, EXECUTION],
};

static WORKFLOWS: MetricSpec = MetricSpec {
    name: "horsies.workflows",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Workflows that are not terminal, by status.",
    labels: &[LabelSpec {
        name: LabelName::Status,
        values: <LiveWorkflowStatus as LabelValue>::VALUES,
    }],
};

static SCHEDULE_LAST_RUN_AGE: MetricSpec = MetricSpec {
    name: "horsies.schedule.last_run.age",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Seconds,
    recorder: Recorder::FleetCollector,
    help: "Time since the last run of a schedule, by schedule.",
    labels: &[LabelSpec {
        name: LabelName::Schedule,
        values: LabelValues::Declared(DeclaredSource::ScheduleNames),
    }],
};

static SCHEMA_VERSION: MetricSpec = MetricSpec {
    name: "horsies.schema.version",
    kind: MetricKind::Gauge,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "The schema version of the database.",
    labels: &[],
};

static COLLECT_DURATION: MetricSpec = MetricSpec {
    name: "horsies.exporter.collect.duration",
    kind: MetricKind::Histogram(&DURATION_BUCKET_BOUNDS),
    unit: MetricUnit::Seconds,
    recorder: Recorder::FleetCollector,
    help: "Time of one query of the fleet collector, by query.",
    labels: &[QUERY],
};

static COLLECT_ERRORS: MetricSpec = MetricSpec {
    name: "horsies.exporter.collect.errors",
    kind: MetricKind::Counter,
    unit: MetricUnit::Count,
    recorder: Recorder::FleetCollector,
    help: "Queries of the fleet collector that failed or passed their time limit, by query.",
    labels: &[QUERY],
};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::telemetry::labels::group_texts;

    /// The committed text of the catalog.
    const CATALOG_SNAPSHOT: &str = include_str!("catalog_snapshot.txt");

    /// The catalog text of the values of one label.
    fn values_text(values: LabelValues) -> String {
        let quoted = |texts: Vec<&'static str>| -> String {
            texts
                .iter()
                .map(|text| format!("{text:?}"))
                .collect::<Vec<String>>()
                .join(" ")
        };
        let declared = |source: DeclaredSource| -> String {
            format!(
                "declared {}, fallback {:?}",
                source.description(),
                source.fallback()
            )
        };
        match values {
            LabelValues::Fixed(groups) => quoted(group_texts(groups)),
            LabelValues::Declared(source) => declared(source),
            LabelValues::FixedThenDeclared(groups, source) => {
                format!("{} then {}", quoted(group_texts(groups)), declared(source))
            }
            LabelValues::OneProcessValue(fact) => {
                format!("the {} of the process", fact.description())
            }
        }
    }

    /// The text of the catalog: one block per metric, in catalog order.
    fn catalog_text() -> String {
        let mut text = String::new();
        for id in MetricId::ALL {
            let spec = id.spec();
            text.push_str(&format!("{}\n", spec.name));
            text.push_str(&format!("  prometheus: {}\n", spec.prometheus_name()));
            text.push_str(&format!("  kind: {}\n", spec.kind.prometheus_type()));
            text.push_str(&format!("  unit: {}\n", spec.unit.symbol()));
            text.push_str(&format!("  recorder: {}\n", spec.recorder.as_str()));
            text.push_str(&format!("  help: {}\n", spec.help));
            match spec.kind {
                MetricKind::Histogram(bounds) => {
                    let bounds: Vec<String> =
                        bounds.iter().map(|bound| seconds_text(*bound)).collect();
                    text.push_str(&format!("  buckets: {}\n", bounds.join(" ")));
                }
                MetricKind::Counter | MetricKind::Gauge => {}
            }
            for label in spec.labels {
                text.push_str(&format!(
                    "  label {} ({}): {}\n",
                    label.name.prometheus_name(),
                    label.name.attribute_key(),
                    values_text(label.values),
                ));
            }
        }
        text
    }

    #[test]
    fn the_catalog_is_its_snapshot() {
        let text = catalog_text();
        assert!(
            text == CATALOG_SNAPSHOT,
            "the catalog differs from catalog_snapshot.txt; the catalog text is:\n{text}",
        );
    }

    #[test]
    fn metric_ids_follow_their_declaration_order() {
        for (index, id) in MetricId::ALL.iter().enumerate() {
            assert_eq!(*id as usize, index, "{id:?}");
        }
    }

    /// A Prometheus metric or label name: `[a-zA-Z_][a-zA-Z0-9_]*`.
    fn is_prometheus_name(name: &str) -> bool {
        name.chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
    }

    #[test]
    fn names_are_unique_and_prometheus_names_are_valid() {
        let mut names = HashSet::new();
        let mut prometheus_names = HashSet::new();
        for id in MetricId::ALL {
            let spec = id.spec();
            assert!(names.insert(spec.name), "{}", spec.name);
            let prometheus_name = spec.prometheus_name();
            assert!(is_prometheus_name(&prometheus_name), "{prometheus_name}");
            assert!(
                prometheus_names.insert(prometheus_name.clone()),
                "{prometheus_name}"
            );
            assert!(!spec.help.is_empty(), "{}", spec.name);
            assert!(!spec.help.contains('\n'), "{}", spec.name);
        }
    }

    #[test]
    fn labels_are_unique_per_metric_with_unique_fixed_values() {
        for id in MetricId::ALL {
            let spec = id.spec();
            let mut labels = HashSet::new();
            for label in spec.labels {
                assert!(labels.insert(label.name), "{} {:?}", spec.name, label.name);
                assert!(is_prometheus_name(label.name.prometheus_name()));
                assert!(label.name.prometheus_name() != "le", "{}", spec.name);
                let fixed = match label.values {
                    LabelValues::Fixed(groups) | LabelValues::FixedThenDeclared(groups, _) => {
                        group_texts(groups)
                    }
                    LabelValues::Declared(_) | LabelValues::OneProcessValue(_) => Vec::new(),
                };
                let unique: HashSet<&str> = fixed.iter().copied().collect();
                assert_eq!(unique.len(), fixed.len(), "{} {:?}", spec.name, label.name);
            }
        }
    }

    #[test]
    fn task_ids_host_names_and_messages_are_never_labels() {
        for id in MetricId::ALL {
            for label in id.spec().labels {
                let name = label.name.prometheus_name();
                assert!(
                    !matches!(name, "task_id" | "host" | "hostname" | "message" | "error"),
                    "{} {name}",
                    id.spec().name
                );
            }
        }
    }

    #[test]
    fn bucket_bounds_increase_and_print_as_seconds() {
        for pair in DURATION_BUCKET_BOUNDS.windows(2) {
            assert!(pair[0] < pair[1], "{pair:?}");
        }
        assert_eq!(seconds_text(Duration::from_millis(5)), "0.005");
        assert_eq!(seconds_text(Duration::from_millis(2_500)), "2.5");
        assert_eq!(seconds_text(Duration::from_secs(86_400)), "86400");
        assert_eq!(seconds_text(Duration::from_nanos(1)), "0.000000001");
    }

    #[test]
    fn the_prometheus_name_adds_the_unit_and_the_counter_suffix() {
        assert_eq!(
            MetricId::TaskAttempts.spec().prometheus_name(),
            "horsies_task_attempts_total"
        );
        assert_eq!(
            MetricId::TaskRunDuration.spec().prometheus_name(),
            "horsies_task_run_duration_seconds"
        );
        assert_eq!(
            MetricId::MemoryRss.spec().prometheus_name(),
            "horsies_worker_memory_rss_bytes"
        );
        assert_eq!(
            MetricId::Seats.spec().prometheus_name(),
            "horsies_worker_seats"
        );
    }
}
