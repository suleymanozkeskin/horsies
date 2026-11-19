//! Tests of the declared label sets, the registry and the exposition.

use std::time::Duration;

use crate::telemetry::catalog::{MetricId, Recorder};
use crate::telemetry::declared::{
    DeclaredLookup, DeclaredNames, DeclaredNamesRefused, DeclaredValue, ErrorCodeValue,
    ErrorCodeValues, QueueName, TaskName, MAX_DECLARED_NAMES,
};
use crate::telemetry::exposition::{escape_help, escape_label_value, gauge_text, render};
use crate::telemetry::labels::{
    group_texts, ClaimPassResult, DeclaredSource, LabelName, LabelValue, ERROR_CODE_FIXED,
};
use crate::telemetry::registry::{
    FamilySnapshot, HandleRefused, ProcessFacts, Recorded, Registry, RegistryLabels,
    RegistryRefused, SampleValue, WorkerFacts, MAX_LABELS_PER_METRIC, MAX_SERIES_PER_METRIC,
};
use crate::{
    error_code_label, BuiltInTaskCode, ContractCode, DeclaredErrorCodes, ErrorCodeLabel, Execution,
    OperationalErrorCode, OutcomeCode, RetrievalCode, TaskErrorCode, UserCode, UNDECLARED_LABEL,
};

/// The labels of `horsies.task.retries`.
type RetryLabels = (
    DeclaredValue<TaskName>,
    DeclaredValue<QueueName>,
    ErrorCodeValue,
);

/// The labels of `horsies.task.attempts.end_not_known`.
type EndNotKnownLabels = (DeclaredValue<TaskName>, DeclaredValue<QueueName>, Execution);

/// The labels of a worker with the task names `tasks`, the queue `default`,
/// and the codes that `codes` declares.
fn labels_of<S: AsRef<str>>(tasks: &[S], codes: &DeclaredErrorCodes) -> RegistryLabels {
    RegistryLabels {
        tasks: DeclaredNames::new(tasks.iter().map(|task| task.as_ref().to_owned()))
            .expect("the test task names are a valid set"),
        queues: DeclaredNames::new(["default"]).expect("the test queue is a valid set"),
        handlers: DeclaredNames::none(),
        schedules: DeclaredNames::none(),
        error_codes: ErrorCodeValues::new([codes]).expect("the test codes are within the bound"),
        process: ProcessFacts::Worker(WorkerFacts {
            version: "0.8.0".to_owned(),
            profile: "default".to_owned(),
            worker_id: "worker-1".to_owned(),
        }),
    }
}

/// The series of `metric` at a snapshot of `registry`.
fn family(registry: &Registry, metric: MetricId) -> FamilySnapshot {
    registry
        .snapshot()
        .into_iter()
        .find(|family| family.metric == metric)
        .expect("the registry holds the metric")
}

fn every_built_in_code() -> Vec<BuiltInTaskCode> {
    OperationalErrorCode::ALL
        .into_iter()
        .map(BuiltInTaskCode::from)
        .chain(ContractCode::ALL.into_iter().map(BuiltInTaskCode::from))
        .chain(RetrievalCode::ALL.into_iter().map(BuiltInTaskCode::from))
        .chain(OutcomeCode::ALL.into_iter().map(BuiltInTaskCode::from))
        .collect()
}

// ---------------------------------------------------------------------------
// Declared label sets
// ---------------------------------------------------------------------------

#[test]
fn a_name_outside_the_declared_names_is_the_fallback() {
    let tasks = DeclaredNames::<TaskName>::new(["send_email", "resize_image", "send_email"])
        .expect("a valid set");
    assert_eq!(
        tasks.names().to_vec(),
        vec!["send_email".to_owned(), "resize_image".to_owned()]
    );
    assert_eq!(
        tasks.lookup("resize_image"),
        DeclaredLookup::Member(tasks.value_of("resize_image"))
    );
    assert_eq!(tasks.value_of("resize_image").position(), 1);
    assert_eq!(tasks.lookup("ghost"), DeclaredLookup::Outside);
    assert_eq!(tasks.value_of("ghost"), tasks.fallback());
    assert_eq!(tasks.value_of("ghost").position(), 2);
    assert_eq!(
        tasks.texts(),
        vec![
            "send_email".to_owned(),
            "resize_image".to_owned(),
            "(unregistered)".to_owned()
        ]
    );
}

#[test]
fn declared_names_refuse_an_empty_name_the_fallback_and_too_many() {
    let count = |names: Result<DeclaredNames<QueueName>, DeclaredNamesRefused>| {
        names.map(|names| names.names().len())
    };
    assert_eq!(
        count(DeclaredNames::new([""])),
        Err(DeclaredNamesRefused::EmptyName {
            set: DeclaredSource::QueueNames
        })
    );
    assert_eq!(
        count(DeclaredNames::new(["(unknown)"])),
        Err(DeclaredNamesRefused::FallbackName {
            set: DeclaredSource::QueueNames,
            name: "(unknown)".to_owned(),
        })
    );
    let at_bound = (0..MAX_DECLARED_NAMES).map(|index| format!("queue_{index}"));
    assert_eq!(count(DeclaredNames::new(at_bound)), Ok(MAX_DECLARED_NAMES));
    let past_bound = (0..=MAX_DECLARED_NAMES).map(|index| format!("queue_{index}"));
    assert_eq!(
        count(DeclaredNames::new(past_bound)),
        Err(DeclaredNamesRefused::TooMany {
            set: DeclaredSource::QueueNames,
            count: MAX_DECLARED_NAMES + 1,
            bound: MAX_DECLARED_NAMES,
        })
    );
}

#[test]
fn error_code_values_hold_no_code_the_built_in_the_declared_and_undeclared() {
    let first = DeclaredErrorCodes::parse(["RATE_LIMITED", "CARD_DECLINED"]).expect("a valid set");
    let second = DeclaredErrorCodes::parse(["CARD_DECLINED", "HTTP2_FAILED"]).expect("a valid set");
    let values = ErrorCodeValues::new([&first, &second]).expect("within the bound");
    assert_eq!(
        values.declared().to_vec(),
        vec![
            "CARD_DECLINED".to_owned(),
            "HTTP2_FAILED".to_owned(),
            "RATE_LIMITED".to_owned()
        ]
    );
    let texts: Vec<String> = group_texts(ERROR_CODE_FIXED)
        .into_iter()
        .map(str::to_owned)
        .chain(values.declared().iter().cloned())
        .chain([UNDECLARED_LABEL.to_owned()])
        .collect();
    let text_of = |value: ErrorCodeValue| texts[value.position() as usize].clone();

    assert_eq!(text_of(ErrorCodeValue::NO_ERROR_CODE), "");
    for code in every_built_in_code() {
        assert_eq!(
            text_of(values.value_of(ErrorCodeLabel::BuiltIn(&code))),
            code.value()
        );
    }
    let declared = TaskErrorCode::User(UserCode::parse("HTTP2_FAILED").expect("a user code"));
    assert_eq!(
        text_of(values.value_of(error_code_label(&second, &declared))),
        "HTTP2_FAILED"
    );
    assert_eq!(
        text_of(values.value_of(ErrorCodeLabel::Undeclared)),
        UNDECLARED_LABEL
    );
    // A task set that these values were not given: its code is undeclared.
    let not_given = DeclaredErrorCodes::parse(["NOT_GIVEN"]).expect("a valid set");
    let code = TaskErrorCode::User(UserCode::parse("NOT_GIVEN").expect("a user code"));
    assert_eq!(
        text_of(values.value_of(error_code_label(&not_given, &code))),
        UNDECLARED_LABEL
    );
    assert_eq!(
        ErrorCodeValues::none()
            .value_of(ErrorCodeLabel::Undeclared)
            .position() as usize,
        group_texts(ERROR_CODE_FIXED).len()
    );
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[test]
fn every_metric_of_the_catalog_fits_a_series_key() {
    for metric in MetricId::ALL {
        assert!(
            metric.spec().labels.len() <= MAX_LABELS_PER_METRIC,
            "{}",
            metric.spec().name
        );
    }
}

#[test]
fn a_new_series_is_made_and_an_existing_series_adds() {
    let registry = Registry::new(
        &[MetricId::ClaimPasses],
        labels_of(&["a"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let passes = registry
        .counter::<(ClaimPassResult,)>(MetricId::ClaimPasses)
        .expect("the handle of the metric");
    assert_eq!(passes.add((ClaimPassResult::Claimed,), 2), Recorded::Added);
    assert_eq!(passes.add((ClaimPassResult::Claimed,), 1), Recorded::Added);
    let series = family(&registry, MetricId::ClaimPasses).series;
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].label_texts, vec!["claimed".to_owned()]);
    assert_eq!(series[0].value, SampleValue::Counter(3));
}

#[test]
fn a_new_series_past_the_series_bound_is_dropped_and_counted() {
    let names: Vec<String> = (0..=MAX_SERIES_PER_METRIC)
        .map(|index| format!("task_{index}"))
        .collect();
    let registry = Registry::new(
        &[MetricId::TaskAttemptsEndNotKnown],
        labels_of(&names, &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let ended = registry
        .counter::<EndNotKnownLabels>(MetricId::TaskAttemptsEndNotKnown)
        .expect("the handle of the metric");
    let labels = registry.labels();
    let default = labels.queues.value_of("default");
    for name in &names[..MAX_SERIES_PER_METRIC] {
        let task = labels.tasks.value_of(name);
        assert_eq!(
            ended.add((task, default, Execution::Isolated), 1),
            Recorded::Added
        );
    }
    let last = labels.tasks.value_of(&names[MAX_SERIES_PER_METRIC]);
    for _ in 0..2 {
        assert_eq!(
            ended.add((last, default, Execution::Isolated), 1),
            Recorded::DroppedAtSeriesBound
        );
    }
    assert_eq!(
        registry.series_dropped(MetricId::TaskAttemptsEndNotKnown),
        2
    );
    let first = labels.tasks.value_of(&names[0]);
    assert_eq!(
        ended.add((first, default, Execution::Isolated), 1),
        Recorded::Added
    );
    assert_eq!(
        family(&registry, MetricId::TaskAttemptsEndNotKnown)
            .series
            .len(),
        MAX_SERIES_PER_METRIC
    );
    assert!(render(&registry).contains(
        "horsies_telemetry_series_dropped_total{metric=\"horsies.task.attempts.end_not_known\"} 2\n"
    ));
}

#[test]
fn a_value_of_another_registry_s_names_is_outside_the_label_set() {
    let other = DeclaredNames::<TaskName>::new(["a", "b", "c"]).expect("a valid set");
    let registry = Registry::new(
        &[MetricId::TaskAttemptsEndNotKnown],
        labels_of(&["a"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let ended = registry
        .counter::<EndNotKnownLabels>(MetricId::TaskAttemptsEndNotKnown)
        .expect("the handle of the metric");
    let default = registry.labels().queues.value_of("default");
    // "c" is at position 2; the task label of the registry has "a" and the
    // fallback.
    assert_eq!(
        ended.add((other.value_of("c"), default, Execution::Isolated), 1),
        Recorded::OutsideLabelSet {
            label: LabelName::Task
        }
    );
    assert!(family(&registry, MetricId::TaskAttemptsEndNotKnown)
        .series
        .is_empty());
    assert_eq!(
        registry.series_dropped(MetricId::TaskAttemptsEndNotKnown),
        0
    );
}

#[test]
fn a_duration_counts_in_the_first_bucket_whose_bound_holds_it() {
    let registry = Registry::new(
        &[MetricId::TaskRecordDuration],
        labels_of(&["a"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let record = registry
        .histogram::<(Execution,)>(MetricId::TaskRecordDuration)
        .expect("the handle of the metric");
    let one_day = Duration::from_secs(86_400);
    for duration in [
        Duration::from_millis(5),
        Duration::from_millis(5) + Duration::from_nanos(1),
        one_day,
        one_day + Duration::from_nanos(1),
    ] {
        assert_eq!(
            record.observe((Execution::Isolated,), duration),
            Recorded::Added
        );
    }
    let mut buckets = vec![0_u64; 22];
    buckets[0] = 1;
    buckets[1] = 1;
    buckets[20] = 1;
    buckets[21] = 1;
    assert_eq!(
        family(&registry, MetricId::TaskRecordDuration).series[0].value,
        SampleValue::Histogram {
            buckets,
            sum_micros: 5_000 + 5_000 + 86_400_000_000 + 86_400_000_000,
        }
    );
}

#[test]
fn a_gauge_holds_the_last_value_set() {
    let registry = Registry::new(
        &[MetricId::Seats],
        labels_of(&["a"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let seats = registry
        .gauge::<(Execution,)>(MetricId::Seats)
        .expect("the handle of the metric");
    assert_eq!(seats.set((Execution::SharedAsync,), 16.0), Recorded::Added);
    assert_eq!(seats.set((Execution::SharedAsync,), 8.0), Recorded::Added);
    assert_eq!(
        family(&registry, MetricId::Seats).series[0].value,
        SampleValue::Gauge(8.0)
    );
}

#[test]
fn a_handle_needs_the_metric_its_kind_and_its_label_sets() {
    let registry = Registry::new(
        &[MetricId::Seats, MetricId::ClaimPasses],
        labels_of(&["a"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    assert!(matches!(
        registry.counter::<(ClaimPassResult,)>(MetricId::TaskRetries),
        Err(HandleRefused::NotInRegistry {
            metric: MetricId::TaskRetries
        })
    ));
    assert!(matches!(
        registry.counter::<(Execution,)>(MetricId::Seats),
        Err(HandleRefused::KindDiffers {
            metric: MetricId::Seats
        })
    ));
    assert!(matches!(
        registry.counter::<(Execution,)>(MetricId::ClaimPasses),
        Err(HandleRefused::LabelsDiffer {
            metric: MetricId::ClaimPasses
        })
    ));
    assert!(matches!(
        registry.counter::<(MetricId,)>(MetricId::SeriesDropped),
        Err(HandleRefused::CountedByTheRegistry {
            metric: MetricId::SeriesDropped
        })
    ));
    assert!(registry
        .counter::<(ClaimPassResult,)>(MetricId::ClaimPasses)
        .is_ok());
}

#[test]
fn the_worker_registry_holds_the_worker_metrics_and_its_info_series_is_one() {
    let registry = Registry::for_worker(labels_of(&["a"], &DeclaredErrorCodes::NONE))
        .expect("the registry builds");
    let held: Vec<MetricId> = registry
        .snapshot()
        .iter()
        .map(|family| family.metric)
        .collect();
    let worker: Vec<MetricId> = MetricId::ALL
        .into_iter()
        .filter(|metric| metric.spec().recorder == Recorder::Worker)
        .collect();
    assert_eq!(held, worker);
    assert!(render(&registry).contains(
        "horsies_worker_info{version=\"0.8.0\",profile=\"default\",worker_id=\"worker-1\"} 1\n"
    ));
}

#[test]
fn a_registry_of_another_process_refuses_a_worker_fact_label() {
    let mut labels = labels_of(&["a"], &DeclaredErrorCodes::NONE);
    labels.process = ProcessFacts::NotAWorker;
    assert_eq!(
        Registry::new(&[MetricId::WorkerInfo], labels.clone()).err(),
        Some(RegistryRefused::NoWorkerFacts {
            metric: MetricId::WorkerInfo
        })
    );
    let registry = Registry::new(&[MetricId::SchemaVersion], labels).expect("the registry builds");
    let version = registry
        .gauge::<()>(MetricId::SchemaVersion)
        .expect("the handle of the metric");
    assert_eq!(version.set((), 61.0), Recorded::Added);
    assert!(render(&registry).contains("\nhorsies_schema_version 61\n"));
}

#[test]
fn concurrent_samples_are_counted_exactly() {
    const THREADS: usize = 8;
    const SAMPLES_PER_THREAD: u64 = 10_000;
    let names: Vec<String> = (0..THREADS).map(|index| format!("task_{index}")).collect();
    let registry = Registry::new(
        &[
            MetricId::TaskAttemptsEndNotKnown,
            MetricId::TaskSlotWaitDuration,
        ],
        labels_of(&names, &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let ended = registry
        .counter::<EndNotKnownLabels>(MetricId::TaskAttemptsEndNotKnown)
        .expect("the handle of the metric");
    let slot_wait = registry
        .histogram::<(Execution,)>(MetricId::TaskSlotWaitDuration)
        .expect("the handle of the metric");
    let labels = registry.labels();
    let default = labels.queues.value_of("default");
    let shared = labels.tasks.value_of(&names[0]);
    std::thread::scope(|scope| {
        for name in &names {
            let own = labels.tasks.value_of(name);
            let ended = ended.clone();
            let slot_wait = slot_wait.clone();
            let _thread = scope.spawn(move || {
                for _ in 0..SAMPLES_PER_THREAD {
                    assert_eq!(
                        ended.add((own, default, Execution::Isolated), 1),
                        Recorded::Added
                    );
                    assert_eq!(
                        ended.add((shared, default, Execution::SharedThread), 1),
                        Recorded::Added
                    );
                    assert_eq!(
                        slot_wait.observe((Execution::Isolated,), Duration::from_millis(1)),
                        Recorded::Added
                    );
                }
            });
        }
    });
    let all_samples = THREADS as u64 * SAMPLES_PER_THREAD;
    let series = family(&registry, MetricId::TaskAttemptsEndNotKnown).series;
    assert_eq!(series.len(), THREADS + 1);
    for one in &series {
        let expected = match one.label_texts[2].as_str() {
            "SHARED_THREAD" => all_samples,
            _ => SAMPLES_PER_THREAD,
        };
        assert_eq!(
            one.value,
            SampleValue::Counter(expected),
            "{:?}",
            one.label_texts
        );
    }
    let mut buckets = vec![0_u64; 22];
    buckets[0] = all_samples;
    assert_eq!(
        family(&registry, MetricId::TaskSlotWaitDuration).series[0].value,
        SampleValue::Histogram {
            buckets,
            sum_micros: all_samples * 1_000,
        }
    );
}

// ---------------------------------------------------------------------------
// Exposition
// ---------------------------------------------------------------------------

/// The exposition of [`golden_registry`].
const GOLDEN: &str = r#"# HELP horsies_task_retries_total Attempts whose outcome write scheduled a retry, by error code.
# TYPE horsies_task_retries_total counter
horsies_task_retries_total{task="(unregistered)",queue="default",error_code="undeclared"} 1
horsies_task_retries_total{task="resize_image",queue="default",error_code="TASK_TIMEOUT"} 1
horsies_task_retries_total{task="send_email",queue="default",error_code="CARD_DECLINED"} 2
# HELP horsies_task_slot_wait_duration_seconds Time from the claim result in the worker to the placement of the attempt on a child.
# TYPE horsies_task_slot_wait_duration_seconds histogram
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.005"} 1
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.01"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.025"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.05"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.1"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.25"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="0.5"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="1"} 2
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="2.5"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="5"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="10"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="30"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="60"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="120"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="300"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="600"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="1800"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="3600"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="7200"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="21600"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="86400"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="ISOLATED",le="+Inf"} 3
horsies_task_slot_wait_duration_seconds_sum{execution="ISOLATED"} 2.01
horsies_task_slot_wait_duration_seconds_count{execution="ISOLATED"} 3
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.005"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.01"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.025"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.05"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.1"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.25"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="0.5"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="1"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="2.5"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="5"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="10"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="30"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="60"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="120"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="300"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="600"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="1800"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="3600"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="7200"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="21600"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="86400"} 0
horsies_task_slot_wait_duration_seconds_bucket{execution="SHARED_ASYNC",le="+Inf"} 1
horsies_task_slot_wait_duration_seconds_sum{execution="SHARED_ASYNC"} 172800
horsies_task_slot_wait_duration_seconds_count{execution="SHARED_ASYNC"} 1
# HELP horsies_telemetry_series_dropped_total Samples that were dropped because their metric holds its greatest number of series, by metric.
# TYPE horsies_telemetry_series_dropped_total counter
horsies_telemetry_series_dropped_total{metric="horsies.task.retries"} 0
horsies_telemetry_series_dropped_total{metric="horsies.task.slot_wait.duration"} 0
horsies_telemetry_series_dropped_total{metric="horsies.worker.info"} 0
horsies_telemetry_series_dropped_total{metric="horsies.worker.seats"} 0
# HELP horsies_worker_info Always 1. The labels hold the version, the profile and the id of the worker.
# TYPE horsies_worker_info gauge
horsies_worker_info{version="0.8.0",profile="default",worker_id="worker-1"} 1
# HELP horsies_worker_seats Seats that the worker runs, by execution class.
# TYPE horsies_worker_seats gauge
horsies_worker_seats{execution="ISOLATED"} 4
horsies_worker_seats{execution="SHARED_ASYNC"} 16
"#;

/// A worker registry of four metrics with samples in each.
fn golden_registry() -> Registry {
    let declared = DeclaredErrorCodes::parse(["CARD_DECLINED"]).expect("a valid set");
    let registry = Registry::new(
        &[
            MetricId::TaskRetries,
            MetricId::TaskSlotWaitDuration,
            MetricId::Seats,
            MetricId::WorkerInfo,
        ],
        labels_of(&["send_email", "resize_image"], &declared),
    )
    .expect("the registry builds");
    let retries = registry
        .counter::<RetryLabels>(MetricId::TaskRetries)
        .expect("the handle of the metric");
    let slot_wait = registry
        .histogram::<(Execution,)>(MetricId::TaskSlotWaitDuration)
        .expect("the handle of the metric");
    let seats = registry
        .gauge::<(Execution,)>(MetricId::Seats)
        .expect("the handle of the metric");
    let labels = registry.labels();
    let default = labels.queues.value_of("default");
    let card_code = TaskErrorCode::User(UserCode::parse("CARD_DECLINED").expect("a user code"));
    let card = labels
        .error_codes
        .value_of(error_code_label(&declared, &card_code));
    let timeout_code = BuiltInTaskCode::Outcome(OutcomeCode::TaskTimeout);
    let timeout = labels
        .error_codes
        .value_of(ErrorCodeLabel::BuiltIn(&timeout_code));
    let undeclared = labels.error_codes.value_of(ErrorCodeLabel::Undeclared);
    let samples = [
        retries.add((labels.tasks.value_of("send_email"), default, card), 2),
        retries.add((labels.tasks.value_of("resize_image"), default, timeout), 1),
        retries.add((labels.tasks.value_of("ghost"), default, undeclared), 1),
        slot_wait.observe((Execution::Isolated,), Duration::from_millis(3)),
        slot_wait.observe((Execution::Isolated,), Duration::from_millis(7)),
        slot_wait.observe((Execution::Isolated,), Duration::from_secs(2)),
        slot_wait.observe((Execution::SharedAsync,), Duration::from_secs(2 * 86_400)),
        seats.set((Execution::Isolated,), 4.0),
        seats.set((Execution::SharedAsync,), 16.0),
    ];
    assert!(
        samples.iter().all(|recorded| *recorded == Recorded::Added),
        "{samples:?}"
    );
    registry
}

#[test]
fn the_exposition_is_the_golden_text() {
    let text = render(&golden_registry());
    assert!(
        text == GOLDEN,
        "the exposition differs from the golden text; it is:\n{text}"
    );
}

#[test]
fn the_same_values_give_the_same_text_in_any_order() {
    let names = ["b", "a", "c"];
    let render_after = |order: &[&str]| -> String {
        let registry = Registry::new(
            &[MetricId::TaskAttemptsEndNotKnown],
            labels_of(&names, &DeclaredErrorCodes::NONE),
        )
        .expect("the registry builds");
        let ended = registry
            .counter::<EndNotKnownLabels>(MetricId::TaskAttemptsEndNotKnown)
            .expect("the handle of the metric");
        let labels = registry.labels();
        let default = labels.queues.value_of("default");
        for name in order {
            assert_eq!(
                ended.add(
                    (labels.tasks.value_of(name), default, Execution::Isolated),
                    1
                ),
                Recorded::Added
            );
        }
        let first = render(&registry);
        assert_eq!(render(&registry), first);
        first
    };
    let forward = render_after(&["a", "b", "c"]);
    assert_eq!(render_after(&["c", "a", "b"]), forward);
    let a = forward.find("task=\"a\"").expect("the series of a");
    let b = forward.find("task=\"b\"").expect("the series of b");
    let c = forward.find("task=\"c\"").expect("the series of c");
    assert!(a < b && b < c, "{forward}");
}

#[test]
fn label_values_and_help_texts_are_escaped() {
    assert_eq!(escape_label_value("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
    assert_eq!(escape_help("a\\b\"c\nd"), "a\\\\b\"c\\nd");
    let registry = Registry::new(
        &[MetricId::TaskAttemptsEndNotKnown],
        labels_of(&["say \"hi\"\\\n"], &DeclaredErrorCodes::NONE),
    )
    .expect("the registry builds");
    let ended = registry
        .counter::<EndNotKnownLabels>(MetricId::TaskAttemptsEndNotKnown)
        .expect("the handle of the metric");
    let labels = registry.labels();
    let task = labels.tasks.value_of("say \"hi\"\\\n");
    let default = labels.queues.value_of("default");
    assert_eq!(
        ended.add((task, default, Execution::Isolated), 1),
        Recorded::Added
    );
    assert!(render(&registry).contains(
        r#"horsies_task_attempts_end_not_known_total{task="say \"hi\"\\\n",queue="default",execution="ISOLATED"} 1"#
    ));
}

#[test]
fn gauge_values_print_in_the_prometheus_form() {
    assert_eq!(gauge_text(f64::NAN), "NaN");
    assert_eq!(gauge_text(f64::INFINITY), "+Inf");
    assert_eq!(gauge_text(f64::NEG_INFINITY), "-Inf");
    assert_eq!(gauge_text(4.0), "4");
    assert_eq!(gauge_text(0.25), "0.25");
    assert_eq!(gauge_text(1e21), "1000000000000000000000");
}
