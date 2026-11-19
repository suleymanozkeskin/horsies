//! The claim pass with classes on scripted database steps (Python
//! `clients/python/tests/unit/test_claim_plan.py`: `TestRowClass`,
//! `TestClaimPassWithClasses`; `clients/python/tests/unit/test_worker_helpers.py`:
//! `TestClaimPass`). The statements themselves are tested on the test
//! database in `broker::lifecycle_tests::claim_start_release`.

use std::num::NonZeroU32;
use std::sync::Mutex;

use chrono::{Duration, TimeZone};

use super::*;
use crate::broker::claims::ClaimClasses;

const ISOLATED: Execution = Execution::Isolated;
const SHARED_ASYNC: Execution = Execution::SharedAsync;
const SHARED_THREAD: Execution = Execution::SharedThread;

fn task(name: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
}

/// The claim generation of the row named `t<n>`: one per task.
fn claim_time(name: &str) -> DateTime<Utc> {
    let minutes: i64 = name
        .trim_start_matches(|character: char| !character.is_ascii_digit())
        .parse()
        .expect("a test row name ends with its number");
    Utc.with_ymd_and_hms(2026, 11, 10, 9, 0, 0)
        .single()
        .expect("a valid test instant")
        + Duration::minutes(minutes)
}

fn row(name: &str, task_name: &str, execution: Execution) -> ClaimedRow {
    ClaimedRow {
        id: task(name),
        task_name: task_name.to_owned(),
        queue_name: "q".to_owned(),
        input: "{}".to_owned(),
        task_options: None,
        execution,
        is_workflow_task: false,
        claimed_at: claim_time(name),
        retry_count: 0,
        max_retries: 0,
        trace: crate::events::RunTrace::NoCarrier,
        priority: crate::events::Priority::try_from(100).unwrap(),
        enqueued_at: claim_time(name),
        read: crate::broker::claims::ClaimRead::now(),
    }
}

fn workflow_row(name: &str) -> ClaimedRow {
    ClaimedRow {
        is_workflow_task: true,
        ..row(name, "node_task", ISOLATED)
    }
}

fn registered(classes: &[(&str, Execution)]) -> RegisteredClasses {
    RegisteredClasses::new(
        classes
            .iter()
            .map(|(name, class)| ((*name).to_owned(), *class))
            .collect(),
    )
}

fn classes(budgets: &[(Execution, u32)], routes: &[(Execution, Execution)]) -> ClaimClasses {
    ClaimClasses::new(
        budgets.iter().copied().collect(),
        routes.iter().copied().collect(),
    )
    .expect("every test route names a held budget")
}

/// The classes of a worker of one isolated child (Python
/// `isolated_children_plan` with one slot).
fn one_isolated_child() -> ClaimClasses {
    classes(
        &[(ISOLATED, 1)],
        &[
            (ISOLATED, ISOLATED),
            (SHARED_ASYNC, ISOLATED),
            (SHARED_THREAD, ISOLATED),
        ],
    )
}

fn request(classes: ClaimClasses) -> ClaimPassRequest {
    ClaimPassRequest {
        worker_id: "worker-1".to_owned(),
        queues: vec!["q".to_owned()],
        queue_priority: HashMap::new(),
        queue_max_concurrency: HashMap::new(),
        hard_cap_mode: true,
        classes,
        prefetch_buffer: 0,
        max_claim_per_worker: 0,
        max_claim_batch: 0,
        cluster_wide_cap: None,
        claim_lease_ms: NonZeroU32::new(60_000).expect("a lease above 0"),
        lock_keys: Vec::new(),
    }
}

fn short_by(rows: u32) -> ClaimTake {
    ClaimTake::Short {
        short_by: NonZeroU32::new(rows).expect("a short take is short by 1 or more"),
    }
}

/// Database steps from a script: the claim answers `pass`; the workflow
/// filter drops `non_runnable`; the release answers `released` for the ids
/// it holds and `NotHeld` for the others. Each call is recorded.
struct ScriptedSteps {
    pass: ClaimPass,
    non_runnable: Vec<Uuid>,
    released: Vec<Uuid>,
    requests: Mutex<Vec<ClaimClasses>>,
    filtered: Mutex<Vec<Vec<Uuid>>>,
    returned: Mutex<Vec<Vec<(Uuid, DateTime<Utc>)>>>,
}

impl ScriptedSteps {
    fn new(claimed: Vec<ClaimedRow>, take: ClaimTake) -> ScriptedSteps {
        ScriptedSteps {
            pass: ClaimPass { claimed, take },
            non_runnable: Vec::new(),
            released: Vec::new(),
            requests: Mutex::new(Vec::new()),
            filtered: Mutex::new(Vec::new()),
            returned: Mutex::new(Vec::new()),
        }
    }

    fn returned(&self) -> Vec<Vec<(Uuid, DateTime<Utc>)>> {
        self.returned.lock().expect("record lock").clone()
    }

    fn filtered(&self) -> Vec<Vec<Uuid>> {
        self.filtered.lock().expect("record lock").clone()
    }
}

impl ClaimPassSteps for ScriptedSteps {
    async fn claim(&self, request: &ClaimPassRequest) -> Result<ClaimPass, BrokerError> {
        self.requests
            .lock()
            .expect("record lock")
            .push(request.classes.clone());
        Ok(self.pass.clone())
    }

    async fn non_runnable_workflow_tasks(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> Result<Vec<Uuid>, BrokerError> {
        self.filtered
            .lock()
            .expect("record lock")
            .push(claims.iter().map(|(task_id, _)| *task_id).collect());
        Ok(self.non_runnable.clone())
    }

    async fn return_as_isolated(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
        self.returned
            .lock()
            .expect("record lock")
            .push(claims.to_vec());
        Ok(claims
            .iter()
            .map(|(task_id, _)| {
                let outcome = match self.released.contains(task_id) {
                    true => ReleaseOutcome::Released,
                    false => ReleaseOutcome::NotHeld,
                };
                (*task_id, outcome)
            })
            .collect())
    }
}

fn ids(rows: &[ClaimedRow]) -> Vec<Uuid> {
    rows.iter().map(|row| row.id).collect()
}

fn counts(claimed: usize, dropped: usize, take: ClaimTake) -> ClaimPassCounts {
    ClaimPassCounts {
        claimed,
        dropped,
        take,
    }
}

// ---------------------------------------------------------------------------
// TestRowClass
// ---------------------------------------------------------------------------

fn three_classes() -> RegisteredClasses {
    registered(&[
        ("iso", ISOLATED),
        ("async_task", SHARED_ASYNC),
        ("thread_task", SHARED_THREAD),
    ])
}

#[test]
fn an_isolated_row_fits_any_function() {
    for task_name in ["iso", "async_task", "thread_task"] {
        assert_eq!(
            check_row_class(ISOLATED, task_name, &three_classes()),
            RowClassCheck::Fits,
            "{task_name}"
        );
    }
}

#[test]
fn a_shared_row_fits_a_function_of_its_class() {
    for (row_class, task_name) in [(SHARED_ASYNC, "async_task"), (SHARED_THREAD, "thread_task")] {
        assert_eq!(
            check_row_class(row_class, task_name, &three_classes()),
            RowClassCheck::Fits,
            "{task_name}"
        );
    }
}

#[test]
fn a_shared_row_of_another_class_is_a_mismatch() {
    for (row_class, task_name, registered_class) in [
        (SHARED_ASYNC, "iso", ISOLATED),
        (SHARED_ASYNC, "thread_task", SHARED_THREAD),
        (SHARED_THREAD, "iso", ISOLATED),
        (SHARED_THREAD, "async_task", SHARED_ASYNC),
    ] {
        assert_eq!(
            check_row_class(row_class, task_name, &three_classes()),
            RowClassCheck::Mismatch {
                row_class,
                registered_class
            },
            "{task_name}"
        );
    }
}

#[test]
fn an_unknown_task_is_not_registered() {
    for row_class in [ISOLATED, SHARED_ASYNC, SHARED_THREAD] {
        assert_eq!(
            check_row_class(row_class, "other", &three_classes()),
            RowClassCheck::TaskNotRegistered,
            "{row_class:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// TestClaimPassWithClasses
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_pass_sends_the_budgets_and_the_routes() {
    let steps = ScriptedSteps::new(Vec::new(), ClaimTake::Full);

    let _claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &RegisteredClasses::default(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(
        *steps.requests.lock().expect("record lock"),
        vec![one_isolated_child()]
    );
}

#[tokio::test]
async fn a_mismatched_row_goes_back_as_isolated() {
    let mut steps = ScriptedSteps::new(
        vec![
            row("t1", "now_def", SHARED_ASYNC),
            row("t2", "iso", ISOLATED),
        ],
        ClaimTake::Full,
    );
    steps.released = vec![task("t1")];

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &registered(&[("now_def", ISOLATED), ("iso", ISOLATED)]),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(2, 1, ClaimTake::Full));
    assert_eq!(ids(&claimed.rows), vec![task("t2")]);
    assert_eq!(steps.returned(), vec![vec![(task("t1"), claim_time("t1"))]]);
}

#[tokio::test]
async fn a_lost_claim_is_neither_returned_nor_dispatched() {
    let steps = ScriptedSteps::new(vec![row("t1", "now_def", SHARED_THREAD)], ClaimTake::Full);

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &registered(&[("now_def", ISOLATED)]),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(1, 1, ClaimTake::Full));
    assert!(claimed.rows.is_empty());
    // The release found the row not held: it wrote nothing and sent no
    // class wake (the wake is per queue of a released row).
    assert_eq!(steps.returned(), vec![vec![(task("t1"), claim_time("t1"))]]);
}

#[tokio::test]
async fn rows_that_fit_or_are_unknown_are_dispatched() {
    let steps = ScriptedSteps::new(
        vec![
            row("t1", "awaited", SHARED_ASYNC),
            row("t2", "awaited", ISOLATED),
            row("t3", "not_here", SHARED_THREAD),
        ],
        ClaimTake::Full,
    );

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &registered(&[("awaited", SHARED_ASYNC)]),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(3, 0, ClaimTake::Full));
    assert!(steps.returned().is_empty());
    assert_eq!(ids(&claimed.rows), vec![task("t1"), task("t2"), task("t3")]);
}

// ---------------------------------------------------------------------------
// The claim and release points
// ---------------------------------------------------------------------------

/// The type of each claim event and its task, in order.
fn claim_events(events: &[crate::events::Event]) -> Vec<(String, Uuid)> {
    use crate::events::Event;
    events
        .iter()
        .map(|event| match event {
            Event::TaskClaimed(claimed) => ("claimed".to_owned(), claimed.task_id),
            Event::ClaimReleased(released) => {
                (format!("released {:?}", released.reason), released.task_id)
            }
            other => panic!("the claim pass emits no {other:?}"),
        })
        .collect()
}

/// The count of each series of a histogram, by its label texts.
fn histogram_counts(
    series: Vec<(Vec<String>, crate::telemetry::registry::SampleValue)>,
) -> Vec<(Vec<String>, u64)> {
    use crate::telemetry::registry::SampleValue;
    series
        .into_iter()
        .map(|(labels, value)| match value {
            SampleValue::Histogram { buckets, .. } => (labels, buckets.iter().sum()),
            other => panic!("a histogram series holds no {other:?}"),
        })
        .collect()
}

/// Each row that the claim statement returned is claimed once, in claim
/// order; then each row that the pass returned to the queue is released
/// once with its reason. A row whose claim was lost before its return is
/// claimed and not released.
#[tokio::test]
async fn each_claimed_row_is_claimed_once_and_each_returned_row_released_once() {
    use crate::telemetry::catalog::MetricId;
    use crate::telemetry::registry::SampleValue;

    let mut steps = ScriptedSteps::new(
        vec![
            row("t1", "now_def", SHARED_ASYNC),
            row("t2", "iso", ISOLATED),
            workflow_row("t3"),
            row("t4", "now_def", SHARED_THREAD),
        ],
        ClaimTake::Full,
    );
    steps.non_runnable = vec![task("t3")];
    steps.released = vec![task("t1")];
    let mut watched = crate::test_support::telemetry::WatchedTelemetry::new();

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &registered(&[("now_def", ISOLATED), ("iso", ISOLATED)]),
        &watched.telemetry,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(ids(&claimed.rows), vec![task("t2")]);
    let events = watched.events();
    assert_eq!(
        claim_events(&events),
        vec![
            ("claimed".to_owned(), task("t1")),
            ("claimed".to_owned(), task("t2")),
            ("claimed".to_owned(), task("t3")),
            ("claimed".to_owned(), task("t4")),
            ("released WorkflowNotRunnable".to_owned(), task("t3")),
            ("released ReturnedAsIsolated".to_owned(), task("t1")),
        ]
    );
    match &events[1] {
        crate::events::Event::TaskClaimed(claimed) => {
            assert_eq!(claimed.attempt.get(), 1);
            assert_eq!(claimed.priority.get(), 100);
            assert_eq!(claimed.queue_wait, std::time::Duration::ZERO);
            assert_eq!(claimed.execution, ISOLATED);
            assert_eq!(claimed.task_name.as_str(), "iso");
        }
        other => panic!("the second event claims t2, not {other:?}"),
    }
    assert_eq!(
        watched.series(MetricId::TaskClaimsReleased),
        vec![
            (
                vec!["returned_as_isolated".to_owned()],
                SampleValue::Counter(1)
            ),
            (
                vec!["workflow_not_runnable".to_owned()],
                SampleValue::Counter(1)
            ),
        ]
    );
    assert_eq!(
        histogram_counts(watched.series(MetricId::TaskQueueDuration)),
        vec![
            (vec!["(unknown)".to_owned(), "ISOLATED".to_owned()], 2),
            (vec!["(unknown)".to_owned(), "SHARED_ASYNC".to_owned()], 1),
            (vec!["(unknown)".to_owned(), "SHARED_THREAD".to_owned()], 1),
        ]
    );
}

/// With telemetry off, a claim pass with claims and releases records no
/// metric and queues no event.
#[tokio::test]
async fn with_telemetry_off_a_claim_pass_emits_nothing() {
    use crate::telemetry::catalog::MetricId;

    let mut steps = ScriptedSteps::new(
        vec![row("t1", "now_def", SHARED_ASYNC), workflow_row("t3")],
        ClaimTake::Full,
    );
    steps.non_runnable = vec![task("t3")];
    steps.released = vec![task("t1")];
    let mut watched = crate::test_support::telemetry::WatchedTelemetry::new();

    let _claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &registered(&[("now_def", ISOLATED)]),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert!(watched.events().is_empty());
    assert!(watched.series(MetricId::TaskClaimsReleased).is_empty());
    assert!(watched.series(MetricId::TaskQueueDuration).is_empty());
}

// ---------------------------------------------------------------------------
// TestClaimPass
// ---------------------------------------------------------------------------

#[test]
fn it_states_when_work_can_remain_unannounced() {
    use ClaimBatchLimit::{CapsThePass, NoCap};
    for (pass, limit, expected) in [
        (counts(0, 0, ClaimTake::Full), NoCap, false),
        (counts(0, 0, ClaimTake::Full), CapsThePass, false),
        (counts(3, 0, ClaimTake::Full), NoCap, false),
        (counts(3, 0, ClaimTake::Full), CapsThePass, true),
        (counts(3, 1, ClaimTake::Full), NoCap, true),
        (counts(3, 3, ClaimTake::Full), NoCap, true),
        (counts(3, 3, ClaimTake::Full), CapsThePass, true),
        (counts(0, 0, short_by(2)), NoCap, true),
        (counts(1, 0, short_by(1)), NoCap, true),
    ] {
        assert_eq!(
            pass.left_unannounced_work(limit, 0),
            expected,
            "{pass:?} {limit:?}"
        );
    }
}

#[test]
fn a_short_take_asks_for_a_bounded_run_of_passes() {
    let pass = counts(0, 0, short_by(1));
    for (short_passes_before, expected) in [
        (0, true),
        (SHORT_PASS_REPASS_LIMIT - 1, true),
        (SHORT_PASS_REPASS_LIMIT, false),
    ] {
        assert_eq!(
            pass.left_unannounced_work(ClaimBatchLimit::NoCap, short_passes_before),
            expected,
            "{short_passes_before}"
        );
    }
}

#[test]
fn short_passes_count_the_run_that_ends_with_this_pass() {
    assert_eq!(counts(0, 0, short_by(1)).short_passes_after(2), 3);
    assert_eq!(counts(2, 0, ClaimTake::Full).short_passes_after(2), 0);
}

#[tokio::test]
async fn the_pass_counts_what_it_claimed_and_dropped() {
    for (kept, dropped) in [(3, 0), (1, 2), (0, 3)] {
        let rows = vec![workflow_row("t1"), workflow_row("t2"), workflow_row("t3")];
        let mut steps = ScriptedSteps::new(rows.clone(), ClaimTake::Full);
        steps.non_runnable = ids(&rows[kept..]);

        let claimed = claim_for_dispatch(
            &steps,
            &request(one_isolated_child()),
            &RegisteredClasses::default(),
            &WorkerTelemetry::Off,
        )
        .await
        .expect("the scripted pass");

        assert_eq!(
            claimed.counts,
            counts(3, dropped, ClaimTake::Full),
            "{kept} kept"
        );
        assert_eq!(ids(&claimed.rows), ids(&rows[..kept]), "{kept} kept");
        assert_eq!(steps.filtered(), vec![ids(&rows)]);
    }
}

#[tokio::test]
async fn a_short_take_is_reported_with_the_rows_it_took() {
    let steps = ScriptedSteps::new(vec![row("t1", "plain", ISOLATED)], short_by(2));

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &RegisteredClasses::default(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(1, 0, short_by(2)));
    assert_eq!(ids(&claimed.rows), vec![task("t1")]);
}

#[tokio::test]
async fn a_pass_that_took_nothing_still_reports_its_short_take() {
    let steps = ScriptedSteps::new(Vec::new(), short_by(2));

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &RegisteredClasses::default(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(0, 0, short_by(2)));
}

#[tokio::test]
async fn an_empty_pass_reports_nothing() {
    let steps = ScriptedSteps::new(Vec::new(), ClaimTake::Full);

    let claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &RegisteredClasses::default(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert_eq!(claimed.counts, counts(0, 0, ClaimTake::Full));
    assert!(claimed.rows.is_empty());
    assert!(steps.filtered().is_empty());
    assert!(steps.returned().is_empty());
}

/// A pass of plain tasks runs no workflow filter statement.
#[tokio::test]
async fn a_pass_with_no_workflow_task_runs_no_workflow_filter() {
    let steps = ScriptedSteps::new(vec![row("t1", "plain", ISOLATED)], ClaimTake::Full);

    let _claimed = claim_for_dispatch(
        &steps,
        &request(one_isolated_child()),
        &RegisteredClasses::default(),
        &WorkerTelemetry::Off,
    )
    .await
    .expect("the scripted pass");

    assert!(steps.filtered().is_empty());
}
