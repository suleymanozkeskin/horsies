//! Execution classes across the fleet: pending streams, coverage, budget load.
//!
//! A pending stream is the `PENDING` rows of one queue and one execution
//! class. A live worker serves a stream when the queue is one of its queues
//! and one of its claim budgets serves the class ([`ClassBudget::serves`]). A
//! worker is live while its latest state snapshot is at most
//! `WORKER_STATE_LIVE_WINDOW_S` old.
//!
//! Python reference: `python/horsies/core/models/class_coverage.py`. The types are
//! frozen reports. The functions are pure; the database reads that feed them
//! are in `broker::class_coverage_reads`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;

use crate::broker::class_budgets::{parse_class_capacity, ClassBudget, ClassBudgets};
use crate::core::task::Execution;

/// Whether live workers serve a pending stream (Python `StreamServed`,
/// `StreamWithoutCapacity`, `StreamUnserved`, `class_coverage.py:29-58`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCoverageState {
    /// Live workers serve the stream, and they have capacity in service for
    /// it. `workers` counts them, 1 or more. `capacity` sums, over those
    /// workers, the capacity in service of the budget that serves the class,
    /// 1 or more. A budget that serves more than one class counts in full for
    /// each class.
    Served { workers: usize, capacity: u64 },
    /// Live workers serve the stream, and none of them has capacity in
    /// service for it: every child or seat of the serving budgets was down at
    /// the worker's snapshot. `workers` counts them, 1 or more.
    WithoutCapacity { workers: usize },
    /// No live worker serves the stream. Its rows wait until a worker that
    /// serves the class runs on the queue.
    Unserved,
}

/// The coverage of one pending stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCoverage {
    pub queue_name: String,
    pub execution: Execution,
    pub state: StreamCoverageState,
}

/// The fleet check: the coverage of every pending stream, read at
/// `checked_at` (database time).
///
/// `live_workers` counts the workers whose latest snapshot was live.
/// `streams` holds one entry per (queue, class) that has `PENDING` rows,
/// ordered by queue name, then by [`Execution`] order. Empty when no row is
/// pending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassCoverage {
    pub checked_at: DateTime<Utc>,
    pub live_workers: usize,
    pub streams: Vec<StreamCoverage>,
}

/// How long the rows of a pending stream wait (Python `OldestWaiting`,
/// `NoneDue`, `class_coverage.py:86-107`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamWait {
    /// At least one row is due. `enqueued_at` is the due time of the oldest
    /// due row: its send time, or the time its retry became due. `wait` is
    /// how long that row has waited at `checked_at`, 0 or more.
    OldestWaiting {
        enqueued_at: DateTime<Utc>,
        wait: TimeDelta,
    },
    /// Every row is due later: a delayed send, or a retry that waits for its
    /// interval. `next_due_at` is the earliest due time.
    NoneDue { next_due_at: DateTime<Utc> },
}

/// One pending stream: how long its rows wait, and whether a live worker
/// serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStream {
    pub queue_name: String,
    pub execution: Execution,
    pub wait: StreamWait,
    pub state: StreamCoverageState,
}

/// One claim budget of a live worker.
///
/// `capacity` and `serves` come from the worker's latest snapshot.
/// `in_flight` counts, at the read, the worker's `RUNNING` rows and its
/// `CLAIMED` rows with a live lease whose class the budget serves. The claim
/// counts the same rows against the budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetLoad {
    pub budget: Execution,
    pub capacity: u64,
    pub serves: Vec<Execution>,
    pub in_flight: u64,
}

impl BudgetLoad {
    /// Capacity that holds no row, at least 0 (Python `BudgetLoad.free`).
    ///
    /// A worker with a prefetch buffer can hold more rows than its capacity,
    /// and a slot that goes down lowers the capacity below the rows already
    /// held.
    pub fn free(&self) -> u64 {
        self.capacity.saturating_sub(self.in_flight)
    }
}

/// The claim budgets of one live worker, in [`Execution`] order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerClassLoad {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub budgets: Vec<BudgetLoad>,
}

/// The class report: pending streams and the budget load of each live
/// worker, read at `checked_at` (database time).
///
/// `streams` is ordered like [`ClassCoverage::streams`]; `workers` (the live
/// workers) by worker id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassReport {
    pub checked_at: DateTime<Utc>,
    pub streams: Vec<PendingStream>,
    pub workers: Vec<WorkerClassLoad>,
}

/// Stored data that the class reads cannot decide on, or a read bound that
/// was reached (Python `ClassReadRefused`, `class_coverage.py:169-177`).
///
/// `reason` names the subject (a worker, a class, a bound) and the cause.
/// Nothing changed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct ClassReadRefused {
    pub reason: String,
}

pub(crate) fn refused(reason: impl Into<String>) -> ClassReadRefused {
    ClassReadRefused {
        reason: reason.into(),
    }
}

/// A worker whose latest snapshot is live, with the budgets it recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveWorker {
    pub(crate) worker_id: String,
    pub(crate) snapshot_at: DateTime<Utc>,
    pub(crate) queues: BTreeSet<String>,
    pub(crate) budgets: ClassBudgets,
}

/// The earliest `enqueued_at` of the pending rows of one (queue, class,
/// priority).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamHead {
    pub(crate) queue_name: String,
    pub(crate) execution: Execution,
    pub(crate) enqueued_at: DateTime<Utc>,
}

/// A live worker from its latest snapshot row (Python `live_worker`,
/// `class_coverage.py:198-222`).
///
/// Errors: [`ClassReadRefused`] when the stored `class_capacity` is
/// unreadable; the reason names the worker and the snapshot. Reads only.
pub(crate) fn live_worker(
    worker_id: String,
    snapshot_at: DateTime<Utc>,
    queues: Vec<String>,
    class_capacity: &Value,
) -> Result<LiveWorker, ClassReadRefused> {
    match parse_class_capacity(class_capacity) {
        Ok(budgets) => Ok(LiveWorker {
            worker_id,
            snapshot_at,
            queues: queues.into_iter().collect(),
            budgets,
        }),
        Err(unreadable) => Err(refused(format!(
            "worker {worker_id} snapshot at {snapshot_at}: class_capacity is unreadable: {}",
            unreadable.reason
        ))),
    }
}

/// Whether the live `workers` serve the stream, and with what capacity
/// (Python `stream_coverage`, `class_coverage.py:225-243`).
///
/// Errors: [`ClassReadRefused`] when the summed capacity does not fit in 64
/// bits. Each stored capacity is below `2^63`, so only three or more absurd
/// stored values reach it; Python sums without a bound.
pub(crate) fn stream_coverage(
    queue_name: &str,
    execution: Execution,
    workers: &[LiveWorker],
) -> Result<StreamCoverageState, ClassReadRefused> {
    let serving: Vec<&ClassBudget> = workers
        .iter()
        .filter(|worker| worker.queues.contains(queue_name))
        .filter_map(|worker| worker.budgets.serving(execution))
        .collect();
    if serving.is_empty() {
        return Ok(StreamCoverageState::Unserved);
    }
    let capacity = serving
        .iter()
        .try_fold(0_u64, |sum, budget| sum.checked_add(budget.capacity))
        .ok_or_else(|| {
            refused(format!(
                "the capacity that serves queue '{queue_name}', class {} does not fit in 64 bits",
                execution.as_str()
            ))
        })?;
    match capacity {
        0 => Ok(StreamCoverageState::WithoutCapacity {
            workers: serving.len(),
        }),
        _ => Ok(StreamCoverageState::Served {
            workers: serving.len(),
            capacity,
        }),
    }
}

/// The wait of a stream from the head of each of its priorities (Python
/// `stream_wait`, `class_coverage.py:246-258`).
///
/// A row is due when its `enqueued_at` is at or before `checked_at`. `heads`
/// holds at least one instant: the caller groups heads by stream, so an
/// empty group is a defect of the caller, and the function panics.
pub(crate) fn stream_wait(heads: &[DateTime<Utc>], checked_at: DateTime<Utc>) -> StreamWait {
    match heads
        .iter()
        .copied()
        .filter(|head| *head <= checked_at)
        .min()
    {
        Some(oldest) => StreamWait::OldestWaiting {
            enqueued_at: oldest,
            wait: checked_at - oldest,
        },
        None => StreamWait::NoneDue {
            next_due_at: heads
                .iter()
                .copied()
                .min()
                .expect("a pending stream has at least one head"),
        },
    }
}

/// The coverage of each pending (queue, class) in `streams` by `workers`
/// (Python `class_coverage`, `class_coverage.py:261-276`).
///
/// Errors: as [`stream_coverage`].
pub(crate) fn class_coverage(
    checked_at: DateTime<Utc>,
    workers: &[LiveWorker],
    mut streams: Vec<(String, Execution)>,
) -> Result<ClassCoverage, ClassReadRefused> {
    streams.sort();
    let streams = streams
        .into_iter()
        .map(|(queue_name, execution)| {
            let state = stream_coverage(&queue_name, execution, workers)?;
            Ok(StreamCoverage {
                queue_name,
                execution,
                state,
            })
        })
        .collect::<Result<Vec<_>, ClassReadRefused>>()?;
    Ok(ClassCoverage {
        checked_at,
        live_workers: workers.len(),
        streams,
    })
}

/// Pending streams from their heads, and the budget load of each live worker
/// (Python `class_report`, `class_coverage.py:279-324`).
///
/// `in_flight` maps (worker id, class) to the rows that the worker holds.
/// Errors: [`ClassReadRefused`] when a worker holds rows of a class that
/// none of its budgets serves, or as [`stream_coverage`].
pub(crate) fn class_report(
    checked_at: DateTime<Utc>,
    workers: &[LiveWorker],
    heads: Vec<StreamHead>,
    in_flight: &BTreeMap<(String, Execution), u64>,
) -> Result<ClassReport, ClassReadRefused> {
    let mut grouped: BTreeMap<(String, Execution), Vec<DateTime<Utc>>> = BTreeMap::new();
    for head in heads {
        grouped
            .entry((head.queue_name, head.execution))
            .or_default()
            .push(head.enqueued_at);
    }
    let streams = grouped
        .into_iter()
        .map(|((queue_name, execution), stream_heads)| {
            let state = stream_coverage(&queue_name, execution, workers)?;
            Ok(PendingStream {
                wait: stream_wait(&stream_heads, checked_at),
                state,
                queue_name,
                execution,
            })
        })
        .collect::<Result<Vec<_>, ClassReadRefused>>()?;
    let mut ordered: Vec<&LiveWorker> = workers.iter().collect();
    ordered.sort_by(|left, right| left.worker_id.cmp(&right.worker_id));
    let loads = ordered
        .into_iter()
        .map(|worker| {
            let held: BTreeMap<Execution, u64> = in_flight
                .iter()
                .filter(|((worker_id, _), _)| *worker_id == worker.worker_id)
                .map(|((_, execution), count)| (*execution, *count))
                .collect();
            Ok(WorkerClassLoad {
                worker_id: worker.worker_id.clone(),
                snapshot_at: worker.snapshot_at,
                budgets: budget_loads(worker, &held)?,
            })
        })
        .collect::<Result<Vec<_>, ClassReadRefused>>()?;
    Ok(ClassReport {
        checked_at,
        streams,
        workers: loads,
    })
}

/// The load of each budget of `worker` from the rows it holds per class
/// (Python `budget_loads`, `class_coverage.py:327-356`).
///
/// Errors: [`ClassReadRefused`] when the worker holds rows of a class that
/// none of its budgets serves. The claim takes only rows of a served class,
/// so such a row means that the stored data and the recorded budgets
/// disagree.
pub(crate) fn budget_loads(
    worker: &LiveWorker,
    held: &BTreeMap<Execution, u64>,
) -> Result<Vec<BudgetLoad>, ClassReadRefused> {
    let unserved = held
        .iter()
        .find(|(execution, count)| **count > 0 && worker.budgets.serving(**execution).is_none());
    if let Some((execution, count)) = unserved {
        return Err(refused(format!(
            "worker {} holds {count} {} row(s), and no budget of its snapshot serves that class",
            worker.worker_id,
            execution.as_str()
        )));
    }
    Ok(worker
        .budgets
        .as_slice()
        .iter()
        .map(|budget| BudgetLoad {
            budget: budget.budget,
            capacity: budget.capacity,
            serves: budget.serves.clone(),
            // The classes of one budget hold disjoint rows of one table, so
            // the sum stays below the row count of `horsies_tasks`.
            in_flight: budget
                .serves
                .iter()
                .map(|member| held.get(member).copied().unwrap_or(0))
                .sum(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    //! One test per outcome of the pure functions. Python reference:
    //! `tests/unit/test_class_coverage.py`.
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    const ISOLATED: Execution = Execution::Isolated;
    const ASYNC: Execution = Execution::SharedAsync;
    const THREAD: Execution = Execution::SharedThread;
    const ALL: [Execution; 3] = [ISOLATED, ASYNC, THREAD];

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 10, 12, 0, 0).unwrap()
    }

    fn worker(worker_id: &str, queues: &[&str], stored: Value) -> LiveWorker {
        live_worker(
            worker_id.to_owned(),
            at(),
            queues.iter().map(|queue| (*queue).to_owned()).collect(),
            &stored,
        )
        .expect("readable budgets")
    }

    /// A worker with isolated children only: its budget serves every class.
    fn isolated_worker(worker_id: &str, capacity: u64, queues: &[&str]) -> LiveWorker {
        let stored = json!({"ISOLATED": {"capacity": capacity, "serves": ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"]}});
        worker(worker_id, queues, stored)
    }

    /// A worker with `SHARED_ASYNC` children and no isolated ones.
    fn async_only_worker(worker_id: &str, capacity: u64, queues: &[&str]) -> LiveWorker {
        let stored = json!({"SHARED_ASYNC": {"capacity": capacity, "serves": ["SHARED_ASYNC"]}});
        worker(worker_id, queues, stored)
    }

    fn coverage(queue: &str, execution: Execution, workers: &[LiveWorker]) -> StreamCoverageState {
        stream_coverage(queue, execution, workers).expect("coverage")
    }

    #[test]
    fn served_sums_the_capacity_of_each_serving_worker() {
        let workers = [
            isolated_worker("w1", 4, &["default"]),
            isolated_worker("w2", 2, &["default"]),
        ];
        assert_eq!(
            coverage("default", ASYNC, &workers),
            StreamCoverageState::Served {
                workers: 2,
                capacity: 6
            }
        );
    }

    #[test]
    fn served_without_capacity_when_every_serving_budget_is_down() {
        let workers = [isolated_worker("w1", 0, &["default"])];
        assert_eq!(
            coverage("default", ISOLATED, &workers),
            StreamCoverageState::WithoutCapacity { workers: 1 }
        );
        let one_up = [
            isolated_worker("w1", 0, &["default"]),
            isolated_worker("w2", 1, &["default"]),
        ];
        assert_eq!(
            coverage("default", ISOLATED, &one_up),
            StreamCoverageState::Served {
                workers: 2,
                capacity: 1
            }
        );
    }

    #[test]
    fn unserved_without_a_worker_on_the_queue_or_a_budget_for_the_class() {
        assert_eq!(
            coverage("default", ISOLATED, &[]),
            StreamCoverageState::Unserved
        );
        let other_queue = [isolated_worker("w1", 4, &["emails"])];
        assert_eq!(
            coverage("default", ISOLATED, &other_queue),
            StreamCoverageState::Unserved
        );
        let async_only = [async_only_worker("w1", 40, &["default"])];
        assert_eq!(
            coverage("default", ISOLATED, &async_only),
            StreamCoverageState::Unserved
        );
        assert_eq!(
            coverage("default", ASYNC, &async_only),
            StreamCoverageState::Served {
                workers: 1,
                capacity: 40
            }
        );
        let serves_nothing = [worker("w1", &["default"], json!({}))];
        for execution in ALL {
            assert_eq!(
                coverage("default", execution, &serves_nothing),
                StreamCoverageState::Unserved
            );
        }
    }

    #[test]
    fn a_capacity_sum_past_64_bits_is_refused() {
        let huge = (1_u64 << 63) - 1;
        let workers = [
            isolated_worker("w1", huge, &["a"]),
            isolated_worker("w2", huge, &["a"]),
            isolated_worker("w3", huge, &["a"]),
        ];
        let refusal = stream_coverage("a", ISOLATED, &workers).expect_err("overflow");
        assert!(refusal.reason.contains("does not fit in 64 bits"));
    }

    #[test]
    fn the_oldest_due_head_is_the_wait() {
        let heads = [
            at() - TimeDelta::seconds(30),
            at() - TimeDelta::seconds(90),
            at() + TimeDelta::minutes(5),
        ];
        assert_eq!(
            stream_wait(&heads, at()),
            StreamWait::OldestWaiting {
                enqueued_at: at() - TimeDelta::seconds(90),
                wait: TimeDelta::seconds(90),
            }
        );
        assert_eq!(
            stream_wait(&[at()], at()),
            StreamWait::OldestWaiting {
                enqueued_at: at(),
                wait: TimeDelta::zero(),
            }
        );
    }

    #[test]
    fn none_due_names_the_earliest_due_time() {
        let heads = [at() + TimeDelta::minutes(5), at() + TimeDelta::minutes(1)];
        assert_eq!(
            stream_wait(&heads, at()),
            StreamWait::NoneDue {
                next_due_at: at() + TimeDelta::minutes(1)
            }
        );
    }

    #[test]
    #[should_panic(expected = "a pending stream has at least one head")]
    fn no_head_is_a_defect() {
        let _ = stream_wait(&[], at());
    }

    /// Python `TestLiveWorker::test_a_recorded_snapshot_builds_a_live_worker`:
    /// the worker id, the snapshot time, the queues and the recorded budgets
    /// reach the live worker.
    #[test]
    fn a_recorded_snapshot_builds_a_live_worker() {
        let stored = json!({"ISOLATED": {"capacity": 2, "serves": ["ISOLATED"]}});

        let live = live_worker(
            "w1".to_owned(),
            at(),
            vec!["a".to_owned(), "b".to_owned()],
            &stored,
        )
        .expect("readable budgets");

        assert_eq!(live.worker_id, "w1");
        assert_eq!(live.snapshot_at, at());
        assert_eq!(
            live.queues,
            BTreeSet::from(["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            live.budgets.as_slice(),
            [ClassBudget {
                budget: ISOLATED,
                capacity: 2,
                serves: vec![ISOLATED],
            }]
        );
    }

    /// Python `TestLiveWorker::test_a_snapshot_without_budgets_is_refused_with_the_worker_named`:
    /// no stored value is not a state (the column is NOT NULL), so a null
    /// is refused, and the reason names the worker and the cause.
    #[test]
    fn a_snapshot_without_budgets_is_refused_with_the_worker_named() {
        let refusal = live_worker(
            "w-none".to_owned(),
            at(),
            vec!["a".to_owned()],
            &Value::Null,
        )
        .expect_err("no budgets");

        assert!(refusal.reason.contains("w-none"), "{}", refusal.reason);
        assert!(
            refusal
                .reason
                .contains("class_capacity is not a JSON object"),
            "{}",
            refusal.reason
        );
    }

    #[test]
    fn an_unreadable_snapshot_is_refused_with_the_worker_named() {
        for stored in [Value::Null, json!({"EXCLUSIVE": {}})] {
            let refusal = live_worker("w-bad".to_owned(), at(), vec![], &stored)
                .expect_err("unreadable budgets");
            assert!(refusal.reason.contains("w-bad"), "{}", refusal.reason);
            assert!(
                refusal.reason.contains("class_capacity is unreadable"),
                "{}",
                refusal.reason
            );
        }
    }

    #[test]
    fn rows_count_against_the_budget_that_serves_their_class() {
        let stored = json!({
            "ISOLATED": {"capacity": 4, "serves": ["ISOLATED", "SHARED_THREAD"]},
            "SHARED_ASYNC": {"capacity": 40, "serves": ["SHARED_ASYNC"]},
        });
        let two_budgets = worker("w1", &["default"], stored);
        let held = BTreeMap::from([(ISOLATED, 1), (THREAD, 2), (ASYNC, 39)]);

        assert_eq!(
            budget_loads(&two_budgets, &held).expect("served rows"),
            [
                BudgetLoad {
                    budget: ISOLATED,
                    capacity: 4,
                    serves: vec![ISOLATED, THREAD],
                    in_flight: 3,
                },
                BudgetLoad {
                    budget: ASYNC,
                    capacity: 40,
                    serves: vec![ASYNC],
                    in_flight: 39,
                },
            ]
        );
        let idle = budget_loads(&isolated_worker("w2", 4, &["default"]), &BTreeMap::new())
            .expect("no rows");
        assert_eq!(idle[0].in_flight, 0);
        assert_eq!(idle[0].free(), 4);
    }

    #[test]
    fn rows_of_a_class_that_no_budget_serves_are_refused() {
        let async_only = async_only_worker("w1", 40, &["default"]);
        let refusal =
            budget_loads(&async_only, &BTreeMap::from([(ISOLATED, 1)])).expect_err("unserved rows");
        assert!(refusal.reason.contains("w1"), "{}", refusal.reason);
        assert!(refusal.reason.contains("ISOLATED"), "{}", refusal.reason);
    }

    #[test]
    fn free_is_never_below_zero() {
        let prefetched = BudgetLoad {
            budget: ISOLATED,
            capacity: 2,
            serves: vec![ISOLATED],
            in_flight: 5,
        };
        assert_eq!(prefetched.free(), 0);
    }

    #[test]
    fn streams_come_in_queue_then_execution_order() {
        let workers = [isolated_worker("w1", 1, &["a", "b"])];
        let streams = vec![
            ("b".to_owned(), ISOLATED),
            ("a".to_owned(), THREAD),
            ("a".to_owned(), ISOLATED),
        ];

        let checked = class_coverage(at(), &workers, streams).expect("coverage");

        let served = StreamCoverageState::Served {
            workers: 1,
            capacity: 1,
        };
        assert_eq!(checked.checked_at, at());
        assert_eq!(checked.live_workers, 1);
        assert_eq!(
            checked.streams,
            [
                StreamCoverage {
                    queue_name: "a".to_owned(),
                    execution: ISOLATED,
                    state: served
                },
                StreamCoverage {
                    queue_name: "a".to_owned(),
                    execution: THREAD,
                    state: served
                },
                StreamCoverage {
                    queue_name: "b".to_owned(),
                    execution: ISOLATED,
                    state: served
                },
            ]
        );
        let empty = class_coverage(at(), &[], vec![]).expect("coverage");
        assert!(empty.streams.is_empty());
        assert_eq!(empty.live_workers, 0);
    }

    #[test]
    fn heads_of_one_stream_merge_into_one_wait() {
        let heads = vec![
            StreamHead {
                queue_name: "a".to_owned(),
                execution: ISOLATED,
                enqueued_at: at() - TimeDelta::seconds(5),
            },
            StreamHead {
                queue_name: "a".to_owned(),
                execution: ISOLATED,
                enqueued_at: at() - TimeDelta::seconds(50),
            },
            StreamHead {
                queue_name: "a".to_owned(),
                execution: ASYNC,
                enqueued_at: at() + TimeDelta::seconds(60),
            },
        ];
        let workers = [async_only_worker("w1", 3, &["a"])];

        let report = class_report(at(), &workers, heads, &BTreeMap::new()).expect("report");

        assert_eq!(
            report.streams,
            [
                PendingStream {
                    queue_name: "a".to_owned(),
                    execution: ISOLATED,
                    wait: StreamWait::OldestWaiting {
                        enqueued_at: at() - TimeDelta::seconds(50),
                        wait: TimeDelta::seconds(50),
                    },
                    state: StreamCoverageState::Unserved,
                },
                PendingStream {
                    queue_name: "a".to_owned(),
                    execution: ASYNC,
                    wait: StreamWait::NoneDue {
                        next_due_at: at() + TimeDelta::seconds(60)
                    },
                    state: StreamCoverageState::Served {
                        workers: 1,
                        capacity: 3
                    },
                },
            ]
        );
    }

    #[test]
    fn workers_come_in_worker_id_order_with_their_rows() {
        let workers = [
            isolated_worker("w2", 2, &["a"]),
            isolated_worker("w1", 4, &["a"]),
        ];
        let in_flight = BTreeMap::from([
            (("w1".to_owned(), ISOLATED), 3),
            (("w2".to_owned(), THREAD), 1),
        ]);

        let report = class_report(at(), &workers, vec![], &in_flight).expect("report");

        let load = |worker_id: &str, capacity: u64, in_flight: u64| WorkerClassLoad {
            worker_id: worker_id.to_owned(),
            snapshot_at: at(),
            budgets: vec![BudgetLoad {
                budget: ISOLATED,
                capacity,
                serves: ALL.to_vec(),
                in_flight,
            }],
        };
        assert_eq!(report.workers, [load("w1", 4, 3), load("w2", 2, 1)]);
    }

    #[test]
    fn rows_that_no_budget_serves_refuse_the_report() {
        let workers = [async_only_worker("w1", 3, &["a"])];
        let in_flight = BTreeMap::from([(("w1".to_owned(), ISOLATED), 1)]);
        assert!(class_report(at(), &workers, vec![], &in_flight).is_err());
    }
}
