use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, TimeZone, Utc};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::broker::terminalization::on_test_pool::terminalize;
use crate::broker::PostgresBroker;
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome, WorkerOwned};
use crate::test_support::history::{covered_history_instant, stored_class_key, SeedInstant};
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::test_support::terminal_writes::{self, TerminalFailure};

use super::*;

mod history_detail_ports;

/// The detail of a task that the read finds; a not-found answer fails the
/// test.
fn found_detail(read: TaskDetailRead) -> TaskDetail {
    match read {
        TaskDetailRead::Found(detail) => *detail,
        TaskDetailRead::NotFound(absent) => panic!("the task detail is not found: {absent:?}"),
    }
}

#[test]
fn monitoring_window_defaults_and_refusals_match_the_contract() {
    let now = Utc.with_ymd_and_hms(2026, 8, 8, 12, 0, 0).unwrap();
    let default = resolve_monitoring_window(None, None, Some(now)).expect("default window");
    assert_eq!(default.lower(), now - MONITORING_WINDOW_DEFAULT);
    assert_eq!(default.upper(), now);
    assert_eq!(MONITORING_WINDOW_DEFAULT, Duration::hours(24));
    assert_eq!(MONITORING_WINDOW_MAX, Duration::days(30));

    let lone_since = resolve_monitoring_window(Some(now - Duration::hours(6)), None, Some(now))
        .expect("lone since");
    assert_eq!(lone_since.lower(), now - Duration::hours(6));
    assert_eq!(lone_since.upper(), now);
    let lone_until = resolve_monitoring_window(None, Some(now), Some(now + Duration::hours(2)))
        .expect("lone until");
    assert_eq!(lone_until.lower(), now - Duration::hours(24));
    assert_eq!(lone_until.upper(), now);

    resolve_monitoring_window(Some(now - MONITORING_WINDOW_MAX), Some(now), Some(now))
        .expect("exact maximum");
    let over = resolve_monitoring_window(
        Some(now - MONITORING_WINDOW_MAX - Duration::seconds(1)),
        Some(now),
        Some(now),
    )
    .expect_err("over maximum");
    assert_eq!(over.reason, "the window exceeds the 30-day maximum");
    let inverted =
        resolve_monitoring_window(Some(now), Some(now), Some(now)).expect_err("non-increasing");
    assert_eq!(
        inverted.reason,
        "the window must be increasing (since < until)"
    );
}

#[test]
fn query_vocabulary_and_bounds_are_exhaustive() {
    assert_eq!(TaskSortField::ALL.len(), 11);
    assert_eq!(SortDirection::ALL.len(), 2);
    assert_eq!(TaskGroupBy::ALL.len(), 3);
    assert_eq!(ErrorCategory::ALL.len(), 5);
    assert_eq!(
        TaskSortField::ALL
            .into_iter()
            .map(|field| field.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "completed_at",
            "enqueued_at",
            "exec_s",
            "failed_at",
            "priority",
            "queue_name",
            "queue_s",
            "retry_count",
            "started_at",
            "status",
            "task_name",
        ])
    );

    let window = resolve_monitoring_window(None, None, None).expect("window");
    TaskListQuery::new(window)
        .with_pagination(300, 200)
        .expect("exact reach");
    let refusal = TaskListQuery::new(window)
        .with_pagination(301, 200)
        .expect_err("over reach");
    assert_eq!(refusal.reason, "offset + limit must be <= 500; got 501");
    assert!(TaskListQuery::new(window).with_pagination(-1, 50).is_err());
    assert!(TaskListQuery::new(window).with_pagination(0, 201).is_err());
    assert!(TaskBreakdownQuery::new(window, TaskGroupBy::Worker)
        .with_limit(501)
        .is_err());
    assert!(WorkflowRunsQuery::new().with_limit(201).is_err());
    let list_columns = super::function_rows::MONITORING_TASK_LIST
        .sql()
        .split("FROM")
        .next()
        .expect("the list statement has a select list");
    for payload_column in ["input", "result", "task_options"] {
        assert!(!list_columns
            .split(|character: char| character == ',' || character.is_whitespace())
            .any(|column| column == payload_column));
    }
}

/// The definition of `horsies_monitoring_task_list` as the baseline ships
/// it: from `CREATE FUNCTION` to the end of its body.
fn task_list_function_definition() -> &'static str {
    let (_, text) = crate::broker::migrations::BASELINE_SQL_FILE_TEXTS
        .iter()
        .find(|(name, _)| *name == "0007_scheduler_monitoring_functions.sql")
        .expect("the baseline has the monitoring functions file");
    let start = text
        .find("CREATE FUNCTION horsies_monitoring_task_list(")
        .expect("the file defines the task list function");
    let definition = &text[start..];
    let end = definition
        .find("\n$$;")
        .expect("the task list function body ends");
    &definition[..end]
}

/// The task list function reads no payload column on either side (the live
/// input and options, the history result, attempt and rerun-input
/// envelopes), and its unfiltered history estimate is a plain
/// `EXPLAIN (FORMAT JSON)` of the window read: it plans the read and does
/// not run it (no `ANALYZE`).
#[test]
fn the_task_list_function_reads_no_payload_and_does_not_run_its_estimate() {
    let definition = task_list_function_definition();
    let words: BTreeSet<String> = definition
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .map(str::to_ascii_lowercase)
        .collect();

    for payload_column in [
        "input",
        "task_options",
        "result_payload",
        "attempt_snapshot",
        "rerun_input_inline",
    ] {
        assert!(!words.contains(payload_column), "{payload_column}");
    }
    assert!(!words.contains("analyze"));
    assert!(definition
        .contains("EXECUTE 'EXPLAIN (FORMAT JSON) SELECT 1 FROM horsies_task_history AS h '\n"));
}

#[test]
fn helpers_and_error_taxonomy_are_exact() {
    assert_eq!(normalize_optional_text(None), None);
    assert_eq!(normalize_optional_text(Some(" \t")), None);
    assert_eq!(
        normalize_optional_text(Some("  code  ")),
        Some("code".to_owned())
    );

    assert_eq!(categorize_error_code(None), None);
    assert_eq!(categorize_error_code(Some("")), None);
    for code in crate::OperationalCode::ALL {
        assert_eq!(
            categorize_error_code(Some(&code.to_string())),
            Some(ErrorCategory::Operational)
        );
    }
    for code in crate::ContractCode::ALL {
        assert_eq!(
            categorize_error_code(Some(&code.to_string())),
            Some(ErrorCategory::Contract)
        );
    }
    for code in crate::RetrievalCode::ALL {
        assert_eq!(
            categorize_error_code(Some(&code.to_string())),
            Some(ErrorCategory::Retrieval)
        );
    }
    for code in crate::OutcomeCode::ALL {
        assert_eq!(
            categorize_error_code(Some(&code.to_string())),
            Some(ErrorCategory::Outcome)
        );
    }
    assert_eq!(
        categorize_error_code(Some("MY_BUSINESS_ERROR")),
        Some(ErrorCategory::Domain)
    );

    let start = Utc.with_ymd_and_hms(2026, 8, 8, 12, 0, 0).unwrap();
    let end = start + Duration::milliseconds(1_999);
    assert_eq!(
        elapsed(Some(start), Some(end)),
        Some(Duration::milliseconds(1_999))
    );
    assert_eq!(
        elapsed(Some(end), Some(start)),
        Some(Duration::milliseconds(-1_999))
    );
    assert_eq!(span(Some(start), None, false), None);
    assert_eq!(
        span(Some(start), Some(end), false),
        Some(Duration::milliseconds(1_999))
    );
}

fn keys(value: impl Serialize) -> Vec<String> {
    let encoded = serde_json::to_string(&value).expect("serialize model");
    let bytes = encoded.as_bytes();
    let mut depth = 0_i32;
    let mut index = 0_usize;
    let mut keys = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'{' | b'[' => {
                depth += 1;
                index += 1;
            }
            b'}' | b']' => {
                depth -= 1;
                index += 1;
            }
            b'"' => {
                let start = index + 1;
                index = start;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                let end = index;
                index += 1;
                let mut following = index;
                while following < bytes.len() && bytes[following].is_ascii_whitespace() {
                    following += 1;
                }
                if depth == 1 && bytes.get(following) == Some(&b':') {
                    keys.push(encoded[start..end].to_owned());
                }
            }
            _ => index += 1,
        }
    }
    keys
}

#[test]
fn every_w2_model_serializes_the_contract_field_set() {
    let now = Utc.with_ymd_and_hms(2026, 8, 8, 12, 0, 0).unwrap();
    let task_id = uuid::Uuid::nil();
    let attempt = TaskAttemptInfo {
        attempt: 1,
        outcome: "FAILED".to_owned(),
        will_retry: false,
        error_code: None,
        error_message: None,
        failed_reason: None,
        worker_hostname: None,
        started_at: Some(now),
        finished_at: Some(now),
        phases: AttemptPhasesInfo::NotCaptured,
    };
    let leaf = LeafTaskInfo {
        task_id,
        status: "FAILED".to_owned(),
        error_code: None,
        failed_reason: None,
        retry_count: 0,
        max_retries: 0,
        enqueued_at: Some(now),
        started_at: None,
        completed_at: None,
        failed_at: Some(now),
        queue_s: Some(Duration::zero()),
        exec_s: None,
        worker_hostname: None,
        good_until: None,
        execution: None,
        trace_id: Some("0af7651916cd43dd8448eb211c80319c".to_owned()),
    };
    let summary = TaskSummary {
        id: task_id,
        task_name: "t".to_owned(),
        queue_name: "q".to_owned(),
        status: "FAILED".to_owned(),
        priority: 100,
        retry_count: 0,
        max_retries: 0,
        is_workflow_task: false,
        error_code: None,
        error_category: None,
        worker_hostname: None,
        worker_id: None,
        enqueued_at: Some(now),
        started_at: None,
        completed_at: None,
        failed_at: Some(now),
        queue_s: Some(Duration::zero()),
        exec_s: None,
    };
    let counts = BreakdownCounts {
        total: 0,
        pending: 0,
        claimed: 0,
        running: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
        expired: 0,
        retried: 0,
    };
    let group = GroupRow {
        counts: counts.clone(),
        group: BreakdownGroup::Value {
            value: "w1".to_owned(),
        },
    };
    let run = WorkflowRunSummary {
        id: task_id,
        name: "w".to_owned(),
        definition_key: None,
        status: "RUNNING".to_owned(),
        created_at: Some(now),
        completed_at: None,
        wall_s: Some(Duration::zero()),
    };
    let node = WorkflowNodeInfo {
        task_index: 0,
        node_id: None,
        task_name: "t".to_owned(),
        node_status: "PENDING".to_owned(),
        is_subworkflow: false,
        sub_workflow_id: None,
        allow_failed_deps: false,
        started_at: None,
        completed_at: None,
        exec_s: None,
        child_total: None,
        child_failed: None,
        timing: NodeTimingInfo::Unavailable {
            reason: UnavailableReason::RunBeforeCapture,
        },
    };
    let segment = TimingSegmentInfo {
        phase: SegmentPhaseInfo::Claimed {
            claim: SegmentClaimInfo::Recorded {
                placement: AttemptPlacementInfo::Placed { placed_at: now },
            },
        },
        started_at: now,
        end: SegmentEndInfo::Ended { at: now },
        source: SegmentSourceInfo::Attempt {
            generation: 1,
            task_id,
            attempt: 1,
        },
    };
    let pause = PauseIntervalInfo {
        started_at: now,
        end: PauseEndInfo::Resumed { at: now },
    };
    let cases = [
        (
            keys(attempt.clone()),
            vec![
                "attempt",
                "outcome",
                "will_retry",
                "error_code",
                "error_message",
                "failed_reason",
                "worker_hostname",
                "started_at",
                "finished_at",
                "phases",
            ],
        ),
        (
            keys(AttemptPhasesInfo::Captured {
                enqueued_at: now,
                claimed_at: now,
                claims: AttemptClaimsInfo::Counted {
                    count: 1,
                    first_claimed_at: now,
                },
                finalizing: AttemptFinalizingInfo::NotMarked,
                recorded_by: AttemptRecordedByInfo::Recovery { last_seen_at: now },
            }),
            vec![
                "kind",
                "enqueued_at",
                "claimed_at",
                "claims",
                "finalizing",
                "recorded_by",
            ],
        ),
        (
            keys(segment.clone()),
            vec!["phase", "started_at", "end", "source"],
        ),
        (
            keys(segment.source),
            vec!["kind", "generation", "task_id", "attempt"],
        ),
        (
            keys(NodeTimingInfo::Partial {
                reasons: vec![UnknownReason::WorkerLost],
                segments: vec![segment.clone()],
                endpoint: NodeEndpointInfo::Terminal {
                    status: TerminalNodeStatus::Failed,
                    at: now,
                },
            }),
            vec!["kind", "reasons", "segments", "endpoint"],
        ),
        (
            keys(TimingConflictInfo::ClockOrder {
                earlier: TimingBoundary::Claim,
                earlier_at: now,
                later: TimingBoundary::Start,
                later_at: now,
            }),
            vec!["kind", "earlier", "earlier_at", "later", "later_at"],
        ),
        (
            keys(RunTimingInfo::Composed {
                as_of: now,
                captured_since: now,
                coverage: RunCoverageInfo::Exact,
                range_started_at: now,
                range_ended_at: now,
                open_segments: false,
                pauses: vec![pause],
            }),
            vec![
                "kind",
                "as_of",
                "captured_since",
                "coverage",
                "range_started_at",
                "range_ended_at",
                "open_segments",
                "pauses",
            ],
        ),
        (keys(pause), vec!["started_at", "end"]),
        (
            keys(leaf.clone()),
            vec![
                "task_id",
                "status",
                "error_code",
                "failed_reason",
                "retry_count",
                "max_retries",
                "enqueued_at",
                "started_at",
                "completed_at",
                "failed_at",
                "queue_s",
                "exec_s",
                "worker_hostname",
                "good_until",
                "execution",
                "trace_id",
            ],
        ),
        (
            keys(StatusCount {
                status: "PENDING".to_owned(),
                count: 0,
            }),
            vec!["status", "count"],
        ),
        (
            keys(FacetValue {
                value: "x".to_owned(),
                count: 1,
            }),
            vec!["value", "count"],
        ),
        (
            keys(ErrorFacet {
                value: "x".to_owned(),
                count: 1,
                category: "DOMAIN".to_owned(),
            }),
            vec!["value", "count", "category"],
        ),
        (
            keys(Facets {
                workers: vec![],
                task_names: vec![],
                queues: vec![],
                error_codes: vec![],
                error_category_totals: BTreeMap::new(),
            }),
            vec![
                "workers",
                "task_names",
                "queues",
                "error_codes",
                "error_category_totals",
            ],
        ),
        (
            keys(counts.clone()),
            vec![
                "total",
                "pending",
                "claimed",
                "running",
                "completed",
                "failed",
                "cancelled",
                "expired",
                "retried",
            ],
        ),
        (
            keys(group.clone()),
            vec![
                "total",
                "pending",
                "claimed",
                "running",
                "completed",
                "failed",
                "cancelled",
                "expired",
                "retried",
                "group",
            ],
        ),
        (keys(group.group.clone()), vec!["kind", "value"]),
        (keys(BreakdownGroup::Unclaimed), vec!["kind"]),
        (
            keys(Breakdown {
                group_by: "worker".to_owned(),
                groups: vec![group],
                total: counts,
                group_count: 1,
            }),
            vec!["group_by", "groups", "total", "group_count"],
        ),
        (
            keys(summary.clone()),
            vec![
                "id",
                "task_name",
                "queue_name",
                "status",
                "priority",
                "retry_count",
                "max_retries",
                "is_workflow_task",
                "error_code",
                "error_category",
                "worker_hostname",
                "worker_id",
                "enqueued_at",
                "started_at",
                "completed_at",
                "failed_at",
                "queue_s",
                "exec_s",
            ],
        ),
        (
            keys(TaskListPage {
                rows: vec![summary],
                total: 1,
            }),
            vec!["rows", "total"],
        ),
        (
            keys(TaskDetail {
                leaf,
                task_name: "t".to_owned(),
                queue_name: "q".to_owned(),
                priority: 100,
                is_workflow_task: false,
                error_category: None,
                attempts: vec![attempt],
                workflow_id: None,
                workflow_task_index: None,
            }),
            vec![
                "leaf",
                "task_name",
                "queue_name",
                "priority",
                "is_workflow_task",
                "error_category",
                "attempts",
                "workflow_id",
                "workflow_task_index",
            ],
        ),
        (
            keys(run.clone()),
            vec![
                "id",
                "name",
                "definition_key",
                "status",
                "created_at",
                "completed_at",
                "wall_s",
            ],
        ),
        (
            keys(node.clone()),
            vec![
                "task_index",
                "node_id",
                "task_name",
                "node_status",
                "is_subworkflow",
                "sub_workflow_id",
                "allow_failed_deps",
                "started_at",
                "completed_at",
                "exec_s",
                "child_total",
                "child_failed",
                "timing",
            ],
        ),
        (
            keys(WorkflowEdge {
                from_index: 0,
                to_index: 1,
            }),
            vec!["from_index", "to_index"],
        ),
        (
            keys(WorkflowRunDetail {
                run,
                nodes: vec![node.clone()],
                edges: vec![],
                failed_count: 0,
                failed_indices: vec![],
                trace_id: None,
                timing: RunTimingInfo::Unavailable {
                    as_of: now,
                    captured_since: now,
                    reason: UnavailableReason::RunBeforeCapture,
                },
            }),
            vec![
                "run",
                "nodes",
                "edges",
                "failed_count",
                "failed_indices",
                "trace_id",
                "timing",
            ],
        ),
        (
            keys(WorkflowTaskDetail {
                task_index: 0,
                node_id: None,
                task_name: "t".to_owned(),
                node_status: "PENDING".to_owned(),
                is_subworkflow: false,
                node_error: None,
                leaf: None,
                attempts: vec![],
            }),
            vec![
                "task_index",
                "node_id",
                "task_name",
                "node_status",
                "is_subworkflow",
                "node_error",
                "leaf",
                "attempts",
            ],
        ),
        (
            keys(WorkerStateInfo {
                worker_id: "w".to_owned(),
                hostname: "h".to_owned(),
                pid: 1,
                snapshot_at: now,
                snapshot_age_s: Some(Duration::zero()),
                stale: false,
                worker_started_at: now,
                uptime_s: Some(Duration::zero()),
                processes: 1,
                queues: vec![],
                queue_max_concurrency: BTreeMap::new(),
                tasks_running: 0,
                tasks_claimed: 0,
                cluster_wide_cap: None,
                memory_usage_mb: None,
                memory_percent: None,
                cpu_percent: None,
                slot_health: SlotHealthKind::SlotsDown,
                down_slots: vec![DownSlotInfo {
                    slot: 1,
                    cause: "the OS ran short of a resource (EMFILE)".to_owned(),
                    detail: "d".to_owned(),
                    down_since: now,
                    failed_boots: 2,
                }],
                profile: SnapshotProfileInfo::NotRecorded,
            }),
            vec![
                "worker_id",
                "hostname",
                "pid",
                "snapshot_at",
                "snapshot_age_s",
                "stale",
                "worker_started_at",
                "uptime_s",
                "processes",
                "queues",
                "queue_max_concurrency",
                "tasks_running",
                "tasks_claimed",
                "cluster_wide_cap",
                "memory_usage_mb",
                "memory_percent",
                "cpu_percent",
                "slot_health",
                "down_slots",
                "profile",
            ],
        ),
        (
            keys(DownSlotInfo {
                slot: 1,
                cause: "c".to_owned(),
                detail: "d".to_owned(),
                down_since: now,
                failed_boots: 1,
            }),
            vec!["slot", "cause", "detail", "down_since", "failed_boots"],
        ),
        (
            keys(WorkerPingInfo {
                worker_id: "w".to_owned(),
                hostname: "h".to_owned(),
                pid: 1,
                round_trip_ms: 1.0,
            }),
            vec!["worker_id", "hostname", "pid", "round_trip_ms"],
        ),
        (
            keys(LivenessReport {
                db_latency_ms: Some(1.0),
                db_reachable: true,
                workers: vec![],
            }),
            vec!["db_latency_ms", "db_reachable", "workers"],
        ),
        (
            keys(WorkerHistoryPoint {
                snapshot_at: now,
                tasks_running: 0,
                tasks_claimed: 0,
                cpu_percent: None,
                memory_usage_mb: None,
                memory_percent: None,
            }),
            vec![
                "snapshot_at",
                "tasks_running",
                "tasks_claimed",
                "cpu_percent",
                "memory_usage_mb",
                "memory_percent",
            ],
        ),
        (
            keys(ScheduleStateInfo {
                schedule_name: "s".to_owned(),
                last_run_at: None,
                next_run_at: now,
                last_task_id: None,
                run_count: 0,
                updated_at: now,
            }),
            vec![
                "schedule_name",
                "last_run_at",
                "next_run_at",
                "last_task_id",
                "run_count",
                "updated_at",
            ],
        ),
    ];
    for (actual, expected) in cases {
        assert_eq!(actual, expected, "model field drift");
    }
    // Python's discriminated union of the breakdown group.
    assert_eq!(
        serde_json::to_value(BreakdownGroup::Value {
            value: "w1".to_owned()
        })
        .unwrap(),
        json!({"kind": "value", "value": "w1"})
    );
    assert_eq!(
        serde_json::to_value(BreakdownGroup::Unclaimed).unwrap(),
        json!({"kind": "unclaimed"})
    );
    assert_eq!(
        serde_json::to_value(task_id).unwrap(),
        json!(task_id.to_string())
    );
    // The tagged unions and the closed enums of the phase timing.
    assert_eq!(
        serde_json::to_value(SegmentPhaseInfo::RetryWait).unwrap(),
        json!({"kind": "retry_wait"})
    );
    assert_eq!(
        serde_json::to_value(SegmentEndInfo::Open).unwrap(),
        json!({"kind": "open"})
    );
    assert_eq!(
        serde_json::to_value(NodeTimingInfo::Unavailable {
            reason: UnavailableReason::TimingBoundExceeded,
        })
        .unwrap(),
        json!({"kind": "unavailable", "reason": "TIMING_BOUND_EXCEEDED"})
    );
    assert_eq!(
        serde_json::to_value(TimingConflictInfo::MissingRecord {
            record: MissingRecord::CompletionTime,
        })
        .unwrap(),
        json!({"kind": "missing_record", "record": "COMPLETION_TIME"})
    );
    assert_eq!(
        serde_json::to_value(RunConflictInfo::Node { task_index: 3 }).unwrap(),
        json!({"kind": "node", "task_index": 3})
    );
    assert_eq!(
        serde_json::to_value(PauseEndInfo::RunEnded { at: now }).unwrap(),
        json!({"kind": "run_ended", "at": "2026-08-08T12:00:00Z"})
    );
}

#[derive(Debug, Clone)]
struct TaskSeed {
    id: Uuid,
    name: String,
    queue: String,
    status: &'static str,
    worker: Option<String>,
    /// Not written by `seed_task`: the baseline live task row has no
    /// `error_code` column. The reads that classify by error code need a
    /// history row instead.
    #[allow(dead_code)]
    error_code: Option<String>,
    retry_count: i32,
    enqueued_seconds_ago: i64,
    started_seconds_ago: Option<i64>,
    workflow_task: bool,
}

#[derive(Debug, Clone)]
struct TestScope {
    prefix: String,
    queue: String,
    worker: String,
}

impl TestScope {
    /// The queue is `w2_<label>_<32 hex digits>_queue`, which the baseline
    /// bounds to 52 bytes (`horsies_tasks_queue_name_length`), so `label`
    /// has 10 bytes at most.
    fn new(label: &str) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let prefix = format!("w2_{label}_{suffix}_");
        Self {
            queue: format!("{prefix}queue"),
            worker: "w2-worker".to_owned(),
            prefix,
        }
    }

    fn task(&self, suffix: impl std::fmt::Display) -> String {
        format!("{}task_{suffix}", self.prefix)
    }

    fn workflow(&self, suffix: impl std::fmt::Display) -> String {
        format!("{}workflow_{suffix}", self.prefix)
    }

    fn schedule(&self, suffix: impl std::fmt::Display) -> String {
        format!("{}schedule_{suffix}", self.prefix)
    }

    fn pattern(&self) -> String {
        format!("{}%", self.prefix)
    }

    fn pending(&self, suffix: impl std::fmt::Display) -> TaskSeed {
        let mut seed = TaskSeed::pending(self.task(suffix));
        seed.queue = self.queue.clone();
        seed
    }

    fn running(&self, suffix: impl std::fmt::Display) -> TaskSeed {
        let mut seed = TaskSeed::running(self.task(suffix));
        seed.queue = self.queue.clone();
        seed.worker = Some(self.worker.clone());
        seed
    }
}

impl TaskSeed {
    fn pending(name: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            queue: "default".to_owned(),
            status: "PENDING",
            worker: None,
            error_code: None,
            retry_count: 0,
            enqueued_seconds_ago: 60,
            started_seconds_ago: None,
            workflow_task: false,
        }
    }

    fn running(name: impl Into<String>) -> Self {
        Self {
            status: "RUNNING",
            worker: Some("w2-worker".to_owned()),
            started_seconds_ago: Some(30),
            ..Self::pending(name)
        }
    }
}

/// Insert one live row. A RUNNING row carries the process facts that its
/// start writes, the facts of the attempt rows of [`seed_attempt`].
async fn seed_task(pool: &TestPool, seed: &TaskSeed) {
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
            started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
            max_retries, command_fingerprint_version, command_fingerprint, retention_class_key,
            retain_rerun_input, created_at, updated_at, execution, claim_expires_at,
            worker_pid, worker_hostname, worker_process_name
        ) VALUES (
            $1, $2, $3, 100, 'null', $4, NOW(), NOW() - make_interval(secs => $5),
            CASE WHEN $6::bigint IS NULL THEN NULL
                  ELSE NOW() - make_interval(secs => $6) END,
            CASE WHEN $7::text IS NULL THEN NULL ELSE NOW() END, $7, $8, $9, 3,
            CASE WHEN $8 THEN NULL ELSE 1 END,
            CASE WHEN $8 THEN NULL ELSE decode(repeat('0a', 32), 'hex') END, 'forever', FALSE,
            NOW(), NOW(), 'ISOLATED',
            CASE WHEN $4 = 'CLAIMED' THEN NOW() + interval '1 hour' END,
            CASE WHEN $4 = 'RUNNING' THEN 4242 END,
            CASE WHEN $4 = 'RUNNING' THEN 'w2-host' END,
            CASE WHEN $4 = 'RUNNING' THEN 'w2-process' END
        )",
    )
    .bind(seed.id)
    .bind(&seed.name)
    .bind(&seed.queue)
    .bind(seed.status)
    .bind(seed.enqueued_seconds_ago)
    .bind(seed.started_seconds_ago)
    .bind(&seed.worker)
    .bind(seed.workflow_task)
    .bind(seed.retry_count)
    .execute(pool)
    .await
    .expect("seed W2 task");
}

/// Insert attempt `attempt` of a live task: FAILED with `code`. Every column
/// of the baseline attempt row is named.
async fn seed_attempt(pool: &TestPool, task_id: Uuid, attempt: i32, code: &str) {
    sql::query(
        "INSERT INTO horsies_task_attempts (
             task_id, attempt, outcome, will_retry, started_at, finished_at,
             error_code, error_message, failed_reason, worker_id, worker_hostname,
             worker_pid, worker_process_name
         ) VALUES (
             $1, $2, 'FAILED', $2 = 1, NOW() - INTERVAL '20 seconds',
             NOW() - INTERVAL '10 seconds', $3, 'attempt message',
             'attempt reason', 'w2-worker', 'w2-host', 4242, 'w2-process'
         )",
    )
    .bind(task_id)
    .bind(attempt)
    .bind(code)
    .execute(pool)
    .await
    .expect("seed W2 attempt");
}

/// The success of a RUNNING plain task of `w2-worker`.
async fn complete_task(pool: &TestPool, task_id: Uuid) {
    let outcome = terminal_writes::complete_plain_task(pool, task_id, "w2-worker", "{\"Ok\":true}")
        .await
        .expect("complete W2 task");
    assert!(matches!(
        outcome.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

/// The terminal failure of a RUNNING task of `w2-worker`, plain or workflow.
async fn fail_task(pool: &TestPool, task_id: Uuid, code: &str) {
    let outcome = terminal_writes::fail_running_task(
        pool,
        task_id,
        "w2-worker",
        &TerminalFailure::of_task("{\"Err\":{}}", Some(code), Some("W2 failure")),
    )
    .await
    .expect("fail W2 task");
    assert!(matches!(
        outcome.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));
}

async fn clean_w2(pool: &TestPool, scope: &TestScope) {
    let pattern = scope.pattern();
    for statement in [
        "DELETE FROM horsies_workflows WHERE name LIKE $1",
        "DELETE FROM horsies_schedule_state WHERE schedule_name LIKE $1",
        "DELETE FROM horsies_task_history WHERE task_name LIKE $1",
        "DELETE FROM horsies_tasks WHERE task_name LIKE $1",
    ] {
        sql::query(statement)
            .bind(&pattern)
            .execute(pool)
            .await
            .expect("clean W2 rows");
    }
}

fn test_window() -> crate::core::history::reads::pages::HistoryWindow {
    crate::core::history::reads::pages::HistoryWindow::new(
        Utc::now() - Duration::days(29),
        Utc::now() + Duration::hours(1),
    )
    .expect("W2 test window")
}

#[tokio::test]
async fn live_and_history_aggregates_merge_before_caps() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("aggregate");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    sql::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, execution, status,
             sent_at, enqueued_at, is_workflow_task,
             retry_count, max_retries,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input, created_at, updated_at
         )
         SELECT gen_random_uuid(), $1 || lpad((g % 50)::text, 3, '0'),
                $2, 100, 'null', 'ISOLATED', 'PENDING', NOW(),
                NOW() - make_interval(secs => g), FALSE, 0, 3,
                1, decode(repeat('0b', 32), 'hex'),
                'forever', FALSE, NOW(), NOW()
         FROM generate_series(1, 100) AS g",
    )
    .bind(scope.task("competitor_"))
    .bind(&scope.queue)
    .execute(&pool)
    .await
    .expect("seed W2 facet competitors");

    let shared_name = scope.task("shared_target");
    let shared_live = scope.pending("shared_target");
    seed_task(&pool, &shared_live).await;
    for _ in 0..2 {
        let shared_history = scope.running("shared_target");
        seed_task(&pool, &shared_history).await;
        complete_task(&pool, shared_history.id).await;
    }
    for index in 0..35 {
        let mut failed = scope.running(format!("domain_{index:03}"));
        failed.error_code = Some(format!("W2_DOMAIN_{index:03}"));
        seed_task(&pool, &failed).await;
        fail_task(&pool, failed.id, failed.error_code.as_deref().unwrap()).await;
    }
    let mut claimed = scope.pending("claimed_status");
    claimed.status = "CLAIMED";
    claimed.worker = Some(scope.worker.clone());
    seed_task(&pool, &claimed).await;

    let running = scope.running("running_status");
    seed_task(&pool, &running).await;

    let cancelled = scope.pending("cancelled_status");
    seed_task(&pool, &cancelled).await;
    let cancelled_outcome = terminal_writes::cancel_live_task(&pool, cancelled.id, false)
        .await
        .expect("cancel W2 task");
    assert!(matches!(
        cancelled_outcome.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));

    let mut expired = scope.pending("expired_status");
    expired.status = "CLAIMED";
    expired.worker = Some(scope.worker.clone());
    seed_task(&pool, &expired).await;
    sql::query("UPDATE horsies_tasks SET good_until = NOW() - INTERVAL '1 minute' WHERE id = $1")
        .bind(expired.id)
        .execute(&pool)
        .await
        .expect("make W2 task expired");
    let expired_outcome = terminalize(
        &pool,
        &TerminalizationCommand::ExpireOwnedClaim {
            task_id: expired.id,
            fence: WorkerOwned {
                worker_id: scope.worker.clone(),
            },
            result_json: "{\"Err\":{}}".to_owned(),
            error_code: "TASK_EXPIRED".to_owned(),
        },
    )
    .await
    .expect("expire W2 task");
    assert!(matches!(
        expired_outcome.as_slice(),
        [TerminalizationOutcome::Applied { .. }]
    ));

    let mut outside_window = scope.running("outside_window");
    outside_window.error_code = Some("W2_OUTSIDE_WINDOW".to_owned());
    seed_task(&pool, &outside_window).await;
    fail_task(&pool, outside_window.id, "W2_OUTSIDE_WINDOW").await;
    let outside_class = stored_class_key(&pool, outside_window.id).await;
    let outside_end = covered_history_instant(
        &pool,
        &outside_class,
        SeedInstant::BeforeNow(Duration::minutes(5)),
    )
    .await;
    sql::query(
        "UPDATE horsies_task_history
         SET sent_at = $2 - INTERVAL '2 minutes',
             enqueued_at = $2 - INTERVAL '2 minutes',
             started_at = $2 - INTERVAL '1 minute',
             created_at = $2 - INTERVAL '2 minutes',
             terminal_at = $2
         WHERE task_id = $1",
    )
    .bind(outside_window.id)
    .bind(outside_end.at())
    .execute(&pool)
    .await
    .expect("age W2 history outside window");

    let bounded_window = crate::core::history::reads::pages::HistoryWindow::new(
        Utc::now() - Duration::minutes(1),
        Utc::now() + Duration::hours(1),
    )
    .expect("bounded W2 aggregate window");
    let scoped_filters = TaskFilters {
        queues: vec![scope.queue.clone()],
        ..TaskFilters::default()
    };
    let stats = task_stats(
        &broker,
        &TaskStatsQuery::new(bounded_window).with_filters(scoped_filters.clone()),
    )
    .await
    .expect("W2 stats");
    let counts: BTreeMap<_, _> = stats
        .into_iter()
        .map(|row| (row.status, row.count))
        .collect();
    assert_eq!(counts["PENDING"], 101);
    assert_eq!(counts["CLAIMED"], 1);
    assert_eq!(counts["RUNNING"], 1);
    assert_eq!(counts["COMPLETED"], 2);
    assert_eq!(counts["FAILED"], 35);
    assert_eq!(counts["CANCELLED"], 1);
    assert_eq!(counts["EXPIRED"], 1);

    let facets = task_facets(
        &broker,
        &TaskFacetsQuery::new(bounded_window).with_filters(scoped_filters.clone()),
    )
    .await
    .expect("W2 facets");
    assert_eq!(facets.task_names.len(), 50);
    assert_eq!(
        facets
            .task_names
            .iter()
            .find(|facet| facet.value == shared_name)
            .map(|facet| facet.count),
        Some(3)
    );
    assert_eq!(facets.error_codes.len(), 30);
    assert_eq!(facets.error_category_totals["DOMAIN"], 35);

    let outcome_only = task_facets(
        &broker,
        &TaskFacetsQuery::new(bounded_window)
            .with_filters(scoped_filters.clone())
            .with_error_categories(vec![ErrorCategory::Outcome]),
    )
    .await
    .expect("W2 category facet");
    assert_eq!(
        outcome_only
            .error_codes
            .iter()
            .map(|facet| facet.value.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["TASK_CANCELLED", "TASK_EXPIRED"])
    );
    assert_eq!(outcome_only.error_category_totals["OUTCOME"], 2);
    assert_eq!(outcome_only.error_category_totals["DOMAIN"], 35);

    let breakdown = task_breakdown(
        &broker,
        &TaskBreakdownQuery::new(bounded_window, TaskGroupBy::TaskName)
            .with_filters(scoped_filters)
            .with_limit(2)
            .expect("W2 breakdown limit"),
    )
    .await
    .expect("W2 breakdown");
    assert_eq!(breakdown.groups.len(), 2);
    assert_eq!(breakdown.group_count, 90);
    assert_eq!(breakdown.total.total, 142);
    assert_eq!(
        breakdown.groups[0].group,
        BreakdownGroup::Value {
            value: shared_name.clone()
        }
    );
    assert_eq!(breakdown.groups[0].counts.total, 3);
    assert_eq!(breakdown.groups[0].counts.pending, 1);
    assert_eq!(breakdown.groups[0].counts.completed, 2);

    clean_w2(&pool, &scope).await;
}

#[tokio::test]
async fn list_merge_sorts_nulls_last_and_uses_estimate_or_exact_totals() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("list");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    for index in 0..5 {
        let mut live = scope.pending(format!("estimate_live_{index}"));
        live.enqueued_seconds_ago = 100 - index;
        seed_task(&pool, &live).await;
        let mut history = scope.running(format!("estimate_history_{index}"));
        history.enqueued_seconds_ago = 200 - index;
        seed_task(&pool, &history).await;
        complete_task(&pool, history.id).await;
    }
    sql::query("ANALYZE horsies_tasks, horsies_task_history")
        .execute(&pool)
        .await
        .expect("sample W2 lifecycle tables");
    for index in 5..8 {
        let mut live = scope.pending(format!("estimate_live_{index}"));
        live.enqueued_seconds_ago = 100 - index;
        seed_task(&pool, &live).await;
        let mut history = scope.running(format!("estimate_history_{index}"));
        history.enqueued_seconds_ago = 200 - index;
        seed_task(&pool, &history).await;
        complete_task(&pool, history.id).await;
    }

    let window = test_window();
    let unfiltered = list_tasks(
        &broker,
        &TaskListQuery::new(window).with_sort(TaskSortField::EnqueuedAt, SortDirection::Ascending),
    )
    .await
    .expect("unfiltered W2 list");
    let live_estimate: i64 = sql::query_scalar(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'horsies_tasks'::regclass",
    )
    .fetch_one(&pool)
    .await
    .expect("read live estimate");
    // The history side of an unfiltered total is the planner's estimate of
    // the window read that `horsies_monitoring_task_list` explains.
    let history_payload: serde_json::Value = sql::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT 1 FROM horsies_task_history AS h
         WHERE h.terminal_at >= $1 AND h.terminal_at < $2",
    )
    .bind(window.lower())
    .bind(window.upper())
    .fetch_one(&pool)
    .await
    .expect("read history estimate");
    let history_estimate = crate::core::history::reads::aggregates::plan_rows_from_explain_text(
        &history_payload.to_string(),
    )
    .expect("decode history estimate");
    assert_eq!(unfiltered.total, live_estimate + history_estimate);
    let pending = list_tasks(
        &broker,
        &TaskListQuery::new(test_window()).with_filters(TaskFilters {
            statuses: vec![crate::TaskStatus::Pending],
            task_names: (0..8)
                .map(|index| scope.task(format!("estimate_live_{index}")))
                .collect(),
            ..TaskFilters::default()
        }),
    )
    .await
    .expect("filtered pending list");
    assert_eq!(pending.total, 8);
    assert_eq!(pending.rows.len(), 8);
    let completed = list_tasks(
        &broker,
        &TaskListQuery::new(test_window())
            .with_filters(TaskFilters {
                statuses: vec![crate::TaskStatus::Completed],
                task_names: (0..8)
                    .map(|index| scope.task(format!("estimate_history_{index}")))
                    .collect(),
                ..TaskFilters::default()
            })
            .with_sort(TaskSortField::CompletedAt, SortDirection::Ascending),
    )
    .await
    .expect("filtered completed list");
    assert_eq!(completed.total, 8);
    assert_eq!(completed.rows.len(), 8);

    for direction in [SortDirection::Ascending, SortDirection::Descending] {
        let mixed = list_tasks(
            &broker,
            &TaskListQuery::new(test_window())
                .with_filters(TaskFilters {
                    task_names: vec![
                        scope.task("estimate_live_0"),
                        scope.task("estimate_history_0"),
                    ],
                    ..TaskFilters::default()
                })
                .with_sort(TaskSortField::CompletedAt, direction),
        )
        .await
        .expect("nullable lifecycle sort");
        assert_eq!(mixed.rows.len(), 2);
        assert_eq!(mixed.rows[0].task_name, scope.task("estimate_history_0"));
        assert_eq!(mixed.rows[1].task_name, scope.task("estimate_live_0"));
    }

    let page = list_tasks(
        &broker,
        &TaskListQuery::new(test_window())
            .with_filters(TaskFilters {
                task_names: vec![
                    scope.task("estimate_history_0"),
                    scope.task("estimate_history_1"),
                    scope.task("estimate_live_0"),
                ],
                ..TaskFilters::default()
            })
            .with_sort(TaskSortField::EnqueuedAt, SortDirection::Ascending)
            .with_pagination(1, 1)
            .expect("merged page"),
    )
    .await
    .expect("merged page query");
    assert_eq!(page.total, 3);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].task_name, scope.task("estimate_history_1"));

    clean_w2(&pool, &scope).await;
}

/// The count of each status under `filters`, by status text.
async fn stats_by_status(broker: &PostgresBroker, filters: TaskFilters) -> BTreeMap<String, i64> {
    task_stats(
        broker,
        &TaskStatsQuery::new(test_window()).with_filters(filters),
    )
    .await
    .expect("filtered W2 stats")
    .into_iter()
    .map(|row| (row.status, row.count))
    .collect()
}

/// Every filter dimension scopes the stats and the list on both sides,
/// with the baseline rule for error codes: a live row has no error code, so
/// an error code or category filter selects history rows only. A history
/// row's spans are closed; a live row's run span counts up.
#[tokio::test]
async fn every_filter_dimension_and_duration_rule_crosses_both_lifecycle_sides() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("filters");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);

    let match_name = scope.task("filter_match");
    let mut live_match = scope.running("filter_match");
    live_match.retry_count = 2;
    live_match.enqueued_seconds_ago = 60;
    live_match.started_seconds_ago = Some(40);
    seed_task(&pool, &live_match).await;

    let mut history_match = live_match.clone();
    history_match.id = Uuid::new_v4();
    history_match.enqueued_seconds_ago = 90;
    history_match.started_seconds_ago = Some(60);
    seed_task(&pool, &history_match).await;
    fail_task(&pool, history_match.id, "TASK_EXCEPTION").await;

    // A history row that differs from the history match only in its code.
    let mut wrong_code = live_match.clone();
    wrong_code.id = Uuid::new_v4();
    seed_task(&pool, &wrong_code).await;
    fail_task(&pool, wrong_code.id, "WAIT_TIMEOUT").await;

    // Live rows that differ from the live match in one dimension each.
    for (name, queue, worker, retry_count) in [
        ("wrong_name", "matching", "w2-worker", 2),
        ("filter_match", "other", "w2-worker", 2),
        ("filter_match", "matching", "another-worker", 2),
        ("filter_match", "matching", "w2-worker", 0),
    ] {
        let mut decoy = scope.running(name);
        decoy.queue = match queue {
            "matching" => scope.queue.clone(),
            other => format!("{}{other}", scope.prefix),
        };
        decoy.worker = Some(worker.to_owned());
        decoy.retry_count = retry_count;
        seed_task(&pool, &decoy).await;
    }

    let without_codes = TaskFilters {
        task_names: vec![match_name.clone()],
        queues: vec![scope.queue.clone()],
        workers: vec!["w2-worker".to_owned()],
        retried_only: true,
        ..TaskFilters::default()
    };
    let with_codes = TaskFilters {
        error_codes: vec!["TASK_EXCEPTION".to_owned()],
        error_categories: vec![ErrorCategory::Operational],
        ..without_codes.clone()
    };

    let both_sides = stats_by_status(&broker, without_codes.clone()).await;
    assert_eq!(both_sides["RUNNING"], 1);
    assert_eq!(both_sides["FAILED"], 2);
    assert_eq!(both_sides.values().sum::<i64>(), 3);
    let history_only = stats_by_status(&broker, with_codes.clone()).await;
    assert_eq!(history_only["FAILED"], 1);
    assert_eq!(history_only.values().sum::<i64>(), 1);

    let rows = list_tasks(
        &broker,
        &TaskListQuery::new(test_window())
            .with_filters(without_codes)
            .with_sort(TaskSortField::ExecutionSeconds, SortDirection::Descending),
    )
    .await
    .expect("filtered W2 list");
    assert_eq!(rows.total, 3);
    assert_eq!(
        rows.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![history_match.id, wrong_code.id, live_match.id]
    );
    assert_eq!(rows.rows[0].queue_s, Some(Duration::seconds(30)));
    assert!(rows.rows[0]
        .exec_s
        .is_some_and(|exec| exec >= Duration::seconds(60)));
    assert_eq!(rows.rows[2].queue_s, Some(Duration::seconds(20)));
    assert!(rows.rows[2]
        .exec_s
        .is_some_and(|exec| exec >= Duration::seconds(40)));

    let history_rows = list_tasks(
        &broker,
        &TaskListQuery::new(test_window())
            .with_filters(with_codes)
            .with_sort(TaskSortField::ExecutionSeconds, SortDirection::Descending),
    )
    .await
    .expect("filtered W2 list with codes");
    assert_eq!(history_rows.total, 1);
    assert_eq!(
        history_rows
            .rows
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![history_match.id]
    );

    let mut pending = scope.pending("pending_duration");
    pending.enqueued_seconds_ago = 30;
    seed_task(&pool, &pending).await;
    let pending_row = list_tasks(
        &broker,
        &TaskListQuery::new(test_window()).with_filters(TaskFilters {
            task_names: vec![pending.name.clone()],
            ..TaskFilters::default()
        }),
    )
    .await
    .unwrap()
    .rows
    .pop()
    .expect("pending duration row");
    assert!(pending_row
        .queue_s
        .is_some_and(|queue| queue >= Duration::seconds(30)));
    assert_eq!(pending_row.exec_s, None);

    // Only the status and `retried_only` scope the facets: the two FAILED
    // retried rows of the name count, the live rows do not.
    let failed_facets = task_facets(
        &broker,
        &TaskFacetsQuery::new(test_window()).with_filters(TaskFilters {
            statuses: vec![crate::TaskStatus::Failed],
            task_names: vec![match_name.clone()],
            queues: vec![scope.queue.clone()],
            retried_only: true,
            ..TaskFilters::default()
        }),
    )
    .await
    .expect("status/retry facets");
    assert_eq!(
        failed_facets
            .task_names
            .iter()
            .find(|facet| facet.value == match_name)
            .map(|facet| facet.count),
        Some(2)
    );

    clean_w2(&pool, &scope).await;
}

async fn seed_workflow(pool: &TestPool, workflow_id: Uuid, name: &str, status: &str) {
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth,
             root_workflow_id, sent_at, created_at, started_at, updated_at
         ) VALUES (
             $1, $2, $3, 'fail', 'w2.definition.v1', 0, $1,
             NOW(), NOW() - INTERVAL '120 seconds',
             NOW() - INTERVAL '60 seconds', NOW()
         )",
    )
    .bind(workflow_id)
    .bind(name)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed W2 workflow");
}

async fn seed_node(
    pool: &TestPool,
    scope: &TestScope,
    workflow_id: Uuid,
    task_index: i32,
    status: &str,
    dependencies: &[i32],
    task_id: Option<Uuid>,
    child_id: Option<Uuid>,
) {
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
            dependencies, allow_failed_deps, join_type, status, task_id, is_subworkflow,
            sub_workflow_params, sub_workflow_id, created_at, started_at, completed_at,
            max_retries, execution,
            retention_class_key
        ) VALUES (
            'NONE', $1, $2, $3, $8 || $3, $8 || 'node_task', 'null', $9, 100, $4, FALSE, 'all', $5, $6,
            $7 IS NOT NULL, CASE WHEN $7 IS NOT NULL THEN 'NOT_GIVEN' END, $7, NOW(),
            CASE WHEN $5 IN ('RUNNING', 'COMPLETED', 'FAILED')
                  THEN NOW() - INTERVAL '30 seconds' ELSE NULL END,
            CASE WHEN $5 IN ('COMPLETED', 'FAILED')
                  THEN NOW() - INTERVAL '10 seconds' ELSE NULL END,
            CASE WHEN $7 IS NOT NULL THEN NULL ELSE 0 END,
            CASE WHEN $7 IS NOT NULL THEN NULL ELSE 'ISOLATED' END,
            CASE WHEN $7 IS NOT NULL THEN NULL ELSE 'standard_30d' END
        )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(task_index)
    .bind(dependencies)
    .bind(status)
    .bind(task_id)
    .bind(child_id)
    .bind(&scope.prefix)
    .bind(&scope.queue)
    .execute(pool)
    .await
    .expect("seed W2 workflow node");
}

#[tokio::test]
async fn task_detail_uses_live_attempts_then_digest_verified_history() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("detail");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let workflow_id = Uuid::new_v4();
    seed_workflow(&pool, workflow_id, &scope.workflow("detail"), "RUNNING").await;
    // The second run: attempt 1 failed and was retried, and the failure of
    // this run writes attempt 2.
    let mut task = scope.running("detail_task");
    task.workflow_task = true;
    task.error_code = Some("TASK_EXCEPTION".to_owned());
    task.retry_count = 1;
    seed_task(&pool, &task).await;
    seed_attempt(&pool, task.id, 1, "TASK_EXCEPTION").await;
    seed_attempt(&pool, task.id, 2, "TASK_EXCEPTION").await;
    seed_node(
        &pool,
        &scope,
        workflow_id,
        3,
        "RUNNING",
        &[],
        Some(task.id),
        None,
    )
    .await;

    let live = found_detail(
        get_task_detail(&broker, task.id)
            .await
            .expect("live detail"),
    );
    assert_eq!(live.leaf.status, "RUNNING");
    assert_eq!(live.workflow_id, Some(workflow_id));
    assert_eq!(live.workflow_task_index, Some(3));
    assert_eq!(
        live.attempts
            .iter()
            .map(|attempt| attempt.attempt)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        live.attempts[0].error_message.as_deref(),
        Some("attempt message")
    );

    fail_task(&pool, task.id, "TASK_EXCEPTION").await;
    let history = found_detail(
        get_task_detail(&broker, task.id)
            .await
            .expect("history detail"),
    );
    assert_eq!(history.leaf.status, "FAILED");
    assert!(history.leaf.failed_at.is_some());
    assert_eq!(history.leaf.completed_at, None);
    assert_eq!(history.attempts.len(), 2);
    assert_eq!(
        history.attempts[1].worker_hostname.as_deref(),
        Some("w2-host")
    );

    let node = get_workflow_node(&broker, workflow_id, 3)
        .await
        .expect("history node detail")
        .expect("workflow node");
    assert_eq!(
        node.leaf.as_ref().map(|leaf| leaf.status.as_str()),
        Some("FAILED")
    );
    assert_eq!(node.attempts.len(), 2);

    sql::query(
        "UPDATE horsies_task_history
         SET attempt_snapshot_digest = decode(repeat('00', 32), 'hex')
         WHERE task_id = $1",
    )
    .bind(task.id)
    .execute(&pool)
    .await
    .expect("corrupt W2 attempt digest");
    let error = get_task_detail(&broker, task.id)
        .await
        .expect_err("digest corruption must fail closed");
    assert_eq!(
        error.code(),
        MonitoringQueryErrorCode::HistoryContractViolation
    );
    assert!(!error.retryable());
    assert!(error.message().contains("digest"));

    let unknown = Uuid::new_v4();
    assert_eq!(
        get_task_detail(&broker, unknown).await.unwrap(),
        TaskDetailRead::NotFound(TaskDetailNotFound {
            task_id: unknown,
            reason: TaskAbsenceReason::NotClassified,
        })
    );
    clean_w2(&pool, &scope).await;
}

#[tokio::test]
async fn workflow_graph_node_and_schedule_reads_match_the_contract() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("workflow");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let root_name = scope.workflow("root");
    let child_name = scope.workflow("child");
    seed_workflow(&pool, root, &root_name, "RUNNING").await;
    seed_workflow(&pool, child, &child_name, "FAILED").await;
    sql::query(
        "UPDATE horsies_workflows
         SET parent_workflow_id = $1, parent_task_index = 2, root_workflow_id = $1, depth = 1
         WHERE id = $2",
    )
    .bind(root)
    .bind(child)
    .execute(&pool)
    .await
    .expect("link W2 child workflow");

    seed_node(&pool, &scope, root, 0, "COMPLETED", &[], None, None).await;
    seed_node(&pool, &scope, root, 1, "FAILED", &[0], None, None).await;
    seed_node(
        &pool,
        &scope,
        root,
        2,
        "RUNNING",
        &[0, 1, 99],
        None,
        Some(child),
    )
    .await;
    seed_node(&pool, &scope, child, 0, "COMPLETED", &[], None, None).await;
    seed_node(&pool, &scope, child, 1, "FAILED", &[0], None, None).await;

    assert!(list_workflow_names(&broker)
        .await
        .unwrap()
        .contains(&root_name));
    let runs = list_workflow_runs(
        &broker,
        &WorkflowRunsQuery::new()
            .with_name(Some(root_name.clone()))
            .with_status(Some("RUNNING".to_owned())),
    )
    .await
    .expect("W2 workflow runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, root);
    assert!(runs[0]
        .wall_s
        .is_some_and(|wall| wall >= Duration::seconds(120)));
    assert!(list_workflow_runs(
        &broker,
        &WorkflowRunsQuery::new().with_status(Some("NOT_A_STATUS".to_owned())),
    )
    .await
    .unwrap()
    .is_empty());

    let detail = get_workflow_run(&broker, root)
        .await
        .expect("W2 workflow detail")
        .expect("root workflow");
    assert_eq!(
        detail
            .nodes
            .iter()
            .map(|node| node.task_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        detail
            .edges
            .iter()
            .map(|edge| (edge.from_index, edge.to_index))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([(0, 1), (0, 2), (1, 2)])
    );
    assert_eq!(detail.failed_count, 1);
    assert_eq!(detail.failed_indices, vec![1]);
    assert_eq!(
        (detail.nodes[2].child_total, detail.nodes[2].child_failed),
        (Some(2), Some(1))
    );
    assert_eq!(
        (detail.nodes[1].child_total, detail.nodes[1].child_failed),
        (None, None)
    );
    assert_eq!(detail.nodes[0].exec_s, Some(Duration::seconds(20)));
    assert!(detail.nodes[2]
        .exec_s
        .is_some_and(|exec| exec >= Duration::seconds(30)));
    assert!(get_workflow_run(&broker, Uuid::new_v4())
        .await
        .unwrap()
        .is_none());
    assert!(get_workflow_node(&broker, root, 99)
        .await
        .unwrap()
        .is_none());
    let subworkflow = get_workflow_node(&broker, root, 2)
        .await
        .unwrap()
        .expect("subworkflow node");
    assert!(subworkflow.is_subworkflow);
    assert!(subworkflow.leaf.is_none());
    assert!(subworkflow.attempts.is_empty());

    // The baseline row names every column: a schedule that ran has its last
    // run time and task, and `next_run_at` is never NULL.
    let now = Utc::now();
    for (name, next, count) in [
        (scope.schedule("later"), now + Duration::hours(3), 1),
        (scope.schedule("tomorrow"), now + Duration::days(1), 2),
        (scope.schedule("soon"), now + Duration::minutes(5), 3),
    ] {
        sql::query(
            "INSERT INTO horsies_schedule_state (
                 schedule_name, next_run_at, last_run_at, last_task_id, run_count,
                 config_hash, updated_at
             ) VALUES (
                 $1, $2, NOW() - INTERVAL '1 hour', $3, $4, decode(repeat('00', 32), 'hex'),
                 NOW()
             )",
        )
        .bind(name)
        .bind(next)
        .bind(Uuid::new_v4())
        .bind(count)
        .execute(&pool)
        .await
        .expect("seed W2 schedule");
    }
    let schedules = list_schedules(&broker).await.expect("W2 schedules");
    let soon = scope.schedule("soon");
    let later = scope.schedule("later");
    let tomorrow = scope.schedule("tomorrow");
    let scoped_schedules = schedules
        .iter()
        .filter(|schedule| {
            [soon.as_str(), later.as_str(), tomorrow.as_str()]
                .contains(&schedule.schedule_name.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        scoped_schedules
            .iter()
            .map(|schedule| schedule.schedule_name.as_str())
            .collect::<Vec<_>>(),
        [soon.as_str(), later.as_str(), tomorrow.as_str()]
    );
    assert_eq!(scoped_schedules[0].run_count, 3);

    clean_w2(&pool, &scope).await;
}

#[tokio::test]
async fn database_failure_is_typed_and_retryable() {
    let pool = TestPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(250))
        .connect_lazy("postgresql://postgres:none@127.0.0.1:1/none");
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let error = list_tasks(&broker, &TaskListQuery::new(test_window()))
        .await
        .expect_err("unreachable monitoring database");
    assert_eq!(error.code(), MonitoringQueryErrorCode::DatabaseOperationFailed);
    assert!(error.retryable(), "{error:?}");
    assert!(error.message().contains("task list query failed"));
}

/// A published reader dropped out of band fails the detail read with a
/// named database error, not an absence; once the readers are published
/// again, the read finds the task (owner decision of 2026-11-16 on #838).
#[tokio::test]
async fn a_dropped_staged_reader_fails_the_detail_until_it_is_published_again() {
    let pool = migrated_pool().await;
    let scope = TestScope::new("unpub");
    clean_w2(&pool, &scope).await;
    let broker = PostgresBroker::from_test_url(pool.url(), crate::ClientFamily::Rust);
    let task = scope.running("unpublished_detail");
    seed_task(&pool, &task).await;
    complete_task(&pool, task.id).await;
    assert!(matches!(
        get_task_detail(&broker, task.id).await.unwrap(),
        TaskDetailRead::Found(_)
    ));

    sql::query(&format!(
        "DROP FUNCTION {}(uuid)",
        crate::core::history::names::TASK_DETAIL_FUNCTION
    ))
    .execute(&pool)
    .await
    .expect("drop staged detail function");
    let error = get_task_detail(&broker, task.id)
        .await
        .expect_err("a dropped reader is an error, not an absence");
    assert_eq!(error.code(), MonitoringQueryErrorCode::DatabaseOperationFailed);
    assert!(
        error
            .message()
            .contains(crate::core::history::names::TASK_DETAIL_FUNCTION),
        "{}",
        error.message()
    );

    let published: String =
        sql::query_scalar("SELECT outcome FROM horsies_publish_history_readers()")
            .fetch_one(&pool)
            .await
            .expect("restore staged readers");
    assert!(
        matches!(published.as_str(), "PUBLISHED" | "ALREADY_CURRENT"),
        "{published}"
    );
    assert!(matches!(
        get_task_detail(&broker, task.id).await.unwrap(),
        TaskDetailRead::Found(_)
    ));
    clean_w2(&pool, &scope).await;
}
