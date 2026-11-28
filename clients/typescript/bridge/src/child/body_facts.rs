//! The body facts of a JavaScript child: the facts of each run that the
//! core gives to a client that traces its task bodies (`BodySpans`).
//!
//! - With `NO_SCOPE` the core calls nothing, and each `runBody` post holds
//!   `notRecorded`.
//! - With `SPANS` the core calls the recorder before each body that starts
//!   (`open`, with `BodyRun`). The recorder keeps the facts under the task
//!   id and the attempt, and the body call of the run takes them into its
//!   `runBody` post. The span that the core enters is
//!   `tracing::Span::none()`: the attempt span is the JavaScript body
//!   observer's.
//! - At `close` the core names the run (`BodyRunKey`) and how it ended
//!   (`BodyOutcome`). The recorder drops the facts of that run that no body
//!   call took, and posts `bodyEnded` with the task id, the attempt and the
//!   core's end. The JavaScript side ends the attempt span of that run with
//!   the core's end, so a cancel that fires when the body returns ends the
//!   span as the core ended the run. A `bodyEnded` post that Node-API
//!   refuses is logged at ERROR: the span of that run does not end. A post
//!   while the JavaScript environment closes is left: the span ends with
//!   the environment.
//! - The table holds at most [`RECORDED_FACTS_BOUND`] runs. A body call takes
//!   its facts at its start, so the table holds only the runs whose bodies
//!   start now; past the bound the oldest entry is dropped.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use horsies_core::frame_protocol::MAX_SEATS_PER_SHARED_ASYNC_CHILD;
use horsies_core::{
    BodyCarrier, BodyOutcome, BodyRun, BodyRunKey, BodyScope, BodySpans, Execution, FencingToken,
    RunWorkflow,
};
use napi_derive::napi;
use uuid::Uuid;

use crate::child::body::{ChildPost, PostRefused, RunPosts};

/// The most runs whose facts wait for their body call: the most seats of a
/// child.
pub const RECORDED_FACTS_BOUND: usize = seats_bound();

/// The most seats of a shared async child, as a `usize`.
const fn seats_bound() -> usize {
    // The core's bound is 1,000: it fits every `usize`.
    MAX_SEATS_PER_SHARED_ASYNC_CHILD as usize
}

/// Whether the child records the facts of its task bodies.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyFactsMode {
    /// The app does not trace its task bodies.
    NoScope,
    /// The app traces its task bodies: each `runBody` post holds the facts.
    Spans,
}

/// The execution class of the child that runs a body.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyExecution {
    Isolated,
    SharedAsync,
    SharedThread,
}

impl BodyExecution {
    /// The name of the core's class. Pure.
    pub(crate) fn of(execution: Execution) -> Self {
        match execution {
            Execution::Isolated => Self::Isolated,
            Execution::SharedAsync => Self::SharedAsync,
            Execution::SharedThread => Self::SharedThread,
        }
    }
}

/// The workflow node that links a run, as its start read it.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BodyWorkflowRecord {
    NotInWorkflow,
    InWorkflow {
        workflow_id: String,
        node_index: u32,
        root_workflow_id: String,
    },
}

impl BodyWorkflowRecord {
    /// The record of the core's node. Pure.
    pub(crate) fn of(workflow: RunWorkflow) -> Self {
        match workflow {
            RunWorkflow::NotInWorkflow => Self::NotInWorkflow,
            RunWorkflow::InWorkflow {
                workflow_id,
                node_index,
                root_workflow_id,
            } => Self::InWorkflow {
                workflow_id: workflow_id.to_string(),
                node_index: node_index.get(),
                root_workflow_id: root_workflow_id.to_string(),
            },
        }
    }
}

/// One entry of a trace carrier, as the send's propagator wrote it.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CarrierEntry {
    pub key: String,
    pub value: String,
}

/// The trace carrier that the send stored on the task row.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BodyCarrierRecord {
    NoCarrier,
    /// The entries in key order.
    Carrier {
        entries: Vec<CarrierEntry>,
    },
}

impl BodyCarrierRecord {
    /// The record of the core's carrier. Pure.
    pub(crate) fn of(carrier: &BodyCarrier) -> Self {
        match carrier {
            BodyCarrier::NoCarrier => Self::NoCarrier,
            BodyCarrier::Carrier(map) => Self::Carrier {
                entries: map
                    .iter()
                    .map(|(key, value)| CarrierEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            },
        }
    }
}

/// The facts of one run, for the body observer.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BodyFactsRecord {
    /// The child records no facts (`NO_SCOPE`).
    NotRecorded,
    /// The facts that the core gave when the body started. `attempt` is
    /// from 1.
    Recorded {
        task_id: String,
        task_name: String,
        queue_name: String,
        attempt: u32,
        worker_id: String,
        execution: BodyExecution,
        workflow: BodyWorkflowRecord,
        carrier: BodyCarrierRecord,
    },
}

impl BodyFactsRecord {
    /// The record of the core's facts of a run. Pure.
    fn of(run: &BodyRun<'_>) -> Self {
        Self::Recorded {
            task_id: run.task_id.to_string(),
            task_name: run.task_name.to_owned(),
            queue_name: run.queue_name.to_owned(),
            attempt: run.attempt.get(),
            worker_id: run.worker_id.to_owned(),
            execution: BodyExecution::of(run.execution),
            workflow: BodyWorkflowRecord::of(run.workflow),
            carrier: BodyCarrierRecord::of(run.carrier),
        }
    }
}

/// How the core ended a body, for the body observer. `code` is the text of
/// the stored error code.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BodyEndRecord {
    /// The body returned a value.
    Returned,
    /// The body returned an error, or panicked.
    Failed { code: String },
    /// The deadline of the run came before the end of the body.
    TimedOut,
    /// An operator cancelled the task before the end of the body.
    Cancelled,
}

impl BodyEndRecord {
    /// The record of the core's end. Pure.
    fn of(end: &BodyOutcome<'_>) -> Self {
        match end {
            BodyOutcome::Returned => Self::Returned,
            BodyOutcome::Failed { error_code } => Self::Failed {
                code: error_code.to_string(),
            },
            BodyOutcome::TimedOut => Self::TimedOut,
            BodyOutcome::Cancelled => Self::Cancelled,
        }
    }
}

/// The run of one entry: its task id and its attempt.
type RunOfFacts = (Uuid, u32);

/// The run of `key`. Pure.
fn run_of_key(key: BodyRunKey) -> RunOfFacts {
    (key.task_id, key.attempt.get())
}

/// The recorder of the body facts of a child.
pub struct BodyFactsRecorder {
    /// The facts in the order the core opened their runs.
    recorded: Mutex<VecDeque<(RunOfFacts, BodyFactsRecord)>>,
    /// Where the `bodyEnded` posts go.
    posts: Arc<dyn RunPosts>,
}

impl BodyFactsRecorder {
    /// The recorder whose `bodyEnded` posts go to `posts`.
    pub fn new(posts: Arc<dyn RunPosts>) -> Self {
        Self {
            recorded: Mutex::new(VecDeque::new()),
            posts,
        }
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<(RunOfFacts, BodyFactsRecord)>> {
        // A poisoned lock means a panic while it was held; each entry is a
        // valid value.
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keep the facts of `run`; past the bound the oldest entry is dropped.
    fn record(&self, run: RunOfFacts, facts: BodyFactsRecord) {
        let mut recorded = self.lock();
        if recorded.len() >= RECORDED_FACTS_BOUND {
            let _oldest_dropped = recorded.pop_front();
        }
        recorded.push_back((run, facts));
    }

    /// The facts of the run of `token`, taken out of the table. A run that
    /// the core did not open has none: `notRecorded`.
    fn take(&self, token: FencingToken) -> BodyFactsRecord {
        self.take_run((token.task_id(), token.value().get()))
    }

    /// The facts of `run`, taken out of the table, or `notRecorded`.
    fn take_run(&self, run: RunOfFacts) -> BodyFactsRecord {
        let mut recorded = self.lock();
        match recorded.iter().position(|(kept, _facts)| *kept == run) {
            Some(index) => recorded
                .remove(index)
                .map_or(BodyFactsRecord::NotRecorded, |(_run, facts)| facts),
            None => BodyFactsRecord::NotRecorded,
        }
    }
}

impl BodySpans for BodyFactsRecorder {
    fn open(&self, run: &BodyRun<'_>) -> tracing::Span {
        self.record((run.task_id, run.attempt.get()), BodyFactsRecord::of(run));
        tracing::Span::none()
    }

    /// Drop the facts of `run` that no body call took, and post the core's
    /// end of `run` (module docs).
    fn close(&self, run: BodyRunKey, _span: &tracing::Span, end: &BodyOutcome<'_>) {
        let _facts_no_body_took = self.take_run(run_of_key(run));
        let post = ChildPost::BodyEnded {
            task_id: run.task_id.to_string(),
            attempt: run.attempt.get(),
            end: BodyEndRecord::of(end),
        };
        match self.posts.post_run(post) {
            Ok(()) | Err(PostRefused::Closing) => {}
            Err(refused @ (PostRefused::QueueFull | PostRefused::NodeApi { .. })) => {
                tracing::error!(
                    task_id = %run.task_id,
                    attempt = run.attempt.get(),
                    "the bodyEnded post of task {} attempt {} did not reach the JavaScript \
                     thread: {refused}; the attempt span of that run does not end",
                    run.task_id,
                    run.attempt.get(),
                );
            }
        }
    }
}

/// What the bodies of a child do with the body facts.
pub enum BodyFacts {
    NoScope,
    Spans(Arc<BodyFactsRecorder>),
}

impl BodyFacts {
    /// The body facts of `mode`; the `bodyEnded` posts of `Spans` go to
    /// `posts`.
    pub fn of(mode: BodyFactsMode, posts: Arc<dyn RunPosts>) -> Self {
        match mode {
            BodyFactsMode::NoScope => Self::NoScope,
            BodyFactsMode::Spans => Self::Spans(Arc::new(BodyFactsRecorder::new(posts))),
        }
    }

    /// The core's body scope of these facts.
    pub fn scope(&self) -> BodyScope {
        match self {
            Self::NoScope => BodyScope::NoScope,
            Self::Spans(recorder) => {
                let spans: Arc<dyn BodySpans> = Arc::clone(recorder) as Arc<dyn BodySpans>;
                BodyScope::Spans(spans)
            }
        }
    }

    /// The facts of the run of `token` for its `runBody` post.
    pub fn take_for_run(&self, token: FencingToken) -> BodyFactsRecord {
        match self {
            Self::NoScope => BodyFactsRecord::NotRecorded,
            Self::Spans(recorder) => recorder.take(token),
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::core::task::error::{OperationalCode, TaskErrorCode};
    use horsies_core::events::AttemptNumber;

    use super::*;

    /// The posts of a test: each post, or the refusal of every post.
    struct RecordedPosts {
        posts: Mutex<Vec<ChildPost>>,
        refusal: Option<PostRefused>,
    }

    impl RunPosts for RecordedPosts {
        fn post_run(&self, post: ChildPost) -> Result<(), PostRefused> {
            match self.refusal {
                Some(refused) => Err(refused),
                None => {
                    self.posts
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(post);
                    Ok(())
                }
            }
        }
    }

    fn recorded_posts(refusal: Option<PostRefused>) -> Arc<RecordedPosts> {
        Arc::new(RecordedPosts {
            posts: Mutex::new(Vec::new()),
            refusal,
        })
    }

    fn run_key(task_id: Uuid, attempt: u32) -> BodyRunKey {
        BodyRunKey {
            task_id,
            attempt: AttemptNumber::try_from(attempt).expect("an attempt from 1"),
        }
    }

    fn recorder_of(posts: &Arc<RecordedPosts>) -> BodyFactsRecorder {
        BodyFactsRecorder::new(Arc::clone(posts) as Arc<dyn RunPosts>)
    }

    fn facts(name: &str) -> BodyFactsRecord {
        BodyFactsRecord::Recorded {
            task_id: Uuid::nil().to_string(),
            task_name: name.to_owned(),
            queue_name: "default".to_owned(),
            attempt: 1,
            worker_id: "worker".to_owned(),
            execution: BodyExecution::SharedAsync,
            workflow: BodyWorkflowRecord::NotInWorkflow,
            carrier: BodyCarrierRecord::NoCarrier,
        }
    }

    #[test]
    fn the_oldest_facts_go_at_the_bound() {
        let recorder = recorder_of(&recorded_posts(None));
        let first = (Uuid::new_v4(), 1);
        recorder.record(first, facts("first"));
        for _ in 1..RECORDED_FACTS_BOUND {
            recorder.record((Uuid::new_v4(), 1), facts("other"));
        }
        assert_eq!(recorder.lock().len(), RECORDED_FACTS_BOUND);
        recorder.record((Uuid::new_v4(), 1), facts("last"));
        assert_eq!(recorder.lock().len(), RECORDED_FACTS_BOUND);
        assert!(recorder.lock().iter().all(|(run, _)| *run != first));
    }

    #[test]
    fn each_carrier_entry_keeps_its_key_and_value_in_key_order() {
        let carrier = BodyCarrier::Carrier(
            [("traceparent", "00-a"), ("baggage", "k=v")]
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        );
        assert_eq!(
            BodyCarrierRecord::of(&carrier),
            BodyCarrierRecord::Carrier {
                entries: vec![
                    CarrierEntry {
                        key: "baggage".to_owned(),
                        value: "k=v".to_owned()
                    },
                    CarrierEntry {
                        key: "traceparent".to_owned(),
                        value: "00-a".to_owned()
                    },
                ]
            }
        );
    }

    #[test]
    fn no_scope_gives_the_core_no_scope_and_records_nothing() {
        let posts = recorded_posts(None);
        assert!(matches!(
            BodyFacts::of(
                BodyFactsMode::NoScope,
                Arc::clone(&posts) as Arc<dyn RunPosts>
            )
            .scope(),
            BodyScope::NoScope
        ));
        assert!(matches!(
            BodyFacts::of(
                BodyFactsMode::Spans,
                Arc::clone(&posts) as Arc<dyn RunPosts>
            )
            .scope(),
            BodyScope::Spans(_)
        ));
    }

    #[test]
    fn a_close_posts_the_end_of_the_run_that_it_names() {
        let posts = recorded_posts(None);
        let recorder = recorder_of(&posts);
        let task_id = Uuid::new_v4();
        recorder.close(
            run_key(task_id, 3),
            &tracing::Span::none(),
            &BodyOutcome::Cancelled,
        );
        assert_eq!(
            *posts.posts.lock().unwrap_or_else(PoisonError::into_inner),
            vec![ChildPost::BodyEnded {
                task_id: task_id.to_string(),
                attempt: 3,
                end: BodyEndRecord::Cancelled,
            }]
        );
    }

    #[test]
    fn a_close_drops_the_facts_of_its_run_only() {
        let posts = recorded_posts(None);
        let recorder = recorder_of(&posts);
        let task_id = Uuid::new_v4();
        recorder.record((task_id, 1), facts("first attempt"));
        recorder.record((task_id, 2), facts("second attempt"));
        recorder.close(
            run_key(task_id, 2),
            &tracing::Span::none(),
            &BodyOutcome::Returned,
        );
        assert_eq!(
            recorder.take_run((task_id, 2)),
            BodyFactsRecord::NotRecorded
        );
        assert_eq!(recorder.take_run((task_id, 1)), facts("first attempt"));
    }

    #[test]
    fn each_end_of_the_core_has_its_record() {
        let code = TaskErrorCode::from(OperationalCode::UnhandledException);
        for (end, record) in [
            (BodyOutcome::Returned, BodyEndRecord::Returned),
            (
                BodyOutcome::Failed { error_code: &code },
                BodyEndRecord::Failed {
                    code: "UNHANDLED_EXCEPTION".to_owned(),
                },
            ),
            (BodyOutcome::TimedOut, BodyEndRecord::TimedOut),
            (BodyOutcome::Cancelled, BodyEndRecord::Cancelled),
        ] {
            assert_eq!(BodyEndRecord::of(&end), record);
        }
    }

    #[test]
    fn a_refused_end_post_posts_nothing_and_the_close_returns() {
        for refused in [
            PostRefused::Closing,
            PostRefused::QueueFull,
            PostRefused::NodeApi {
                status: napi::Status::GenericFailure,
            },
        ] {
            let posts = recorded_posts(Some(refused));
            recorder_of(&posts).close(
                run_key(Uuid::new_v4(), 1),
                &tracing::Span::none(),
                &BodyOutcome::Returned,
            );
            assert!(posts
                .posts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty());
        }
    }
}
