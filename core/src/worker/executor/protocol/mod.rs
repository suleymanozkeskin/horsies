//! The parent-child protocol: one codec for the worker parent, the preload
//! process and every child, Python or Rust.
//!
//! A frame is a header, JSON metadata and a raw body:
//!
//! - Header: 12 bytes, `>IQ`: the metadata length as an unsigned 32-bit
//!   number, then the body length as an unsigned 64-bit number, big-endian.
//! - Metadata: one JSON object, [`MIN_META_BYTES`] to [`MAX_META_BYTES`].
//!   Its `kind` names the frame.
//! - Body: raw bytes, at most [`MAX_BODY_BYTES`]. Task input and results
//!   travel here, so they are not escaped a second time.
//!
//! The header is checked before any buffer is allocated for its frame.
//! Nothing is pickled: each frame is built from a fixed set of fields, and a
//! field outside that set makes the frame malformed.
//!
//! Version 3 ([`PROTOCOL_VERSION`]) keeps the framing and the content of
//! version 2, and adds:
//!
//! - The run frames (`run`, `shared_run`) carry `trace`: the trace carrier
//!   that the send stored on the task row (`carrier` with its text, one JSON
//!   object of texts), or `no_carrier`.
//! - The run frames carry the queue and the attempt of the run, for the
//!   attempt span and the log records of the child: `queue_name` and
//!   `attempt` (the retry count of the row plus 1, from 1).
//! - The frames that end a run carry `started_at`, the time that the start
//!   write returned to the child. `result` and `shared_result` carry
//!   `reported` with the time, or `not_reported` for a run that stopped
//!   before running; `body_returned`, `leftover_cancel` and
//!   `leftover_stuck` carry the time, since the body ran.
//! - The child kind `handler` in the init frame: a child that runs the
//!   event handlers of the app and no task.
//! - The parent frame `events`: a batch of lifecycle events for a handler
//!   child. The body is the JSON array of their wire forms
//!   ([`EventBatch`]).
//! - The child frame `handler_stats`: the cumulative error and drop counts
//!   of each event handler of a handler child, and its RSS.
//!
//! Version 2 kept the framing of version 1 and changed the content:
//!
//! - The run frames carry the one `input` value as the whole body, in place
//!   of the `args` and `kwargs` texts and their two size fields.
//! - The claim generation (`claimed_at`) is required RFC 3339 text.
//! - The isolated run frame carries `work`: `run`, or the `refusal` of the
//!   stored options. The parent holds the deadline of an isolated task.
//! - The end of a run is closed: `result` (the envelope in the body),
//!   `stopped_before_running` with a named reason, or `worker_failure` with
//!   a detail. Version 1 sent `ok` and one free text field for both a
//!   reason and a detail.
//! - The run frames do not carry `is_workflow_task`: the start statement
//!   reads it from the locked row.
//! - The init frame names the child program (`python` with the import
//!   facts, or `rust`), a closed log level, and no driver option map: the
//!   connection settings of the parent's config are the typed `connection`
//!   (TCP keepalives, TCP user timeout, and the server sessions of the URL).
//! - `init` and `ready` carry `protocol`. Each side compares it before any
//!   other field, so a mismatch is named, not read as a bad frame.
//! - The result frames (`result`, `shared_result`) carry `child`:
//!   `serves_on`, or `ends_itself` when the core of the child stopped and
//!   the child ends itself once the frame is written. The parent sends no
//!   run to a child that ends itself.
//!
//! This module does no I/O. [`FrameReader`] holds the read buffer of one
//! channel; everything else is pure.

mod bounds;
mod child_frames;
mod event_batch;
mod exit_codes;
mod header;
mod metadata;
mod parent_frames;
mod preload_frames;
mod reader;
mod timing;
mod values;

#[cfg(test)]
mod tests;

pub use bounds::{
    AsyncSeats, CancelGraceMs, ExecutorThreads, PoolMaxSize, PoolMinSize, TaskSendPool,
    TaskTimeoutMs, ThreadSeats, MAX_BLOCKED_WHERE_CHARS, MAX_BOOT_DETAIL_CHARS,
    MAX_BOOT_ERROR_NAME_CHARS, MAX_CANCEL_GRACE_MS, MAX_CHILD_POOL_CONNECTIONS,
    MAX_EVENTS_PER_FRAME, MAX_EVENT_HANDLERS, MAX_EVENT_WIRE_BYTES, MAX_EXECUTOR_THREADS,
    MAX_FAILURE_DETAIL_CHARS, MAX_HANDLER_NAME_CHARS, MAX_LEFTOVER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES, MAX_PRELOAD_REASON_CHARS, MAX_REFUSAL_REASON_CHARS,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD, MAX_SEATS_PER_SHARED_THREAD_CHILD, MIN_CANCEL_GRACE_MS,
};
pub use child_frames::{
    decode_child_frame, encode_child_frame, BodyEnd, BodyReturnedFrame, BootFailedFrame,
    ChildAfterFrame, ChildFrame, ChildFrameRefusal, HandlerCounts, HandlerCountsInvalid,
    HandlerCountsList, HandlerStatsFrame, LeftWorkEnd, LeftWorkEndedFrame, LeftWorkStuckFrame,
    LeftoverCancelFrame, LeftoverStuckFrame, LoopBlockedFrame, LoopCulprit, NoLeftWork, ReadyFrame,
    ReportedBootCause, ResultFrame, RunEnd, RunRef, RunStuckFrame, SeatEnd, SharedResultFrame,
    ShortageErrno, SpanReadCounts, StopReason,
};
pub use event_batch::{EventBatch, EventCountOutOfBounds, WireEvent, WireEventRefusal};
pub use exit_codes::{
    ChildExitCode, BAD_ARGUMENTS_EXIT_CODE, BAD_PARENT_FRAME_EXIT_CODE, BOOT_FAILED_EXIT_CODE,
    CHILD_HOOK_FAILED_EXIT_CODE, CORE_STOPPED_EXIT_CODE, PARENT_GONE_EXIT_CODE,
    PROTOCOL_MISMATCH_EXIT_CODE, REPARENT_FAILED_EXIT_CODE,
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
};
pub use header::{
    parse_header, EncodedFrame, FrameEncodeError, FrameOverBound, FrameSizes, HEADER_BYTES,
    MAX_BODY_BYTES, MAX_META_BYTES, MIN_META_BYTES,
};
pub use metadata::{ExpectedType, MalformedFrame, ProtocolMismatch, ReceivedProtocol};
pub use parent_frames::{
    decode_parent_frame, encode_parent_frame, ChildConnection, ChildKind, ChildLogLevel, ChildPool,
    ChildProgram, DatabaseUrl, EventsFrame, HandlerChild, InitFrame, IsolatedChild, IsolatedWork,
    ParentFrame, ParentFrameRefusal, PythonProgram, RunFrame, ServerSessions, SharedAsyncChild,
    SharedRunFrame, SharedThreadChild, SharedWork, StoredOptionsRefusal, TaskKind,
};
pub use preload_frames::{
    decode_preload_frame, encode_preload_frame, PreloadFrame, SPAWN_REQUEST_BYTE,
};
pub use reader::{
    next_step, plan_read, FrameReader, FrameStep, RawFrame, ReadPlan, DIRECT_BODY_BYTES,
    FRAME_READ_BUFFER_BYTES, READ_BUFFER_BOUND,
};
pub use timing::{ISOLATED_LEFTOVER_GRACE, LOOP_BLOCK_LIMIT, STUCK_REPORT_DELAY};
pub use values::{
    BlockedWhere, BootDetail, BootErrorName, BoundedCount, BoundedText, DispatchNumber,
    FailureDetail, HandlerName, LeftoverName, LoopHoldsTasks, NameCountMismatch, PreloadReason,
    ProcessId, RefusalReason, ResultEnvelopeText, RssMb, RssReading, StuckWork, StuckWorkInvalid,
    TaskInputText, TextOverBound,
};

/// The version of the frame content. `init` and `ready` carry it; a child
/// or a parent that reads another number stops with a named mismatch.
pub const PROTOCOL_VERSION: u32 = 3;
