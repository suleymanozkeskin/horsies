//! The bounds of frame fields (Python `core/models/worker_profile.py`,
//! `core/models/tasks.py`).
//!
//! Text bounds count characters (Unicode scalar values), as Python's `len`.

use super::values::BoundedCount;
use crate::core::workflow::spec::MAX_WORKFLOW_NODES;
pub use crate::worker::retry::{MAX_CANCEL_GRACE_MS, MIN_CANCEL_GRACE_MS};
use crate::worker::retry::{MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS};

/// A report of work left on a task loop names at most this many tasks.
pub const MAX_LEFTOVER_TASK_NAMES: usize = 10;
/// Each name in such a report has at most this many characters.
pub const MAX_LEFTOVER_NAME_CHARS: usize = 200;
/// The exception type name of a boot failure.
pub const MAX_BOOT_ERROR_NAME_CHARS: usize = 200;
/// The exception text of a boot failure.
pub const MAX_BOOT_DETAIL_CHARS: usize = 2000;
/// The source line that a blocked loop ran.
pub const MAX_BLOCKED_WHERE_CHARS: usize = 500;
/// The detail of a worker-level failure in a child (Python
/// `MAX_FAILED_REASON_CHARS`). One character takes at most 12 bytes as JSON
/// text, so the detail stays well under the metadata bound.
pub const MAX_FAILURE_DETAIL_CHARS: usize = 64 * 1024;
/// The reason of a fork failure or a preload refusal (Python
/// `core/worker/preload_main.py`, the refusal bound).
pub const MAX_PRELOAD_REASON_CHARS: usize = 4096;
/// The reason of a refusal of stored options. The parent builds it from the
/// stored text and cuts it to this bound.
pub const MAX_REFUSAL_REASON_CHARS: usize = 4096;

/// The most events of one `events` frame. A frame carries one event or
/// more.
pub const MAX_EVENTS_PER_FRAME: usize = 256;
/// The most bytes of the wire form of one event in an `events` frame. The
/// largest event is the `AttemptEnded` of an attempt that the worker ended:
/// its internal trace carrier has at most
/// [`crate::telemetry::trace_carrier::MAX_TRACE_CARRIER_BYTES`] bytes of
/// printable ASCII, which the JSON escape at most doubles, and its task name
/// has at most 255 characters of at most 6 JSON bytes each. Together with
/// its other fields that is less than 6,200 bytes.
pub const MAX_EVENT_WIRE_BYTES: usize = 8 * 1024;
/// The most event handlers of one handler child, and so the most entries of
/// one `handler_stats` frame.
pub const MAX_EVENT_HANDLERS: usize = 32;
/// The most characters of the name of an event handler. A name has one
/// character or more.
pub const MAX_HANDLER_NAME_CHARS: usize = 255;

/// The most seats of one shared async child.
pub const MAX_SEATS_PER_SHARED_ASYNC_CHILD: u64 = 1000;
/// The most seats of one shared thread child.
pub const MAX_SEATS_PER_SHARED_THREAD_CHILD: u64 = 256;
/// The most connections of one pool of a child.
pub const MAX_CHILD_POOL_CONNECTIONS: u64 = 1000;
/// The most threads of the default executor of a shared async child.
pub const MAX_EXECUTOR_THREADS: u64 = 256;
/// The largest index of a workflow node: a workflow spec holds at most
/// [`MAX_WORKFLOW_NODES`] nodes, numbered from 0.
pub const MAX_WORKFLOW_NODE_INDEX: u64 = MAX_WORKFLOW_NODES as u64 - 1;

/// The deadline of a task, in milliseconds after the child takes it.
pub type TaskTimeoutMs = BoundedCount<MIN_TASK_TIMEOUT_MS, MAX_TASK_TIMEOUT_MS>;
/// The time an async task gets to stop after its first cancel, in
/// milliseconds.
pub type CancelGraceMs = BoundedCount<MIN_CANCEL_GRACE_MS, MAX_CANCEL_GRACE_MS>;
/// The seats of a shared async child.
pub type AsyncSeats = BoundedCount<1, MAX_SEATS_PER_SHARED_ASYNC_CHILD>;
/// The seats of a shared thread child.
pub type ThreadSeats = BoundedCount<1, MAX_SEATS_PER_SHARED_THREAD_CHILD>;
/// The default executor threads of a shared async child.
pub type ExecutorThreads = BoundedCount<1, MAX_EXECUTOR_THREADS>;
/// The connections a child pool keeps open when idle.
pub type PoolMinSize = BoundedCount<0, MAX_CHILD_POOL_CONNECTIONS>;
/// The most connections of a child pool.
pub type PoolMaxSize = BoundedCount<1, MAX_CHILD_POOL_CONNECTIONS>;
/// The connections of the pool that task code opens when it sends.
pub type TaskSendPool = BoundedCount<1, MAX_CHILD_POOL_CONNECTIONS>;
/// The index of a node in its workflow spec.
pub type WorkflowNodeIndex = BoundedCount<0, MAX_WORKFLOW_NODE_INDEX>;
