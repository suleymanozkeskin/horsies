//! Bounded values that frames carry. Each type is built only through its
//! checked constructor, so a frame value is always inside its bound.

use std::num::NonZeroU64;

use serde::{Serialize, Serializer};

use super::bounds::{
    MAX_BLOCKED_WHERE_CHARS, MAX_BOOT_DETAIL_CHARS, MAX_BOOT_ERROR_NAME_CHARS,
    MAX_FAILURE_DETAIL_CHARS, MAX_HANDLER_NAME_CHARS, MAX_LEFTOVER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES, MAX_PRELOAD_REASON_CHARS, MAX_REFUSAL_REASON_CHARS,
};

/// Text of at most `MAX_CHARS` characters (Unicode scalar values, as
/// Python's `len`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundedText<const MAX_CHARS: usize>(String);

/// A text is longer than its bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the text has {chars} characters; the bound is {bound}")]
pub struct TextOverBound {
    pub chars: usize,
    pub bound: usize,
}

impl<const MAX_CHARS: usize> BoundedText<MAX_CHARS> {
    /// Keep `text` when it has at most `MAX_CHARS` characters.
    pub fn new(text: String) -> Result<Self, TextOverBound> {
        let chars = text.chars().count();
        match chars <= MAX_CHARS {
            true => Ok(Self(text)),
            false => Err(TextOverBound {
                chars,
                bound: MAX_CHARS,
            }),
        }
    }

    /// The first `MAX_CHARS` characters of `text`.
    pub fn cut(text: &str) -> Self {
        Self(text.chars().take(MAX_CHARS).collect())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl<const MAX_CHARS: usize> std::fmt::Display for BoundedText<MAX_CHARS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<const MAX_CHARS: usize> Serialize for BoundedText<MAX_CHARS> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// The exception type name of a boot failure.
pub type BootErrorName = BoundedText<MAX_BOOT_ERROR_NAME_CHARS>;
/// The exception text of a boot failure.
pub type BootDetail = BoundedText<MAX_BOOT_DETAIL_CHARS>;
/// The name of one task left on a task loop.
pub type LeftoverName = BoundedText<MAX_LEFTOVER_NAME_CHARS>;
/// The source line that a blocked loop ran.
pub type BlockedWhere = BoundedText<MAX_BLOCKED_WHERE_CHARS>;
/// The detail of a worker-level failure in a child.
pub type FailureDetail = BoundedText<MAX_FAILURE_DETAIL_CHARS>;
/// The reason of a fork failure or a preload refusal.
pub type PreloadReason = BoundedText<MAX_PRELOAD_REASON_CHARS>;
/// The reason of a refusal of stored options.
pub type RefusalReason = BoundedText<MAX_REFUSAL_REASON_CHARS>;
/// The name of an event handler, at most [`MAX_HANDLER_NAME_CHARS`]
/// characters. A `handler_stats` frame refuses an empty name.
pub type HandlerName = BoundedText<MAX_HANDLER_NAME_CHARS>;

/// A whole number from `MIN` to `MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BoundedCount<const MIN: u64, const MAX: u64>(u64);

impl<const MIN: u64, const MAX: u64> BoundedCount<MIN, MAX> {
    /// Keep `value` when it is from `MIN` to `MAX`.
    pub fn new(value: u64) -> Option<Self> {
        match (MIN..=MAX).contains(&value) {
            true => Some(Self(value)),
            false => None,
        }
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl<const MIN: u64, const MAX: u64> Serialize for BoundedCount<MIN, MAX> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

/// The largest process id a frame carries: the largest `pid_t`.
const MAX_PROCESS_ID: u32 = i32::MAX as u32;

/// The id of a live process, from 1 to the largest `pid_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessId(u32);

impl ProcessId {
    pub fn new(pid: u32) -> Option<Self> {
        match (1..=MAX_PROCESS_ID).contains(&pid) {
            true => Some(Self(pid)),
            false => None,
        }
    }

    pub fn get(self) -> u32 {
        self.0
    }

    /// The id as a `pid_t`. Lossless: the id is at most `i32::MAX`.
    pub fn as_pid_t(self) -> i32 {
        // MAX_PROCESS_ID is i32::MAX, so the value fits.
        self.0 as i32
    }
}

/// The number of one run in a shared child, from 1. Every frame about the
/// run carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DispatchNumber(NonZeroU64);

impl DispatchNumber {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// The resident set size of a child, in MiB: finite and not negative.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RssMb(f64);

impl RssMb {
    pub fn new(value: f64) -> Option<Self> {
        match value.is_finite() && value >= 0.0 {
            true => Some(Self(value)),
            false => None,
        }
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

/// What a child read as its resident set size. An unreadable size never
/// recycles a child.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RssReading {
    Mb(RssMb),
    Unreadable,
}

impl RssReading {
    /// The `rss_mb` field: the size, or `null`.
    pub(super) fn wire(self) -> Option<f64> {
        match self {
            Self::Mb(rss) => Some(rss.get()),
            Self::Unreadable => None,
        }
    }
}

/// The task input: the text of the one `input` JSON value. The child
/// decodes it with the task's declared input type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInputText(String);

impl TaskInputText {
    pub fn new(text: String) -> Self {
        Self(text)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// The result envelope of a task, as JSON text. The parent checks it against
/// the strict core envelope before it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultEnvelopeText(String);

impl ResultEnvelopeText {
    pub fn new(text: String) -> Self {
        Self(text)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// The names of a report do not match its count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the report names {names} tasks; it must name {expected}")]
pub struct NameCountMismatch {
    pub names: usize,
    pub expected: usize,
}

/// The number of names a report of `count` tasks carries.
fn expected_names(count: u64) -> usize {
    match usize::try_from(count) {
        Ok(count) => count.min(MAX_LEFTOVER_TASK_NAMES),
        Err(_) => MAX_LEFTOVER_TASK_NAMES,
    }
}

/// At least one asyncio task that is not done is on a task loop: work that a
/// task started and did not await. `names` names `min(count, 10)` of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopHoldsTasks {
    count: NonZeroU64,
    names: Vec<LeftoverName>,
}

impl LoopHoldsTasks {
    pub fn new(count: NonZeroU64, names: Vec<LeftoverName>) -> Result<Self, NameCountMismatch> {
        let expected = expected_names(count.get());
        match names.len() == expected {
            true => Ok(Self { count, names }),
            false => Err(NameCountMismatch {
                names: names.len(),
                expected,
            }),
        }
    }

    pub fn count(&self) -> NonZeroU64 {
        self.count
    }

    pub fn names(&self) -> &[LeftoverName] {
        &self.names
    }
}

/// Why a stuck report is not valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StuckWorkInvalid {
    #[error("a stuck report names no live task and no live executor item")]
    NothingAlive,
    #[error(transparent)]
    NameCount(NameCountMismatch),
}

/// The work of a run that did not stop after its cancel rounds: `tasks`
/// asyncio tasks and `executor_items` default-executor items, at least one
/// in all. `names` names `min(tasks, 10)` of the tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckWork {
    tasks: u64,
    executor_items: u64,
    names: Vec<LeftoverName>,
}

impl StuckWork {
    pub fn new(
        tasks: u64,
        executor_items: u64,
        names: Vec<LeftoverName>,
    ) -> Result<Self, StuckWorkInvalid> {
        let expected = expected_names(tasks);
        match ((tasks, executor_items), names.len() == expected) {
            ((0, 0), _) => Err(StuckWorkInvalid::NothingAlive),
            (_, false) => Err(StuckWorkInvalid::NameCount(NameCountMismatch {
                names: names.len(),
                expected,
            })),
            (_, true) => Ok(Self {
                tasks,
                executor_items,
                names,
            }),
        }
    }

    pub fn tasks(&self) -> u64 {
        self.tasks
    }

    pub fn executor_items(&self) -> u64 {
        self.executor_items
    }

    pub fn names(&self) -> &[LeftoverName] {
        &self.names
    }
}
