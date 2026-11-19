//! The label values that are names declared at start: task, queue, handler
//! and schedule names, and the user error codes of the `error_code` label.
//!
//! A declared set holds its names in the order given, each once. A value of
//! the set is the position of one name, or of the fallback of the set. Only
//! the set builds a value, so a value is never a name outside the set: a
//! name outside it is the fallback ([`DeclaredNames::value_of`]).

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::telemetry::labels::{
    group_texts, DeclaredSource, LabelValue, LabelValues, ERROR_CODE_FIXED, ERROR_CODE_VALUES,
};
use crate::{DeclaredErrorCodes, ErrorCodeLabel};

/// The most names that one declared set holds.
pub const MAX_DECLARED_NAMES: usize = 100_000;

/// A label whose values are names declared at start.
pub trait DeclaredLabel: 'static {
    /// Where the names come from; it also gives the fallback.
    const SOURCE: DeclaredSource;
}

/// The `task` label: the task names of the worker manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskName {}

impl DeclaredLabel for TaskName {
    const SOURCE: DeclaredSource = DeclaredSource::TaskNames;
}

/// The `queue` label: the queue names of the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueName {}

impl DeclaredLabel for QueueName {
    const SOURCE: DeclaredSource = DeclaredSource::QueueNames;
}

/// The `handler` label: the names of the registered event handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerName {}

impl DeclaredLabel for HandlerName {
    const SOURCE: DeclaredSource = DeclaredSource::HandlerNames;
}

/// The `schedule` label: the schedule names of the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleName {}

impl DeclaredLabel for ScheduleName {
    const SOURCE: DeclaredSource = DeclaredSource::ScheduleNames;
}

/// Why a declared set is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclaredNamesRefused {
    /// A name is the empty text, which Prometheus reads as no label.
    #[error("an empty name in the declared {}", .set.description())]
    EmptyName { set: DeclaredSource },
    /// A name is the fallback text of its set.
    #[error(
        "the declared {} hold {name:?}, the value that stands for a name outside them",
        .set.description()
    )]
    FallbackName { set: DeclaredSource, name: String },
    /// The set holds more than [`MAX_DECLARED_NAMES`] names.
    #[error(
        "the declared {} hold {count} names; the most is {bound}",
        .set.description()
    )]
    TooMany {
        set: DeclaredSource,
        count: usize,
        bound: usize,
    },
}

/// What one name adds to a set that is being built.
enum NameCheck {
    /// The name is new; it takes the next position.
    New,
    /// The name is in the set already.
    Repeated,
    /// The name is refused.
    Refused(DeclaredNamesRefused),
}

/// The names of one declared label, fixed at start, each once.
#[derive(Debug, Clone)]
pub struct DeclaredNames<L> {
    names: Vec<String>,
    positions: HashMap<String, u32>,
    label: PhantomData<fn() -> L>,
}

/// A value of a declared label: a declared name or the fallback.
pub struct DeclaredValue<L> {
    position: u32,
    label: PhantomData<fn() -> L>,
}

impl<L> Clone for DeclaredValue<L> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<L> Copy for DeclaredValue<L> {}

impl<L> PartialEq for DeclaredValue<L> {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}

impl<L> Eq for DeclaredValue<L> {}

impl<L> std::fmt::Debug for DeclaredValue<L> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "DeclaredValue({})", self.position)
    }
}

impl<L: DeclaredLabel> LabelValue for DeclaredValue<L> {
    const VALUES: LabelValues = LabelValues::Declared(L::SOURCE);

    fn position(self) -> u32 {
        self.position
    }
}

/// Whether a name is one of the declared names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredLookup<L> {
    /// The name is declared; this is its value.
    Member(DeclaredValue<L>),
    /// The name is not declared.
    Outside,
}

impl<L: DeclaredLabel> DeclaredNames<L> {
    /// The set with no declared name: every name is the fallback.
    pub fn none() -> Self {
        Self {
            names: Vec::new(),
            positions: HashMap::new(),
            label: PhantomData,
        }
    }

    /// The set of `names`, in their order; a repeated name counts once.
    ///
    /// Errors: [`DeclaredNamesRefused::EmptyName`] for an empty name,
    /// [`DeclaredNamesRefused::FallbackName`] for the fallback text of the
    /// set, [`DeclaredNamesRefused::TooMany`] for more than
    /// [`MAX_DECLARED_NAMES`] names.
    pub fn new<I, S>(names: I) -> Result<Self, DeclaredNamesRefused>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut set = Self::none();
        for name in names {
            let name: String = name.into();
            match set.check(&name) {
                NameCheck::New => {
                    let position = position_of(set.names.len());
                    let _first = set.positions.insert(name.clone(), position);
                    set.names.push(name);
                }
                NameCheck::Repeated => {}
                NameCheck::Refused(refused) => return Err(refused),
            }
        }
        Ok(set)
    }

    /// What `name` adds to this set.
    fn check(&self, name: &str) -> NameCheck {
        let set = L::SOURCE;
        match (
            name.is_empty(),
            name == set.fallback(),
            self.positions.contains_key(name),
            self.names.len() < MAX_DECLARED_NAMES,
        ) {
            (true, _, _, _) => NameCheck::Refused(DeclaredNamesRefused::EmptyName { set }),
            (false, true, _, _) => NameCheck::Refused(DeclaredNamesRefused::FallbackName {
                set,
                name: name.to_owned(),
            }),
            (false, false, true, _) => NameCheck::Repeated,
            (false, false, false, true) => NameCheck::New,
            (false, false, false, false) => NameCheck::Refused(DeclaredNamesRefused::TooMany {
                set,
                count: self.names.len() + 1,
                bound: MAX_DECLARED_NAMES,
            }),
        }
    }

    /// The value of `name`: its declared value, or the fallback.
    pub fn value_of(&self, name: &str) -> DeclaredValue<L> {
        match self.lookup(name) {
            DeclaredLookup::Member(value) => value,
            DeclaredLookup::Outside => self.fallback(),
        }
    }

    /// Whether `name` is declared.
    pub fn lookup(&self, name: &str) -> DeclaredLookup<L> {
        match self.positions.get(name) {
            Some(position) => DeclaredLookup::Member(DeclaredValue {
                position: *position,
                label: PhantomData,
            }),
            None => DeclaredLookup::Outside,
        }
    }

    /// The fallback value: the position after the declared names.
    pub fn fallback(&self) -> DeclaredValue<L> {
        DeclaredValue {
            position: position_of(self.names.len()),
            label: PhantomData,
        }
    }

    /// The declared names, in their order.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The label texts by position: the declared names, then the fallback.
    pub fn texts(&self) -> Vec<String> {
        let mut texts = self.names.clone();
        texts.push(L::SOURCE.fallback().to_owned());
        texts
    }
}

/// The position of the entry at `index`. A set holds at most
/// [`MAX_DECLARED_NAMES`] names, so every index of a set and the one after
/// it fit in `u32`.
fn position_of(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

/// The values of the `error_code` label in one registry: no code, every
/// built-in code, the user codes that the tasks declare (each once, sorted
/// by text), and `undeclared`.
#[derive(Debug, Clone)]
pub struct ErrorCodeValues {
    declared: Vec<String>,
    positions: HashMap<String, u32>,
    fallback: u32,
}

/// A value of the `error_code` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCodeValue(u32);

impl ErrorCodeValue {
    /// The value of an attempt with no error code (a `COMPLETED` attempt):
    /// the empty text, the first fixed value.
    pub const NO_ERROR_CODE: Self = Self(0);
}

impl LabelValue for ErrorCodeValue {
    const VALUES: LabelValues = ERROR_CODE_VALUES;

    fn position(self) -> u32 {
        self.0
    }
}

impl ErrorCodeValues {
    /// The values with the declared codes of every task in `sets`.
    ///
    /// Errors: [`DeclaredNamesRefused::TooMany`] when the tasks declare more
    /// than [`MAX_DECLARED_NAMES`] codes together.
    pub fn new<'a, I>(sets: I) -> Result<Self, DeclaredNamesRefused>
    where
        I: IntoIterator<Item = &'a DeclaredErrorCodes>,
    {
        let mut declared: Vec<String> = sets
            .into_iter()
            .flat_map(|set| set.iter().map(|code| code.as_str().to_owned()))
            .collect();
        declared.sort();
        declared.dedup();
        match declared.len() <= MAX_DECLARED_NAMES {
            true => Ok(Self::of_sorted(declared)),
            false => Err(DeclaredNamesRefused::TooMany {
                set: DeclaredSource::UserErrorCodes,
                count: declared.len(),
                bound: MAX_DECLARED_NAMES,
            }),
        }
    }

    /// The values with no declared code: every user code is `undeclared`.
    pub fn none() -> Self {
        Self::of_sorted(Vec::new())
    }

    /// The values of `declared`: sorted, each once, at most
    /// [`MAX_DECLARED_NAMES`] codes.
    fn of_sorted(declared: Vec<String>) -> Self {
        let fixed = group_texts(ERROR_CODE_FIXED);
        let mut positions: HashMap<String, u32> = HashMap::new();
        for (index, text) in fixed.iter().enumerate() {
            let _first = positions.insert((*text).to_owned(), position_of(index));
        }
        for (index, code) in declared.iter().enumerate() {
            let _first = positions.insert(code.clone(), position_of(fixed.len() + index));
        }
        let fallback = position_of(fixed.len() + declared.len());
        Self {
            declared,
            positions,
            fallback,
        }
    }

    /// The value of the label of an error code. A built-in code is its own
    /// value; a user code that these values declare is its own value;
    /// another user code is `undeclared`. A task declares its codes in the
    /// registry's set, so a code that a task declares and that these values
    /// do not hold (a set that was not given to [`ErrorCodeValues::new`])
    /// is `undeclared` too.
    pub fn value_of(&self, label: ErrorCodeLabel<'_>) -> ErrorCodeValue {
        match self.positions.get(label.value()) {
            Some(position) => ErrorCodeValue(*position),
            None => ErrorCodeValue(self.fallback),
        }
    }

    /// The declared user codes, sorted by text.
    pub fn declared(&self) -> &[String] {
        &self.declared
    }
}
