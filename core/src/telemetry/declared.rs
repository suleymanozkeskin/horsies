//! The label values that are names declared at start: task, queue, handler
//! and schedule names, and the custom error codes of the `error_code` label.
//!
//! A declared set holds its names in the order given, each once. A value of
//! the set is the position of one name, or of the fallback of the set. Only
//! the set builds a value, so a value is never a name outside the set: a
//! name outside it is the fallback ([`DeclaredNames::value_of`]).
//!
//! A set never refuses its names. A name that cannot be a value of its own
//! is not declared ([`FoldReason`]): the empty text, the fallback text of
//! the set, and a name after [`MAX_DECLARED_NAMES`] names. Its samples count
//! in the fallback series. The build names each such name
//! ([`FoldedNames`]), and the start logs them in one WARNING line
//! ([`warn_folded_names`]).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;

use crate::telemetry::labels::{
    group_texts, DeclaredSource, LabelValue, LabelValues, ERROR_CODE_FIXED, ERROR_CODE_VALUES,
};
use crate::{DeclaredErrorCodes, ErrorCodeLabel};

/// The most names that one declared set holds.
pub const MAX_DECLARED_NAMES: usize = 100_000;

/// The most folded names that the WARNING line of a start names; the line
/// counts the others.
pub const MAX_FOLDED_NAMES_SHOWN: usize = 32;

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

/// Why a set does not declare a name. The samples of the name count in the
/// fallback series of its label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldReason {
    /// The empty text, which Prometheus reads as no label.
    EmptyName,
    /// The fallback text of the set. As a name of its own it would give a
    /// second series with the label set of the fallback series.
    FallbackText,
    /// The set holds [`MAX_DECLARED_NAMES`] names before it.
    PastBound,
}

impl fmt::Display for FoldReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => formatter.write_str("an empty name"),
            Self::FallbackText => formatter.write_str("the fallback text of the label"),
            Self::PastBound => write!(formatter, "past the bound of {MAX_DECLARED_NAMES} names"),
        }
    }
}

/// A name that a declared set does not declare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedName {
    /// The set that the name was given to.
    pub set: DeclaredSource,
    /// The name, as given.
    pub name: String,
    /// Why the set does not declare it.
    pub reason: FoldReason,
}

/// The names that the declared sets of one start do not declare: each name
/// once per set, in the order given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldedNames {
    names: Vec<FoldedName>,
}

impl FoldedNames {
    /// No folded name.
    pub fn none() -> Self {
        Self::default()
    }

    /// The folded names, in the order given.
    pub fn names(&self) -> &[FoldedName] {
        &self.names
    }

    /// These names, then the names of `other`.
    pub fn and(mut self, other: FoldedNames) -> Self {
        self.names.extend(other.names);
        self
    }
}

/// One line: each folded name with its set and its reason, at most
/// [`MAX_FOLDED_NAMES_SHOWN`] of them, then the count of the others.
impl fmt::Display for FoldedNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "these names are not declared, and their samples count in the fallback series of \
             their label: ",
        )?;
        for (index, folded) in self.names.iter().take(MAX_FOLDED_NAMES_SHOWN).enumerate() {
            let separator = match index {
                0 => "",
                _ => "; ",
            };
            write!(
                formatter,
                "{separator}{:?} in the {} ({})",
                folded.name,
                folded.set.description(),
                folded.reason
            )?;
        }
        match self.names.len().saturating_sub(MAX_FOLDED_NAMES_SHOWN) {
            0 => Ok(()),
            others => write!(formatter, "; and {others} more"),
        }
    }
}

/// A declared set, or the sets of one start, and the names that it does not
/// declare.
#[derive(Debug, Clone)]
pub struct Folded<T> {
    /// The set.
    pub declared: T,
    /// The names that the set does not declare.
    pub folded: FoldedNames,
}

/// Log one WARNING line that names each name of `folded`, after `subject`,
/// the start that built the sets. No line when `folded` holds no name.
pub fn warn_folded_names(subject: &dyn fmt::Display, folded: &FoldedNames) {
    match folded.names() {
        [] => {}
        [_, ..] => tracing::warn!("{subject}: {folded}"),
    }
}

/// What one name adds to a set that is being built.
enum NameCheck {
    /// The name is new; it takes the next position.
    New,
    /// The name is in the set already.
    Repeated,
    /// The name is not declared; its samples count in the fallback series.
    Folded(FoldReason),
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
    /// Never refuses: the empty text, the fallback text of the set, and a
    /// name after [`MAX_DECLARED_NAMES`] names are not declared, and
    /// `folded` names each of them once. Pure.
    pub fn new<I, S>(names: I) -> Folded<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut set = Self::none();
        let mut folded: Vec<FoldedName> = Vec::new();
        let mut folded_texts: HashSet<String> = HashSet::new();
        for name in names {
            let name: String = name.into();
            match set.check(&name) {
                NameCheck::New => {
                    let position = position_of(set.names.len());
                    let _first = set.positions.insert(name.clone(), position);
                    set.names.push(name);
                }
                NameCheck::Repeated => {}
                NameCheck::Folded(reason) => match folded_texts.insert(name.clone()) {
                    true => folded.push(FoldedName {
                        set: L::SOURCE,
                        name,
                        reason,
                    }),
                    false => {}
                },
            }
        }
        Folded {
            declared: set,
            folded: FoldedNames { names: folded },
        }
    }

    /// What `name` adds to this set.
    fn check(&self, name: &str) -> NameCheck {
        match (
            name.is_empty(),
            name == L::SOURCE.fallback(),
            self.positions.contains_key(name),
            self.names.len() < MAX_DECLARED_NAMES,
        ) {
            (true, _, _, _) => NameCheck::Folded(FoldReason::EmptyName),
            (false, true, _, _) => NameCheck::Folded(FoldReason::FallbackText),
            (false, false, true, _) => NameCheck::Repeated,
            (false, false, false, true) => NameCheck::New,
            (false, false, false, false) => NameCheck::Folded(FoldReason::PastBound),
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
/// built-in code, the custom codes that the tasks declare (each once, sorted
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
    /// The values with the declared codes of every task in `sets`, each
    /// code once, sorted by text. Never refuses: the codes after the first
    /// [`MAX_DECLARED_NAMES`] are not declared, so they are `undeclared`,
    /// and `folded` names each of them. Pure.
    pub fn new<'a, I>(sets: I) -> Folded<Self>
    where
        I: IntoIterator<Item = &'a DeclaredErrorCodes>,
    {
        let mut declared: Vec<String> = sets
            .into_iter()
            .flat_map(|set| set.iter().map(|code| code.as_str().to_owned()))
            .collect();
        declared.sort();
        declared.dedup();
        let past_bound = declared.split_off(declared.len().min(MAX_DECLARED_NAMES));
        let folded = past_bound
            .into_iter()
            .map(|code| FoldedName {
                set: DeclaredSource::CustomErrorCodes,
                name: code,
                reason: FoldReason::PastBound,
            })
            .collect();
        Folded {
            declared: Self::of_sorted(declared),
            folded: FoldedNames { names: folded },
        }
    }

    /// The values with no declared code: every custom code is `undeclared`.
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
    /// value; a custom code that these values declare is its own value;
    /// another custom code is `undeclared`. A task declares its codes in the
    /// registry's set, so a code that a task declares and that these values
    /// do not hold (a set that was not given to [`ErrorCodeValues::new`])
    /// is `undeclared` too.
    pub fn value_of(&self, label: ErrorCodeLabel<'_>) -> ErrorCodeValue {
        match self.positions.get(label.value()) {
            Some(position) => ErrorCodeValue(*position),
            None => ErrorCodeValue(self.fallback),
        }
    }

    /// The declared custom codes, sorted by text.
    pub fn declared(&self) -> &[String] {
        &self.declared
    }
}
