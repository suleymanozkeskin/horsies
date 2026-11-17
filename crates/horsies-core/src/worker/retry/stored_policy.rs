//! The stored `retry_policy` of task options, read by Python's rule:
//! `RetryPolicy.model_validate(policy, strict=True)` (Python
//! `stored_retry_policy`, `python/horsies/core/codec/task_options.py:85-117`).
//!
//! A refusal has Python's text after the prefix of the caller: the entries of
//! pydantic's field errors as `validation_summary` writes them
//! (`python/horsies/core/codec/validation_errors.py:93-115`), or the error of
//! the first model validator that refuses. The baseline SQL reads the policy of
//! a workflow node by the same rule (`horsies_workflow_retry_policy_refusal`,
//! `baseline/0005_workflow_functions.sql`).
//!
//! A value is read as Python's `json.loads` gives it. A number with no
//! fraction and no exponent is an `int` of any size; any other number is a
//! `float`. So an int above `u64::MAX` is compared with the bounds of its
//! field, as Python compares it, and a `max_delay_seconds` above `u64::MAX`
//! reads (pydantic's `PositiveInt` has no upper bound).
//!
//! Does no I/O and logs nothing.

use std::collections::HashMap;
use std::fmt;

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;

use super::{Backoff, DelayCap, RetryRule, MAX_RETRY_ATTEMPTS, MAX_RETRY_INTERVAL_SECONDS};
use crate::core::task::error::check_error_code_text;

/// `max_retries` of a stored policy that leaves the key out (Python
/// `RetryPolicy.max_retries`).
const DEFAULT_POLICY_MAX_RETRIES: u32 = 3;
/// `intervals` of a stored policy that leaves the key out (Python
/// `RetryPolicy.intervals`).
const DEFAULT_POLICY_INTERVALS: [u32; 3] = [60, 300, 900];
/// `jitter` of a stored policy that leaves the key out (Python
/// `RetryPolicy.jitter`).
const DEFAULT_POLICY_JITTER: bool = true;
/// The most items of `intervals` (Python `RetryPolicy.intervals`,
/// `max_length=20`).
const MAX_POLICY_INTERVALS: usize = 20;
/// The fewest items of `intervals` and of `auto_retry_for` (Python
/// `min_length=1`).
const MIN_POLICY_LIST_ITEMS: usize = 1;
/// The entries that a refusal text writes; the text then counts the rest
/// (Python `MAX_VALIDATION_ERRORS_IN_TEXT`).
const MAX_VALIDATION_ERRORS_IN_TEXT: usize = 10;
/// The fields of `RetryPolicy`, in declaration order. Any other key of a
/// stored policy is refused (`extra='forbid'`).
const POLICY_FIELDS: [&str; 6] = [
    "max_retries",
    "intervals",
    "backoff_strategy",
    "jitter",
    "max_delay_seconds",
    "auto_retry_for",
];
/// pydantic's location label of the enum member of an `auto_retry_for` item
/// (`Union[str, BuiltInTaskCode]`, Python `core/models/tasks.py:58-63`).
const BUILT_IN_CODE_UNION_LABEL: &str = "union[str-enum[OperationalErrorCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]]";
/// The Python classes of the built-in code families, in the order of
/// `BuiltInTaskCode`.
const BUILT_IN_CODE_FAMILIES: [&str; 4] = [
    "OperationalErrorCode",
    "ContractCode",
    "RetrievalCode",
    "OutcomeCode",
];

/// The `retry_policy` of stored task options, read by Python's rule.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum PolicyRead {
    /// No `retry_policy` key, or `null`.
    Absent,
    /// A policy that validates.
    Valid(RetryRule),
    /// Any other value. `detail` is Python's text (`InvalidRetryPolicy.detail`).
    Refused { detail: String },
}

/// Read the `retry_policy` of a stored options text.
///
/// Precondition: `options_text` parsed as a `serde_json::Value` object. Each
/// read below decodes a part of that text, so none of them can fail; a failed
/// one is a defect and panics. A repeated key keeps its first place and takes
/// its last value, as `serde_json::Map` keeps it. Pure.
pub(super) fn read_retry_policy(options_text: &str) -> PolicyRead {
    let options: RawMembers<'_> = serde_json::from_str(options_text)
        .expect("a text that parsed as a JSON object parses with its members kept raw");
    let Some(raw) = options.get("retry_policy") else {
        return PolicyRead::Absent;
    };
    match python_value(raw) {
        PythonValue::None => PolicyRead::Absent,
        PythonValue::Dict => policy_from_object(raw),
        other @ (PythonValue::Bool(_)
        | PythonValue::Int(_)
        | PythonValue::Float
        | PythonValue::Str(_)
        | PythonValue::List(_)) => PolicyRead::Refused {
            detail: format!(
                "retry_policy is not an object: {}",
                other.python_type_name()
            ),
        },
    }
}

/// Validate a policy object: the fields first, then the model validators.
fn policy_from_object(raw: &RawValue) -> PolicyRead {
    let members: RawMembers<'_> = serde_json::from_str(raw.get())
        .expect("a raw JSON object parses with its members kept raw");
    let fields = match read_fields(&members) {
        Ok(fields) => fields,
        Err(entries) => {
            return PolicyRead::Refused {
                detail: summary(&entries),
            }
        }
    };
    match rule_from_fields(fields) {
        Ok(rule) => PolicyRead::Valid(rule),
        Err(entry) => PolicyRead::Refused {
            detail: entry.to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
// Values as Python's json.loads gives them
// ---------------------------------------------------------------------------

/// The members of a JSON object, each value as its raw text. A key is kept
/// once, at its first place, with its last value.
struct RawMembers<'a> {
    members: Vec<(String, &'a RawValue)>,
}

impl<'a> RawMembers<'a> {
    fn get(&self, key: &str) -> Option<&'a RawValue> {
        self.members
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| *value)
    }
}

impl<'de> Deserialize<'de> for RawMembers<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawMembersVisitor)
    }
}

struct RawMembersVisitor;

impl<'de> Visitor<'de> for RawMembersVisitor {
    type Value = RawMembers<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    /// One pass over the members; the text bounds their count.
    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut members: Vec<(String, &'de RawValue)> = Vec::new();
        let mut places: HashMap<String, usize> = HashMap::new();
        while let Some(key) = access.next_key::<String>()? {
            let value: &'de RawValue = access.next_value()?;
            match places.get(&key) {
                Some(&place) => members[place].1 = value,
                None => {
                    places.insert(key.clone(), members.len());
                    members.push((key, value));
                }
            }
        }
        Ok(RawMembers { members })
    }
}

/// A JSON integer as Python reads it: an `int` of any size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonInt {
    /// Below 0.
    Negative,
    /// From 0 to `u64::MAX`. `-0` is 0.
    Natural(u64),
    /// Above `u64::MAX`.
    AboveU64,
}

/// A JSON value as Python's `json.loads` gives it, one level deep. An array
/// keeps the raw text of each item.
enum PythonValue<'a> {
    None,
    Bool(bool),
    Int(JsonInt),
    Float,
    Str(String),
    List(Vec<&'a RawValue>),
    Dict,
}

impl PythonValue<'_> {
    /// The Python type name, as `type(value).__name__` writes it.
    fn python_type_name(&self) -> &'static str {
        match self {
            Self::None => "NoneType",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float => "float",
            Self::Str(_) => "str",
            Self::List(_) => "list",
            Self::Dict => "dict",
        }
    }
}

/// The value that a raw JSON text holds. The first byte names the kind; a
/// number is an int when its text has no `.`, `e` or `E`.
fn python_value(raw: &RawValue) -> PythonValue<'_> {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'n') => PythonValue::None,
        Some(b't') => PythonValue::Bool(true),
        Some(b'f') => PythonValue::Bool(false),
        Some(b'"') => PythonValue::Str(
            serde_json::from_str(text).expect("a raw JSON string decodes as it did in the options"),
        ),
        Some(b'[') => PythonValue::List(
            serde_json::from_str(text).expect("a raw JSON array parses with its items kept raw"),
        ),
        Some(b'{') => PythonValue::Dict,
        Some(b'-' | b'0'..=b'9') => {
            match text.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                true => PythonValue::Float,
                false => PythonValue::Int(json_int(text)),
            }
        }
        Some(_) | None => unreachable!("a raw JSON value starts with the first byte of a value"),
    }
}

/// Read the text of a JSON integer (`-?(0|[1-9][0-9]*)`).
fn json_int(text: &str) -> JsonInt {
    match text.strip_prefix('-') {
        Some(digits) if digits.bytes().all(|digit| digit == b'0') => JsonInt::Natural(0),
        Some(_) => JsonInt::Negative,
        // The text is digits, so the parse fails only above `u64::MAX`.
        None => match text.parse::<u64>() {
            Ok(value) => JsonInt::Natural(value),
            Err(_) => JsonInt::AboveU64,
        },
    }
}

// ---------------------------------------------------------------------------
// pydantic's entries
// ---------------------------------------------------------------------------

/// One error of pydantic's validation of a stored policy, as
/// `validation_summary` writes it: `<loc>: <msg> [<type>]`, or `<msg>
/// [<type>]` when the location is empty.
struct Entry {
    /// The parts of pydantic's `loc`, joined by `.`. Empty for a model
    /// validator, and for a member whose key is empty.
    location: String,
    error: EntryError,
}

impl Entry {
    fn at(location: &str, error: EntryError) -> Self {
        Self {
            location: location.to_owned(),
            error,
        }
    }

    /// The error of a model validator, which has no location.
    fn of_model(text: String) -> Self {
        Self {
            location: String::new(),
            error: EntryError::ValueError(text),
        }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.location.is_empty() {
            true => write!(
                formatter,
                "{} [{}]",
                self.error.message(),
                self.error.error_type()
            ),
            false => write!(
                formatter,
                "{}: {} [{}]",
                self.location,
                self.error.message(),
                self.error.error_type()
            ),
        }
    }
}

/// A pydantic error type that a stored policy can meet, with the context its
/// message needs. The messages are those of pydantic-core 2.46.4.
enum EntryError {
    IntType,
    /// `ge`.
    GreaterThanEqual(u64),
    /// `gt`.
    GreaterThan(u64),
    /// `le`.
    LessThanEqual(u64),
    ListType,
    TooShort {
        min_length: usize,
        actual_length: usize,
    },
    TooLong {
        max_length: usize,
        actual_length: usize,
    },
    /// `backoff_strategy` is not `'fixed'` or `'exponential'`.
    BackoffLiteral,
    BoolType,
    Missing,
    ExtraForbidden,
    StringType,
    /// The class name of the enum.
    IsInstanceOf(&'static str),
    /// The text that a model validator raised.
    ValueError(String),
}

impl EntryError {
    fn message(&self) -> String {
        match self {
            Self::IntType => "Input should be a valid integer".to_owned(),
            Self::GreaterThanEqual(bound) => {
                format!("Input should be greater than or equal to {bound}")
            }
            Self::GreaterThan(bound) => format!("Input should be greater than {bound}"),
            Self::LessThanEqual(bound) => format!("Input should be less than or equal to {bound}"),
            Self::ListType => "Input should be a valid list".to_owned(),
            Self::TooShort {
                min_length,
                actual_length,
            } => format!(
                "List should have at least {min_length} item{} after validation, not {actual_length}",
                plural_suffix(*min_length)
            ),
            Self::TooLong {
                max_length,
                actual_length,
            } => format!(
                "List should have at most {max_length} item{} after validation, not {actual_length}",
                plural_suffix(*max_length)
            ),
            Self::BackoffLiteral => "Input should be 'fixed' or 'exponential'".to_owned(),
            Self::BoolType => "Input should be a valid boolean".to_owned(),
            Self::Missing => "Field required".to_owned(),
            Self::ExtraForbidden => "Extra inputs are not permitted".to_owned(),
            Self::StringType => "Input should be a valid string".to_owned(),
            Self::IsInstanceOf(class) => format!("Input should be an instance of {class}"),
            Self::ValueError(text) => format!("Value error, {text}"),
        }
    }

    fn error_type(&self) -> &'static str {
        match self {
            Self::IntType => "int_type",
            Self::GreaterThanEqual(_) => "greater_than_equal",
            Self::GreaterThan(_) => "greater_than",
            Self::LessThanEqual(_) => "less_than_equal",
            Self::ListType => "list_type",
            Self::TooShort { .. } => "too_short",
            Self::TooLong { .. } => "too_long",
            Self::BackoffLiteral => "literal_error",
            Self::BoolType => "bool_type",
            Self::Missing => "missing",
            Self::ExtraForbidden => "extra_forbidden",
            Self::StringType => "string_type",
            Self::IsInstanceOf(_) => "is_instance_of",
            Self::ValueError(_) => "value_error",
        }
    }
}

/// pydantic's `expected_plural`.
fn plural_suffix(count: usize) -> &'static str {
    match count {
        1 => "",
        _ => "s",
    }
}

/// The entries joined by `; `, at most [`MAX_VALIDATION_ERRORS_IN_TEXT`] of
/// them, then `(+<count> more)` (Python `validation_summary`).
fn summary(entries: &[Entry]) -> String {
    let mut shown: Vec<String> = entries
        .iter()
        .take(MAX_VALIDATION_ERRORS_IN_TEXT)
        .map(Entry::to_string)
        .collect();
    let hidden = entries.len().saturating_sub(MAX_VALIDATION_ERRORS_IN_TEXT);
    if hidden > 0 {
        shown.push(format!("(+{hidden} more)"));
    }
    shown.join("; ")
}

// ---------------------------------------------------------------------------
// Field validation (strict mode)
// ---------------------------------------------------------------------------

/// The fields of a policy whose field validation passed.
struct PolicyFields {
    max_retries: u32,
    intervals: Vec<u32>,
    backoff: Backoff,
    jitter: bool,
    delay_cap: DelayCap,
    auto_retry_for: Vec<String>,
}

/// Validate each field in declaration order, then refuse each key that is
/// not a field, in the order of the text. Every error is an entry, as
/// pydantic collects them. A field the policy leaves out takes its default;
/// `auto_retry_for` has none.
fn read_fields(members: &RawMembers<'_>) -> Result<PolicyFields, Vec<Entry>> {
    let mut entries: Vec<Entry> = Vec::new();
    let max_retries = keep_entries(&mut entries, read_max_retries(members.get("max_retries")));
    let intervals = keep_entries(&mut entries, read_intervals(members.get("intervals")));
    let backoff = keep_entries(&mut entries, read_backoff(members.get("backoff_strategy")));
    let jitter = keep_entries(&mut entries, read_jitter(members.get("jitter")));
    let delay_cap = keep_entries(
        &mut entries,
        read_delay_cap(members.get("max_delay_seconds")),
    );
    let auto_retry_for = keep_entries(&mut entries, read_codes(members.get("auto_retry_for")));
    entries.extend(
        members
            .members
            .iter()
            .filter(|(key, _)| !POLICY_FIELDS.contains(&key.as_str()))
            .map(|(key, _)| Entry::at(key, EntryError::ExtraForbidden)),
    );
    match (
        max_retries,
        intervals,
        backoff,
        jitter,
        delay_cap,
        auto_retry_for,
        entries.is_empty(),
    ) {
        (
            Some(max_retries),
            Some(intervals),
            Some(backoff),
            Some(jitter),
            Some(delay_cap),
            Some(auto_retry_for),
            true,
        ) => Ok(PolicyFields {
            max_retries,
            intervals,
            backoff,
            jitter,
            delay_cap,
            auto_retry_for,
        }),
        _ => {
            assert!(
                !entries.is_empty(),
                "a field that does not validate leaves at least one entry"
            );
            Err(entries)
        }
    }
}

/// The value of a field read, or `None` with its entries moved to `entries`.
fn keep_entries<T>(entries: &mut Vec<Entry>, read: Result<T, Vec<Entry>>) -> Option<T> {
    match read {
        Ok(value) => Some(value),
        Err(found) => {
            entries.extend(found);
            None
        }
    }
}

/// The lower bound of an int field, as pydantic names it. Both admit 1 and
/// up.
#[derive(Debug, Clone, Copy)]
enum LowerBound {
    /// `ge=1`.
    AtLeastOne,
    /// `gt=0` (`PositiveInt`).
    AboveZero,
}

impl LowerBound {
    fn error(self) -> EntryError {
        match self {
            Self::AtLeastOne => EntryError::GreaterThanEqual(1),
            Self::AboveZero => EntryError::GreaterThan(0),
        }
    }
}

/// An int from 1 up.
#[derive(Debug, Clone, Copy)]
enum PositiveInt {
    Fits(u64),
    AboveU64,
}

/// Read a value as pydantic's strict `int` with a lower bound of 1. A bool
/// or a float is not an int.
fn positive_int(value: &PythonValue<'_>, lower: LowerBound) -> Result<PositiveInt, EntryError> {
    match value {
        PythonValue::Int(JsonInt::Negative | JsonInt::Natural(0)) => Err(lower.error()),
        PythonValue::Int(JsonInt::Natural(value)) => Ok(PositiveInt::Fits(*value)),
        PythonValue::Int(JsonInt::AboveU64) => Ok(PositiveInt::AboveU64),
        PythonValue::None
        | PythonValue::Bool(_)
        | PythonValue::Float
        | PythonValue::Str(_)
        | PythonValue::List(_)
        | PythonValue::Dict => Err(EntryError::IntType),
    }
}

/// [`positive_int`] with an upper bound (`le=maximum`).
fn bounded_int(
    value: &PythonValue<'_>,
    lower: LowerBound,
    maximum: u32,
) -> Result<u32, EntryError> {
    match positive_int(value, lower)? {
        PositiveInt::Fits(value) => match u32::try_from(value) {
            Ok(fits) if fits <= maximum => Ok(fits),
            Ok(_) | Err(_) => Err(EntryError::LessThanEqual(u64::from(maximum))),
        },
        PositiveInt::AboveU64 => Err(EntryError::LessThanEqual(u64::from(maximum))),
    }
}

/// `max_retries`: an int from 1 to [`MAX_RETRY_ATTEMPTS`].
fn read_max_retries(raw: Option<&RawValue>) -> Result<u32, Vec<Entry>> {
    match raw {
        None => Ok(DEFAULT_POLICY_MAX_RETRIES),
        Some(raw) => bounded_int(
            &python_value(raw),
            LowerBound::AtLeastOne,
            MAX_RETRY_ATTEMPTS,
        )
        .map_err(|error| vec![Entry::at("max_retries", error)]),
    }
}

/// `intervals`: a list of 1 to [`MAX_POLICY_INTERVALS`] ints, each from 1 to
/// [`MAX_RETRY_INTERVAL_SECONDS`]. pydantic stops at the item past the
/// longest list and keeps only the `too_long` entry; the `too_short` entry
/// comes when no item has an entry.
fn read_intervals(raw: Option<&RawValue>) -> Result<Vec<u32>, Vec<Entry>> {
    let items = match raw.map(python_value) {
        None => return Ok(DEFAULT_POLICY_INTERVALS.to_vec()),
        Some(PythonValue::List(items)) => items,
        Some(
            PythonValue::None
            | PythonValue::Bool(_)
            | PythonValue::Int(_)
            | PythonValue::Float
            | PythonValue::Str(_)
            | PythonValue::Dict,
        ) => return Err(vec![Entry::at("intervals", EntryError::ListType)]),
    };
    if items.len() > MAX_POLICY_INTERVALS {
        return Err(vec![Entry::at(
            "intervals",
            EntryError::TooLong {
                max_length: MAX_POLICY_INTERVALS,
                actual_length: items.len(),
            },
        )]);
    }
    let mut intervals: Vec<u32> = Vec::with_capacity(items.len());
    let mut entries: Vec<Entry> = Vec::new();
    for (position, item) in items.iter().enumerate() {
        match bounded_int(
            &python_value(item),
            LowerBound::AboveZero,
            MAX_RETRY_INTERVAL_SECONDS,
        ) {
            Ok(seconds) => intervals.push(seconds),
            Err(error) => entries.push(Entry::at(&format!("intervals.{position}"), error)),
        }
    }
    list_or_entries("intervals", intervals, entries)
}

/// The items of a list field when none has an entry and there are at least
/// [`MIN_POLICY_LIST_ITEMS`]; else the entries.
fn list_or_entries<T>(
    field: &str,
    items: Vec<T>,
    entries: Vec<Entry>,
) -> Result<Vec<T>, Vec<Entry>> {
    match (entries.is_empty(), items.len() < MIN_POLICY_LIST_ITEMS) {
        (false, _) => Err(entries),
        (true, true) => Err(vec![Entry::at(
            field,
            EntryError::TooShort {
                min_length: MIN_POLICY_LIST_ITEMS,
                actual_length: items.len(),
            },
        )]),
        (true, false) => Ok(items),
    }
}

/// `backoff_strategy`: the text `fixed` or `exponential`.
fn read_backoff(raw: Option<&RawValue>) -> Result<Backoff, Vec<Entry>> {
    match raw.map(python_value) {
        None => Ok(Backoff::Fixed),
        Some(PythonValue::Str(text)) => match text.as_str() {
            "fixed" => Ok(Backoff::Fixed),
            "exponential" => Ok(Backoff::Exponential),
            _ => Err(vec![Entry::at(
                "backoff_strategy",
                EntryError::BackoffLiteral,
            )]),
        },
        Some(
            PythonValue::None
            | PythonValue::Bool(_)
            | PythonValue::Int(_)
            | PythonValue::Float
            | PythonValue::List(_)
            | PythonValue::Dict,
        ) => Err(vec![Entry::at(
            "backoff_strategy",
            EntryError::BackoffLiteral,
        )]),
    }
}

/// `jitter`: a bool.
fn read_jitter(raw: Option<&RawValue>) -> Result<bool, Vec<Entry>> {
    match raw.map(python_value) {
        None => Ok(DEFAULT_POLICY_JITTER),
        Some(PythonValue::Bool(jitter)) => Ok(jitter),
        Some(
            PythonValue::None
            | PythonValue::Int(_)
            | PythonValue::Float
            | PythonValue::Str(_)
            | PythonValue::List(_)
            | PythonValue::Dict,
        ) => Err(vec![Entry::at("jitter", EntryError::BoolType)]),
    }
}

/// `max_delay_seconds`: `null`, or an int from 1 with no upper bound.
fn read_delay_cap(raw: Option<&RawValue>) -> Result<DelayCap, Vec<Entry>> {
    let value = match raw.map(python_value) {
        None | Some(PythonValue::None) => return Ok(DelayCap::Uncapped),
        Some(value) => value,
    };
    match positive_int(&value, LowerBound::AboveZero) {
        Ok(PositiveInt::Fits(seconds)) => Ok(DelayCap::AtMost(seconds)),
        Ok(PositiveInt::AboveU64) => Ok(DelayCap::AboveU64Seconds),
        Err(error) => Err(vec![Entry::at("max_delay_seconds", error)]),
    }
}

/// `auto_retry_for`: a list of at least [`MIN_POLICY_LIST_ITEMS`] texts. An
/// item that is not a text fails each member of `Union[str,
/// BuiltInTaskCode]`: one entry for `str`, one per built-in code family (a
/// stored value is never an enum instance). The text bounds the item count.
fn read_codes(raw: Option<&RawValue>) -> Result<Vec<String>, Vec<Entry>> {
    let items = match raw.map(python_value) {
        None => return Err(vec![Entry::at("auto_retry_for", EntryError::Missing)]),
        Some(PythonValue::List(items)) => items,
        Some(
            PythonValue::None
            | PythonValue::Bool(_)
            | PythonValue::Int(_)
            | PythonValue::Float
            | PythonValue::Str(_)
            | PythonValue::Dict,
        ) => return Err(vec![Entry::at("auto_retry_for", EntryError::ListType)]),
    };
    let mut codes: Vec<String> = Vec::with_capacity(items.len());
    let mut entries: Vec<Entry> = Vec::new();
    for (position, item) in items.iter().enumerate() {
        match python_value(item) {
            PythonValue::Str(code) => codes.push(code),
            PythonValue::None
            | PythonValue::Bool(_)
            | PythonValue::Int(_)
            | PythonValue::Float
            | PythonValue::List(_)
            | PythonValue::Dict => entries.extend(not_a_code_entries(position)),
        }
    }
    list_or_entries("auto_retry_for", codes, entries)
}

/// The entries of an `auto_retry_for` item that is not a text.
fn not_a_code_entries(position: usize) -> Vec<Entry> {
    let mut entries = vec![Entry::at(
        &format!("auto_retry_for.{position}.str"),
        EntryError::StringType,
    )];
    entries.extend(BUILT_IN_CODE_FAMILIES.into_iter().map(|family| {
        Entry::at(
            &format!("auto_retry_for.{position}.{BUILT_IN_CODE_UNION_LABEL}.str-enum[{family}]"),
            EntryError::IsInstanceOf(family),
        )
    }));
    entries
}

// ---------------------------------------------------------------------------
// Model validators
// ---------------------------------------------------------------------------

/// Run the model validators in Python's order; the first error ends the
/// validation (`validate_strategy_consistency`, then
/// `validate_error_code_fields`, `core/models/tasks.py:569-600`).
fn rule_from_fields(fields: PolicyFields) -> Result<RetryRule, Entry> {
    let interval_count = fields.intervals.len();
    let max_retries = fields.max_retries as usize;
    match fields.backoff {
        Backoff::Fixed if interval_count != max_retries => {
            return Err(Entry::of_model(format!(
                "Fixed backoff strategy requires intervals length ({interval_count}) to match \
                 max_retries ({max_retries}). Either adjust intervals list or use exponential \
                 strategy."
            )))
        }
        Backoff::Exponential if interval_count != 1 => {
            return Err(Entry::of_model(format!(
                "Exponential backoff strategy requires exactly one base interval, got \
                 {interval_count} intervals. Use intervals=[base_seconds] for exponential \
                 backoff."
            )))
        }
        Backoff::Fixed | Backoff::Exponential => {}
    }
    if let Some(refused) = fields.auto_retry_for.iter().find_map(|code| {
        check_error_code_text(code)
            .err()
            .map(|refusal| refusal.message("auto_retry_for", code))
    }) {
        return Err(Entry::of_model(refused));
    }
    Ok(RetryRule {
        max_retries: fields.max_retries,
        intervals: fields.intervals,
        backoff: fields.backoff,
        jitter: fields.jitter,
        delay_cap: fields.delay_cap,
        auto_retry_for: fields.auto_retry_for,
    })
}

#[cfg(test)]
mod tests;
