//! The forms of the event contract: what each value is on the wire and in
//! Python, the traits that `#[derive(horsies_core_derive::EventSchema)]`
//! implements, the value types that implement them by hand, and the
//! refusals of the codec.
//!
//! Pure: no I/O.

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::catalog::CodeCatalog;
use crate::core::config::bounded::BoundedU32;
use crate::worker::executor::protocol::{BoundedText, ProcessId};

/// A JSON value of the wire form.
pub type WireValue = serde_json::Value;

/// The most characters of a name from the wire that a refusal quotes: an
/// event tag, a variant tag or a choice value that the contract does not
/// have. A longer name is cut.
pub const MAX_REFUSED_NAME_CHARS: usize = 64;

/// A name from the wire that the contract does not have, cut to
/// [`MAX_REFUSED_NAME_CHARS`].
pub type RefusedName = BoundedText<MAX_REFUSED_NAME_CHARS>;

/// What one value is on the wire and in Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueForm {
    /// A UUID. Wire: its hyphenated lower-case text. Python: `uuid.UUID`.
    Uuid,
    /// Text of at most `max_chars` characters (Unicode scalar values, as
    /// Python's `len`). Wire: a JSON string. Python: `str`.
    Text { max_chars: usize },
    /// A whole number from `min` to `max`. Wire: a JSON number. Python:
    /// `int`.
    WholeNumber { min: i64, max: i64 },
    /// A duration. Wire: whole nanoseconds, from 0 to `u64::MAX`. Python:
    /// `datetime.timedelta`, to the microsecond (the nanoseconds below a
    /// microsecond are dropped).
    Duration,
    /// A point in time. Wire: whole nanoseconds since the Unix epoch, as an
    /// `i64`. Python: a `datetime.datetime` in UTC, to the microsecond (the
    /// nanoseconds below a microsecond are dropped).
    Time,
    /// A member of one of the code catalogs of a [`CodeUnion`]. Wire: the
    /// member's value. Python: the member of its generated enum class.
    Codes(&'static CodeUnion),
    /// A closed type of the contract ([`NamedType`]), by its name.
    Named(&'static str),
}

/// Several code catalogs whose members one value can be. The wire values
/// are unique across the catalogs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeUnion {
    /// The Python name of the union alias of the catalog classes.
    pub python_name: &'static str,
    pub doc: &'static str,
    pub catalogs: &'static [CodeCatalog],
}

/// Who reads a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldAccess {
    /// A field of the public contract.
    Public,
    /// A field that only the handler child reads. It is on the wire and on
    /// the Python class (left out of `repr`), and it is not a field of the
    /// stable contract.
    Internal,
}

/// One field of an event type or of a variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventField {
    /// The field name; the Python attribute has the same name.
    pub name: &'static str,
    /// The field doc, from the field's `///` lines.
    pub doc: &'static str,
    pub form: ValueForm,
    pub access: FieldAccess,
}

/// One event type. Wire form: `[name, field, ...]`, the fields in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventType {
    /// The wire tag and the Python class: the Rust type name.
    pub name: &'static str,
    /// The type doc, from the type's `///` lines.
    pub doc: &'static str,
    /// The fields in wire order.
    pub fields: &'static [EventField],
}

/// A closed type that event fields hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamedType {
    /// The Rust type name; the Python alias has the same name.
    pub name: &'static str,
    pub doc: &'static str,
    pub shape: NamedShape,
}

/// How a closed type is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedShape {
    /// An enum of unit variants. Wire: the value's text. Python: a
    /// `Literal` alias.
    Choice { values: &'static [ChoiceValue] },
    /// An enum whose variants are unit variants or have named fields. Wire:
    /// `[tag, field, ...]`. Python: one frozen dataclass per variant and a
    /// union alias.
    Variants { variants: &'static [VariantType] },
}

/// One value of a choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChoiceValue {
    /// The wire value: the variant name in SCREAMING_SNAKE_CASE.
    pub value: &'static str,
    pub doc: &'static str,
}

/// One variant of a variant type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VariantType {
    /// The wire tag: the Rust variant name.
    pub tag: &'static str,
    /// The Python class: the type name, then the variant name.
    pub python_class: &'static str,
    pub doc: &'static str,
    /// The fields in wire order; none for a unit variant.
    pub fields: &'static [EventField],
}

/// A Rust type that an event field can have.
pub trait EventValue: Sized {
    /// The form of the value on the wire and in Python.
    const FORM: ValueForm;

    /// The wire form of the value.
    ///
    /// Errors: a duration or a time that its wire form cannot hold.
    fn to_wire(&self) -> Result<WireValue, EncodeRefusal>;

    /// The value of a wire form.
    ///
    /// Errors: a JSON value of another kind, or a value outside the bound
    /// or the set of the type.
    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal>;
}

/// A closed type of the contract, recorded by the derive.
pub trait EventNamedValue: EventValue {
    const NAMED: NamedType;
}

/// An event type, recorded by the derive.
pub trait EventRecord: Sized {
    const TYPE: EventType;

    /// Push the wire form of each field, in order. The caller pushes the tag
    /// first.
    ///
    /// Errors: the first field that does not encode.
    fn fields_to_wire(&self, items: &mut Vec<WireValue>) -> Result<(), EncodeRefusal>;

    /// The event of the wire items after the tag.
    ///
    /// Errors: another number of items, or the first item that does not
    /// decode.
    fn from_wire_fields(fields: &[WireValue]) -> Result<Self, DecodeRefusal>;
}

/// The names from an event down to one value: field names, and the tag of
/// each variant on the way. Its length is bounded by the nesting of the
/// contract's types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WirePath(Vec<&'static str>);

impl WirePath {
    /// The names, outermost first.
    pub fn names(&self) -> &[&'static str] {
        &self.0
    }
}

impl fmt::Display for WirePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.is_empty() {
            true => formatter.write_str("the items of the event"),
            false => formatter.write_str(&self.0.join(".")),
        }
    }
}

/// The kind of JSON value that a form needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    Text,
    WholeNumber,
    Array,
}

impl fmt::Display for JsonKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Text => "a JSON string",
            Self::WholeNumber => "a whole JSON number",
            Self::Array => "a JSON array",
        };
        formatter.write_str(text)
    }
}

/// Why a wire value does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeCause {
    #[error("expected {expected}")]
    WrongJsonKind { expected: JsonKind },
    #[error("expected {expected} items, found {found}")]
    ItemCount { expected: usize, found: usize },
    #[error("the number is outside {min}..={max}")]
    OutOfRange { min: i128, max: i128 },
    #[error("the text has {chars} characters; the bound is {bound}")]
    TextOverBound { chars: usize, bound: usize },
    #[error("the text is not a UUID in hyphenated lower-case form")]
    NotCanonicalUuid,
    #[error("{type_name} has no value '{found}'")]
    UnknownName {
        type_name: &'static str,
        found: RefusedName,
    },
    #[error("a value of {type_name} is a JSON array whose first item is its variant's name")]
    NoVariantTag { type_name: &'static str },
}

/// A wire value that does not decode, and where it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {cause}")]
pub struct DecodeRefusal {
    pub path: WirePath,
    pub cause: DecodeCause,
}

impl DecodeRefusal {
    /// A refusal of the value itself.
    pub fn new(cause: DecodeCause) -> Self {
        Self {
            path: WirePath::default(),
            cause,
        }
    }

    /// The same refusal, inside the field or variant `name`.
    #[must_use]
    pub fn inside(mut self, name: &'static str) -> Self {
        self.path.0.insert(0, name);
        self
    }

    /// `found` items where the type has `expected`.
    pub fn item_count(expected: usize, found: usize) -> Self {
        Self::new(DecodeCause::ItemCount { expected, found })
    }

    /// `found` is not a value of `type_name`.
    pub fn unknown_name(type_name: &'static str, found: &str) -> Self {
        Self::new(DecodeCause::UnknownName {
            type_name,
            found: RefusedName::cut(found),
        })
    }

    fn wrong_kind(expected: JsonKind) -> Self {
        Self::new(DecodeCause::WrongJsonKind { expected })
    }

    fn out_of_range(min: i128, max: i128) -> Self {
        Self::new(DecodeCause::OutOfRange { min, max })
    }
}

/// Why a value has no wire form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncodeCause {
    #[error("the duration has {nanos} ns; the wire holds at most u64::MAX ns")]
    DurationOverBound { nanos: u128 },
    #[error("the time {time} is outside the i64 nanoseconds of the wire")]
    TimeOutOfRange { time: DateTime<Utc> },
}

/// A value that has no wire form, and where it is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {cause}")]
pub struct EncodeRefusal {
    pub path: WirePath,
    pub cause: EncodeCause,
}

impl EncodeRefusal {
    /// A refusal of the value itself.
    pub fn new(cause: EncodeCause) -> Self {
        Self {
            path: WirePath::default(),
            cause,
        }
    }

    /// The same refusal, inside the field or variant `name`.
    #[must_use]
    pub fn inside(mut self, name: &'static str) -> Self {
        self.path.0.insert(0, name);
        self
    }
}

/// The text of a JSON string.
///
/// Errors: `WrongJsonKind` for any other JSON value.
pub fn wire_text(wire: &WireValue) -> Result<&str, DecodeRefusal> {
    match wire {
        WireValue::String(text) => Ok(text.as_str()),
        WireValue::Null
        | WireValue::Bool(_)
        | WireValue::Number(_)
        | WireValue::Array(_)
        | WireValue::Object(_) => Err(DecodeRefusal::wrong_kind(JsonKind::Text)),
    }
}

/// The tag and the field items of a value of the variant type `type_name`.
///
/// Errors: `WrongJsonKind` for a value that is not an array;
/// `NoVariantTag` for an array whose first item is not a string.
pub fn wire_variant<'a>(
    wire: &'a WireValue,
    type_name: &'static str,
) -> Result<(&'a str, &'a [WireValue]), DecodeRefusal> {
    let WireValue::Array(items) = wire else {
        return Err(DecodeRefusal::wrong_kind(JsonKind::Array));
    };
    match items.split_first() {
        Some((WireValue::String(tag), fields)) => Ok((tag.as_str(), fields)),
        Some(_) | None => Err(DecodeRefusal::new(DecodeCause::NoVariantTag { type_name })),
    }
}

/// The value of a whole JSON number.
///
/// Errors: `WrongJsonKind` for any other JSON value, a fraction included.
fn wire_whole_number(wire: &WireValue) -> Result<i128, DecodeRefusal> {
    let WireValue::Number(number) = wire else {
        return Err(DecodeRefusal::wrong_kind(JsonKind::WholeNumber));
    };
    match (number.as_i64(), number.as_u64()) {
        (Some(signed), _) => Ok(i128::from(signed)),
        (None, Some(unsigned)) => Ok(i128::from(unsigned)),
        (None, None) => Err(DecodeRefusal::wrong_kind(JsonKind::WholeNumber)),
    }
}

impl EventValue for Uuid {
    const FORM: ValueForm = ValueForm::Uuid;

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        Ok(WireValue::String(self.hyphenated().to_string()))
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let text = wire_text(wire)?;
        match Uuid::parse_str(text) {
            Ok(uuid) if uuid.hyphenated().to_string() == text => Ok(uuid),
            Ok(_) | Err(_) => Err(DecodeRefusal::new(DecodeCause::NotCanonicalUuid)),
        }
    }
}

impl<const MAX_CHARS: usize> EventValue for BoundedText<MAX_CHARS> {
    const FORM: ValueForm = ValueForm::Text {
        max_chars: MAX_CHARS,
    };

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        Ok(WireValue::String(self.as_str().to_owned()))
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let text = wire_text(wire)?;
        BoundedText::new(text.to_owned()).map_err(|over| {
            DecodeRefusal::new(DecodeCause::TextOverBound {
                chars: over.chars,
                bound: over.bound,
            })
        })
    }
}

impl<const MIN: u32, const MAX: u32> EventValue for BoundedU32<MIN, MAX> {
    const FORM: ValueForm = ValueForm::WholeNumber {
        min: MIN as i64,
        max: MAX as i64,
    };

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        Ok(WireValue::from(self.get()))
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let number = wire_whole_number(wire)?;
        u32::try_from(number)
            .ok()
            .and_then(|value| BoundedU32::try_from(value).ok())
            .ok_or_else(|| DecodeRefusal::out_of_range(i128::from(MIN), i128::from(MAX)))
    }
}

/// The largest process id: the largest `pid_t`, as [`ProcessId`] bounds it.
const MAX_PROCESS_ID: i64 = i32::MAX as i64;

impl EventValue for ProcessId {
    const FORM: ValueForm = ValueForm::WholeNumber {
        min: 1,
        max: MAX_PROCESS_ID,
    };

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        Ok(WireValue::from(self.get()))
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let number = wire_whole_number(wire)?;
        u32::try_from(number)
            .ok()
            .and_then(ProcessId::new)
            .ok_or_else(|| DecodeRefusal::out_of_range(1, i128::from(MAX_PROCESS_ID)))
    }
}

impl EventValue for Duration {
    const FORM: ValueForm = ValueForm::Duration;

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        let nanos = self.as_nanos();
        match u64::try_from(nanos) {
            Ok(whole) => Ok(WireValue::from(whole)),
            Err(_over_u64) => Err(EncodeRefusal::new(EncodeCause::DurationOverBound { nanos })),
        }
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let number = wire_whole_number(wire)?;
        match u64::try_from(number) {
            Ok(nanos) => Ok(Duration::from_nanos(nanos)),
            Err(_outside_u64) => Err(DecodeRefusal::out_of_range(0, i128::from(u64::MAX))),
        }
    }
}

impl EventValue for DateTime<Utc> {
    const FORM: ValueForm = ValueForm::Time;

    fn to_wire(&self) -> Result<WireValue, EncodeRefusal> {
        match self.timestamp_nanos_opt() {
            Some(nanos) => Ok(WireValue::from(nanos)),
            None => Err(EncodeRefusal::new(EncodeCause::TimeOutOfRange {
                time: *self,
            })),
        }
    }

    fn from_wire(wire: &WireValue) -> Result<Self, DecodeRefusal> {
        let number = wire_whole_number(wire)?;
        match i64::try_from(number) {
            Ok(nanos) => Ok(DateTime::<Utc>::from_timestamp_nanos(nanos)),
            Err(_outside_i64) => Err(DecodeRefusal::out_of_range(
                i128::from(i64::MIN),
                i128::from(i64::MAX),
            )),
        }
    }
}
