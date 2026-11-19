//! The reader of frame metadata: one field at a time, each checked, with the
//! path of the field in every error.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::bounds::{MAX_EVENTS_PER_FRAME, MAX_EVENT_WIRE_BYTES};
use super::values::{BoundedCount, BoundedText, DispatchNumber, RssMb, RssReading};
use super::PROTOCOL_VERSION;
use crate::core::codec::{check_json_bytes, StrictJsonError};

/// The longest text of an unknown kind that an error repeats.
const MAX_KIND_ECHO_CHARS: usize = 64;

/// The type a field must have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedType {
    Text,
    WholeNumber,
    Number,
    Flag,
    Object,
    ListOfText,
    ListOfObjects,
    TaskId,
    Rfc3339Time,
}

impl fmt::Display for ExpectedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Text => "text",
            Self::WholeNumber => "a whole number",
            Self::Number => "a number or null",
            Self::Flag => "true or false",
            Self::Object => "an object",
            Self::ListOfText => "a list of texts",
            Self::ListOfObjects => "a list of objects",
            Self::TaskId => "a UUID text",
            Self::Rfc3339Time => "an RFC 3339 time",
        };
        f.write_str(text)
    }
}

/// The bytes are not a valid frame of the expected direction. The reader
/// kills a child that sends one; a child exits with the bad-frame code.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MalformedFrame {
    #[error("the frame metadata is not strict JSON: {0}")]
    MetadataNotJson(StrictJsonError),
    #[error("the frame metadata could not be read: {detail}")]
    MetadataNotReadable { detail: String },
    #[error("the frame metadata is not a JSON object")]
    MetadataNotObject,
    #[error("field '{field}' is missing")]
    MissingField { field: String },
    #[error("field '{field}' is not {expected}")]
    WrongType {
        field: String,
        expected: ExpectedType,
    },
    #[error("field '{field}' is out of its bounds")]
    OutOfBounds { field: String },
    #[error("field '{field}' names an unknown kind '{kind}'")]
    UnknownKind {
        field: String,
        kind: BoundedText<MAX_KIND_ECHO_CHARS>,
    },
    #[error("field '{field}' is not part of this frame")]
    UnknownField { field: String },
    #[error("the frame body is not UTF-8 from byte {valid_up_to}")]
    BodyNotUtf8 { valid_up_to: usize },
    #[error("a '{kind}' frame or end carries no body, but {body_bytes} bytes came")]
    UnexpectedBody {
        kind: &'static str,
        body_bytes: usize,
    },
    #[error("field '{field}': {rule}")]
    BrokenRule { field: String, rule: &'static str },
    #[error("the body of an events frame is not strict JSON: {0}")]
    EventsBodyNotJson(StrictJsonError),
    #[error("the body of an events frame is not a JSON array")]
    EventsBodyNotArray,
    #[error(
        "an events frame carries 1 to {max} events, not {count}",
        max = MAX_EVENTS_PER_FRAME
    )]
    EventCount { count: usize },
    #[error(
        "event {index} of an events frame has {bytes} bytes; the bound is {max}",
        max = MAX_EVENT_WIRE_BYTES
    )]
    EventOverBound { index: usize, bytes: usize },
}

/// The protocol version that a peer named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivedProtocol {
    /// The frame has no `protocol` field: a version-1 peer.
    NotNamed,
    Named(u64),
}

impl fmt::Display for ReceivedProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotNamed => f.write_str("no version (version 1)"),
            Self::Named(version) => write!(f, "version {version}"),
        }
    }
}

/// The peer speaks another protocol version than this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the peer speaks protocol {received}; this build speaks version {PROTOCOL_VERSION}")]
pub struct ProtocolMismatch {
    pub received: ReceivedProtocol,
}

/// Parse frame metadata: strict JSON (no repeated key, no `NaN`), one object.
pub(super) fn parse_metadata(meta: &[u8]) -> Result<Map<String, Value>, MalformedFrame> {
    check_json_bytes(meta).map_err(MalformedFrame::MetadataNotJson)?;
    match serde_json::from_slice::<Value>(meta) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Err(MalformedFrame::MetadataNotObject),
        Err(error) => Err(MalformedFrame::MetadataNotReadable {
            detail: error.to_string(),
        }),
    }
}

/// The body as UTF-8 text, without a copy.
pub(super) fn text_body(body: Vec<u8>) -> Result<String, MalformedFrame> {
    String::from_utf8(body).map_err(|error| MalformedFrame::BodyNotUtf8 {
        valid_up_to: error.utf8_error().valid_up_to(),
    })
}

/// Refuse a body on a frame or an end of kind `kind`, which carries none.
pub(super) fn no_body(kind: &'static str, body: &[u8]) -> Result<(), MalformedFrame> {
    match body.is_empty() {
        true => Ok(()),
        false => Err(MalformedFrame::UnexpectedBody {
            kind,
            body_bytes: body.len(),
        }),
    }
}

/// A time as RFC 3339 text in UTC with every sub-second digit it has, so the
/// peer reads the exact value. Pure.
pub(super) fn time_text(time: &DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// The protocol version that a frame names, compared with this build.
pub(super) fn check_protocol(map: &Map<String, Value>) -> Result<(), ProtocolCheck> {
    match map.get("protocol") {
        None => Err(ProtocolCheck::Mismatch(ProtocolMismatch {
            received: ReceivedProtocol::NotNamed,
        })),
        Some(value) => match value.as_u64() {
            Some(version) if version == u64::from(PROTOCOL_VERSION) => Ok(()),
            Some(version) => Err(ProtocolCheck::Mismatch(ProtocolMismatch {
                received: ReceivedProtocol::Named(version),
            })),
            None => Err(ProtocolCheck::Malformed(MalformedFrame::WrongType {
                field: "protocol".to_owned(),
                expected: ExpectedType::WholeNumber,
            })),
        },
    }
}

/// The result of [`check_protocol`] when it does not pass.
pub(super) enum ProtocolCheck {
    Mismatch(ProtocolMismatch),
    Malformed(MalformedFrame),
}

/// The fields of one metadata object, and its path for errors.
pub(super) struct Fields<'a> {
    map: &'a Map<String, Value>,
    path: String,
}

impl<'a> Fields<'a> {
    pub(super) fn root(map: &'a Map<String, Value>) -> Self {
        Self {
            map,
            path: String::new(),
        }
    }

    fn field_path(&self, key: &str) -> String {
        match self.path.is_empty() {
            true => key.to_owned(),
            false => format!("{}.{}", self.path, key),
        }
    }

    fn wrong_type(&self, key: &str, expected: ExpectedType) -> MalformedFrame {
        MalformedFrame::WrongType {
            field: self.field_path(key),
            expected,
        }
    }

    pub(super) fn out_of_bounds(&self, key: &str) -> MalformedFrame {
        MalformedFrame::OutOfBounds {
            field: self.field_path(key),
        }
    }

    pub(super) fn broken_rule(&self, key: &str, rule: &'static str) -> MalformedFrame {
        MalformedFrame::BrokenRule {
            field: self.field_path(key),
            rule,
        }
    }

    /// Refuse every field outside `allowed`.
    pub(super) fn only(&self, allowed: &[&str]) -> Result<(), MalformedFrame> {
        match self.map.keys().find(|key| !allowed.contains(&key.as_str())) {
            None => Ok(()),
            Some(key) => Err(MalformedFrame::UnknownField {
                field: self.field_path(key),
            }),
        }
    }

    fn value(&self, key: &str) -> Result<&'a Value, MalformedFrame> {
        self.map
            .get(key)
            .ok_or_else(|| MalformedFrame::MissingField {
                field: self.field_path(key),
            })
    }

    pub(super) fn text(&self, key: &str) -> Result<&'a str, MalformedFrame> {
        match self.value(key)? {
            Value::String(text) => Ok(text.as_str()),
            _ => Err(self.wrong_type(key, ExpectedType::Text)),
        }
    }

    pub(super) fn owned_text(&self, key: &str) -> Result<String, MalformedFrame> {
        self.text(key).map(str::to_owned)
    }

    pub(super) fn bounded_text<const MAX_CHARS: usize>(
        &self,
        key: &str,
    ) -> Result<BoundedText<MAX_CHARS>, MalformedFrame> {
        BoundedText::new(self.owned_text(key)?).map_err(|_| self.out_of_bounds(key))
    }

    /// The `kind` of this object.
    pub(super) fn kind(&self) -> Result<&'a str, MalformedFrame> {
        self.text("kind")
    }

    /// The error for a `kind` that is not one of the known kinds.
    pub(super) fn unknown_kind(&self, kind: &str) -> MalformedFrame {
        MalformedFrame::UnknownKind {
            field: self.field_path("kind"),
            kind: BoundedText::cut(kind),
        }
    }

    pub(super) fn whole(&self, key: &str) -> Result<u64, MalformedFrame> {
        match self.value(key)? {
            Value::Number(number) => match (number.as_u64(), number.is_i64()) {
                (Some(whole), _) => Ok(whole),
                (None, true) => Err(self.out_of_bounds(key)),
                (None, false) => Err(self.wrong_type(key, ExpectedType::WholeNumber)),
            },
            _ => Err(self.wrong_type(key, ExpectedType::WholeNumber)),
        }
    }

    pub(super) fn bounded_count<const MIN: u64, const MAX: u64>(
        &self,
        key: &str,
    ) -> Result<BoundedCount<MIN, MAX>, MalformedFrame> {
        BoundedCount::new(self.whole(key)?).ok_or_else(|| self.out_of_bounds(key))
    }

    pub(super) fn dispatch(&self, key: &str) -> Result<DispatchNumber, MalformedFrame> {
        DispatchNumber::new(self.whole(key)?).ok_or_else(|| self.out_of_bounds(key))
    }

    pub(super) fn flag(&self, key: &str) -> Result<bool, MalformedFrame> {
        match self.value(key)? {
            Value::Bool(flag) => Ok(*flag),
            _ => Err(self.wrong_type(key, ExpectedType::Flag)),
        }
    }

    pub(super) fn text_list(&self, key: &str) -> Result<Vec<String>, MalformedFrame> {
        let items = match self.value(key)? {
            Value::Array(items) => items,
            _ => return Err(self.wrong_type(key, ExpectedType::ListOfText)),
        };
        items
            .iter()
            .map(|item| match item {
                Value::String(text) => Ok(text.clone()),
                _ => Err(self.wrong_type(key, ExpectedType::ListOfText)),
            })
            .collect()
    }

    /// A list of texts, each at most `MAX_CHARS` characters.
    pub(super) fn bounded_text_list<const MAX_CHARS: usize>(
        &self,
        key: &str,
    ) -> Result<Vec<BoundedText<MAX_CHARS>>, MalformedFrame> {
        self.text_list(key)?
            .into_iter()
            .map(|text| BoundedText::new(text).map_err(|_| self.out_of_bounds(key)))
            .collect()
    }

    /// The `rss_mb` field: a finite number from 0, or `null` when the child
    /// could not read it.
    pub(super) fn rss(&self, key: &str) -> Result<RssReading, MalformedFrame> {
        match self.value(key)? {
            Value::Null => Ok(RssReading::Unreadable),
            Value::Number(number) => match number.as_f64().and_then(RssMb::new) {
                Some(rss) => Ok(RssReading::Mb(rss)),
                None => Err(self.out_of_bounds(key)),
            },
            _ => Err(self.wrong_type(key, ExpectedType::Number)),
        }
    }

    pub(super) fn task_id(&self, key: &str) -> Result<Uuid, MalformedFrame> {
        Uuid::parse_str(self.text(key)?).map_err(|_| self.wrong_type(key, ExpectedType::TaskId))
    }

    pub(super) fn time(&self, key: &str) -> Result<DateTime<Utc>, MalformedFrame> {
        DateTime::parse_from_rfc3339(self.text(key)?)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|_| self.wrong_type(key, ExpectedType::Rfc3339Time))
    }

    /// The objects of the list in field `key`, each with its path
    /// (`key[index]`).
    pub(super) fn objects(&self, key: &str) -> Result<Vec<Fields<'a>>, MalformedFrame> {
        let items = match self.value(key)? {
            Value::Array(items) => items,
            _ => return Err(self.wrong_type(key, ExpectedType::ListOfObjects)),
        };
        items
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                Value::Object(map) => Ok(Fields {
                    map,
                    path: format!("{}[{index}]", self.field_path(key)),
                }),
                _ => Err(self.wrong_type(key, ExpectedType::ListOfObjects)),
            })
            .collect()
    }

    /// The number of items of the list in field `key`.
    pub(super) fn list_len(&self, key: &str) -> Result<usize, MalformedFrame> {
        match self.value(key)? {
            Value::Array(items) => Ok(items.len()),
            _ => Err(self.wrong_type(key, ExpectedType::ListOfObjects)),
        }
    }

    /// The object in field `key`, with its path.
    pub(super) fn object(&self, key: &str) -> Result<Fields<'a>, MalformedFrame> {
        match self.value(key)? {
            Value::Object(map) => Ok(Fields {
                map,
                path: self.field_path(key),
            }),
            _ => Err(self.wrong_type(key, ExpectedType::Object)),
        }
    }
}
