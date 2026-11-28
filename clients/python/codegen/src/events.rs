//! Render the event contract of horsies-core
//! (`horsies_core::events::EVENT_CONTRACT`) as `_catalog/events.py`:
//!
//! - the contract version;
//! - the union alias of each code union that a field holds;
//! - each closed type in render order: a `Literal` alias for a choice, and
//!   one frozen dataclass per variant and a union alias for a variant type;
//! - one frozen dataclass per event type, the union `AnyEvent` and the tuple
//!   `EVENT_TYPES`;
//! - the refusals `NotAnEvent`, `UnknownEventType`, `MalformedEvent` and
//!   their union `EventDecodeError`;
//! - `decode_event`, which reads the wire form of one event and returns
//!   `Result[AnyEvent, EventDecodeError]`, and its private readers: one per
//!   form, code union, closed type, variant and event type.
//!
//! The module imports the standard library, the catalog modules of its code
//! unions and `horsies.core.types.result`. It does not parse JSON text: it
//! reads the value that `json.loads` gives.

use std::collections::BTreeMap;

use horsies_core::events::{
    CodeUnion, EventContract, EventField, FieldAccess, NamedShape, NamedType, ValueForm,
    MAX_REFUSED_NAME_CHARS,
};

use crate::manifest::snake_case;
use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// The file name of the events module, inside `_catalog`.
pub const EVENTS_MODULE_FILE: &str = "events.py";

/// The decorator of every generated event and variant class.
const DATACLASS: &str = "@dataclass(frozen=True, slots=True, kw_only=True)";

/// The doc of the module.
const MODULE_DOC: &str = "The lifecycle events of the core worker, generated from horsies-core.

One frozen dataclass per event type and per variant of a closed type that an
event holds, a `Literal` alias per closed set of names, and `decode_event`,
which reads the wire form of one event: the JSON array `[type name, field,
...]`, as `json.loads` gives it. A handler subscribes to one event type, never
to `AnyEvent`: a later contract version can add an event type.";

/// The doc of `AnyEvent`.
const ANY_EVENT_DOC: &str =
    "One event of the contract. A handler subscribes to one event type, never to
this union: a later contract version can add a type.";

/// The doc of `EVENT_TYPES`.
const EVENT_TYPES_DOC: &str =
    "The event types that a handler can subscribe to, in the order of the core's
union.";

/// The names that the module defines besides the types of the contract, in
/// `__all__` order.
pub const FIXED_EXPORTS: [&str; 7] = [
    "AnyEvent",
    "EVENT_TYPES",
    "NotAnEvent",
    "UnknownEventType",
    "MalformedEvent",
    "EventDecodeError",
    "decode_event",
];

/// The kind of record that a reader builds, as its doc names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordRole {
    Event,
    Variant,
}

/// The forms that the fields of a contract use. Each selects imports and
/// helpers, so that the module defines nothing that it does not use.
struct Usage {
    uuid: bool,
    text: bool,
    /// A whole number, a duration or a time: each reads a whole number.
    whole_number: bool,
    duration: bool,
    time: bool,
    choice: bool,
    variants: bool,
    internal: bool,
    /// The code unions, each once, in the order of first use.
    unions: Vec<&'static CodeUnion>,
}

impl Usage {
    fn of(contract: &EventContract) -> Self {
        let mut usage = Usage {
            uuid: false,
            text: false,
            whole_number: false,
            duration: false,
            time: false,
            choice: false,
            variants: false,
            internal: false,
            unions: Vec::new(),
        };
        for field in contract_fields(contract) {
            match field.form {
                ValueForm::Uuid => usage.uuid = true,
                ValueForm::Text { .. } => usage.text = true,
                ValueForm::WholeNumber { .. } => usage.whole_number = true,
                ValueForm::Duration => {
                    usage.duration = true;
                    usage.whole_number = true;
                }
                ValueForm::Time => {
                    usage.time = true;
                    usage.whole_number = true;
                }
                ValueForm::Codes(union) => {
                    if !usage.unions.contains(&union) {
                        usage.unions.push(union);
                    }
                }
                ValueForm::Named(_) => {}
            }
            match field.access {
                FieldAccess::Internal => usage.internal = true,
                FieldAccess::Public => {}
            }
        }
        for named in contract.value_types {
            match named.shape {
                NamedShape::Choice { .. } => usage.choice = true,
                NamedShape::Variants { .. } => usage.variants = true,
            }
        }
        usage
    }
}

/// Every field of the contract: the fields of the events, then the fields
/// of the variants of the closed types.
fn contract_fields(contract: &EventContract) -> Vec<EventField> {
    let mut fields: Vec<EventField> = contract
        .events
        .iter()
        .flat_map(|event| event.fields.iter().copied())
        .collect();
    for named in contract.value_types {
        match named.shape {
            NamedShape::Variants { variants } => fields.extend(
                variants
                    .iter()
                    .flat_map(|variant| variant.fields.iter().copied()),
            ),
            NamedShape::Choice { .. } => {}
        }
    }
    fields
}

/// The text of `_catalog/events.py`.
pub fn render_events_module(contract: &EventContract) -> String {
    let usage = Usage::of(contract);
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&docstring(MODULE_DOC, "").unwrap_or_default());
    text.push('\n');
    text.push_str(&render_imports(&usage));
    text.push_str(&render_all_list(contract, &usage));
    text.push_str(&format!(
        "\nEVENT_CONTRACT_VERSION: Final = {}\n\
         \"\"\"The version of the event contract that this module reads.\"\"\"\n",
        contract.version
    ));
    for union in &usage.unions {
        text.push_str("\n\n");
        text.push_str(&render_code_union(union));
    }
    for named in contract.value_types {
        text.push_str(&render_value_type(named));
    }
    for event in contract.events {
        text.push_str("\n\n");
        text.push_str(&render_record_class(event.name, event.doc, event.fields));
    }
    text.push_str("\n\n");
    text.push_str(&render_event_union(contract));
    text.push_str(&render_error_classes());
    text.push_str(&render_decode_event(contract));
    text.push_str(&render_helpers(&usage));
    for union in &usage.unions {
        text.push_str(&render_code_union_reader(union));
    }
    for named in contract.value_types {
        text.push_str(&render_value_reader(named));
    }
    for event in contract.events {
        text.push_str(&render_record_reader(
            event.name,
            event.fields,
            RecordRole::Event,
        ));
    }
    text
}

/// The imports: the standard library, then the catalog modules of the code
/// unions, then the result type.
fn render_imports(usage: &Usage) -> String {
    let mut text = String::from("from __future__ import annotations\n\n");
    if usage.uuid {
        text.push_str("import uuid\n");
    }
    text.push_str(match usage.internal {
        true => "from dataclasses import dataclass, field\n",
        false => "from dataclasses import dataclass\n",
    });
    let mut datetime_names: Vec<&str> = Vec::new();
    if usage.time {
        datetime_names.push("datetime");
    }
    if usage.duration || usage.time {
        datetime_names.push("timedelta");
    }
    if usage.time {
        datetime_names.push("timezone");
    }
    if !datetime_names.is_empty() {
        text.push_str(&format!(
            "from datetime import {}\n",
            datetime_names.join(", ")
        ));
    }
    text.push_str(match usage.choice {
        true => "from typing import Final, Literal, cast\n",
        false => "from typing import Final, cast\n",
    });
    text.push('\n');
    let mut catalog_imports: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for union in &usage.unions {
        for catalog in union.catalogs {
            let names = catalog_imports.entry(catalog.module).or_default();
            if !names.contains(&catalog.type_name) {
                names.push(catalog.type_name);
            }
        }
    }
    for (module, names) in &mut catalog_imports {
        names.sort_unstable();
        text.push_str(&format!(
            "from horsies._catalog.{module} import {}\n",
            names.join(", ")
        ));
    }
    text.push_str("from horsies.core.types.result import Err, Ok, Result\n\n");
    text
}

/// `__all__`: the version, the code unions, each closed type after its
/// variant classes, the events, then [`FIXED_EXPORTS`].
fn render_all_list(contract: &EventContract, usage: &Usage) -> String {
    let mut names: Vec<&str> = vec!["EVENT_CONTRACT_VERSION"];
    names.extend(usage.unions.iter().map(|union| union.alias_name));
    for named in contract.value_types {
        match named.shape {
            NamedShape::Variants { variants } => {
                names.extend(variants.iter().map(|variant| variant.type_name));
            }
            NamedShape::Choice { .. } => {}
        }
        names.push(named.name);
    }
    names.extend(contract.events.iter().map(|event| event.name));
    names.extend(FIXED_EXPORTS);
    let mut text = String::from("__all__ = [\n");
    for name in names {
        text.push_str(&format!("    {},\n", string_literal(name)));
    }
    text.push_str("]\n");
    text
}

/// The union alias of the catalog classes of a code union, with its doc.
fn render_code_union(union: &CodeUnion) -> String {
    let classes: Vec<&str> = union
        .catalogs
        .iter()
        .map(|catalog| catalog.type_name)
        .collect();
    let mut text = format!("type {} = {}\n", union.alias_name, classes.join(" | "));
    text.push_str(&docstring(union.doc, "").unwrap_or_default());
    text
}

/// A choice as a `Literal` alias whose doc lists the doc of each value; a
/// variant type as one class per variant and a union alias.
fn render_value_type(named: &NamedType) -> String {
    match named.shape {
        NamedShape::Choice { values } => {
            let literals: Vec<String> = values
                .iter()
                .map(|value| string_literal(value.value))
                .collect();
            let mut text = format!(
                "\n\ntype {} = Literal[{}]\n",
                named.name,
                literals.join(", ")
            );
            let mut doc = named.doc.trim().to_owned();
            for value in values {
                let value_doc = value.doc.split_whitespace().collect::<Vec<_>>().join(" ");
                if !value_doc.is_empty() {
                    doc.push_str(&format!("\n\n`{}`: {value_doc}", value.value));
                }
            }
            text.push_str(&docstring(&doc, "").unwrap_or_default());
            text
        }
        NamedShape::Variants { variants } => {
            let mut text = String::new();
            for variant in variants {
                text.push_str("\n\n");
                text.push_str(&render_record_class(
                    variant.type_name,
                    variant.doc,
                    variant.fields,
                ));
            }
            let members: Vec<&str> = variants.iter().map(|variant| variant.type_name).collect();
            text.push_str("\n\n");
            text.push_str(&render_union_alias(named.name, &members));
            text.push_str(&docstring(named.doc, "").unwrap_or_default());
            text
        }
    }
}

/// `type name = A | B`, one member per line when there are several.
fn render_union_alias(name: &str, members: &[&str]) -> String {
    match members {
        [single] => format!("type {name} = {single}\n"),
        _ => {
            let mut text = format!("type {name} = (\n");
            for (position, member) in members.iter().enumerate() {
                match position {
                    0 => text.push_str(&format!("    {member}\n")),
                    _ => text.push_str(&format!("    | {member}\n")),
                }
            }
            text.push_str(")\n");
            text
        }
    }
}

/// The annotation of a field.
fn python_type(form: ValueForm) -> &'static str {
    match form {
        ValueForm::Uuid => "uuid.UUID",
        ValueForm::Text { .. } => "str",
        ValueForm::WholeNumber { .. } => "int",
        ValueForm::Duration => "timedelta",
        ValueForm::Time => "datetime",
        ValueForm::Codes(union) => union.alias_name,
        ValueForm::Named(name) => name,
    }
}

/// One frozen dataclass: its doc, then each field with its doc. An internal
/// field is left out of `repr`.
fn render_record_class(class: &str, doc: &str, fields: &[EventField]) -> String {
    let mut text = format!("{DATACLASS}\nclass {class}:\n");
    let class_doc = docstring(doc, "    ");
    let has_doc = class_doc.is_some();
    if let Some(rendered) = class_doc {
        text.push_str(&rendered);
    }
    if fields.is_empty() && !has_doc {
        text.push_str("    pass\n");
    }
    for (position, field) in fields.iter().enumerate() {
        if position > 0 || has_doc {
            text.push('\n');
        }
        let annotation = python_type(field.form);
        match field.access {
            FieldAccess::Public => text.push_str(&format!("    {}: {annotation}\n", field.name)),
            FieldAccess::Internal => text.push_str(&format!(
                "    {}: {annotation} = field(repr=False)\n",
                field.name
            )),
        }
        if let Some(rendered) = docstring(field.doc, "    ") {
            text.push_str(&rendered);
        }
    }
    text
}

/// `AnyEvent` and `EVENT_TYPES`.
fn render_event_union(contract: &EventContract) -> String {
    let names: Vec<&str> = contract.events.iter().map(|event| event.name).collect();
    let mut text = render_union_alias("AnyEvent", &names);
    text.push_str(&docstring(ANY_EVENT_DOC, "").unwrap_or_default());
    text.push_str("\nEVENT_TYPES: Final = (\n");
    for name in &names {
        text.push_str(&format!("    {name},\n"));
    }
    text.push_str(")\n");
    text.push_str(&docstring(EVENT_TYPES_DOC, "").unwrap_or_default());
    text
}

/// The refusal classes and their union.
fn render_error_classes() -> String {
    format!(
        "\n\n{DATACLASS}\nclass NotAnEvent:\n    \
         \"\"\"The value is not a JSON array whose first item is a string.\"\"\"\n\
         \n\n{DATACLASS}\nclass UnknownEventType:\n    \
         \"\"\"The first item names no event type of this contract version.\n\
         \n    \
         A reader skips this event; the other events of its batch still decode.\n    \
         \"\"\"\n\
         \n    \
         tag: str\n    \
         \"\"\"The first item, cut to {MAX_REFUSED_NAME_CHARS} characters.\"\"\"\n\
         \n\n{DATACLASS}\nclass MalformedEvent:\n    \
         \"\"\"An event of a known type whose items do not match the fields of the type.\"\"\"\n\
         \n    \
         event: str\n    \
         \"\"\"The event type.\"\"\"\n\
         \n    \
         path: str\n    \
         \"\"\"The field and variant names from the event down to the refused value,\n    \
         joined by dots. Empty for the items of the event itself.\n    \
         \"\"\"\n\
         \n    \
         reason: str\n    \
         \"\"\"Why the value does not decode.\"\"\"\n\
         \n\n\
         type EventDecodeError = NotAnEvent | UnknownEventType | MalformedEvent\n\
         \"\"\"Why one wire value is not an event of this contract version.\"\"\"\n"
    )
}

/// `decode_event`: the tag dispatch to the reader of each event type.
fn render_decode_event(contract: &EventContract) -> String {
    let mut text = String::from(
        "\n\ndef decode_event(wire: object) -> Result[AnyEvent, EventDecodeError]:\n    \
         \"\"\"The event of one wire value: the JSON array `[type name, field, ...]` that\n    \
         `json.loads` gives for one event.\n\
         \n    \
         Err with `NotAnEvent`, `UnknownEventType` or `MalformedEvent`; it never\n    \
         raises.\n    \
         \"\"\"\n    \
         match wire:\n        \
         case list():\n            \
         items = cast('list[object]', wire)\n        \
         case _:\n            \
         return Err(NotAnEvent())\n    \
         match items:\n        \
         case [str() as tag, *fields]:\n            \
         pass\n        \
         case _:\n            \
         return Err(NotAnEvent())\n    \
         match tag:\n",
    );
    for event in contract.events {
        let tag = string_literal(event.name);
        text.push_str(&format!(
            "        case {tag}:\n            return _read_{}(fields, _At({tag}, ()))\n",
            snake_case(event.name)
        ));
    }
    text.push_str(
        "        case _:\n            \
         return Err(UnknownEventType(tag=tag[:_MAX_REFUSED_NAME_CHARS]))\n",
    );
    text
}

/// `_At`: the place of a value, which builds the refusal of a value there.
fn render_place_class() -> String {
    format!(
        "\n\n_MAX_REFUSED_NAME_CHARS: Final = {MAX_REFUSED_NAME_CHARS}\n\
         \"\"\"The most characters of a name from the wire that a refusal quotes.\"\"\"\n\
         \n\n\
         @dataclass(frozen=True, slots=True)\n\
         class _At:\n    \
         \"\"\"Where a value is: the event type, and the field and variant names inside it.\"\"\"\n\
         \n    \
         event: str\n    \
         path: tuple[str, ...]\n\
         \n    \
         def inside(self, name: str) -> _At:\n        \
         \"\"\"The place of the field or variant `name` inside this place.\"\"\"\n        \
         return _At(self.event, (*self.path, name))\n\
         \n    \
         def refuse(self, reason: str) -> Err[MalformedEvent]:\n        \
         \"\"\"The refusal of the value at this place.\"\"\"\n        \
         return Err(MalformedEvent(event=self.event, path='.'.join(self.path), reason=reason))\n"
    )
}

/// The reader of a whole number in a range.
const READ_WHOLE_NUMBER: &str = "

def _read_whole_number(value: object, at: _At, low: int, high: int) -> Result[int, MalformedEvent]:
    \"\"\"A whole JSON number from `low` to `high`. A JSON `true` or `false` is no number.\"\"\"
    match value:
        case bool():
            return at.refuse('expected a whole JSON number')
        case int():
            pass
        case _:
            return at.refuse('expected a whole JSON number')
    if low <= value <= high:
        return Ok(value)
    return at.refuse(f'the number is outside {low}..={high}')
";

/// The reader of a bounded text.
const READ_TEXT: &str = "

def _read_text(value: object, at: _At, max_chars: int) -> Result[str, MalformedEvent]:
    \"\"\"A JSON string of at most `max_chars` characters.\"\"\"
    match value:
        case str():
            pass
        case _:
            return at.refuse('expected a JSON string')
    if len(value) <= max_chars:
        return Ok(value)
    return at.refuse(f'the text has {len(value)} characters; the bound is {max_chars}')
";

/// The reader of a UUID. `uuid.UUID` raises `ValueError` for a text that
/// is no UUID; the reader turns it into a refusal.
const READ_UUID: &str = "

def _read_uuid(value: object, at: _At) -> Result[uuid.UUID, MalformedEvent]:
    \"\"\"A UUID in its hyphenated lower-case text.\"\"\"
    match value:
        case str():
            pass
        case _:
            return at.refuse('expected a JSON string')
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return at.refuse('the text is not a UUID in hyphenated lower-case form')
    if str(parsed) == value:
        return Ok(parsed)
    return at.refuse('the text is not a UUID in hyphenated lower-case form')
";

/// The reader of a duration.
const READ_DURATION: &str = "

_U64_MAX: Final = 2**64 - 1
\"\"\"The largest duration of the wire, in nanoseconds.\"\"\"


def _read_duration(value: object, at: _At) -> Result[timedelta, MalformedEvent]:
    \"\"\"A duration in whole nanoseconds, to the microsecond.\"\"\"
    match _read_whole_number(value, at, 0, _U64_MAX):
        case Ok(nanos):
            return Ok(timedelta(microseconds=nanos // 1000))
        case Err() as refused:
            return refused
";

/// The reader of a time.
const READ_TIME: &str = "

_I64_MIN: Final = -(2**63)
\"\"\"The earliest time of the wire, in nanoseconds since the Unix epoch.\"\"\"

_I64_MAX: Final = 2**63 - 1
\"\"\"The latest time of the wire, in nanoseconds since the Unix epoch.\"\"\"

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
\"\"\"The Unix epoch in UTC.\"\"\"


def _read_time(value: object, at: _At) -> Result[datetime, MalformedEvent]:
    \"\"\"A time in nanoseconds since the Unix epoch, in UTC, to the microsecond.\"\"\"
    match _read_whole_number(value, at, _I64_MIN, _I64_MAX):
        case Ok(nanos):
            return Ok(_EPOCH + timedelta(microseconds=nanos // 1000))
        case Err() as refused:
            return refused
";

/// The reader of the tag and the items of a value of a variant type.
const READ_VARIANT: &str = "

def _read_variant(value: object, at: _At, type_name: str) -> Result[tuple[str, list[object]], MalformedEvent]:
    \"\"\"The tag and the field items of a value of a variant type.\"\"\"
    match value:
        case list():
            items = cast('list[object]', value)
        case _:
            return at.refuse('expected a JSON array')
    match items:
        case [str() as tag, *fields]:
            return Ok((tag, fields))
        case _:
            return at.refuse(f'a value of {type_name} is a JSON array whose first item is its variant name')
";

/// The place class and the readers of the forms that the contract uses.
fn render_helpers(usage: &Usage) -> String {
    let mut text = render_place_class();
    let parts = [
        (usage.whole_number, READ_WHOLE_NUMBER),
        (usage.text, READ_TEXT),
        (usage.uuid, READ_UUID),
        (usage.duration, READ_DURATION),
        (usage.time, READ_TIME),
        (usage.variants, READ_VARIANT),
    ];
    for (used, part) in parts {
        if used {
            text.push_str(part);
        }
    }
    text
}

/// The name of the reader of a type: `_read_` and the snake case of its
/// Python name.
fn reader_name(alias_name: &str) -> String {
    format!("_read_{}", snake_case(alias_name))
}

/// The members of a code union by their wire value, and its reader.
fn render_code_union_reader(union: &CodeUnion) -> String {
    let name = union.alias_name;
    let constant = format!("_{}_MEMBERS", snake_case(name).to_uppercase());
    let classes: Vec<&str> = union
        .catalogs
        .iter()
        .map(|catalog| catalog.type_name)
        .collect();
    format!(
        "\n\n{constant}: Final[dict[str, {name}]] = {{\n    \
         member.value: member for catalog in ({classes}) for member in catalog\n\
         }}\n\
         \"\"\"The members of `{name}` by their wire value.\"\"\"\n\
         \n\n\
         def {reader}(value: object, at: _At) -> Result[{name}, MalformedEvent]:\n    \
         \"\"\"A member of `{name}`, by its wire value.\"\"\"\n    \
         match value:\n        \
         case str():\n            \
         pass\n        \
         case _:\n            \
         return at.refuse('expected a JSON string')\n    \
         if value in {constant}:\n        \
         return Ok({constant}[value])\n    \
         return at.refuse(f'{name} has no value {{value[:_MAX_REFUSED_NAME_CHARS]!r}}')\n",
        classes = classes.join(", "),
        reader = reader_name(name),
    )
}

/// The reader of a closed type: one case per value of a choice; the tag
/// dispatch of a variant type, then the reader of each variant.
fn render_value_reader(named: &NamedType) -> String {
    let name = named.name;
    let reader = reader_name(name);
    match named.shape {
        NamedShape::Choice { values } => {
            let mut text = format!(
                "\n\ndef {reader}(value: object, at: _At) -> Result[{name}, MalformedEvent]:\n    \
                 \"\"\"A value of `{name}`.\"\"\"\n    \
                 match value:\n"
            );
            for value in values {
                let literal = string_literal(value.value);
                text.push_str(&format!(
                    "        case {literal}:\n            return Ok({literal})\n"
                ));
            }
            text.push_str(&format!(
                "        case str():\n            \
                 return at.refuse(f'{name} has no value {{value[:_MAX_REFUSED_NAME_CHARS]!r}}')\n        \
                 case _:\n            \
                 return at.refuse('expected a JSON string')\n"
            ));
            text
        }
        NamedShape::Variants { variants } => {
            let mut text = format!(
                "\n\ndef {reader}(value: object, at: _At) -> Result[{name}, MalformedEvent]:\n    \
                 \"\"\"A value of `{name}`: `[variant tag, field, ...]`.\"\"\"\n    \
                 match _read_variant(value, at, {type_name}):\n        \
                 case Ok((tag, fields)):\n            \
                 pass\n        \
                 case Err() as refused:\n            \
                 return refused\n    \
                 match tag:\n",
                type_name = string_literal(name),
            );
            for variant in variants {
                let tag = string_literal(variant.tag);
                text.push_str(&format!(
                    "        case {tag}:\n            return {}(fields, at.inside({tag}))\n",
                    reader_name(variant.type_name)
                ));
            }
            text.push_str(&format!(
                "        case _:\n            \
                 return at.refuse(f'{name} has no value {{tag[:_MAX_REFUSED_NAME_CHARS]!r}}')\n"
            ));
            for variant in variants {
                text.push_str(&render_record_reader(
                    variant.type_name,
                    variant.fields,
                    RecordRole::Variant,
                ));
            }
            text
        }
    }
}

/// The call that reads the wire item `wire` of a field of `form` at the
/// place `place`.
fn reader_call(form: ValueForm, wire: &str, place: &str) -> String {
    match form {
        ValueForm::Uuid => format!("_read_uuid({wire}, {place})"),
        ValueForm::Text { max_chars } => format!("_read_text({wire}, {place}, {max_chars})"),
        ValueForm::WholeNumber { min, max } => {
            format!("_read_whole_number({wire}, {place}, {min}, {max})")
        }
        ValueForm::Duration => format!("_read_duration({wire}, {place})"),
        ValueForm::Time => format!("_read_time({wire}, {place})"),
        ValueForm::Codes(union) => format!("{}({wire}, {place})", reader_name(union.alias_name)),
        ValueForm::Named(name) => format!("{}({wire}, {place})", reader_name(name)),
    }
}

/// The reader of an event or a variant from the items of its fields: the
/// item count, then each field, then the class. The wire item of a field
/// `x` is `wire_x`; its value is `value_x`.
fn render_record_reader(class: &str, fields: &[EventField], role: RecordRole) -> String {
    let what = match role {
        RecordRole::Event => "An event",
        RecordRole::Variant => "A variant",
    };
    let mut text = format!(
        "\n\ndef {}(fields: list[object], here: _At) -> Result[{class}, MalformedEvent]:\n    \
         \"\"\"{what} `{class}` from the items of its fields.\"\"\"\n    \
         match fields:\n",
        reader_name(class)
    );
    match fields.is_empty() {
        true => text.push_str("        case []:\n            pass\n"),
        false => {
            text.push_str("        case [\n");
            for field in fields {
                text.push_str(&format!("            wire_{},\n", field.name));
            }
            text.push_str("        ]:\n            pass\n");
        }
    }
    text.push_str(&format!(
        "        case _:\n            \
         return here.refuse(f'expected {} items, found {{len(fields)}}')\n",
        fields.len()
    ));
    for field in fields {
        let call = reader_call(
            field.form,
            &format!("wire_{}", field.name),
            &format!("here.inside({})", string_literal(field.name)),
        );
        text.push_str(&format!(
            "    match {call}:\n        \
             case Ok(value_{name}):\n            \
             pass\n        \
             case Err() as refused:\n            \
             return refused\n",
            name = field.name
        ));
    }
    match fields.is_empty() {
        true => text.push_str(&format!("    return Ok({class}())\n")),
        false => {
            text.push_str(&format!("    return Ok(\n        {class}(\n"));
            for field in fields {
                text.push_str(&format!(
                    "            {name}=value_{name},\n",
                    name = field.name
                ));
            }
            text.push_str("        )\n    )\n");
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use horsies_core::catalog::{
        CallForms, CatalogMember, CodeCatalog, EnumValues, TerminalStates,
    };
    use horsies_core::events::{ChoiceValue, EventType, VariantType, EVENT_CONTRACT};

    use super::*;

    const CODES: CodeCatalog = CodeCatalog {
        module: "task_codes",
        type_name: "OutcomeCode",
        values: EnumValues::EqualText,
        doc: "",
        members: &[CatalogMember {
            name: "TASK_TIMEOUT",
            value: "TASK_TIMEOUT",
            doc: "",
            forms: CallForms::AnyForm,
        }],
        terminal_states: TerminalStates::NotALifecycle,
    };

    const UNION: CodeUnion = CodeUnion {
        alias_name: "OutcomeCodes",
        doc: "An outcome code.",
        catalogs: &[CODES],
    };

    const AT_FIELD: EventField = EventField {
        name: "at",
        doc: "When.",
        form: ValueForm::Time,
        access: FieldAccess::Public,
    };

    const TRACE_FIELD: EventField = EventField {
        name: "trace",
        doc: "",
        form: ValueForm::Text { max_chars: 8 },
        access: FieldAccess::Internal,
    };

    const FIELDS: &[EventField] = &[AT_FIELD, TRACE_FIELD];

    const MODE: NamedType = NamedType {
        name: "Mode",
        doc: "The mode.",
        shape: NamedShape::Choice {
            values: &[
                ChoiceValue {
                    value: "ONE",
                    doc: "The first\nmode.",
                },
                ChoiceValue {
                    value: "TWO",
                    doc: "",
                },
            ],
        },
    };

    const START: NamedType = NamedType {
        name: "Start",
        doc: "When it started.",
        shape: NamedShape::Variants {
            variants: &[
                VariantType {
                    tag: "Reported",
                    type_name: "StartReported",
                    doc: "",
                    fields: &[AT_FIELD],
                },
                VariantType {
                    tag: "NotReported",
                    type_name: "StartNotReported",
                    doc: "No time.",
                    fields: &[],
                },
            ],
        },
    };

    const SMALL_EVENTS: &[EventType] = &[EventType {
        name: "Probe",
        doc: "",
        fields: &[EventField {
            name: "code",
            doc: "",
            form: ValueForm::Codes(&UNION),
            access: FieldAccess::Public,
        }],
    }];

    const SMALL: EventContract = EventContract {
        version: 7,
        events: SMALL_EVENTS,
        value_types: &[],
    };

    /// The keywords of Python 3.13 that cannot be an attribute name.
    const PYTHON_KEYWORDS: [&str; 35] = [
        "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
        "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
        "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return",
        "try", "while", "with", "yield",
    ];

    #[test]
    fn a_record_class_lists_its_fields_and_hides_an_internal_one_from_repr() {
        assert_eq!(
            render_record_class("Probe", "A probe.", FIELDS),
            "@dataclass(frozen=True, slots=True, kw_only=True)\nclass Probe:\n    \
             \"\"\"A probe.\"\"\"\n\n    at: datetime\n    \"\"\"When.\"\"\"\n\n    \
             trace: str = field(repr=False)\n"
        );
        assert_eq!(
            render_record_class("Empty", "", &[]),
            "@dataclass(frozen=True, slots=True, kw_only=True)\nclass Empty:\n    pass\n"
        );
    }

    #[test]
    fn a_choice_is_a_literal_alias_that_lists_its_value_docs() {
        assert_eq!(
            render_value_type(&MODE),
            "\n\ntype Mode = Literal['ONE', 'TWO']\n\"\"\"The mode.\n\n`ONE`: The first mode.\n\"\"\"\n"
        );
        let reader = render_value_reader(&MODE);
        assert!(reader.contains("\n        case 'ONE':\n            return Ok('ONE')\n"));
        assert!(reader.contains(
            "\n        case str():\n            return at.refuse(f'Mode has no value {value[:_MAX_REFUSED_NAME_CHARS]!r}')\n"
        ));
    }

    #[test]
    fn a_variant_type_is_one_class_per_variant_then_its_union_and_its_readers() {
        let text = render_value_type(&START);
        let reported = text
            .find("class StartReported:")
            .expect("a class per variant");
        let not_reported = text
            .find("class StartNotReported:")
            .expect("a class per variant");
        let union = text
            .find("type Start = (\n    StartReported\n    | StartNotReported\n)\n\"\"\"When it started.\"\"\"\n")
            .expect("the union follows the classes");
        assert!(reported < not_reported && not_reported < union);
        let reader = render_value_reader(&START);
        assert!(reader.contains("match _read_variant(value, at, 'Start'):\n"));
        assert!(reader.contains(
            "        case 'NotReported':\n            return _read_start_not_reported(fields, at.inside('NotReported'))\n"
        ));
        assert!(reader.contains("\ndef _read_start_reported(fields: list[object], here: _At) -> Result[StartReported, MalformedEvent]:\n"));
    }

    #[test]
    fn a_record_reader_checks_the_item_count_then_reads_each_field_in_order() {
        let text = render_record_reader("Probe", FIELDS, RecordRole::Event);
        assert_eq!(
            text,
            "\n\ndef _read_probe(fields: list[object], here: _At) -> Result[Probe, MalformedEvent]:\n    \
             \"\"\"An event `Probe` from the items of its fields.\"\"\"\n    \
             match fields:\n        \
             case [\n            \
             wire_at,\n            \
             wire_trace,\n        \
             ]:\n            \
             pass\n        \
             case _:\n            \
             return here.refuse(f'expected 2 items, found {len(fields)}')\n    \
             match _read_time(wire_at, here.inside('at')):\n        \
             case Ok(value_at):\n            \
             pass\n        \
             case Err() as refused:\n            \
             return refused\n    \
             match _read_text(wire_trace, here.inside('trace'), 8):\n        \
             case Ok(value_trace):\n            \
             pass\n        \
             case Err() as refused:\n            \
             return refused\n    \
             return Ok(\n        Probe(\n            at=value_at,\n            trace=value_trace,\n        )\n    )\n"
        );
        let unit = render_record_reader("StartNotReported", &[], RecordRole::Variant);
        assert!(unit.contains("        case []:\n            pass\n"));
        assert!(unit.ends_with("    return Ok(StartNotReported())\n"));
    }

    #[test]
    fn the_imports_and_the_helpers_follow_the_forms_that_the_contract_uses() {
        let text = render_events_module(&SMALL);
        assert!(text.contains(
            "from __future__ import annotations\n\nfrom dataclasses import dataclass\nfrom typing import Final, cast\n\nfrom horsies._catalog.task_codes import OutcomeCode\nfrom horsies.core.types.result import Err, Ok, Result\n"
        ));
        assert!(text.contains("\nEVENT_CONTRACT_VERSION: Final = 7\n"));
        assert!(text.contains("\ntype OutcomeCodes = OutcomeCode\n\"\"\"An outcome code.\"\"\"\n"));
        assert!(text.contains("\n_OUTCOME_CODES_MEMBERS: Final[dict[str, OutcomeCodes]] = {\n"));
        for unused in [
            "_read_uuid",
            "_read_text",
            "_read_time",
            "_read_duration",
            "_read_variant",
            "_read_whole_number",
        ] {
            assert!(!text.contains(unused), "{unused}");
        }
        let full = render_events_module(&EVENT_CONTRACT);
        assert!(full.contains(
            "import uuid\nfrom dataclasses import dataclass, field\nfrom datetime import datetime, timedelta, timezone\nfrom typing import Final, Literal, cast\n\nfrom horsies._catalog.task_codes import ContractCode, OperationalCode, OutcomeCode, RetrievalCode\nfrom horsies.core.types.result import Err, Ok, Result\n"
        ));
    }

    #[test]
    fn the_events_module_defines_every_type_and_reader_of_the_contract() {
        let text = render_events_module(&EVENT_CONTRACT);
        assert!(text.starts_with(GENERATED_HEADER));
        assert!(text.contains(&format!(
            "\nEVENT_CONTRACT_VERSION: Final = {}\n",
            EVENT_CONTRACT.version
        )));
        for event in EVENT_CONTRACT.events {
            assert!(
                text.contains(&format!("\nclass {}:\n", event.name)),
                "{}",
                event.name
            );
            assert!(
                text.contains(&format!("    '{}',\n", event.name)),
                "{}",
                event.name
            );
            assert!(
                text.contains(&format!(
                    "        case '{name}':\n            return {reader}(fields, _At('{name}', ()))\n",
                    name = event.name,
                    reader = reader_name(event.name)
                )),
                "{}",
                event.name
            );
        }
        for named in EVENT_CONTRACT.value_types {
            assert!(
                text.contains(&format!("\ntype {} = ", named.name)),
                "{}",
                named.name
            );
            assert!(
                text.contains(&format!(
                    "\ndef {}(value: object, at: _At)",
                    reader_name(named.name)
                )),
                "{}",
                named.name
            );
            if let NamedShape::Variants { variants } = named.shape {
                for variant in variants {
                    assert!(text.contains(&format!("\nclass {}:\n", variant.type_name)));
                    assert!(text.contains(&format!(
                        "\ndef {}(fields: list[object], here: _At)",
                        reader_name(variant.type_name)
                    )));
                }
            }
        }
        for name in FIXED_EXPORTS {
            assert!(text.contains(&format!("    '{name}',\n")), "{name}");
        }
        assert!(text.contains(
            "\ntype EventDecodeError = NotAnEvent | UnknownEventType | MalformedEvent\n"
        ));
        assert!(text
            .contains("\ndef decode_event(wire: object) -> Result[AnyEvent, EventDecodeError]:\n"));
        assert!(text.contains("    trace: RunTrace = field(repr=False)\n"));
        for line in text.lines().filter(|line| line.starts_with("from horsies")) {
            assert!(
                line.starts_with("from horsies._catalog.")
                    || line == "from horsies.core.types.result import Err, Ok, Result",
                "{line}"
            );
        }
        assert!(
            !text.lines().any(|line| line == "import json"),
            "the module parses no JSON text"
        );
    }

    #[test]
    fn reader_names_are_unique_and_field_names_are_no_python_keywords() {
        let text = render_events_module(&EVENT_CONTRACT);
        let readers: Vec<&str> = text
            .lines()
            .filter_map(|line| line.strip_prefix("def _read_"))
            .filter_map(|rest| rest.split('(').next())
            .collect();
        let unique: BTreeSet<&str> = readers.iter().copied().collect();
        assert_eq!(unique.len(), readers.len());
        for field in contract_fields(&EVENT_CONTRACT) {
            assert!(!PYTHON_KEYWORDS.contains(&field.name), "{}", field.name);
        }
        let mut exports: Vec<&str> = FIXED_EXPORTS.to_vec();
        exports.extend(EVENT_CONTRACT.events.iter().map(|event| event.name));
        exports.extend(EVENT_CONTRACT.value_types.iter().map(|named| named.name));
        let unique_exports: BTreeSet<&str> = exports.iter().copied().collect();
        assert_eq!(unique_exports.len(), exports.len());
    }
}
