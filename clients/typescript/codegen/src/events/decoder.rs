//! The decoder of `events.ts`: the refusal types, `decodeEvent`, and one
//! reader per form, code union, closed type, variant and event type.
//!
//! `decodeEvent` reads the wire items that the bridge builds from the core's
//! `WireValue` (`wire_json.ts`): every integer is a `bigint`, and a `number`
//! is a float. It refuses what the core's decoder refuses, in the same order
//! and with the same facts: the item count of a record first, then each
//! field in wire order; the path to the refused item in the names of the
//! wire (field names and variant tags), and the cause. A name from the wire
//! in a refusal is cut to `MAX_REFUSED_NAME_CHARS` Unicode scalar values, as
//! the core cuts it. An internal field is read and checked, and the record
//! does not keep it. Every record and refusal is frozen.
//!
//! The module defines only the helpers that the contract's forms use: the
//! lint of the package refuses an unused function.

use horsies_core::events::{
    CodeUnion, EventContract, EventField, EventType, FieldAccess, NamedShape, NamedType, ValueForm,
    VariantType, MAX_REFUSED_NAME_CHARS,
};

use super::types::{
    member_name, public_fields, variant_kind, whole_number_type, WholeNumberType, ANY_EVENT,
    KIND_MEMBER,
};
use super::{code_union_type, code_unions, contract_fields};
use crate::model::event_variant_type_name;
use crate::ts_text::{bigint_literal, integer_literal, pascal_case_of_snake, string_literal};

/// The widest function head on one line. A longer head has one parameter
/// per line.
const LINE_WIDTH: usize = 100;

/// The kind of JSON value that an item needs, one interface per cause of
/// the core's `DecodeCause`, and their union.
const CAUSE_SECTIONS: [&str; 9] = [
    r#"/** The kind of JSON value that a wire item needs. */
export type JsonKind =
  | "text"
  | "wholeNumber"
  | "array";"#,
    r#"export interface WrongJsonKind {
  readonly kind: "wrongJsonKind";
  readonly expected: JsonKind;
}"#,
    r#"export interface ItemCount {
  readonly kind: "itemCount";
  readonly expected: number;
  readonly found: number;
}"#,
    r#"export interface OutOfRange {
  readonly kind: "outOfRange";
  readonly min: bigint;
  readonly max: bigint;
}"#,
    r#"export interface TextOverBound {
  readonly kind: "textOverBound";
  readonly chars: number;
  readonly bound: number;
}"#,
    r#"export interface NotCanonicalUuid {
  readonly kind: "notCanonicalUuid";
}"#,
    r#"export interface UnknownName {
  readonly kind: "unknownName";
  readonly typeName: string;
  readonly found: string;
}"#,
    r#"export interface NoVariantTag {
  readonly kind: "noVariantTag";
  readonly typeName: string;
}"#,
    r#"/** Why one wire item of a known event type does not decode. */
export type EventDecodeCause =
  | WrongJsonKind
  | ItemCount
  | OutOfRange
  | TextOverBound
  | NotCanonicalUuid
  | UnknownName
  | NoVariantTag;"#,
];

const NOT_AN_EVENT_TYPE: &str = r#"/** The items are empty, or the first item is not a string. */
export interface NotAnEvent {
  readonly kind: "notAnEvent";
}"#;

const UNKNOWN_EVENT_TYPE: &str = r#"/** The first item names no event type of this contract version. */
export interface UnknownEventType {
  readonly kind: "unknownEventType";
  readonly tag: string;
}"#;

const EVENT_DECODE_ERROR: &str = r#"/** Why the wire items of one event are not an event of this contract version. */
export type EventDecodeError =
  | NotAnEvent
  | UnknownEventType
  | MalformedEvent;"#;

const VALUE_REFUSAL: &str = r#"interface ValueRefusal {
  readonly path: ReadonlyArray<string>;
  readonly cause: EventDecodeCause;
}"#;

const NOT_AN_EVENT_VALUE: &str =
    r#"const NOT_AN_EVENT: NotAnEvent = Object.freeze({ kind: "notAnEvent" });"#;

const ACCEPT: &str = r#"function accept<T>(value: T): DecodeResult<T, never> {
  return { ok: true, value };
}"#;

const REFUSE: &str = r#"function refuse(cause: EventDecodeCause): DecodeResult<never, ValueRefusal> {
  const refusal: ValueRefusal = Object.freeze({
    path: Object.freeze([]),
    cause: Object.freeze(cause),
  });

  return { ok: false, error: refusal };
}"#;

const CUT_NAME: &str = r#"function cutName(name: string): string {
  let cut = "";
  let chars = 0;

  for (const character of name) {
    if (chars === MAX_REFUSED_NAME_CHARS) {
      break;
    }

    cut += character;
    chars += 1;
  }

  return cut;
}"#;

const REFUSE_INSIDE: &str = r#"function refuseInside(name: string, inner: ValueRefusal): DecodeResult<never, ValueRefusal> {
  const refusal: ValueRefusal = Object.freeze({
    path: Object.freeze([name, ...inner.path]),
    cause: inner.cause,
  });

  return { ok: false, error: refusal };
}"#;

const REFUSE_UNKNOWN_NAME: &str = r#"function refuseUnknownName(typeName: string, found: string): DecodeResult<never, ValueRefusal> {
  return refuse({ kind: "unknownName", typeName, found: cutName(found) });
}"#;

const ITEM_AT: &str = r#"function itemAt(items: ReadonlyArray<WireJson>, index: number): WireJson {
  const item = items[index];

  if (item === undefined) {
    throw new Error(`decodeEvent: no item at index ${index} after the item count check`);
  }

  return item;
}"#;

const READ_WHOLE_NUMBER: &str = r#"function readWholeNumber(
  value: WireJson,
  min: bigint,
  max: bigint,
): DecodeResult<bigint, ValueRefusal> {
  if (typeof value !== "bigint") {
    return refuse({ kind: "wrongJsonKind", expected: "wholeNumber" });
  }

  if (value < min || value > max) {
    return refuse({ kind: "outOfRange", min, max });
  }

  return accept(value);
}"#;

const READ_SAFE_WHOLE_NUMBER: &str = r#"function readSafeWholeNumber(
  value: WireJson,
  min: bigint,
  max: bigint,
): DecodeResult<number, ValueRefusal> {
  const whole = readWholeNumber(value, min, max);

  if (!whole.ok) {
    return whole;
  }

  return accept(Number(whole.value));
}"#;

const COUNT_CHARS: &str = r#"function countChars(text: string): number {
  let chars = 0;

  for (const _character of text) {
    chars += 1;
  }

  return chars;
}"#;

const READ_TEXT: &str = r#"function readText(value: WireJson, maxChars: number): DecodeResult<string, ValueRefusal> {
  if (typeof value !== "string") {
    return refuse({ kind: "wrongJsonKind", expected: "text" });
  }

  if (value.length <= maxChars) {
    return accept(value);
  }

  const chars = countChars(value);

  if (chars > maxChars) {
    return refuse({ kind: "textOverBound", chars, bound: maxChars });
  }

  return accept(value);
}"#;

const CANONICAL_UUID: &str =
    r"const CANONICAL_UUID = /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/u;";

const READ_UUID: &str = r#"function readUuid(value: WireJson): DecodeResult<string, ValueRefusal> {
  if (typeof value !== "string") {
    return refuse({ kind: "wrongJsonKind", expected: "text" });
  }

  if (!CANONICAL_UUID.test(value)) {
    return refuse({ kind: "notCanonicalUuid" });
  }

  return accept(value);
}"#;

const NANOSECOND_UNITS: [&str; 3] = [
    "const NANOSECONDS_PER_SECOND = 1_000_000_000n;",
    "const NANOSECONDS_PER_MILLISECOND = 1_000_000n;",
    "const NANOSECONDS_PER_MICROSECOND = 1000n;",
];

const READ_DURATION: &str = r#"function readDuration(value: WireJson): DecodeResult<Temporal.Duration, ValueRefusal> {
  const nanoseconds = readWholeNumber(value, 0n, DURATION_NANOSECONDS_MAX);

  if (!nanoseconds.ok) {
    return nanoseconds;
  }

  const subsecond = nanoseconds.value % NANOSECONDS_PER_SECOND;

  const duration = Temporal.Duration.from({
    seconds: Number(nanoseconds.value / NANOSECONDS_PER_SECOND),
    milliseconds: Number(subsecond / NANOSECONDS_PER_MILLISECOND),
    microseconds: Number((subsecond % NANOSECONDS_PER_MILLISECOND) / NANOSECONDS_PER_MICROSECOND),
    nanoseconds: Number(subsecond % NANOSECONDS_PER_MICROSECOND),
  });

  return accept(duration);
}"#;

const READ_TIME: &str = r#"function readTime(value: WireJson): DecodeResult<Temporal.Instant, ValueRefusal> {
  const nanoseconds = readWholeNumber(value, TIME_NANOSECONDS_MIN, TIME_NANOSECONDS_MAX);

  if (!nanoseconds.ok) {
    return nanoseconds;
  }

  return accept(Temporal.Instant.fromEpochNanoseconds(nanoseconds.value));
}"#;

const READ_CHOICE: &str = r#"function readChoice<T extends string>(
  value: WireJson,
  typeName: string,
  values: ReadonlyArray<T>,
): DecodeResult<T, ValueRefusal> {
  if (typeof value !== "string") {
    return refuse({ kind: "wrongJsonKind", expected: "text" });
  }

  const member = values.find((candidate) => candidate === value);

  if (member === undefined) {
    return refuseUnknownName(typeName, value);
  }

  return accept(member);
}"#;

const VARIANT_HELPERS: [&str; 4] = [
    r#"interface VariantItems {
  readonly tag: string;
  readonly fields: ReadonlyArray<WireJson>;
}"#,
    r#"function isItems(value: WireJson): value is ReadonlyArray<WireJson> {
  return Array.isArray(value);
}"#,
    r#"function readVariant(value: WireJson, typeName: string): DecodeResult<VariantItems, ValueRefusal> {
  if (!isItems(value)) {
    return refuse({ kind: "wrongJsonKind", expected: "array" });
  }

  const [tag, ...fields] = value;

  if (typeof tag !== "string") {
    return refuse({ kind: "noVariantTag", typeName });
  }

  return accept({ tag, fields });
}"#,
    r#"function within<T>(
  name: string,
  decoded: DecodeResult<T, ValueRefusal>,
): DecodeResult<T, ValueRefusal> {
  if (decoded.ok) {
    return decoded;
  }

  return refuseInside(name, decoded.error);
}"#,
];

const DECODE_EVENT_DEFAULT: &str = r#"    default:
      return { ok: false, error: Object.freeze({ kind: "unknownEventType", tag: cutName(tag) }) };
  }
}"#;

/// The forms that a contract uses. Each selects helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FormUse {
    /// A record has a field: its reader reads items by index.
    any_field: bool,
    uuid: bool,
    text: bool,
    /// A whole number of any range, a duration or a time: each reads a
    /// `bigint`.
    whole_number: bool,
    /// A whole number that a `number` holds.
    safe_whole_number: bool,
    duration: bool,
    time: bool,
    /// A choice or a code union: each reads a name of a closed set.
    names: bool,
    variants: bool,
}

impl FormUse {
    /// The forms of the fields and closed types of `contract`. Pure.
    fn of(contract: &EventContract) -> Self {
        let mut usage = Self {
            any_field: false,
            uuid: false,
            text: false,
            whole_number: false,
            safe_whole_number: false,
            duration: false,
            time: false,
            names: false,
            variants: false,
        };
        for field in contract_fields(contract) {
            usage.any_field = true;
            match field.form {
                ValueForm::Uuid => usage.uuid = true,
                ValueForm::Text { .. } => usage.text = true,
                ValueForm::WholeNumber { min, max } => {
                    usage.whole_number = true;
                    match whole_number_type(min, max) {
                        WholeNumberType::Number => usage.safe_whole_number = true,
                        WholeNumberType::BigInt => {}
                    }
                }
                ValueForm::Duration => {
                    usage.duration = true;
                    usage.whole_number = true;
                }
                ValueForm::Time => {
                    usage.time = true;
                    usage.whole_number = true;
                }
                ValueForm::Codes(_) => usage.names = true,
                ValueForm::Named(_) => {}
            }
        }
        for named in contract.value_types {
            match named.shape {
                NamedShape::Choice { .. } => usage.names = true,
                NamedShape::Variants { .. } => usage.variants = true,
            }
        }
        usage
    }
}

/// Who can import a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Visibility {
    /// `export function`.
    Exported,
    /// `function`: only the module calls it.
    Module,
}

/// The sections of the decoder, after the types: the causes and the
/// refusals, the helpers, the reader of each code union, closed type and
/// event type, then `decodeEvent`. Pure; panics on a field name that is no
/// member name or a code union that `task_code.ts` does not render.
pub fn decoder_sections(contract: &EventContract) -> Vec<String> {
    let mut sections: Vec<String> = CAUSE_SECTIONS.map(str::to_owned).into();
    sections.extend(refusal_sections());
    sections.extend(helper_sections(FormUse::of(contract)));
    for union in code_unions(contract) {
        sections.extend(code_union_reader_sections(union));
    }
    for named in contract.value_types {
        sections.extend(named_reader_sections(named));
    }
    for event in contract.events {
        sections.push(record_reader(event.name, event.name, event.fields));
    }
    sections.push(decode_event_section(contract.events));
    sections
}

/// `NotAnEvent`, `UnknownEventType`, `MalformedEvent` and their union. Pure.
fn refusal_sections() -> Vec<String> {
    vec![
        NOT_AN_EVENT_TYPE.to_owned(),
        UNKNOWN_EVENT_TYPE.to_owned(),
        format!(
            r#"/** The items after the tag do not match the fields of the event type. */
export interface MalformedEvent {{
  readonly kind: "malformedEvent";
  readonly event: {ANY_EVENT}["kind"];
  /** The wire names of the fields and variants from the event down to the refused item. */
  readonly path: ReadonlyArray<string>;
  readonly cause: EventDecodeCause;
}}"#
        ),
        EVENT_DECODE_ERROR.to_owned(),
    ]
}

/// The helpers that every module has, then those of the forms in `usage`,
/// then `eventResult`. Pure.
///
/// Every part is built, and only the parts of the used forms are kept.
fn helper_sections(usage: FormUse) -> Vec<String> {
    let mut sections: Vec<String> = [
        VALUE_REFUSAL.to_owned(),
        format!(
            "const MAX_REFUSED_NAME_CHARS = {};",
            count_literal(MAX_REFUSED_NAME_CHARS)
        ),
        NOT_AN_EVENT_VALUE.to_owned(),
        ACCEPT.to_owned(),
        REFUSE.to_owned(),
        CUT_NAME.to_owned(),
    ]
    .into();
    let parts: [(bool, Vec<String>); 11] = [
        (
            usage.any_field || usage.variants,
            vec![REFUSE_INSIDE.to_owned()],
        ),
        (
            usage.names || usage.variants,
            vec![REFUSE_UNKNOWN_NAME.to_owned()],
        ),
        (usage.any_field, vec![ITEM_AT.to_owned()]),
        (usage.whole_number, vec![READ_WHOLE_NUMBER.to_owned()]),
        (
            usage.safe_whole_number,
            vec![READ_SAFE_WHOLE_NUMBER.to_owned()],
        ),
        (
            usage.text,
            vec![COUNT_CHARS.to_owned(), READ_TEXT.to_owned()],
        ),
        (
            usage.uuid,
            vec![CANONICAL_UUID.to_owned(), READ_UUID.to_owned()],
        ),
        (usage.duration, duration_sections()),
        (usage.time, time_sections()),
        (usage.names, vec![READ_CHOICE.to_owned()]),
        (
            usage.variants,
            Vec::from(VARIANT_HELPERS.map(str::to_owned)),
        ),
    ];
    for (used, part) in parts {
        if used {
            sections.extend(part);
        }
    }
    sections.push(event_result_section());
    sections
}

/// The bound of a duration, the units of a second, and `readDuration`. The
/// wire holds a duration as whole nanoseconds in a `u64`. Pure.
fn duration_sections() -> Vec<String> {
    let mut sections = vec![format!(
        "const DURATION_NANOSECONDS_MAX = {};",
        bigint_literal(i128::from(u64::MAX))
    )];
    sections.extend(NANOSECOND_UNITS.map(str::to_owned));
    sections.push(READ_DURATION.to_owned());
    sections
}

/// The bounds of a time and `readTime`. The wire holds a time as whole
/// nanoseconds since the Unix epoch in an `i64`. Pure.
fn time_sections() -> Vec<String> {
    vec![
        format!(
            "const TIME_NANOSECONDS_MIN = {};",
            bigint_literal(i128::from(i64::MIN))
        ),
        format!(
            "const TIME_NANOSECONDS_MAX = {};",
            bigint_literal(i128::from(i64::MAX))
        ),
        READ_TIME.to_owned(),
    ]
}

/// `eventResult`: the result of an event reader, with a refusal as a
/// `MalformedEvent` of the event type. Pure.
fn event_result_section() -> String {
    format!(
        r#"function eventResult(
  event: {ANY_EVENT}["kind"],
  decoded: DecodeResult<{ANY_EVENT}, ValueRefusal>,
): DecodeResult<{ANY_EVENT}, EventDecodeError> {{
  if (decoded.ok) {{
    return decoded;
  }}

  const refusal: MalformedEvent = Object.freeze({{
    kind: "malformedEvent",
    event,
    path: decoded.error.path,
    cause: decoded.error.cause,
  }});

  return {{ ok: false, error: refusal }};
}}"#
    )
}

/// The values of a code union, from the members of its catalogs, and its
/// reader. Pure.
fn code_union_reader_sections(union: &CodeUnion) -> Vec<String> {
    let type_name = code_union_type(union);
    let values: Vec<&'static str> = union
        .catalogs
        .iter()
        .flat_map(|catalog| catalog.members.iter())
        .map(|member| member.value)
        .collect();
    vec![
        values_constant(type_name, &values),
        choice_reader(type_name),
    ]
}

/// A choice: its values and its reader. A variant type: the reader of each
/// variant, then the reader that dispatches on the wire tag. Pure.
fn named_reader_sections(named: &NamedType) -> Vec<String> {
    match named.shape {
        NamedShape::Choice { values } => {
            let wire_values: Vec<&'static str> = values.iter().map(|value| value.value).collect();
            vec![
                values_constant(named.name, &wire_values),
                choice_reader(named.name),
            ]
        }
        NamedShape::Variants { variants } => {
            let mut sections: Vec<String> = variants
                .iter()
                .map(|variant| {
                    record_reader(
                        event_variant_type_name(variant),
                        &variant_kind(variant),
                        variant.fields,
                    )
                })
                .collect();
            sections.push(variants_reader(named.name, variants));
            sections
        }
    }
}

/// The wire values of a closed set of names, as a constant. Pure.
fn values_constant(type_name: &str, values: &[&str]) -> String {
    let head = format!(
        "const {}: ReadonlyArray<{type_name}> =",
        values_constant_name(type_name)
    );
    match values {
        [single] => format!("{head} [{}];", string_literal(single)),
        several => {
            let mut text = format!("{head} [\n");
            for value in several {
                text.push_str(&format!("  {},\n", string_literal(value)));
            }
            text.push_str("];");
            text
        }
    }
}

/// The reader of a choice or a code union. Pure.
fn choice_reader(type_name: &str) -> String {
    format!(
        "{}\n  return readChoice(value, {}, {});\n}}",
        function_head(
            Visibility::Module,
            &reader_name(type_name),
            &["value: WireJson"],
            &format!("DecodeResult<{type_name}, ValueRefusal>"),
        ),
        string_literal(type_name),
        values_constant_name(type_name),
    )
}

/// The reader of a variant type: the variant items, then one case per
/// wire tag. Pure.
fn variants_reader(type_name: &str, variants: &[VariantType]) -> String {
    let mut text = format!(
        "{}\n  const variant = readVariant(value, {});\n\n  if (!variant.ok) {{\n    \
         return variant;\n  }}\n\n  const {{ tag, fields }} = variant.value;\n\n  switch (tag) {{\n",
        function_head(
            Visibility::Module,
            &reader_name(type_name),
            &["value: WireJson"],
            &format!("DecodeResult<{type_name}, ValueRefusal>"),
        ),
        string_literal(type_name),
    );
    for variant in variants {
        let tag = string_literal(variant.tag);
        text.push_str(&format!(
            "    case {tag}:\n      return within({tag}, {}(fields));\n",
            reader_name(event_variant_type_name(variant))
        ));
    }
    text.push_str(&format!(
        "    default:\n      return refuseUnknownName({}, tag);\n  }}\n}}",
        string_literal(type_name)
    ));
    text
}

/// The reader of an event type or a variant: the item count, then each
/// field in wire order, then the frozen record of the public fields. Pure.
fn record_reader(type_name: &str, kind: &str, fields: &[EventField]) -> String {
    let mut text = function_head(
        Visibility::Module,
        &reader_name(type_name),
        &["fields: ReadonlyArray<WireJson>"],
        &format!("DecodeResult<{type_name}, ValueRefusal>"),
    );
    let count = count_literal(fields.len());
    text.push_str(&format!(
        "\n  if (fields.length !== {count}) {{\n    return refuse({{ kind: \"itemCount\", \
         expected: {count}, found: fields.length }});\n  }}\n"
    ));
    for (index, field) in fields.iter().enumerate() {
        text.push_str(&field_read(field, index));
    }
    text.push_str(&record_return(kind, fields));
    text
}

/// The read of the item at `index` of a field, and the refusal inside the
/// field's name. Pure.
fn field_read(field: &EventField, index: usize) -> String {
    let local = local_name(field);
    format!(
        "\n  const {local} = {};\n\n  if (!{local}.ok) {{\n    return refuseInside({}, {local}.error);\n  }}\n",
        reader_call(field.form, index),
        string_literal(field.name),
    )
}

/// The end of a record reader: the frozen record of `kind` and the public
/// fields. Pure.
fn record_return(kind: &str, fields: &[EventField]) -> String {
    let kind = string_literal(kind);
    match public_fields(fields).as_slice() {
        [] => format!("\n  return accept(Object.freeze({{ {KIND_MEMBER}: {kind} }}));\n}}"),
        members => {
            let mut text =
                format!("\n  return accept(\n    Object.freeze({{\n      {KIND_MEMBER}: {kind},\n");
            for field in members {
                text.push_str(&format!(
                    "      {}: {}.value,\n",
                    member_name(field),
                    local_name(field)
                ));
            }
            text.push_str("    }),\n  );\n}");
            text
        }
    }
}

/// The local name of the read of a field: `decoded` and the field name for
/// a public field, `checked` and the field name for an internal field. The
/// prefix keeps the name apart from the parameter `fields`, from the
/// helpers and from the reserved words. Pure.
fn local_name(field: &EventField) -> String {
    let prefix = match field.access {
        FieldAccess::Public => "decoded",
        FieldAccess::Internal => "checked",
    };
    format!("{prefix}{}", pascal_case_of_snake(field.name))
}

/// The call that reads the item at `index` of a field of `form`. Pure;
/// panics on a code union that `task_code.ts` does not render.
fn reader_call(form: ValueForm, index: usize) -> String {
    let item = format!("itemAt(fields, {})", count_literal(index));
    match form {
        ValueForm::Uuid => format!("readUuid({item})"),
        ValueForm::Text { max_chars } => format!("readText({item}, {})", count_literal(max_chars)),
        ValueForm::WholeNumber { min, max } => {
            let reader = match whole_number_type(min, max) {
                WholeNumberType::Number => "readSafeWholeNumber",
                WholeNumberType::BigInt => "readWholeNumber",
            };
            format!(
                "{reader}({item}, {}, {})",
                bigint_literal(i128::from(min)),
                bigint_literal(i128::from(max))
            )
        }
        ValueForm::Duration => format!("readDuration({item})"),
        ValueForm::Time => format!("readTime({item})"),
        ValueForm::Codes(union) => format!("{}({item})", reader_name(code_union_type(union))),
        ValueForm::Named(name) => format!("{}({item})", reader_name(name)),
    }
}

/// `decodeEvent`: the tag, then one case per event type. Pure.
fn decode_event_section(events: &[EventType]) -> String {
    let head = function_head(
        Visibility::Exported,
        "decodeEvent",
        &["items: ReadonlyArray<WireJson>"],
        &format!("DecodeResult<{ANY_EVENT}, EventDecodeError>"),
    );
    let mut text = format!(
        "/**\n * The event of the wire items of one event, `[type name, field, ...]`, as the\n \
         * bridge gives them: every integer is a `bigint`. A refusal quotes a name from\n \
         * the wire cut to {MAX_REFUSED_NAME_CHARS} characters.\n */\n{head}\n  \
         const [tag, ...fields] = items;\n\n  if (typeof tag !== \"string\") {{\n    \
         return {{ ok: false, error: NOT_AN_EVENT }};\n  }}\n\n  switch (tag) {{\n"
    );
    for event in events {
        let tag = string_literal(event.name);
        text.push_str(&format!(
            "    case {tag}:\n      return eventResult({tag}, {}(fields));\n",
            reader_name(event.name)
        ));
    }
    text.push_str(DECODE_EVENT_DEFAULT);
    text
}

/// The head of a function, `function name(parameters): ReturnType {`: on
/// one line when it has at most [`LINE_WIDTH`] characters, else with one
/// parameter per line. Pure.
fn function_head(
    visibility: Visibility,
    name: &str,
    parameters: &[&str],
    return_type: &str,
) -> String {
    let keyword = match visibility {
        Visibility::Exported => "export function",
        Visibility::Module => "function",
    };
    let one_line = format!(
        "{keyword} {name}({}): {return_type} {{",
        parameters.join(", ")
    );
    match one_line.chars().count() <= LINE_WIDTH {
        true => one_line,
        false => {
            let mut text = format!("{keyword} {name}(\n");
            for parameter in parameters {
                text.push_str(&format!("  {parameter},\n"));
            }
            text.push_str(&format!("): {return_type} {{"));
            text
        }
    }
}

/// The name of the reader of a type: `read` and the type name. Pure.
fn reader_name(type_name: &str) -> String {
    format!("read{type_name}")
}

/// The name of the constant of the wire values of a closed set of names:
/// `EXECUTION_VALUES`. Pure.
fn values_constant_name(type_name: &str) -> String {
    format!("{}_VALUES", screaming_snake_of_pascal(type_name))
}

/// `PascalCase` to `SCREAMING_SNAKE_CASE`: `BuiltInTaskCode` gives
/// `BUILT_IN_TASK_CODE`. An underscore goes before each upper-case letter
/// that follows a lower-case letter or a digit. Pure.
fn screaming_snake_of_pascal(pascal: &str) -> String {
    let mut text = String::with_capacity(pascal.len() * 2);
    let mut previous: Option<char> = None;
    for character in pascal.chars() {
        let starts_word = character.is_ascii_uppercase()
            && previous
                .is_some_and(|before| before.is_ascii_lowercase() || before.is_ascii_digit());
        if starts_word {
            text.push('_');
        }
        text.push(character.to_ascii_uppercase());
        previous = Some(character);
    }
    text
}

/// The literal of a count or an index of the model. Pure.
fn count_literal(count: usize) -> String {
    integer_literal(i128::try_from(count).expect("a count of the event model fits an i128"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use horsies_core::events::{
        ChoiceValue, DecodeCause, DecodeRefusal, EventDecodeError, JsonKind, RefusedName,
        BUILT_IN_TASK_CODES, EVENT_CONTRACT,
    };

    use super::*;
    use crate::events::types::type_sections;
    use crate::model::event_variant;

    const AT: EventField = EventField {
        name: "at",
        doc: "",
        form: ValueForm::Time,
        access: FieldAccess::Public,
    };

    const TASK_NAME: EventField = EventField {
        name: "task_name",
        doc: "",
        form: ValueForm::Text { max_chars: 255 },
        access: FieldAccess::Public,
    };

    const TRACE: EventField = EventField {
        name: "trace",
        doc: "",
        form: ValueForm::Text { max_chars: 8 },
        access: FieldAccess::Internal,
    };

    const MODE: NamedType = NamedType {
        name: "Mode",
        doc: "",
        shape: NamedShape::Choice {
            values: &[
                ChoiceValue {
                    value: "ONE",
                    doc: "",
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
        doc: "",
        shape: NamedShape::Variants {
            variants: &[
                event_variant("Reported", "StartReported", &[AT]),
                event_variant("NotReported", "StartNotReported", &[]),
            ],
        },
    };

    const PROBE: EventType = EventType {
        name: "Probe",
        doc: "",
        fields: &[AT, TASK_NAME, TRACE],
    };

    const SECOND: EventType = EventType {
        name: "Second",
        doc: "",
        fields: &[AT],
    };

    /// The TypeScript kind of each cause of the core's `DecodeCause`. The
    /// match has no wildcard arm: a new cause of the core fails the build of
    /// these tests until the cause union has its interface.
    fn cause_kind(cause: &DecodeCause) -> &'static str {
        match cause {
            DecodeCause::WrongJsonKind { .. } => "wrongJsonKind",
            DecodeCause::ItemCount { .. } => "itemCount",
            DecodeCause::OutOfRange { .. } => "outOfRange",
            DecodeCause::TextOverBound { .. } => "textOverBound",
            DecodeCause::NotCanonicalUuid => "notCanonicalUuid",
            DecodeCause::UnknownName { .. } => "unknownName",
            DecodeCause::NoVariantTag { .. } => "noVariantTag",
        }
    }

    /// The TypeScript literal of each JSON kind of the core. No wildcard arm.
    fn json_kind_literal(kind: JsonKind) -> &'static str {
        match kind {
            JsonKind::Text => "\"text\"",
            JsonKind::WholeNumber => "\"wholeNumber\"",
            JsonKind::Array => "\"array\"",
        }
    }

    /// The TypeScript kind of each refusal of the core's `decode_event`. No
    /// wildcard arm.
    fn refusal_kind(error: &EventDecodeError) -> &'static str {
        match error {
            EventDecodeError::NotAnEvent => "notAnEvent",
            EventDecodeError::UnknownEventType { .. } => "unknownEventType",
            EventDecodeError::Malformed { .. } => "malformedEvent",
        }
    }

    /// The name that a section defines: the word after `function`, `const`,
    /// `interface` or `type`, past the doc comment.
    fn defined_name(section: &str) -> String {
        let line = section
            .lines()
            .find(|line| !line.starts_with("/**") && !line.starts_with(" *"))
            .expect("a section has a line past its doc comment");
        let definition = line.trim_start_matches("export ");
        let rest = ["function ", "const ", "interface ", "type "]
            .iter()
            .find_map(|keyword| definition.strip_prefix(keyword))
            .unwrap_or_else(|| panic!("the section does not start with a definition: {line}"));
        rest.chars()
            .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect()
    }

    #[test]
    fn a_record_reader_checks_the_item_count_then_each_field_in_wire_order() {
        assert_eq!(
            record_reader(PROBE.name, PROBE.name, PROBE.fields),
            PROBE_READER
        );
    }

    #[test]
    fn an_internal_field_is_read_and_checked_and_not_kept() {
        let text = record_reader(PROBE.name, PROBE.name, PROBE.fields);
        assert!(text.contains("  const checkedTrace = readText(itemAt(fields, 2), 8);\n"));
        assert!(text.contains("    return refuseInside(\"trace\", checkedTrace.error);\n"));
        assert!(!text.contains("checkedTrace.value"));
        assert_eq!(local_name(&TRACE), "checkedTrace");
        assert_eq!(local_name(&TASK_NAME), "decodedTaskName");
    }

    #[test]
    fn a_record_without_public_fields_is_its_kind_alone() {
        assert_eq!(
            record_reader("StartNotReported", "notReported", &[]),
            UNIT_READER
        );
    }

    #[test]
    fn a_choice_has_its_wire_values_and_a_reader() {
        assert_eq!(named_reader_sections(&MODE), [MODE_VALUES, MODE_READER]);
    }

    #[test]
    fn the_values_of_a_set_of_one_name_are_one_line() {
        assert_eq!(
            values_constant("Only", &["ONE"]),
            "const ONLY_VALUES: ReadonlyArray<Only> = [\"ONE\"];"
        );
    }

    #[test]
    fn a_variant_type_has_a_reader_per_variant_then_the_tag_dispatch() {
        assert_eq!(
            named_reader_sections(&START),
            [START_REPORTED_READER, UNIT_READER, START_READER]
        );
    }

    #[test]
    fn each_form_has_its_reader_call() {
        let cases = [
            (ValueForm::Uuid, "readUuid(itemAt(fields, 3))"),
            (
                ValueForm::Text { max_chars: 2048 },
                "readText(itemAt(fields, 3), 2048)",
            ),
            (
                ValueForm::WholeNumber {
                    min: 1,
                    max: 2_147_483_648,
                },
                "readSafeWholeNumber(itemAt(fields, 3), 1n, 2_147_483_648n)",
            ),
            (ValueForm::Duration, "readDuration(itemAt(fields, 3))"),
            (ValueForm::Time, "readTime(itemAt(fields, 3))"),
            (
                ValueForm::Codes(&BUILT_IN_TASK_CODES),
                "readBuiltInTaskCode(itemAt(fields, 3))",
            ),
            (
                ValueForm::Named("Execution"),
                "readExecution(itemAt(fields, 3))",
            ),
        ];
        for (form, expected) in cases {
            assert_eq!(reader_call(form, 3), expected, "{form:?}");
        }
    }

    #[test]
    fn a_whole_number_past_the_safe_range_is_read_as_a_bigint() {
        assert_eq!(
            reader_call(
                ValueForm::WholeNumber {
                    min: 0,
                    max: i64::MAX
                },
                0
            ),
            "readWholeNumber(itemAt(fields, 0), 0n, 9_223_372_036_854_775_807n)"
        );
    }

    #[test]
    fn the_code_union_reader_lists_every_member_of_its_catalogs() {
        let sections = code_union_reader_sections(&BUILT_IN_TASK_CODES);
        let members: usize = BUILT_IN_TASK_CODES
            .catalogs
            .iter()
            .map(|catalog| catalog.members.len())
            .sum();
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].lines().count(), members + 2);
        assert!(sections[0]
            .starts_with("const BUILT_IN_TASK_CODE_VALUES: ReadonlyArray<BuiltInTaskCode> = [\n"));
        assert!(sections[0].contains("\n  \"TASK_EXCEPTION\",\n"));
        assert!(sections[0].contains("\n  \"ASYNC_CONTEXT\",\n"));
        assert_eq!(
            sections[1],
            "function readBuiltInTaskCode(value: WireJson): DecodeResult<BuiltInTaskCode, ValueRefusal> {\n  \
             return readChoice(value, \"BuiltInTaskCode\", BUILT_IN_TASK_CODE_VALUES);\n}"
        );
    }

    #[test]
    fn decode_event_dispatches_each_event_type_and_names_another_tag() {
        assert_eq!(decode_event_section(&[PROBE, SECOND]), DECODE_EVENT);
    }

    #[test]
    fn a_function_head_past_the_line_width_has_one_parameter_per_line() {
        let fits = "R".repeat(84);
        assert_eq!(
            function_head(Visibility::Module, "f", &[], &fits),
            format!("function f(): {fits} {{")
        );
        let over = "R".repeat(85);
        assert_eq!(
            function_head(Visibility::Exported, "f", &["a: A", "b: B"], &over),
            format!("export function f(\n  a: A,\n  b: B,\n): {over} {{")
        );
    }

    #[test]
    fn screaming_snake_case_puts_an_underscore_before_each_word() {
        assert_eq!(
            screaming_snake_of_pascal("BuiltInTaskCode"),
            "BUILT_IN_TASK_CODE"
        );
        assert_eq!(screaming_snake_of_pascal("ShortageErrno"), "SHORTAGE_ERRNO");
        assert_eq!(screaming_snake_of_pascal("Execution"), "EXECUTION");
        assert_eq!(screaming_snake_of_pascal("Utf8Text"), "UTF8_TEXT");
    }

    #[test]
    fn the_helpers_follow_the_forms_that_the_contract_uses() {
        let small = EventContract {
            version: 7,
            events: &[SECOND],
            value_types: &[],
        };
        let heads: Vec<String> = helper_sections(FormUse::of(&small))
            .iter()
            .map(|section| section.lines().next().unwrap_or_default().to_owned())
            .collect();
        assert_eq!(heads, SMALL_HELPER_HEADS);
    }

    #[test]
    fn the_real_contract_uses_every_helper() {
        let usage = FormUse::of(&EVENT_CONTRACT);
        assert_eq!(
            usage,
            FormUse {
                any_field: true,
                uuid: true,
                text: true,
                whole_number: true,
                safe_whole_number: true,
                duration: true,
                time: true,
                names: true,
                variants: true,
            }
        );
        let text = helper_sections(usage).join("\n\n");
        for helper in [
            format!("const MAX_REFUSED_NAME_CHARS = {MAX_REFUSED_NAME_CHARS};"),
            "function refuseUnknownName(".to_owned(),
            "function readSafeWholeNumber(".to_owned(),
            "function readText(".to_owned(),
            "function readUuid(".to_owned(),
            "const DURATION_NANOSECONDS_MAX = 18_446_744_073_709_551_615n;".to_owned(),
            "function readDuration(".to_owned(),
            "const TIME_NANOSECONDS_MIN = -9_223_372_036_854_775_808n;".to_owned(),
            "const TIME_NANOSECONDS_MAX = 9_223_372_036_854_775_807n;".to_owned(),
            "function readChoice<T extends string>(".to_owned(),
            "function readVariant(".to_owned(),
            "function within<T>(".to_owned(),
        ] {
            assert!(text.contains(&helper), "{helper}");
        }
    }

    #[test]
    fn each_cause_of_the_core_is_a_member_of_the_cause_union() {
        let causes = [
            DecodeCause::WrongJsonKind {
                expected: JsonKind::Text,
            },
            DecodeCause::ItemCount {
                expected: 1,
                found: 0,
            },
            DecodeCause::OutOfRange { min: 0, max: 1 },
            DecodeCause::TextOverBound { chars: 2, bound: 1 },
            DecodeCause::NotCanonicalUuid,
            DecodeCause::UnknownName {
                type_name: "Execution",
                found: RefusedName::cut("SHARED"),
            },
            DecodeCause::NoVariantTag {
                type_name: "AttemptEnd",
            },
        ];
        let text = CAUSE_SECTIONS.join("\n\n");
        for cause in &causes {
            let kind = cause_kind(cause);
            assert!(
                text.contains(&format!("  readonly kind: \"{kind}\";\n")),
                "{kind}"
            );
        }
        assert_eq!(text.matches("\n  readonly kind: ").count(), causes.len());
        for kind in [JsonKind::Text, JsonKind::WholeNumber, JsonKind::Array] {
            let literal = json_kind_literal(kind);
            assert!(text.contains(&format!("\n  | {literal}")), "{literal}");
        }
    }

    #[test]
    fn each_refusal_of_the_core_is_a_member_of_the_error_union() {
        let refusals = [
            EventDecodeError::NotAnEvent,
            EventDecodeError::UnknownEventType {
                tag: RefusedName::cut("TaskStarted"),
            },
            EventDecodeError::Malformed {
                event: "TaskClaimed",
                refusal: DecodeRefusal::item_count(8, 7),
            },
        ];
        let text = refusal_sections().join("\n\n");
        for refusal in &refusals {
            let kind = refusal_kind(refusal);
            assert!(
                text.contains(&format!("  readonly kind: \"{kind}\";\n")),
                "{kind}"
            );
        }
        assert!(text.contains("  readonly event: AnyEvent[\"kind\"];\n"));
    }

    #[test]
    fn every_name_of_the_real_module_is_defined_once() {
        let mut sections = type_sections(&EVENT_CONTRACT);
        sections.extend(decoder_sections(&EVENT_CONTRACT));
        let names: Vec<String> = sections
            .iter()
            .map(|section| defined_name(section))
            .collect();
        let unique: BTreeSet<&String> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
        let mut readers = vec!["readBuiltInTaskCode".to_owned()];
        for named in EVENT_CONTRACT.value_types {
            readers.push(reader_name(named.name));
            match named.shape {
                NamedShape::Variants { variants } => readers.extend(
                    variants
                        .iter()
                        .map(|variant| reader_name(event_variant_type_name(variant))),
                ),
                NamedShape::Choice { .. } => readers.push(values_constant_name(named.name)),
            }
        }
        readers.extend(
            EVENT_CONTRACT
                .events
                .iter()
                .map(|event| reader_name(event.name)),
        );
        for reader in &readers {
            assert!(names.contains(reader), "{reader}");
        }
        assert_eq!(names.last().map(String::as_str), Some("decodeEvent"));
    }

    #[test]
    fn the_real_decoder_reads_the_internal_fields_of_a_worker_decided_end() {
        let text = decoder_sections(&EVENT_CONTRACT).join("\n\n");
        for check in [
            "  const checkedErrorCode = readBuiltInTaskCode(itemAt(fields, 1));\n",
            "  const checkedTrace = readRunTrace(itemAt(fields, 2));\n",
            "  const checkedPlacedAt = readTime(itemAt(fields, 3));\n",
            "  const checkedChildKind = readExecution(itemAt(fields, 4));\n",
            "  const checkedWorkerId = readText(itemAt(fields, 5), 255);\n",
            "  const checkedWorkflow = readWorkflowPlace(itemAt(fields, 6));\n",
        ] {
            assert!(text.contains(check), "{check}");
        }
        assert!(text.contains(
            "    Object.freeze({\n      kind: \"decidedByWorker\",\n      \
             reason: decodedReason.value,\n    }),\n"
        ));
    }

    const PROBE_READER: &str = r#"function readProbe(fields: ReadonlyArray<WireJson>): DecodeResult<Probe, ValueRefusal> {
  if (fields.length !== 3) {
    return refuse({ kind: "itemCount", expected: 3, found: fields.length });
  }

  const decodedAt = readTime(itemAt(fields, 0));

  if (!decodedAt.ok) {
    return refuseInside("at", decodedAt.error);
  }

  const decodedTaskName = readText(itemAt(fields, 1), 255);

  if (!decodedTaskName.ok) {
    return refuseInside("task_name", decodedTaskName.error);
  }

  const checkedTrace = readText(itemAt(fields, 2), 8);

  if (!checkedTrace.ok) {
    return refuseInside("trace", checkedTrace.error);
  }

  return accept(
    Object.freeze({
      kind: "Probe",
      at: decodedAt.value,
      taskName: decodedTaskName.value,
    }),
  );
}"#;

    const UNIT_READER: &str = r#"function readStartNotReported(
  fields: ReadonlyArray<WireJson>,
): DecodeResult<StartNotReported, ValueRefusal> {
  if (fields.length !== 0) {
    return refuse({ kind: "itemCount", expected: 0, found: fields.length });
  }

  return accept(Object.freeze({ kind: "notReported" }));
}"#;

    const MODE_VALUES: &str = r#"const MODE_VALUES: ReadonlyArray<Mode> = [
  "ONE",
  "TWO",
];"#;

    const MODE_READER: &str = r#"function readMode(value: WireJson): DecodeResult<Mode, ValueRefusal> {
  return readChoice(value, "Mode", MODE_VALUES);
}"#;

    const START_REPORTED_READER: &str = r#"function readStartReported(
  fields: ReadonlyArray<WireJson>,
): DecodeResult<StartReported, ValueRefusal> {
  if (fields.length !== 1) {
    return refuse({ kind: "itemCount", expected: 1, found: fields.length });
  }

  const decodedAt = readTime(itemAt(fields, 0));

  if (!decodedAt.ok) {
    return refuseInside("at", decodedAt.error);
  }

  return accept(
    Object.freeze({
      kind: "reported",
      at: decodedAt.value,
    }),
  );
}"#;

    const START_READER: &str = r#"function readStart(value: WireJson): DecodeResult<Start, ValueRefusal> {
  const variant = readVariant(value, "Start");

  if (!variant.ok) {
    return variant;
  }

  const { tag, fields } = variant.value;

  switch (tag) {
    case "Reported":
      return within("Reported", readStartReported(fields));
    case "NotReported":
      return within("NotReported", readStartNotReported(fields));
    default:
      return refuseUnknownName("Start", tag);
  }
}"#;

    const DECODE_EVENT: &str = r#"/**
 * The event of the wire items of one event, `[type name, field, ...]`, as the
 * bridge gives them: every integer is a `bigint`. A refusal quotes a name from
 * the wire cut to 64 characters.
 */
export function decodeEvent(
  items: ReadonlyArray<WireJson>,
): DecodeResult<AnyEvent, EventDecodeError> {
  const [tag, ...fields] = items;

  if (typeof tag !== "string") {
    return { ok: false, error: NOT_AN_EVENT };
  }

  switch (tag) {
    case "Probe":
      return eventResult("Probe", readProbe(fields));
    case "Second":
      return eventResult("Second", readSecond(fields));
    default:
      return { ok: false, error: Object.freeze({ kind: "unknownEventType", tag: cutName(tag) }) };
  }
}"#;

    const SMALL_HELPER_HEADS: [&str; 13] = [
        "interface ValueRefusal {",
        "const MAX_REFUSED_NAME_CHARS = 64;",
        "const NOT_AN_EVENT: NotAnEvent = Object.freeze({ kind: \"notAnEvent\" });",
        "function accept<T>(value: T): DecodeResult<T, never> {",
        "function refuse(cause: EventDecodeCause): DecodeResult<never, ValueRefusal> {",
        "function cutName(name: string): string {",
        "function refuseInside(name: string, inner: ValueRefusal): DecodeResult<never, ValueRefusal> {",
        "function itemAt(items: ReadonlyArray<WireJson>, index: number): WireJson {",
        "function readWholeNumber(",
        "const TIME_NANOSECONDS_MIN = -9_223_372_036_854_775_808n;",
        "const TIME_NANOSECONDS_MAX = 9_223_372_036_854_775_807n;",
        "function readTime(value: WireJson): DecodeResult<Temporal.Instant, ValueRefusal> {",
        "function eventResult(",
    ];
}
