//! Render a manifest of horsies-core as a module of `_catalog`
//! (`manifest.py`, `worker_manifest.py`, `scheduler_manifest.py`,
//! `workflow_spec.py`): one
//! frozen dataclass per manifest
//! type, the integer bounds the core's parser enforces, and the dump
//! function that gives the manifest's JSON text.
//!
//! The renderer has no Python form for a time of day, an optional value or
//! a variant that holds the fields of a record type: only the schedule
//! pattern schema (`SCHEDULE_PATTERN`) uses them, and the Python package
//! keeps its own pattern models. A module that holds one of them is a
//! defect of the caller, and the renderer stops with a panic that names it.

use horsies_core::manifest_schema::{
    ChoiceValue, IntBound, ManifestField, ManifestModule, ManifestShape, ManifestType, PythonValue,
    TaggedVariant, VariantFields,
};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// The file name of the producer manifest module, inside `_catalog`.
pub const MANIFEST_MODULE_FILE: &str = "manifest.py";

/// The file name of the worker manifest module, inside `_catalog`.
pub const WORKER_MANIFEST_MODULE_FILE: &str = "worker_manifest.py";

/// The file name of the scheduler manifest module, inside `_catalog`.
pub const SCHEDULER_MANIFEST_MODULE_FILE: &str = "scheduler_manifest.py";

/// The file name of the workflow spec data module, inside `_catalog`.
pub const WORKFLOW_SPEC_MODULE_FILE: &str = "workflow_spec.py";

/// The decorator of every generated dataclass. `kw_only` lets a tag field
/// with a default come before the fields without one.
const DATACLASS: &str = "@dataclass(frozen=True, slots=True, kw_only=True)";

/// The JSON encoder of a module's dump function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpEncoder {
    /// `json.dumps` of `dataclasses.asdict`; the dump returns `str`. For a
    /// manifest of library config.
    PlainJson,
    /// `dumps_json` of `horsies.core.codec.json_io`, the strict encoder of
    /// every stored form, on `dataclasses.asdict` with each tuple field as a
    /// list (`dumps_json` refuses a tuple); the dump returns
    /// `SerdeResult[str]`, an `Err` for a text that UTF-8 cannot encode. For
    /// data that holds user text: a character outside ASCII is written as it
    /// is, not as a `\u` escape.
    StrictJsonIo,
}

/// The imports of a module whose dump uses `json.dumps`.
const PLAIN_JSON_IMPORTS: &str =
    "from __future__ import annotations\n\nimport dataclasses\nimport json\n\
     from dataclasses import dataclass\nfrom typing import Final, Literal\n\n";

/// The imports of a module whose dump uses `dumps_json`.
const STRICT_JSON_IO_IMPORTS: &str = "from __future__ import annotations\n\nimport dataclasses\n\
     from dataclasses import dataclass\nfrom typing import Final, Literal, cast\n\n\
     from horsies.core.codec.json_io import SerdeResult, dumps_json\n\n";

/// The helpers of a `dumps_json` dump: `dataclasses.asdict` keeps a tuple
/// field as a tuple, and `dumps_json` takes only lists.
const STRICT_JSON_IO_HELPERS: &str =
    "\n\ndef _json_object(members: list[tuple[str, object]]) -> dict[str, object]:\n    \
     \"\"\"A dataclass as a JSON object for `dumps_json`: a tuple field as a list.\"\"\"\n    \
     return {name: _json_member(value) for name, value in members}\n\
     \n\ndef _json_member(value: object) -> object:\n    \
     match value:\n        \
     case tuple():\n            \
     return list(cast('tuple[object, ...]', value))\n        \
     case _:\n            \
     return value\n";

/// The text of the manifest module.
pub fn render_manifest_module(module: &ManifestModule, encoder: DumpEncoder) -> String {
    let dump = dump_function_name(module.root);
    let bounds = module.int_bounds();
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&format!(
        "\"\"\"The {} of horsies-core, generated from its Rust types.\n\n\
         One frozen dataclass per manifest type, the integer bounds that the core's\n\
         parser enforces, and `{dump}`, which gives the JSON text that\n\
         {} takes.\n\"\"\"\n\n",
        words(module.root),
        module.reader
    ));
    text.push_str(match encoder {
        DumpEncoder::PlainJson => PLAIN_JSON_IMPORTS,
        DumpEncoder::StrictJsonIo => STRICT_JSON_IO_IMPORTS,
    });
    text.push_str(&render_all_list(module, &dump, &bounds));
    text.push_str(&format!(
        "\nMANIFEST_VERSION: Final = {}\n\"\"\"The manifest version this core reads.\"\"\"\n",
        module.version
    ));
    for bound in &bounds {
        text.push_str(&format!(
            "\n{name}_MIN: Final = {min}\n{name}_MAX: Final = {max}\n",
            name = bound.name,
            min = bound.min,
            max = bound.max
        ));
    }
    for manifest_type in module.types {
        text.push_str(&render_type(manifest_type));
    }
    text.push_str(&render_dump(module, &dump, encoder));
    text
}

/// The dump function of `module`, named `dump`, with its helpers.
fn render_dump(module: &ManifestModule, dump: &str, encoder: DumpEncoder) -> String {
    match encoder {
        DumpEncoder::PlainJson => format!(
            "\n\ndef {dump}(manifest: {root}) -> str:\n    \
             \"\"\"The JSON text of `manifest`, in the form that {reader} takes.\"\"\"\n    \
             return json.dumps(dataclasses.asdict(manifest), separators=(',', ':'), allow_nan=False)\n",
            root = module.root,
            reader = module.reader_short
        ),
        DumpEncoder::StrictJsonIo => format!(
            "{STRICT_JSON_IO_HELPERS}\n\ndef {dump}(manifest: {root}) -> SerdeResult[str]:\n    \
             \"\"\"The JSON text of `manifest`, in the form that {reader} takes.\n\n    \
             The encoder is `dumps_json`, as for every stored form: a character\n    \
             outside ASCII is written as it is. Err when a text of `manifest` holds\n    \
             a lone surrogate, which UTF-8 cannot encode.\n    \
             \"\"\"\n    \
             return dumps_json(dataclasses.asdict(manifest, dict_factory=_json_object))\n",
            root = module.root,
            reader = module.reader_short
        ),
    }
}

fn render_all_list(module: &ManifestModule, dump: &str, bounds: &[IntBound]) -> String {
    let mut names: Vec<String> = Vec::new();
    for manifest_type in module.types {
        if let ManifestShape::Tagged { variants, .. } = manifest_type.shape {
            names.extend(
                variants
                    .iter()
                    .map(|variant| variant.python_class.to_owned()),
            );
        }
        names.push(manifest_type.python_name.to_owned());
    }
    names.push(dump.to_owned());
    names.push("MANIFEST_VERSION".to_owned());
    for bound in bounds {
        names.push(format!("{}_MIN", bound.name));
        names.push(format!("{}_MAX", bound.name));
    }
    let quoted: Vec<String> = names.iter().map(|name| string_literal(name)).collect();
    let mut text = String::from("__all__ = [\n");
    for name in quoted {
        text.push_str(&format!("    {name},\n"));
    }
    text.push_str("]\n");
    text
}

fn render_type(manifest_type: &ManifestType) -> String {
    match manifest_type.shape {
        ManifestShape::Record { fields } => format!(
            "\n\n{}",
            render_dataclass(manifest_type.python_name, manifest_type.doc, None, fields)
        ),
        ManifestShape::Tagged {
            tag_field,
            variants,
        } => render_tagged(manifest_type, tag_field, variants),
        ManifestShape::Choice { values } => render_choice(manifest_type, values),
    }
}

/// One dataclass per variant, then the union alias.
fn render_tagged(
    manifest_type: &ManifestType,
    tag_field: &str,
    variants: &[TaggedVariant],
) -> String {
    let mut text = String::new();
    for variant in variants {
        let tag = TagField {
            name: tag_field,
            value: variant.tag_value,
        };
        let fields = match variant.fields {
            VariantFields::Own(fields) => fields,
            VariantFields::OfRecord(record) => panic!(
                "{}: the Python renderer has no form for a variant that holds the fields of \
                 the record type {record}",
                variant.python_class
            ),
        };
        text.push_str("\n\n");
        text.push_str(&render_dataclass(
            variant.python_class,
            variant.doc,
            Some(tag),
            fields,
        ));
    }
    let members: Vec<&str> = variants
        .iter()
        .map(|variant| variant.python_class)
        .collect();
    text.push_str(&format!(
        "\n\ntype {} = {}\n",
        manifest_type.python_name,
        members.join(" | ")
    ));
    if let Some(doc) = docstring(manifest_type.doc, "") {
        text.push_str(&doc);
    }
    text
}

/// The `Literal` alias, with each value's doc listed in the alias doc.
fn render_choice(manifest_type: &ManifestType, values: &[ChoiceValue]) -> String {
    let literals: Vec<String> = values
        .iter()
        .map(|choice| string_literal(choice.value))
        .collect();
    let mut text = format!(
        "\n\ntype {} = Literal[{}]\n",
        manifest_type.python_name,
        literals.join(", ")
    );
    let mut doc = manifest_type.doc.trim().to_owned();
    for choice in values {
        let value_doc = choice.doc.split_whitespace().collect::<Vec<_>>().join(" ");
        if !value_doc.is_empty() {
            doc.push_str(&format!("\n\n`{}`: {value_doc}", choice.value));
        }
    }
    if let Some(rendered) = docstring(&doc, "") {
        text.push_str(&rendered);
    }
    text
}

/// The tag field of a variant class: its name and its one value.
#[derive(Clone, Copy)]
struct TagField<'a> {
    name: &'a str,
    value: &'a str,
}

fn render_dataclass(
    class: &str,
    doc: &str,
    tag: Option<TagField<'_>>,
    fields: &[ManifestField],
) -> String {
    let mut text = format!("{DATACLASS}\nclass {class}:\n");
    let class_doc = docstring(doc, "    ");
    let has_doc = class_doc.is_some();
    if let Some(rendered) = class_doc {
        text.push_str(&rendered);
    }
    let mut lines: Vec<(String, &str)> = Vec::with_capacity(fields.len() + 1);
    if let Some(tag) = tag {
        let literal = string_literal(tag.value);
        lines.push((format!("{}: Literal[{literal}] = {literal}", tag.name), ""));
    }
    for field in fields {
        lines.push((
            format!("{}: {}", field.name, python_type(field.value)),
            field.doc,
        ));
    }
    if lines.is_empty() && !has_doc {
        text.push_str("    pass\n");
    }
    for (position, (line, field_doc)) in lines.iter().enumerate() {
        if position > 0 || has_doc {
            text.push('\n');
        }
        text.push_str(&format!("    {line}\n"));
        if let Some(rendered) = docstring(field_doc, "    ") {
            text.push_str(&rendered);
        }
    }
    text
}

/// The annotation of a field: a `Vec` is a tuple of any length, a map a
/// `dict` with text keys.
fn python_type(value: PythonValue) -> String {
    match value {
        PythonValue::Bool => "bool".to_owned(),
        PythonValue::Str => "str".to_owned(),
        PythonValue::Int(_) => "int".to_owned(),
        PythonValue::Named(name) => name.to_owned(),
        PythonValue::List(item) => format!("tuple[{}, ...]", python_type(item.value())),
        PythonValue::Map(item) => format!("dict[str, {}]", python_type(item.value())),
        PythonValue::Optional(_) | PythonValue::TimeOfDay => {
            panic!("the Python renderer has no form for the manifest value {value:?}")
        }
    }
}

/// `dump_<root in snake case>`.
fn dump_function_name(root: &str) -> String {
    format!("dump_{}", snake_case(root))
}

/// `ProducerManifest` as `producer manifest`.
fn words(root: &str) -> String {
    snake_case(root).replace('_', " ")
}

/// `ProducerManifest` as `producer_manifest`: an underscore before each
/// upper-case letter but the first, then all lower case.
pub(crate) fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (position, character) in name.char_indices() {
        if position > 0 && character.is_uppercase() {
            out.push('_');
        }
        out.push(character.to_ascii_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use horsies_core::manifest_schema::{
        MemberDefault, PythonItem, PRODUCER_MANIFEST, SCHEDULER_MANIFEST, WORKER_MANIFEST,
        WORKFLOW_SPEC_DATA,
    };

    use super::*;

    const BOUND: IntBound = IntBound {
        name: "POOL_CONNECTIONS",
        min: 0,
        max: 10,
    };

    const FIELDS: &[ManifestField] = &[
        ManifestField {
            name: "pool_size",
            doc: "Connections of the pool.",
            value: PythonValue::Int(BOUND),
            default: MemberDefault::NoDefault,
        },
        ManifestField {
            name: "echo",
            doc: "",
            value: PythonValue::Bool,
            default: MemberDefault::NoDefault,
        },
    ];

    #[test]
    fn a_record_is_a_frozen_keyword_only_dataclass_with_field_docs() {
        let rendered = render_dataclass("Pool", "Pool settings.", None, FIELDS);
        assert_eq!(
            rendered,
            "@dataclass(frozen=True, slots=True, kw_only=True)\nclass Pool:\n    \
             \"\"\"Pool settings.\"\"\"\n\n    pool_size: int\n    \
             \"\"\"Connections of the pool.\"\"\"\n\n    echo: bool\n"
        );
    }

    #[test]
    fn a_tagged_variant_has_its_tag_as_a_literal_field_with_a_default() {
        let tag = TagField {
            name: "kind",
            value: "SEPARATE",
        };
        let rendered = render_dataclass("SessionSeparate", "", Some(tag), &FIELDS[1..]);
        assert_eq!(
            rendered,
            "@dataclass(frozen=True, slots=True, kw_only=True)\nclass SessionSeparate:\n    \
             kind: Literal['SEPARATE'] = 'SEPARATE'\n\n    echo: bool\n"
        );
    }

    #[test]
    fn a_choice_is_a_literal_alias_that_lists_its_value_docs() {
        let manifest_type = ManifestType {
            python_name: "Mode",
            doc: "The mode.",
            shape: ManifestShape::Choice {
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
        let ManifestShape::Choice { values } = manifest_type.shape else {
            panic!("the fixture is a choice");
        };
        let rendered = render_choice(&manifest_type, values);
        assert_eq!(
            rendered,
            "\n\ntype Mode = Literal['ONE', 'TWO']\n\"\"\"The mode.\n\n`ONE`: The first mode.\n\"\"\"\n"
        );
    }

    #[test]
    fn the_producer_manifest_module_has_every_type_bound_and_the_dump() {
        let rendered = render_manifest_module(&PRODUCER_MANIFEST, DumpEncoder::PlainJson);
        assert!(rendered.starts_with(GENERATED_HEADER));
        for manifest_type in PRODUCER_MANIFEST.types {
            let defined = match manifest_type.shape {
                ManifestShape::Record { .. } => format!("\nclass {}:\n", manifest_type.python_name),
                ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                    format!("\ntype {} = ", manifest_type.python_name)
                }
            };
            assert!(rendered.contains(&defined), "{}", manifest_type.python_name);
        }
        for bound in PRODUCER_MANIFEST.int_bounds() {
            assert!(rendered.contains(&format!("\n{}_MIN: Final = {}\n", bound.name, bound.min)));
            assert!(rendered.contains(&format!("\n{}_MAX: Final = {}\n", bound.name, bound.max)));
        }
        assert!(
            rendered.contains("\ndef dump_producer_manifest(manifest: ProducerManifest) -> str:\n")
        );
        assert!(rendered.contains("\nMANIFEST_VERSION: Final = 1\n"));
    }

    #[test]
    fn the_dump_function_is_named_after_the_root() {
        assert_eq!(
            dump_function_name("ProducerManifest"),
            "dump_producer_manifest"
        );
        assert_eq!(words("ProducerManifest"), "producer manifest");
        assert_eq!(dump_function_name("WorkerManifest"), "dump_worker_manifest");
    }

    #[test]
    fn a_container_field_is_a_tuple_or_a_dict_of_its_item() {
        assert_eq!(
            python_type(PythonValue::List(PythonItem::Str)),
            "tuple[str, ...]"
        );
        assert_eq!(
            python_type(PythonValue::Map(PythonItem::Named("Execution"))),
            "dict[str, Execution]"
        );
    }

    #[test]
    fn the_worker_manifest_module_has_every_type_bound_and_the_dump() {
        let rendered = render_manifest_module(&WORKER_MANIFEST, DumpEncoder::PlainJson);
        assert!(rendered.starts_with(GENERATED_HEADER));
        for manifest_type in WORKER_MANIFEST.types {
            let defined = match manifest_type.shape {
                ManifestShape::Record { .. } => format!("\nclass {}:\n", manifest_type.python_name),
                ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                    format!("\ntype {} = ", manifest_type.python_name)
                }
            };
            assert!(rendered.contains(&defined), "{}", manifest_type.python_name);
        }
        for bound in WORKER_MANIFEST.int_bounds() {
            assert!(rendered.contains(&format!("\n{}_MIN: Final = {}\n", bound.name, bound.min)));
        }
        assert!(rendered.contains("\ndef dump_worker_manifest(manifest: WorkerManifest) -> str:\n"));
        assert!(rendered.contains("    profiles: dict[str, WorkerProfileManifest]\n"));
        assert!(rendered.contains("JSON text that\nthe worker of the core takes."));
    }

    #[test]
    fn the_scheduler_manifest_module_has_every_type_bound_and_the_dump() {
        let rendered = render_manifest_module(&SCHEDULER_MANIFEST, DumpEncoder::PlainJson);
        assert!(rendered.starts_with(GENERATED_HEADER));
        for manifest_type in SCHEDULER_MANIFEST.types {
            let defined = match manifest_type.shape {
                ManifestShape::Record { .. } => format!("\nclass {}:\n", manifest_type.python_name),
                ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                    format!("\ntype {} = ", manifest_type.python_name)
                }
            };
            assert!(rendered.contains(&defined), "{}", manifest_type.python_name);
        }
        for bound in SCHEDULER_MANIFEST.int_bounds() {
            assert!(rendered.contains(&format!("\n{}_MIN: Final = {}\n", bound.name, bound.min)));
        }
        assert!(rendered
            .contains("\ndef dump_scheduler_manifest(manifest: SchedulerManifest) -> str:\n"));
        assert!(rendered.contains("    schedules: tuple[ScheduleManifest, ...]\n"));
        assert!(rendered.contains("    pattern_json: str\n"));
    }

    #[test]
    fn the_workflow_spec_data_module_has_every_type_bound_and_the_dump() {
        let rendered = render_manifest_module(&WORKFLOW_SPEC_DATA, DumpEncoder::StrictJsonIo);
        assert!(rendered.starts_with(GENERATED_HEADER));
        for manifest_type in WORKFLOW_SPEC_DATA.types {
            let defined = match manifest_type.shape {
                ManifestShape::Record { .. } => format!("\nclass {}:\n", manifest_type.python_name),
                ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                    format!("\ntype {} = ", manifest_type.python_name)
                }
            };
            assert!(rendered.contains(&defined), "{}", manifest_type.python_name);
        }
        for bound in WORKFLOW_SPEC_DATA.int_bounds() {
            assert!(rendered.contains(&format!("\n{}_MIN: Final = {}\n", bound.name, bound.min)));
        }
        assert!(rendered.contains(
            "\ndef dump_workflow_spec_data(manifest: WorkflowSpecData) -> SerdeResult[str]:\n"
        ));
        assert!(rendered.contains(
            "    return dumps_json(dataclasses.asdict(manifest, dict_factory=_json_object))\n"
        ));
        assert!(
            rendered.contains("\nfrom horsies.core.codec.json_io import SerdeResult, dumps_json\n")
        );
        assert!(!rendered.contains("import json\n"));
        assert!(!rendered.contains("json.dumps("));
        assert!(rendered.contains("    nodes: tuple[SpecNode, ...]\n"));
        assert!(rendered.contains("    args_from: dict[str, int]\n"));
        assert!(rendered.contains("    queue_classes: dict[str, str]\n"));
    }

    #[test]
    #[should_panic(expected = "the Python renderer has no form for the manifest value TimeOfDay")]
    fn a_time_of_day_has_no_python_form() {
        let _refused = python_type(PythonValue::TimeOfDay);
    }

    #[test]
    #[should_panic(expected = "the Python renderer has no form for the manifest value Optional")]
    fn an_optional_value_has_no_python_form() {
        let _refused = python_type(PythonValue::Optional(PythonItem::Str));
    }

    #[test]
    #[should_panic(
        expected = "the Python renderer has no form for a variant that holds the fields"
    )]
    fn a_variant_that_holds_a_record_has_no_python_form() {
        let manifest_type = ManifestType {
            python_name: "Pattern",
            doc: "",
            shape: ManifestShape::Tagged {
                tag_field: "type",
                variants: &[TaggedVariant {
                    python_class: "PatternDaily",
                    tag_value: "daily",
                    doc: "",
                    fields: VariantFields::OfRecord("Daily"),
                }],
            },
        };
        let _refused = render_type(&manifest_type);
    }
}
