//! Render `wire_json.ts`: the type of a JSON value as the core reads and
//! writes it, and the result type of a decode. Types only; the text is a
//! constant of this renderer, not a fact of the core model.
//!
//! The manifest converters return `WireJson` values, and the event decoder
//! reads them. In a `WireJson` value an integer is always a `bigint` and a
//! `number` is always a float.

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{module_text, union_alias};

/// The stem of the module, `src/_catalog/wire_json.ts`.
pub const WIRE_JSON_MODULE: &str = "wire_json";

/// The name of the JSON value type.
pub const WIRE_JSON_TYPE: &str = "WireJson";

/// The name of the decode result type. It takes the value type and the
/// error type as its type parameters.
pub const DECODE_RESULT_TYPE: &str = "DecodeResult";

/// The comment of the JSON value type.
const WIRE_JSON_COMMENT: &str = "/**\n \
     * A JSON value as the core reads and writes it. An integer is a `bigint` and\n \
     * a `number` is a float. An object is a map: its keys keep their order, and\n \
     * `__proto__` is a plain key.\n \
     */";

/// The members of the JSON value type, in their order.
const WIRE_JSON_MEMBERS: [&str; 7] = [
    "null",
    "boolean",
    "number",
    "bigint",
    "string",
    "ReadonlyArray<WireJson>",
    "ReadonlyMap<string, WireJson>",
];

/// The comment of the decode result type.
const DECODE_RESULT_COMMENT: &str =
    "/** The outcome of a decode: the value, or why the input does not decode. */";

/// The members of the decode result type, in their order.
const DECODE_RESULT_MEMBERS: [&str; 2] = [
    "{ readonly ok: true; readonly value: T }",
    "{ readonly ok: false; readonly error: E }",
];

/// The file of the module. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    vec![catalog_file(WIRE_JSON_MODULE, render_wire_json_module())]
}

/// The text of `wire_json.ts`. Pure.
fn render_wire_json_module() -> String {
    module_text(&[
        commented_union(WIRE_JSON_COMMENT, WIRE_JSON_TYPE, &WIRE_JSON_MEMBERS),
        commented_union(
            DECODE_RESULT_COMMENT,
            &format!("{DECODE_RESULT_TYPE}<T, E>"),
            &DECODE_RESULT_MEMBERS,
        ),
    ])
}

/// `comment`, then the exported union `name` of `members`. Pure.
fn commented_union(comment: &str, name: &str, members: &[&str]) -> String {
    let members: Vec<String> = members.iter().map(|member| (*member).to_owned()).collect();
    format!("{comment}\n{}", union_alias(name, &members))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    #[test]
    fn the_module_is_the_agreed_text() {
        let expected = format!(
            "{GENERATED_HEADER}\n\
             \n\
             /**\n \
             * A JSON value as the core reads and writes it. An integer is a `bigint` and\n \
             * a `number` is a float. An object is a map: its keys keep their order, and\n \
             * `__proto__` is a plain key.\n \
             */\n\
             export type WireJson =\n  \
             | null\n  \
             | boolean\n  \
             | number\n  \
             | bigint\n  \
             | string\n  \
             | ReadonlyArray<WireJson>\n  \
             | ReadonlyMap<string, WireJson>;\n\
             \n\
             /** The outcome of a decode: the value, or why the input does not decode. */\n\
             export type DecodeResult<T, E> =\n  \
             | {{ readonly ok: true; readonly value: T }}\n  \
             | {{ readonly ok: false; readonly error: E }};\n"
        );
        assert_eq!(render_wire_json_module(), expected);
    }

    #[test]
    fn the_module_is_one_file_of_the_catalog_folder() {
        let files = generated_files();
        let paths: Vec<&Path> = files.iter().map(GeneratedFile::relative_path).collect();
        assert_eq!(paths, [Path::new("src/_catalog/wire_json.ts")]);
        assert_eq!(files[0].text(), render_wire_json_module());
    }
}
