//! The text new types of the core (`TEXT_NEW_TYPES`), one module each:
//! today `retention_class.ts` with `RetentionClassKey`.
//!
//! A new type is a branded text type. Each named constant of the new type
//! is an exported literal constant, and the literal type of each constant
//! is also a member of the new type. So the module needs no type assertion:
//! a named constant is a value of the type, any other value of the type
//! comes from the core through the declared types of the bridge, and a
//! plain `string` is not a value of the type.

use horsies_core::error_catalog::{TextNewType, TEXT_NEW_TYPES};

use crate::model::new_type_module;
use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{camel_case_of_pascal, module_text, string_literal, union_alias};

/// The comment on the type of a new type.
const NEW_TYPE_COMMENT: &str =
    "/** A named constant of this module, or a value from the core. A plain string is not one. */";

/// One module per new type, in the order of `TEXT_NEW_TYPES`. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    TEXT_NEW_TYPES
        .iter()
        .map(|new_type| {
            catalog_file(
                new_type_module(new_type),
                module_text(&new_type_sections(new_type)),
            )
        })
        .collect()
}

/// The module stem and the type name of `new_type`, for a renderer that
/// names the type from another module: `("retention_class",
/// "RetentionClassKey")`. Pure.
pub fn new_type_reference(new_type: &TextNewType) -> (&'static str, &'static str) {
    (new_type_module(new_type), new_type.name)
}

/// The name of the brand symbol of `new_type`: `retentionClassKeyBrand`.
/// Pure.
fn brand_name(new_type: &TextNewType) -> String {
    format!("{}Brand", camel_case_of_pascal(new_type.name))
}

/// The sections of the module: the brand symbol, each named constant, then
/// the type. Pure.
fn new_type_sections(new_type: &TextNewType) -> Vec<String> {
    let brand = brand_name(new_type);
    let mut sections = vec![format!("declare const {brand}: unique symbol;")];
    sections.extend(new_type.constants.iter().map(|constant| {
        format!(
            "export const {} = {};",
            constant.name,
            string_literal(constant.value)
        )
    }));
    let mut members: Vec<String> = new_type
        .constants
        .iter()
        .map(|constant| format!("typeof {}", constant.name))
        .collect();
    members.push(format!(
        "(string & {{ readonly [{brand}]: {} }})",
        string_literal(new_type.name)
    ));
    sections.push(format!(
        "{NEW_TYPE_COMMENT}\n{}",
        union_alias(new_type.name, &members)
    ));
    sections
}

#[cfg(test)]
mod tests {
    use horsies_core::error_catalog::{NewTypeConstant, RETENTION_CLASS_KEY};

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    const SAMPLE: TextNewType = TextNewType {
        module: "probe_key",
        name: "ProbeKey",
        doc: "",
        constants: &[
            NewTypeConstant {
                name: "FIRST_KEY",
                doc: "",
                value: "first",
            },
            NewTypeConstant {
                name: "SECOND_KEY",
                doc: "",
                value: "second",
            },
        ],
    };

    #[test]
    fn a_new_type_has_its_brand_its_constants_and_its_type() {
        assert_eq!(
            module_text(&new_type_sections(&SAMPLE)),
            format!(
                "{GENERATED_HEADER}\n\
                 \n\
                 declare const probeKeyBrand: unique symbol;\n\
                 \n\
                 export const FIRST_KEY = \"first\";\n\
                 \n\
                 export const SECOND_KEY = \"second\";\n\
                 \n\
                 {NEW_TYPE_COMMENT}\n\
                 export type ProbeKey =\n  \
                 | typeof FIRST_KEY\n  \
                 | typeof SECOND_KEY\n  \
                 | (string & {{ readonly [probeKeyBrand]: \"ProbeKey\" }});\n"
            )
        );
    }

    #[test]
    fn a_new_type_without_constants_is_the_branded_string() {
        let bare = TextNewType {
            constants: &[],
            ..SAMPLE
        };
        assert_eq!(
            new_type_sections(&bare),
            [
                "declare const probeKeyBrand: unique symbol;".to_owned(),
                format!(
                    "{NEW_TYPE_COMMENT}\nexport type ProbeKey = \
                     (string & {{ readonly [probeKeyBrand]: \"ProbeKey\" }});"
                ),
            ]
        );
    }

    #[test]
    fn the_model_renders_the_retention_class_key_and_its_default() {
        let files = generated_files();
        let [file] = files.as_slice() else {
            panic!("one file, got {}", files.len());
        };
        assert_eq!(
            file.relative_path().display().to_string(),
            "src/_catalog/retention_class.ts"
        );
        assert!(file
            .text()
            .contains("\nexport const DEFAULT_RETENTION_CLASS_KEY = \"standard_30d\";\n"));
        assert!(file.text().ends_with(
            "export type RetentionClassKey =\n  | typeof DEFAULT_RETENTION_CLASS_KEY\n  \
             | (string & { readonly [retentionClassKeyBrand]: \"RetentionClassKey\" });\n"
        ));
    }

    #[test]
    fn a_new_type_reference_is_its_module_and_its_name() {
        assert_eq!(
            new_type_reference(&RETENTION_CLASS_KEY),
            ("retention_class", "RetentionClassKey")
        );
    }
}
