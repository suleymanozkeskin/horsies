//! Render `_core.pyi`, the type stubs of `horsies._core`.

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};
use crate::surface::{
    Constructor, ModuleSurface, ParameterKind, StubClass, StubFunction, StubParameter, TypeText,
};

/// The file name of the stubs, inside the package folder.
pub const CORE_STUB_FILE: &str = "_core.pyi";

/// The text of `_core.pyi`.
pub fn render_core_stub(surface: &ModuleSurface) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str("\"\"\"Type stubs of the extension module horsies._core.\"\"\"\n\n");
    text.push_str(
        "import asyncio\nimport datetime\nimport uuid\nfrom collections.abc import Callable\nfrom typing import Literal, final\n",
    );
    for exception in surface.exceptions {
        text.push_str("\n\n");
        text.push_str(&format!("class {}({}):\n", exception.name, exception.base));
        match docstring(exception.doc, "    ") {
            Some(doc) => text.push_str(&doc),
            None => text.push_str("    ...\n"),
        }
    }
    for class in surface.classes {
        text.push_str("\n\n");
        text.push_str(&render_class(class));
    }
    for function in surface.functions {
        text.push_str("\n\n");
        text.push_str(&render_function(function, ""));
    }
    text
}

fn render_class(class: &StubClass) -> String {
    let mut text = format!("@final\nclass {}:\n", class.name);
    if let Some(doc) = docstring(class.doc, "    ") {
        text.push_str(&doc);
    }
    if let Constructor::Parameters(parameters) = class.constructor {
        text.push_str(&format!(
            "    def __init__(self{}) -> None: ...\n",
            parameter_list(parameters)
        ));
    }
    for property in class.properties {
        text.push_str(&format!(
            "    @property\n    def {}(self) -> {}: ...\n",
            property.name,
            type_text(&property.annotation)
        ));
    }
    for method in class.methods {
        text.push_str(&render_function(method, "    "));
    }
    let empty_body = class.doc.trim().is_empty()
        && matches!(class.constructor, Constructor::NoConstructor)
        && class.properties.is_empty()
        && class.methods.is_empty();
    if empty_body {
        text.push_str("    ...\n");
    }
    text
}

fn render_function(function: &StubFunction, indent: &str) -> String {
    let receiver = match indent.is_empty() {
        true => "",
        false => "self",
    };
    let parameters = parameter_list(function.parameters);
    let parameters = match receiver.is_empty() {
        // A free function has no `self`, so the list loses its first ", ".
        true => parameters.trim_start_matches(", ").to_owned(),
        false => format!("{receiver}{parameters}"),
    };
    let signature = format!(
        "{indent}def {}({parameters}) -> {}:",
        function.name,
        type_text(&function.returns)
    );
    let body_indent = format!("{indent}    ");
    match docstring(function.doc, &body_indent) {
        Some(doc) => format!("{signature}\n{doc}"),
        None => format!("{signature} ...\n"),
    }
}

/// `, name: type` for each parameter, with `, *` before the first
/// keyword-only one.
fn parameter_list(parameters: &[StubParameter]) -> String {
    let mut text = String::new();
    let mut keyword_only_started = false;
    for parameter in parameters {
        if parameter.kind == ParameterKind::KeywordOnly && !keyword_only_started {
            text.push_str(", *");
            keyword_only_started = true;
        }
        text.push_str(&format!(
            ", {}: {}",
            parameter.name,
            type_text(&parameter.annotation)
        ));
    }
    text
}

fn type_text(annotation: &TypeText) -> String {
    match annotation {
        TypeText::Plain(text) => (*text).to_owned(),
        TypeText::FutureOf(text) => format!("asyncio.Future[{text}]"),
        TypeText::WireValuesOf(catalog) => {
            let values: Vec<String> = catalog
                .members
                .iter()
                .map(|member| string_literal(member.value))
                .collect();
            format!("Literal[{}]", values.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::CodeEnum;

    use super::*;
    use crate::surface::CORE_MODULE;

    #[test]
    fn the_stub_names_every_exception_class_and_function() {
        let stub = render_core_stub(&CORE_MODULE);
        for exception in CORE_MODULE.exceptions {
            assert!(stub.contains(&format!("class {}(", exception.name)));
        }
        for class in CORE_MODULE.classes {
            assert!(stub.contains(&format!("@final\nclass {}:\n", class.name)));
        }
        for function in CORE_MODULE.functions {
            assert!(stub.contains(&format!("\ndef {}(", function.name)));
        }
    }

    #[test]
    fn a_catalog_code_is_typed_by_its_wire_values() {
        let catalog = <horsies_core::BrokerErrorCode as CodeEnum>::CATALOG;
        let rendered = type_text(&TypeText::WireValuesOf(catalog));
        assert!(rendered.starts_with("Literal['"));
        for member in catalog.members {
            assert!(rendered.contains(&string_literal(member.value)));
        }
    }

    #[test]
    fn a_method_takes_self_and_a_function_does_not() {
        let function = StubFunction {
            name: "probe",
            parameters: &[
                StubParameter {
                    name: "value",
                    annotation: TypeText::Plain("int"),
                    kind: ParameterKind::Positional,
                },
                StubParameter {
                    name: "flag",
                    annotation: TypeText::Plain("bool"),
                    kind: ParameterKind::KeywordOnly,
                },
            ],
            returns: TypeText::Plain("None"),
            doc: "",
        };
        assert_eq!(
            render_function(&function, ""),
            "def probe(value: int, *, flag: bool) -> None: ...\n"
        );
        assert_eq!(
            render_function(&function, "    "),
            "    def probe(self, value: int, *, flag: bool) -> None: ...\n"
        );
    }
}
