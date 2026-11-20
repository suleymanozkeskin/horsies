//! Render `_core.pyi`, the type stubs of `horsies._core`.

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};
use crate::surface::{
    Constructor, ExceptionBody, ModuleSurface, ParameterKind, StubClass, StubException,
    StubFunction, StubImport, StubParameter, StubProperty, StubStaticMethod, TypeText,
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
    text.push_str(&render_imports(surface.imports));
    for exception in surface.exceptions {
        text.push_str("\n\n");
        text.push_str(&render_exception(exception));
    }
    for class in surface.classes {
        text.push_str("\n\n");
        text.push_str(&render_class(class, surface.static_methods));
    }
    for function in surface.functions {
        text.push_str("\n\n");
        text.push_str(&render_function(function, ""));
    }
    text
}

/// The package imports, after a blank line; nothing when there are none.
fn render_imports(imports: &[StubImport]) -> String {
    match imports.is_empty() {
        true => String::new(),
        false => {
            let mut text = String::from("\n");
            for import in imports {
                text.push_str(&format!(
                    "from {} import {}\n",
                    import.module,
                    import.names.join(", ")
                ));
            }
            text
        }
    }
}

/// An exception class: its doc, then, for a class with attributes, its
/// constructor and its read-only attributes.
fn render_exception(exception: &StubException) -> String {
    let mut text = format!("class {}({}):\n", exception.name, exception.base);
    let doc = docstring(exception.doc, "    ");
    match exception.body {
        ExceptionBody::ArgsOnly => match doc {
            Some(doc) => text.push_str(&doc),
            None => text.push_str("    ...\n"),
        },
        ExceptionBody::Attributes {
            constructor,
            properties,
        } => {
            let empty_body = doc.is_none()
                && matches!(constructor, Constructor::NoConstructor)
                && properties.is_empty();
            if let Some(doc) = doc {
                text.push_str(&doc);
            }
            text.push_str(&render_members(constructor, properties));
            if empty_body {
                text.push_str("    ...\n");
            }
        }
    }
    text
}

/// The `__init__` of `constructor` and one read-only property per entry of
/// `properties`, indented as members of a class.
fn render_members(constructor: Constructor, properties: &[StubProperty]) -> String {
    let mut text = String::new();
    if let Constructor::Parameters(parameters) = constructor {
        text.push_str(&format!(
            "    def __init__(self{}) -> None: ...\n",
            parameter_list(parameters)
        ));
    }
    for property in properties {
        text.push_str(&format!(
            "    @property\n    def {}(self) -> {}: ...\n",
            property.name,
            type_text(&property.annotation)
        ));
    }
    text
}

fn render_class(class: &StubClass, static_methods: &[StubStaticMethod]) -> String {
    let mut text = format!("@final\nclass {}:\n", class.name);
    if let Some(doc) = docstring(class.doc, "    ") {
        text.push_str(&doc);
    }
    text.push_str(&render_members(class.constructor, class.properties));
    for method in class.methods {
        text.push_str(&render_function(method, "    "));
    }
    let own_static_methods: Vec<&StubStaticMethod> = static_methods
        .iter()
        .filter(|method| method.class_name == class.name)
        .collect();
    for method in &own_static_methods {
        text.push_str(&render_static_method(&method.function));
    }
    let empty_body = class.doc.trim().is_empty()
        && matches!(class.constructor, Constructor::NoConstructor)
        && class.properties.is_empty()
        && class.methods.is_empty()
        && own_static_methods.is_empty();
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

/// A static method in its class: `@staticmethod`, then the function with no
/// `self`.
fn render_static_method(function: &StubFunction) -> String {
    let free = render_function(function, "");
    let indented: Vec<String> = free
        .lines()
        .map(|line| match line.is_empty() {
            true => String::new(),
            false => format!("    {line}"),
        })
        .collect();
    format!("    @staticmethod\n{}\n", indented.join("\n"))
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

    /// Two pyclasses of one name in `horsies._core`: the one that the module
    /// registers last replaces the other, and the stub then names one class
    /// twice.
    #[test]
    fn each_exception_class_and_function_of_the_stub_has_its_own_name() {
        let mut seen = std::collections::BTreeSet::new();
        let names = CORE_MODULE
            .exceptions
            .iter()
            .map(|exception| exception.name)
            .chain(CORE_MODULE.classes.iter().map(|class| class.name))
            .chain(CORE_MODULE.functions.iter().map(|function| function.name));
        let repeated: Vec<&str> = names.filter(|name| !seen.insert(*name)).collect();
        assert!(
            repeated.is_empty(),
            "names given twice in horsies._core: {repeated:?}"
        );
    }

    #[test]
    fn an_exception_with_attributes_has_its_constructor_and_its_properties() {
        let exception = StubException {
            name: "ProbeError",
            base: "Exception",
            doc: "A probe failed.",
            body: ExceptionBody::Attributes {
                constructor: Constructor::Parameters(&[StubParameter {
                    name: "subject",
                    annotation: TypeText::Plain("str"),
                    kind: ParameterKind::Positional,
                }]),
                properties: &[StubProperty {
                    name: "subject",
                    annotation: TypeText::Plain("str"),
                }],
            },
        };
        assert_eq!(
            render_exception(&exception),
            "class ProbeError(Exception):\n    \"\"\"A probe failed.\"\"\"\n    \
             def __init__(self, subject: str) -> None: ...\n    @property\n    \
             def subject(self) -> str: ...\n"
        );
    }

    #[test]
    fn the_package_imports_follow_the_standard_library_after_a_blank_line() {
        assert_eq!(render_imports(&[]), "");
        assert_eq!(
            render_imports(&[StubImport {
                module: "horsies._catalog.monitoring_errors",
                names: &["TaskActionCoreError", "TaskActionNotFound"],
            }]),
            "\nfrom horsies._catalog.monitoring_errors import TaskActionCoreError, TaskActionNotFound\n"
        );
        let stub = render_core_stub(&CORE_MODULE);
        assert!(stub.contains(
            "from typing import Literal, final\n\nfrom horsies._catalog.broker_errors import BrokerCoreEnqueueError, BrokerInvalidInput, BrokerInvalidJsonPayload, BrokerListenerDisabled, BrokerOperationFailed\nfrom horsies._catalog.monitoring_errors import MonitoringQueryCoreError, TaskActionCoreError\n"
        ));
    }

    #[test]
    fn an_exception_of_args_only_has_its_doc_only() {
        let exception = StubException {
            name: "ProbeError",
            base: "Exception",
            doc: "",
            body: ExceptionBody::ArgsOnly,
        };
        assert_eq!(
            render_exception(&exception),
            "class ProbeError(Exception):\n    ...\n"
        );
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
    fn a_static_method_has_no_self_and_sits_in_its_class() {
        let stub = render_core_stub(&CORE_MODULE);
        let class_at = stub
            .find("@final\nclass CoreChildHandle:\n")
            .expect("the class is in the stub");
        let method_at = stub
            .find("    @staticmethod\n    def from_init(init: InitFrame) -> CoreChildHandle:\n")
            .expect("the static method is in the stub");
        let next_class_at = stub[class_at + 1..]
            .find("@final\nclass ")
            .map(|offset| class_at + 1 + offset)
            .expect("another class follows");
        assert!(class_at < method_at && method_at < next_class_at);
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
