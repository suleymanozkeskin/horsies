//! Render integer constants of horsies-core as modules of `_catalog`
//! (`release_bounds.py`, `monitoring_bounds.py`, `broker_bounds.py`). Each
//! constant is a `Final` integer whose value is the core's constant, so a
//! Python bound cannot differ from the bound that the core enforces: `check`
//! fails when a core value changes and the module is not written again.

use horsies_core::monitoring::{
    LIST_ROW_BOUND, MAX_BREAKDOWN_LIMIT, MAX_TASK_PAGE_LIMIT, MAX_TASK_PAGE_REACH,
    MAX_WORKFLOW_RUNS_LIMIT, MONITORING_WINDOW_DEFAULT, MONITORING_WINDOW_MAX,
};
use horsies_core::{MAX_RELEASED_CLAIMS, TASK_LIST_BOUND};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// One integer constant of a generated module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreConstant {
    /// The Python name.
    pub name: &'static str,
    /// The docstring under the constant.
    pub doc: &'static str,
    /// The core's value.
    pub value: i64,
}

/// One generated module of core constants, inside `_catalog`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstantModule {
    /// The file name inside `_catalog`.
    pub file: &'static str,
    /// The module docstring.
    pub doc: &'static str,
    /// The constants, in module order.
    pub constants: &'static [CoreConstant],
}

const RELEASE_CONSTANTS: [CoreConstant; 1] = [CoreConstant {
    name: "MAX_RELEASED_CLAIMS",
    doc:
        "The most claims of one release request (`horsies_release_claims`); the core refuses more.",
    value: MAX_RELEASED_CLAIMS as i64,
}];

/// The bound of the claim release.
pub const RELEASE_BOUNDS: ConstantModule = ConstantModule {
    file: "release_bounds.py",
    doc: "The bound of the claim release of horsies-core, generated from its Rust constant.",
    constants: &RELEASE_CONSTANTS,
};

const MONITORING_CONSTANTS: [CoreConstant; 7] = [
    CoreConstant {
        name: "MONITORING_WINDOW_DEFAULT_SECONDS",
        doc: "The span of the window of a read that gives no bound.",
        value: MONITORING_WINDOW_DEFAULT.num_seconds(),
    },
    CoreConstant {
        name: "MONITORING_WINDOW_MAX_SECONDS",
        doc: "The longest window; a longer one is refused.",
        value: MONITORING_WINDOW_MAX.num_seconds(),
    },
    CoreConstant {
        name: "MAX_TASK_PAGE_REACH",
        doc: "The most `offset + limit` of one task list page.",
        value: MAX_TASK_PAGE_REACH,
    },
    CoreConstant {
        name: "MAX_TASK_PAGE_LIMIT",
        doc: "The most rows of one task list page.",
        value: MAX_TASK_PAGE_LIMIT,
    },
    CoreConstant {
        name: "MAX_BREAKDOWN_LIMIT",
        doc: "The most groups of one task breakdown.",
        value: MAX_BREAKDOWN_LIMIT,
    },
    CoreConstant {
        name: "MAX_WORKFLOW_RUNS_LIMIT",
        doc: "The most runs of one workflow runs list.",
        value: MAX_WORKFLOW_RUNS_LIMIT,
    },
    CoreConstant {
        name: "LIST_ROW_BOUND",
        doc: "The most rows of the workflow names list and of the schedules list; a\n\
              longer list is cut at the bound.",
        value: LIST_ROW_BOUND as i64,
    },
];

/// The bounds of the monitoring reads. The window spans are whole seconds.
pub const MONITORING_BOUNDS: ConstantModule = ConstantModule {
    file: "monitoring_bounds.py",
    doc: "The bounds of the monitoring reads of horsies-core, generated from its Rust\n\
          constants. The window spans are in whole seconds.",
    constants: &MONITORING_CONSTANTS,
};

const BROKER_CONSTANTS: [CoreConstant; 1] = [CoreConstant {
    name: "TASK_LIST_ROW_BOUND",
    doc: "The most rows of the stale task list and of the expired task list; a\n\
          longer list is cut at the bound.",
    value: TASK_LIST_BOUND as i64,
}];

/// The bound of the stale and expired task lists of the broker.
pub const BROKER_BOUNDS: ConstantModule = ConstantModule {
    file: "broker_bounds.py",
    doc: "The bound of the task lists of the broker of horsies-core, generated from its\n\
          Rust constant.",
    constants: &BROKER_CONSTANTS,
};

/// Every generated module of core constants, in file order.
pub const CONSTANT_MODULES: [ConstantModule; 3] =
    [RELEASE_BOUNDS, MONITORING_BOUNDS, BROKER_BOUNDS];

/// The text of one module of core constants.
pub fn render_constant_module(module: &ConstantModule) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    if let Some(doc) = docstring(module.doc, "") {
        text.push_str(&doc);
    }
    text.push_str(
        "\nfrom __future__ import annotations\n\nfrom typing import Final\n\n__all__ = [\n",
    );
    for constant in module.constants {
        text.push_str(&format!("    {},\n", string_literal(constant.name)));
    }
    text.push_str("]\n");
    for constant in module.constants {
        text.push_str(&format!(
            "\n{}: Final = {}\n",
            constant.name, constant.value
        ));
        if let Some(doc) = docstring(constant.doc, "") {
            text.push_str(&doc);
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn a_module_lists_its_constants_and_gives_each_its_value_and_doc() {
        let module = ConstantModule {
            file: "bounds.py",
            doc: "Bounds.",
            constants: &[
                CoreConstant {
                    name: "MAX_ROWS",
                    doc: "The most rows.",
                    value: 1_000,
                },
                CoreConstant {
                    name: "SPAN_SECONDS",
                    doc: "",
                    value: 86_400,
                },
            ],
        };
        assert_eq!(
            render_constant_module(&module),
            format!(
                "{GENERATED_HEADER}\n\"\"\"Bounds.\"\"\"\n\nfrom __future__ import annotations\n\n\
                 from typing import Final\n\n__all__ = [\n    'MAX_ROWS',\n    'SPAN_SECONDS',\n]\n\n\
                 MAX_ROWS: Final = 1000\n\"\"\"The most rows.\"\"\"\n\nSPAN_SECONDS: Final = 86400\n"
            )
        );
    }

    #[test]
    fn each_module_holds_the_core_values() {
        let rendered = render_constant_module(&RELEASE_BOUNDS);
        assert!(rendered.contains(&format!(
            "\nMAX_RELEASED_CLAIMS: Final = {MAX_RELEASED_CLAIMS}\n"
        )));
        let rendered = render_constant_module(&MONITORING_BOUNDS);
        for (name, value) in [
            (
                "MONITORING_WINDOW_DEFAULT_SECONDS",
                MONITORING_WINDOW_DEFAULT.num_seconds(),
            ),
            (
                "MONITORING_WINDOW_MAX_SECONDS",
                MONITORING_WINDOW_MAX.num_seconds(),
            ),
            ("MAX_TASK_PAGE_REACH", MAX_TASK_PAGE_REACH),
            ("MAX_TASK_PAGE_LIMIT", MAX_TASK_PAGE_LIMIT),
            ("MAX_BREAKDOWN_LIMIT", MAX_BREAKDOWN_LIMIT),
            ("MAX_WORKFLOW_RUNS_LIMIT", MAX_WORKFLOW_RUNS_LIMIT),
            ("LIST_ROW_BOUND", LIST_ROW_BOUND as i64),
        ] {
            assert!(
                rendered.contains(&format!("\n{name}: Final = {value}\n")),
                "{name}"
            );
        }
        let rendered = render_constant_module(&BROKER_BOUNDS);
        assert!(rendered.contains(&format!(
            "\nTASK_LIST_ROW_BOUND: Final = {TASK_LIST_BOUND}\n"
        )));
    }

    /// A span in whole seconds loses nothing of the core's span.
    #[test]
    fn the_window_spans_are_whole_seconds() {
        assert_eq!(MONITORING_WINDOW_DEFAULT.subsec_nanos(), 0);
        assert_eq!(MONITORING_WINDOW_MAX.subsec_nanos(), 0);
    }

    #[test]
    fn names_and_files_are_distinct() {
        let files: BTreeSet<&str> = CONSTANT_MODULES.iter().map(|module| module.file).collect();
        assert_eq!(files.len(), CONSTANT_MODULES.len());
        for module in &CONSTANT_MODULES {
            let names: BTreeSet<&str> = module
                .constants
                .iter()
                .map(|constant| constant.name)
                .collect();
            assert_eq!(names.len(), module.constants.len(), "{}", module.file);
        }
    }
}
