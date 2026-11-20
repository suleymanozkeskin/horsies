//! Render the producer metrics of the core's metric catalog as
//! `_catalog/producer_metrics.py`: the name, unit and help text of
//! `horsies.task.sent` and `horsies.task.send.duration`, the attribute keys
//! of their labels, the `ok` value of the send result, and the bucket bounds
//! of the duration. The Python client records the two metrics with the
//! user's OpenTelemetry meter, so each name and bound is written once, in
//! the core catalog.

use horsies_core::telemetry::catalog::{seconds_text, MetricId, MetricKind, MetricSpec, Recorder};
use horsies_core::telemetry::labels::{LabelName, SEND_OK_RESULT};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// The file name of the module inside `_catalog`.
pub const PRODUCER_METRICS_MODULE_FILE: &str = "producer_metrics.py";

const MODULE_DOC: &str =
    "The producer metrics of the metric catalog of horsies-core: the two metrics\n\
     that a producer records with the user's OpenTelemetry meter, the attribute\n\
     keys of their labels, and the bucket bounds of the duration in seconds.";

/// The labels of `horsies.task.sent`, in catalog order.
const SENT_LABELS: [LabelName; 3] = [LabelName::Task, LabelName::Queue, LabelName::Result];

/// The labels of `horsies.task.send.duration`, in catalog order.
const DURATION_LABELS: [LabelName; 1] = [LabelName::Queue];

/// One constant of the module: its Python name, its value as Python text,
/// and its docstring.
struct Constant {
    name: &'static str,
    value: String,
    doc: &'static str,
}

/// The text of `_catalog/producer_metrics.py`.
///
/// Panics: the catalog entry of a producer metric is not a producer metric
/// of the kind and the labels that the Python client records (a change of
/// the catalog that the client does not follow; a defect).
pub fn render_producer_metrics_module() -> String {
    let sent = producer_spec(MetricId::TaskSent, &SENT_LABELS);
    let duration = producer_spec(MetricId::TaskSendDuration, &DURATION_LABELS);
    assert!(
        matches!(sent.kind, MetricKind::Counter),
        "{} is recorded as a counter",
        sent.name
    );
    let bounds = match duration.kind {
        MetricKind::Histogram(bounds) => bounds,
        other => panic!(
            "{} is recorded as a histogram, not {other:?}",
            duration.name
        ),
    };
    let bound_texts: Vec<String> = bounds.iter().map(|bound| seconds_text(*bound)).collect();
    let constants = [
        Constant {
            name: "TASK_SENT_NAME",
            value: string_literal(sent.name),
            doc: "The name of the counter of sends.",
        },
        Constant {
            name: "TASK_SENT_UNIT",
            value: string_literal(sent.unit.symbol()),
            doc: "The unit of the counter of sends.",
        },
        Constant {
            name: "TASK_SENT_DESCRIPTION",
            value: string_literal(sent.help),
            doc: "The description of the counter of sends.",
        },
        Constant {
            name: "TASK_SEND_DURATION_NAME",
            value: string_literal(duration.name),
            doc: "The name of the histogram of send durations.",
        },
        Constant {
            name: "TASK_SEND_DURATION_UNIT",
            value: string_literal(duration.unit.symbol()),
            doc: "The unit of the histogram of send durations.",
        },
        Constant {
            name: "TASK_SEND_DURATION_DESCRIPTION",
            value: string_literal(duration.help),
            doc: "The description of the histogram of send durations.",
        },
        Constant {
            name: "TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS",
            value: format!("({},)", bound_texts.join(", ")),
            doc: "The upper bounds of the buckets of the send duration, in seconds.",
        },
        Constant {
            name: "TASK_ATTRIBUTE",
            value: string_literal(LabelName::Task.attribute_key()),
            doc: "The attribute key of the task name label.",
        },
        Constant {
            name: "QUEUE_ATTRIBUTE",
            value: string_literal(LabelName::Queue.attribute_key()),
            doc: "The attribute key of the queue label.",
        },
        Constant {
            name: "RESULT_ATTRIBUTE",
            value: string_literal(LabelName::Result.attribute_key()),
            doc: "The attribute key of the send result label: `SEND_OK_RESULT`, or the\n\
                  `TaskSendErrorCode` value of the error.",
        },
        Constant {
            name: "SEND_OK_RESULT",
            value: string_literal(SEND_OK_RESULT),
            doc: "The send result of a send that succeeded.",
        },
    ];
    render_module(&constants)
}

/// The catalog entry of `id`, which must be a producer metric with the
/// labels `labels`.
fn producer_spec(id: MetricId, labels: &[LabelName]) -> &'static MetricSpec {
    let spec = id.spec();
    assert!(
        matches!(spec.recorder, Recorder::Producer),
        "{} is a producer metric",
        spec.name
    );
    let names: Vec<LabelName> = spec.labels.iter().map(|label| label.name).collect();
    assert_eq!(
        names, labels,
        "the labels of {} that the Python client records",
        spec.name
    );
    spec
}

fn render_module(constants: &[Constant]) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    if let Some(doc) = docstring(MODULE_DOC, "") {
        text.push_str(&doc);
    }
    text.push_str(
        "\nfrom __future__ import annotations\n\nfrom typing import Final\n\n__all__ = [\n",
    );
    for constant in constants {
        text.push_str(&format!("    {},\n", string_literal(constant.name)));
    }
    text.push_str("]\n");
    for constant in constants {
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
    use super::*;

    #[test]
    fn the_module_holds_the_catalog_values_of_the_producer_metrics() {
        let rendered = render_producer_metrics_module();
        for line in [
            "TASK_SENT_NAME: Final = 'horsies.task.sent'",
            "TASK_SENT_UNIT: Final = '1'",
            "TASK_SEND_DURATION_NAME: Final = 'horsies.task.send.duration'",
            "TASK_SEND_DURATION_UNIT: Final = 's'",
            "TASK_ATTRIBUTE: Final = 'horsies.task.name'",
            "QUEUE_ATTRIBUTE: Final = 'horsies.queue'",
            "RESULT_ATTRIBUTE: Final = 'horsies.result'",
            "SEND_OK_RESULT: Final = 'ok'",
        ] {
            assert!(rendered.contains(&format!("\n{line}\n")), "{line}");
        }
        assert!(rendered.contains(&format!(
            "\nTASK_SENT_DESCRIPTION: Final = {}\n",
            string_literal(MetricId::TaskSent.spec().help)
        )));
    }

    #[test]
    fn the_bounds_are_the_catalog_bounds_in_seconds() {
        let rendered = render_producer_metrics_module();
        let MetricKind::Histogram(bounds) = MetricId::TaskSendDuration.spec().kind else {
            panic!("the send duration is a histogram");
        };
        let texts: Vec<String> = bounds.iter().map(|bound| seconds_text(*bound)).collect();
        assert!(rendered.contains(&format!(
            "\nTASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS: Final = ({},)\n",
            texts.join(", ")
        )));
        assert!(rendered.contains("(0.005, 0.01, 0.025,"));
        assert!(rendered.contains(", 86400,)\n"));
    }

    #[test]
    fn each_constant_is_listed_once_in_all() {
        let rendered = render_producer_metrics_module();
        let all_block = rendered
            .split("__all__ = [\n")
            .nth(1)
            .and_then(|rest| rest.split("]\n").next())
            .expect("the module has __all__");
        let listed: Vec<&str> = all_block.lines().map(str::trim).collect();
        assert_eq!(listed.len(), 11);
        for name in listed {
            let name = name.trim_end_matches(',').trim_matches('\'');
            assert_eq!(
                rendered.matches(&format!("\n{name}: Final = ")).count(),
                1,
                "{name}"
            );
        }
    }
}
