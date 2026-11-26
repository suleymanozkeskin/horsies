//! Renders the producer metrics of the core's metric catalog as
//! `producer_metrics.ts`: the name, unit and description of
//! `horsies.task.sent` and `horsies.task.send.duration`, the bucket bounds of
//! the duration in seconds, the attribute keys of their labels, and the
//! `ok` value of the send result.
//!
//! The TypeScript client records the two metrics with the user's
//! OpenTelemetry meter, so each name, text and bound comes from the core
//! catalog. The description of a metric is data (a string constant), not a
//! doc comment.

use std::time::Duration;

use horsies_core::telemetry::catalog::{seconds_text, MetricId, MetricKind, MetricSpec, Recorder};
use horsies_core::telemetry::labels::{LabelName, SEND_OK_RESULT};

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{integer_literal, module_text, string_literal};

/// The stem of the module in the catalog folder.
const PRODUCER_METRICS_MODULE: &str = "producer_metrics";

/// The labels of `horsies.task.sent` that the client records, in catalog
/// order.
const SENT_LABELS: [LabelName; 3] = [LabelName::Task, LabelName::Queue, LabelName::Result];

/// The labels of `horsies.task.send.duration` that the client records, in
/// catalog order.
const DURATION_LABELS: [LabelName; 1] = [LabelName::Queue];

/// The module of the producer metrics. Pure.
///
/// Panics: see `producer_metrics_text`.
pub fn generated_files() -> Vec<GeneratedFile> {
    vec![catalog_file(
        PRODUCER_METRICS_MODULE,
        producer_metrics_text(MetricId::TaskSent.spec(), MetricId::TaskSendDuration.spec()),
    )]
}

/// The text of the module from the catalog entry of the send counter and of
/// the send duration. Pure.
///
/// Panics: an entry is not a producer metric of the kind and the labels
/// that the TypeScript client records, or a bucket bound does not read back
/// as the same number. Each is a change of the catalog that the client does
/// not follow; a defect.
fn producer_metrics_text(sent: &MetricSpec, duration: &MetricSpec) -> String {
    assert_producer_metric(sent, &SENT_LABELS);
    assert_producer_metric(duration, &DURATION_LABELS);
    assert_counter(sent);
    let bounds = histogram_bounds(duration);
    let sections = [
        string_constant("TASK_SENT_NAME", sent.name),
        string_constant("TASK_SENT_UNIT", sent.unit.symbol()),
        string_constant("TASK_SENT_DESCRIPTION", sent.help),
        string_constant("TASK_SEND_DURATION_NAME", duration.name),
        string_constant("TASK_SEND_DURATION_UNIT", duration.unit.symbol()),
        string_constant("TASK_SEND_DURATION_DESCRIPTION", duration.help),
        seconds_array_constant("TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS", bounds),
        string_constant("TASK_ATTRIBUTE", LabelName::Task.attribute_key()),
        string_constant("QUEUE_ATTRIBUTE", LabelName::Queue.attribute_key()),
        string_constant("RESULT_ATTRIBUTE", LabelName::Result.attribute_key()),
        string_constant("SEND_OK_RESULT", SEND_OK_RESULT),
    ];
    module_text(&sections)
}

/// Panics: `spec` is not recorded by the producer, or its labels are not
/// `labels` in this order.
fn assert_producer_metric(spec: &MetricSpec, labels: &[LabelName]) {
    match spec.recorder {
        Recorder::Producer => {}
        Recorder::Worker | Recorder::FleetCollector => panic!(
            "{} is recorded by the {}, not by the producer",
            spec.name,
            spec.recorder.as_str()
        ),
    }
    let names: Vec<LabelName> = spec.labels.iter().map(|label| label.name).collect();
    assert_eq!(
        names, labels,
        "the labels of {} that the TypeScript client records",
        spec.name
    );
}

/// Panics: `spec` is not a counter.
fn assert_counter(spec: &MetricSpec) {
    match spec.kind {
        MetricKind::Counter => {}
        MetricKind::Gauge | MetricKind::Histogram(_) => panic!(
            "{} is recorded as a counter, not as {:?}",
            spec.name, spec.kind
        ),
    }
}

/// The bucket upper bounds of `spec`.
///
/// Panics: `spec` is not a histogram.
fn histogram_bounds(spec: &MetricSpec) -> &'static [Duration] {
    match spec.kind {
        MetricKind::Histogram(bounds) => bounds,
        MetricKind::Counter | MetricKind::Gauge => panic!(
            "{} is recorded as a histogram, not as {:?}",
            spec.name, spec.kind
        ),
    }
}

/// `export const NAME = "<text>";`. Pure.
fn string_constant(name: &str, text: &str) -> String {
    format!("export const {name} = {};", string_literal(text))
}

/// A frozen array of the bounds in seconds, one bound per line. Pure.
///
/// Panics: see `bucket_bound_literal`.
fn seconds_array_constant(name: &str, bounds: &[Duration]) -> String {
    let mut text = format!("export const {name}: ReadonlyArray<number> = Object.freeze([\n");
    for bound in bounds {
        text.push_str("  ");
        text.push_str(&bucket_bound_literal(*bound));
        text.push_str(",\n");
    }
    text.push_str("]);");
    text
}

/// The number literal of a bucket bound in seconds: the catalog's decimal
/// text of the bound (`seconds_text`, the text of the `le` label), with a
/// numeric separator between groups of three digits of the whole seconds
/// when they have five digits or more (`86_400`). The fraction keeps no
/// separator: the oxlint rule `unicorn/numeric-separators-style` refuses a
/// separator in a fraction (`0.000_01`) and accepts a fraction of any
/// length without one. Pure.
///
/// Panics: the decimal reads back as a number that prints as another
/// decimal, so a `number` does not hold the bound. A defect of the catalog.
fn bucket_bound_literal(bound: Duration) -> String {
    let decimal = seconds_text(bound);
    let read_back: f64 = decimal
        .parse()
        .expect("the seconds text of a bound is a decimal number");
    assert_eq!(
        read_back.to_string(),
        decimal,
        "the bucket bound {decimal} s does not read back as the same number"
    );
    let whole_seconds = integer_literal(i128::from(bound.as_secs()));
    match decimal.split_once('.') {
        Some((_whole_digits, fraction)) => format!("{whole_seconds}.{fraction}"),
        None => whole_seconds,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use horsies_core::telemetry::catalog::{LabelSpec, MetricUnit};
    use horsies_core::telemetry::labels::LabelValues;

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    static SENT_LABEL_SPECS: [LabelSpec; 3] = [
        LabelSpec {
            name: LabelName::Task,
            values: LabelValues::Fixed(&[]),
        },
        LabelSpec {
            name: LabelName::Queue,
            values: LabelValues::Fixed(&[]),
        },
        LabelSpec {
            name: LabelName::Result,
            values: LabelValues::Fixed(&[]),
        },
    ];

    static QUEUE_LABEL_SPECS: [LabelSpec; 1] = [LabelSpec {
        name: LabelName::Queue,
        values: LabelValues::Fixed(&[]),
    }];

    static SAMPLE_BOUNDS: [Duration; 3] = [
        Duration::from_millis(5),
        Duration::from_millis(2_500),
        Duration::from_secs(21_600),
    ];

    static SAMPLE_SENT: MetricSpec = MetricSpec {
        name: "sample.sent",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
        recorder: Recorder::Producer,
        help: "Sends, by \"result\".",
        labels: &SENT_LABEL_SPECS,
    };

    static SAMPLE_DURATION: MetricSpec = MetricSpec {
        name: "sample.send.duration",
        kind: MetricKind::Histogram(&SAMPLE_BOUNDS),
        unit: MetricUnit::Seconds,
        recorder: Recorder::Producer,
        help: "Time of one send.",
        labels: &QUEUE_LABEL_SPECS,
    };

    /// The text of the module that the renderer writes from the catalog.
    fn catalog_module_text() -> String {
        let files = generated_files();
        assert_eq!(files.len(), 1);
        let file = files.first().expect("the module is generated");
        assert_eq!(
            file.relative_path(),
            Path::new("src/_catalog/producer_metrics.ts")
        );
        file.text().to_owned()
    }

    #[test]
    fn a_module_holds_the_names_units_descriptions_bounds_and_keys() {
        assert_eq!(
            producer_metrics_text(&SAMPLE_SENT, &SAMPLE_DURATION),
            format!(
                r#"{GENERATED_HEADER}

export const TASK_SENT_NAME = "sample.sent";

export const TASK_SENT_UNIT = "1";

export const TASK_SENT_DESCRIPTION = "Sends, by \"result\".";

export const TASK_SEND_DURATION_NAME = "sample.send.duration";

export const TASK_SEND_DURATION_UNIT = "s";

export const TASK_SEND_DURATION_DESCRIPTION = "Time of one send.";

export const TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS: ReadonlyArray<number> = Object.freeze([
  0.005,
  2.5,
  21_600,
]);

export const TASK_ATTRIBUTE = "horsies.task.name";

export const QUEUE_ATTRIBUTE = "horsies.queue";

export const RESULT_ATTRIBUTE = "horsies.result";

export const SEND_OK_RESULT = "ok";
"#
            )
        );
    }

    #[test]
    fn the_module_holds_the_catalog_values_of_the_producer_metrics() {
        let text = catalog_module_text();
        for line in [
            "export const TASK_SENT_NAME = \"horsies.task.sent\";",
            "export const TASK_SENT_UNIT = \"1\";",
            "export const TASK_SEND_DURATION_NAME = \"horsies.task.send.duration\";",
            "export const TASK_SEND_DURATION_UNIT = \"s\";",
            "export const TASK_ATTRIBUTE = \"horsies.task.name\";",
            "export const QUEUE_ATTRIBUTE = \"horsies.queue\";",
            "export const RESULT_ATTRIBUTE = \"horsies.result\";",
            "export const SEND_OK_RESULT = \"ok\";",
        ] {
            assert!(text.contains(&format!("\n{line}\n")), "{line}");
        }
        for (name, help) in [
            ("TASK_SENT_DESCRIPTION", MetricId::TaskSent.spec().help),
            (
                "TASK_SEND_DURATION_DESCRIPTION",
                MetricId::TaskSendDuration.spec().help,
            ),
        ] {
            assert!(
                text.contains(&format!(
                    "\nexport const {name} = {};\n",
                    string_literal(help)
                )),
                "{name}"
            );
        }
        assert_eq!(text.matches("export const ").count(), 11);
    }

    #[test]
    fn the_bounds_are_the_21_catalog_bounds_and_read_back_as_the_same_numbers() {
        let text = catalog_module_text();
        let bounds = histogram_bounds(MetricId::TaskSendDuration.spec());
        let array = text
            .split("Object.freeze([\n")
            .nth(1)
            .and_then(|rest| rest.split("]);").next())
            .expect("the module has the array of the bounds");
        let literals: Vec<&str> = array
            .lines()
            .map(|line| line.trim().trim_end_matches(','))
            .collect();
        assert_eq!(literals.len(), 21);
        assert_eq!(literals.len(), bounds.len());
        for (literal, bound) in literals.iter().zip(bounds) {
            let read_back: f64 = literal
                .replace('_', "")
                .parse()
                .expect("a bound literal is a number");
            assert_eq!(
                read_back.to_bits(),
                bound.as_secs_f64().to_bits(),
                "{literal}"
            );
        }
        assert_eq!(literals.first(), Some(&"0.005"));
        assert_eq!(literals.last(), Some(&"86_400"));
    }

    #[test]
    fn a_bound_groups_its_whole_seconds_and_keeps_its_fraction() {
        assert_eq!(bucket_bound_literal(Duration::from_millis(5)), "0.005");
        assert_eq!(bucket_bound_literal(Duration::from_secs(7_200)), "7200");
        assert_eq!(bucket_bound_literal(Duration::from_secs(86_400)), "86_400");
        assert_eq!(
            bucket_bound_literal(Duration::new(12_345, 678_900_000)),
            "12_345.6789"
        );
        assert_eq!(bucket_bound_literal(Duration::from_nanos(1)), "0.000000001");
    }

    #[test]
    #[should_panic(
        expected = "the bucket bound 9007199254740993 s does not read back as the same number"
    )]
    fn a_bound_that_a_number_does_not_hold_is_a_defect() {
        let _literal = bucket_bound_literal(Duration::from_secs(9_007_199_254_740_993));
    }

    #[test]
    #[should_panic(expected = "sample.sent is recorded by the worker, not by the producer")]
    fn a_metric_of_another_recorder_is_a_defect() {
        let worker_metric = MetricSpec {
            recorder: Recorder::Worker,
            ..SAMPLE_SENT
        };
        let _text = producer_metrics_text(&worker_metric, &SAMPLE_DURATION);
    }

    #[test]
    #[should_panic(
        expected = "the labels of sample.send.duration that the TypeScript client records"
    )]
    fn a_metric_with_other_labels_is_a_defect() {
        let relabelled = MetricSpec {
            labels: &SENT_LABEL_SPECS,
            ..SAMPLE_DURATION
        };
        let _text = producer_metrics_text(&SAMPLE_SENT, &relabelled);
    }

    #[test]
    #[should_panic(expected = "sample.sent is recorded as a counter, not as Gauge")]
    fn a_send_counter_of_another_kind_is_a_defect() {
        let gauge = MetricSpec {
            kind: MetricKind::Gauge,
            ..SAMPLE_SENT
        };
        let _text = producer_metrics_text(&gauge, &SAMPLE_DURATION);
    }

    #[test]
    #[should_panic(expected = "sample.send.duration is recorded as a histogram, not as Counter")]
    fn a_send_duration_of_another_kind_is_a_defect() {
        let counter = MetricSpec {
            kind: MetricKind::Counter,
            ..SAMPLE_DURATION
        };
        let _text = producer_metrics_text(&SAMPLE_SENT, &counter);
    }
}
