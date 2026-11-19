//! The Prometheus text exposition (format 0.0.4) of a registry.
//!
//! - One `# HELP` and one `# TYPE` line per metric, then its series.
//! - The metrics in the order of their Prometheus names; the series of a
//!   metric in the order of their label values, label by label. The same
//!   values give the same text.
//! - The labels of a line in the label order of the catalog; `le` last.
//! - A label value escapes `\`, `"` and the line feed; a help text escapes
//!   `\` and the line feed.
//! - A histogram series gives one `_bucket` line per bound (cumulative),
//!   `le="+Inf"`, `_sum` (seconds) and `_count`. `+Inf` and `_count` are
//!   the same number.

use std::time::Duration;

use crate::telemetry::catalog::{seconds_text, MetricKind, MetricSpec};
use crate::telemetry::registry::{FamilySnapshot, Registry, SampleValue, SeriesSnapshot};

/// The `Content-Type` of the text exposition.
pub const PROMETHEUS_TEXT_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The text exposition of `registry`.
pub fn render(registry: &Registry) -> String {
    render_snapshot(registry.snapshot())
}

/// The text exposition of the metrics of `families`.
pub fn render_snapshot(families: Vec<FamilySnapshot>) -> String {
    let mut named: Vec<(String, FamilySnapshot)> = families
        .into_iter()
        .map(|family| (family.metric.spec().prometheus_name(), family))
        .collect();
    named.sort_by(|left, right| left.0.cmp(&right.0));
    let mut text = String::new();
    for (name, family) in named {
        render_family(&mut text, &name, family);
    }
    text
}

fn render_family(text: &mut String, name: &str, family: FamilySnapshot) {
    let spec = family.metric.spec();
    text.push_str(&format!("# HELP {name} {}\n", escape_help(spec.help)));
    text.push_str(&format!("# TYPE {name} {}\n", spec.kind.prometheus_type()));
    let mut series = family.series;
    series.sort_by(|left, right| left.label_texts.cmp(&right.label_texts));
    for one in series {
        render_series(text, name, spec, one);
    }
}

fn render_series(text: &mut String, name: &str, spec: &MetricSpec, series: SeriesSnapshot) {
    let pairs: Vec<(&str, &str)> = spec
        .labels
        .iter()
        .map(|label| label.name.prometheus_name())
        .zip(series.label_texts.iter().map(String::as_str))
        .collect();
    match (series.value, spec.kind) {
        (SampleValue::Counter(count), _) => {
            text.push_str(&format!("{name}{} {count}\n", label_block(&pairs, None)));
        }
        (SampleValue::Gauge(value), _) => {
            text.push_str(&format!(
                "{name}{} {}\n",
                label_block(&pairs, None),
                gauge_text(value)
            ));
        }
        (
            SampleValue::Histogram {
                buckets,
                sum_micros,
            },
            MetricKind::Histogram(bounds),
        ) => render_histogram(text, name, &pairs, bounds, &buckets, sum_micros),
        (SampleValue::Histogram { .. }, MetricKind::Counter | MetricKind::Gauge) => {
            unreachable!("a histogram series belongs to a histogram metric")
        }
    }
}

fn render_histogram(
    text: &mut String,
    name: &str,
    pairs: &[(&str, &str)],
    bounds: &[Duration],
    buckets: &[u64],
    sum_micros: u64,
) {
    let mut cumulative: u64 = 0;
    for (bound, count) in bounds.iter().zip(buckets) {
        cumulative = cumulative.saturating_add(*count);
        let le = seconds_text(*bound);
        text.push_str(&format!(
            "{name}_bucket{} {cumulative}\n",
            label_block(pairs, Some(le.as_str()))
        ));
    }
    let total: u64 = buckets
        .iter()
        .fold(0_u64, |total, count| total.saturating_add(*count));
    text.push_str(&format!(
        "{name}_bucket{} {total}\n",
        label_block(pairs, Some("+Inf"))
    ));
    text.push_str(&format!(
        "{name}_sum{} {}\n",
        label_block(pairs, None),
        seconds_text(Duration::from_micros(sum_micros))
    ));
    text.push_str(&format!(
        "{name}_count{} {total}\n",
        label_block(pairs, None)
    ));
}

/// `{name="value",...}` with `le` last, or nothing for no label.
fn label_block(pairs: &[(&str, &str)], le: Option<&str>) -> String {
    let mut parts: Vec<String> = pairs
        .iter()
        .map(|(name, value)| format!("{name}=\"{}\"", escape_label_value(value)))
        .collect();
    if let Some(bound) = le {
        parts.push(format!("le=\"{bound}\""));
    }
    match parts.is_empty() {
        true => String::new(),
        false => format!("{{{}}}", parts.join(",")),
    }
}

/// A label value with `\`, `"` and the line feed escaped.
pub fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// A help text with `\` and the line feed escaped.
pub fn escape_help(help: &str) -> String {
    let mut escaped = String::with_capacity(help.len());
    for character in help.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// A gauge value: `NaN`, `+Inf`, `-Inf`, or the shortest decimal text that
/// reads back as the same `f64`.
pub fn gauge_text(value: f64) -> String {
    match (
        value.is_nan(),
        value.is_infinite(),
        value.is_sign_positive(),
    ) {
        (true, _, _) => "NaN".to_owned(),
        (false, true, true) => "+Inf".to_owned(),
        (false, true, false) => "-Inf".to_owned(),
        (false, false, _) => format!("{value}"),
    }
}
