//! The parse of a scheduler manifest and its refusals.

use serde_json::{json, Value};

use super::*;
use crate::core::config::IntervalSchedule;

/// The send facts of an enabled schedule.
fn send() -> Value {
    json!({
        "task_name": "reports.nightly",
        "queue_name": "default",
        "priority": 100,
        "input_json": "{\"day\":\"monday\",\"limit\":5}",
        "task_options": {"kind": "ENCODED", "json": "{\"task_name\":\"reports.nightly\"}"},
        "retention_class_key": "standard_30d",
        "retain_rerun_input": false
    })
}

/// An enabled schedule of a 60 s interval in Python's pattern text.
fn enabled_schedule(name: &str) -> Value {
    json!({
        "name": name,
        "activation": {
            "kind": "ENABLED",
            "pattern_json":
                "{\"type\":\"interval\",\"seconds\":60,\"minutes\":null,\"hours\":null,\"days\":null}",
            "timezone": "Europe/Berlin",
            "catch_up_missed": true,
            "max_catch_up_runs": 7,
            "send": send()
        }
    })
}

fn manifest(schedules: &Value) -> Value {
    json!({
        "manifest_version": 1,
        "enabled": true,
        "check_interval_seconds": 5,
        "schedules": schedules
    })
}

fn parsed(document: &Value) -> SchedulerManifest {
    parse_scheduler_manifest(&document.to_string())
        .unwrap_or_else(|refused| panic!("the manifest is complete: {refused}"))
}

fn refused_path(document: &Value) -> String {
    parse_scheduler_manifest(&document.to_string())
        .expect_err("the manifest is refused")
        .field_path
}

/// The path of a refusal inside the first schedule's activation. An
/// internally tagged enum reads its content after its tag, so the path ends
/// at the enum.
fn assert_refused_in_first_activation(document: &Value) {
    let path = refused_path(document);
    assert!(path.starts_with("schedules[0].activation"), "{path}");
}

#[test]
fn a_manifest_of_an_enabled_and_a_disabled_schedule_parses() {
    let document = manifest(&json!([
        enabled_schedule("nightly"),
        {"name": "paused", "activation": {"kind": "DISABLED"}}
    ]));

    let manifest = parsed(&document);

    assert_eq!(manifest.manifest_version, SchedulerManifestVersion::CURRENT);
    assert!(manifest.enabled);
    assert_eq!(manifest.check_interval_seconds.0.get(), 5);
    let [nightly, paused] = manifest.schedules.as_slice() else {
        panic!("two schedules");
    };
    assert_eq!(paused.name, "paused");
    assert!(matches!(paused.activation, ScheduleActivation::Disabled));
    assert_eq!(nightly.name, "nightly");
    let ScheduleActivation::Enabled {
        pattern_json,
        timezone,
        catch_up_missed,
        max_catch_up_runs,
        send,
    } = &nightly.activation
    else {
        panic!("the first schedule is enabled");
    };
    assert!(matches!(
        pattern_json.pattern(),
        SchedulePattern::Interval(IntervalSchedule {
            seconds: Some(60),
            minutes: None,
            hours: None,
            days: None,
        })
    ));
    assert_eq!(timezone, "Europe/Berlin");
    assert!(*catch_up_missed);
    assert_eq!(max_catch_up_runs.0.get(), 7);
    assert_eq!(send.task_name, "reports.nightly");
    assert_eq!(send.priority.0.get(), 100);
    assert_eq!(send.input_json, "{\"day\":\"monday\",\"limit\":5}");
    assert_eq!(
        send.task_options.text(),
        Some("{\"task_name\":\"reports.nightly\"}")
    );
    assert_eq!(
        send.retention_class_key.retention().class_key(),
        "standard_30d"
    );
    assert!(!send.retain_rerun_input);
}

#[test]
fn a_task_without_options_and_a_forever_class_parse() {
    let mut schedule = enabled_schedule("forever");
    schedule["activation"]["send"]["task_options"] = json!({"kind": "NO_OPTIONS"});
    schedule["activation"]["send"]["retention_class_key"] = json!("forever");

    let manifest = parsed(&manifest(&json!([schedule])));

    let ScheduleActivation::Enabled { send, .. } = &manifest.schedules[0].activation else {
        panic!("the schedule is enabled");
    };
    assert_eq!(send.task_options.text(), None);
    assert_eq!(
        send.retention_class_key.retention(),
        EnqueueRetention::Forever
    );
}

#[test]
fn a_manifest_without_schedules_parses() {
    let mut document = manifest(&json!([]));
    document["enabled"] = json!(false);
    let manifest = parsed(&document);
    assert!(!manifest.enabled);
    assert!(manifest.schedules.is_empty());
}

#[test]
fn a_repeated_schedule_name_is_refused_at_the_repeat() {
    let document = manifest(&json!([
        enabled_schedule("nightly"),
        {"name": "other", "activation": {"kind": "DISABLED"}},
        {"name": "nightly", "activation": {"kind": "DISABLED"}}
    ]));
    assert_eq!(refused_path(&document), "schedules[2].name");
}

#[test]
fn another_manifest_version_is_refused() {
    let mut document = manifest(&json!([]));
    document["manifest_version"] = json!(2);
    assert_eq!(refused_path(&document), "manifest_version");
}

#[test]
fn an_unknown_field_is_refused_with_its_path() {
    let mut schedule = enabled_schedule("nightly");
    schedule["activation"]["send"]["args_json"] = json!("[]");
    assert_refused_in_first_activation(&manifest(&json!([schedule])));
}

#[test]
fn a_check_interval_outside_one_to_sixty_seconds_is_refused() {
    for seconds in [0, 61] {
        let mut document = manifest(&json!([]));
        document["check_interval_seconds"] = json!(seconds);
        assert_eq!(refused_path(&document), "check_interval_seconds");
    }
}

#[test]
fn a_catch_up_bound_outside_one_to_ten_thousand_is_refused() {
    for runs in [0, 10_001] {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["max_catch_up_runs"] = json!(runs);
        assert_refused_in_first_activation(&manifest(&json!([schedule])));
    }
}

#[test]
fn a_priority_outside_one_to_one_hundred_is_refused() {
    for priority in [0, 101] {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["send"]["priority"] = json!(priority);
        assert_refused_in_first_activation(&manifest(&json!([schedule])));
    }
}

/// Pattern texts that the parser refuses: not JSON, not a pattern kind, a
/// field the pattern does not have, and a pattern that breaks its rules.
#[test]
fn a_pattern_text_that_is_not_a_valid_pattern_is_refused() {
    for text in [
        "not json",
        "{\"type\":\"yearly\",\"day\":1}",
        "{\"type\":\"hourly\",\"minute\":5,\"second\":0,\"offset\":1}",
        "{\"type\":\"interval\",\"seconds\":0,\"minutes\":null,\"hours\":null,\"days\":null}",
        "{\"type\":\"interval\",\"seconds\":null,\"minutes\":null,\"hours\":null,\"days\":null}",
        "{\"type\":\"weekly\",\"days\":[\"monday\",\"monday\"],\"time\":\"09:00:00\"}",
    ] {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["pattern_json"] = json!(text);
        let path = refused_path(&manifest(&json!([schedule])));
        assert!(
            path.starts_with("schedules[0].activation"),
            "{text}: {path}"
        );
    }
}

/// The pattern texts of a daily, a weekly and a monthly schedule at `time`.
fn timed_patterns(time: &str) -> [String; 3] {
    [
        format!("{{\"type\":\"daily\",\"time\":\"{time}\"}}"),
        format!("{{\"type\":\"weekly\",\"days\":[\"monday\"],\"time\":\"{time}\"}}"),
        format!("{{\"type\":\"monthly\",\"day\":1,\"time\":\"{time}\"}}"),
    ]
}

/// The zone of a schedule comes only from its `timezone`: Python refuses a
/// time with `tzinfo` at config, and a time of day with a UTC offset
/// (`09:00:00+02:00`) has no form in the core's pattern, so the parser
/// refuses the text in each pattern that has a time.
#[test]
fn a_time_of_day_with_an_offset_is_refused() {
    for text in timed_patterns("09:00:00+02:00") {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["pattern_json"] = json!(text);
        let refused = parse_scheduler_manifest(&manifest(&json!([schedule])).to_string())
            .expect_err("a time with an offset is refused");
        assert!(
            refused.field_path.starts_with("schedules[0].activation"),
            "{text}: {}",
            refused.field_path
        );
        assert!(
            refused.cause.starts_with("value does not fit the field"),
            "{text}: {}",
            refused.cause
        );
    }
}

/// Slots fire at whole seconds: Python refuses a time with microseconds at
/// config, and the parser refuses a time of day with a fraction of a second
/// in each pattern that has a time, so the calculator never sees one. The
/// refusal names the field and the error class, never the value; the rule
/// is `SchedulePattern::validate` (its test names it), and the same texts at
/// a whole second parse (`a_time_of_day_in_whole_seconds_parses`).
#[test]
fn a_time_of_day_with_a_fraction_of_a_second_is_refused() {
    for text in timed_patterns("09:00:00.000500") {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["pattern_json"] = json!(text);
        let refused = parse_scheduler_manifest(&manifest(&json!([schedule])).to_string())
            .expect_err("a time with microseconds is refused");
        assert!(
            refused.field_path.starts_with("schedules[0].activation"),
            "{text}: {}",
            refused.field_path
        );
        assert!(
            refused.cause.starts_with("value does not fit the field"),
            "{text}: {}",
            refused.cause
        );
    }
}

/// The same patterns at a whole second parse.
#[test]
fn a_time_of_day_in_whole_seconds_parses() {
    for text in timed_patterns("09:00:00") {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["pattern_json"] = json!(text);
        let manifest = parsed(&manifest(&json!([schedule])));
        assert_eq!(manifest.schedules.len(), 1, "{text}");
    }
}

#[test]
fn a_retention_key_outside_the_class_key_rule_is_refused_without_its_text() {
    for key in [
        "standard_0d",
        "standard_030d",
        "keep",
        "postgresql://u:secret@h/d",
    ] {
        let mut schedule = enabled_schedule("nightly");
        schedule["activation"]["send"]["retention_class_key"] = json!(key);
        let refused = parse_scheduler_manifest(&manifest(&json!([schedule])).to_string())
            .expect_err("the key is refused");
        assert!(
            refused.field_path.starts_with("schedules[0].activation"),
            "{key}: {}",
            refused.field_path
        );
        assert!(!refused.to_string().contains("secret"), "{refused}");
    }
}
