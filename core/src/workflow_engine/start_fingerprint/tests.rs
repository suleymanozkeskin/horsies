//! Tests of the start command and its fingerprint (pure).

use super::*;

/// The lowercase hex text of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A command of two task nodes, with a value in each element of the
/// canonical array.
fn command() -> StartCommandV1<'static> {
    StartCommandV1 {
        name: "nightly_report",
        definition_key: Some("tests.start_once.nightly.v1"),
        on_error: "fail",
        output_task_index: Some(1),
        success_policy: None,
        nodes: vec![
            StartNodeV1 {
                task_index: 0,
                node_id: Some("fetch"),
                task_name: "fetch",
                input: r#"{"day":"2026-11-23"}"#,
                queue_name: "default",
                priority: 100,
                dependencies: "[]",
                args_from: None,
                workflow_ctx_from: None,
                allow_failed_deps: false,
                join_type: "all",
                min_success: None,
                task_options: Some(r#"{"retry":"none"}"#),
                retention_class_key: Some("forever"),
                is_subworkflow: false,
                sub_definition_key: None,
                sub_workflow_params: None,
            },
            StartNodeV1 {
                task_index: 1,
                node_id: Some("report"),
                task_name: "report",
                input: "{}",
                queue_name: "default",
                priority: 50,
                dependencies: "[0]",
                args_from: Some(r#"{"rows":0}"#),
                workflow_ctx_from: Some(r#"["fetch"]"#),
                allow_failed_deps: false,
                join_type: "all",
                min_success: None,
                task_options: None,
                retention_class_key: Some("standard_30d"),
                is_subworkflow: false,
                sub_definition_key: None,
                sub_workflow_params: None,
            },
        ],
    }
}

/// The canonical form and the digest of a fixed command. A release that
/// changes either turns every repeat start of a run that an earlier release
/// started into a conflict, so a change of the form is a new version.
#[test]
fn a_fixed_command_has_its_released_canonical_form_and_digest() {
    let command = command();

    assert_eq!(
        String::from_utf8(command.canonical_bytes()).expect("UTF-8"),
        concat!(
            r#"[1,"nightly_report","tests.start_once.nightly.v1","fail",1,null,"#,
            r#"[[0,"fetch","fetch","{\"day\":\"2026-11-23\"}","default",100,"[]",null,null,false,"#,
            r#""all",null,"{\"retry\":\"none\"}","forever",false,null,null],"#,
            r#"[1,"report","report","{}","default",50,"[0]","{\"rows\":0}","[\"fetch\"]",false,"#,
            r#""all",null,null,"standard_30d",false,null,null]]]"#,
        )
    );
    let fingerprint = command.fingerprint();
    assert_eq!(fingerprint.version(), 1);
    assert_eq!(
        hex(fingerprint.digest()),
        "b69a0678bdb63da2cb4eb8e7c9f8244723a016dc740fd9ddc3cef561f8531f9f"
    );
}

/// The fingerprint is SHA-256 of the canonical form: the digest that the
/// writer computes is the digest of the bytes.
#[test]
fn the_fingerprint_is_the_sha256_of_the_canonical_form() {
    let command = command();

    assert_eq!(
        command.fingerprint().digest(),
        Sha256::digest(command.canonical_bytes()).as_slice()
    );
    assert_eq!(
        command.fingerprint().digest().len(),
        START_COMMAND_FINGERPRINT_BYTES
    );
}

#[test]
fn two_equal_commands_have_one_fingerprint() {
    assert_eq!(command().fingerprint(), command().fingerprint());
}

/// Each element of the command changes the digest.
#[test]
fn each_element_of_the_command_changes_the_fingerprint() {
    type Change = fn(&mut StartCommandV1<'static>);
    let changes: [(&str, Change); 23] = [
        ("name", |c| c.name = "weekly_report"),
        ("definition_key", |c| c.definition_key = None),
        ("on_error", |c| c.on_error = "pause"),
        ("output_task_index", |c| c.output_task_index = None),
        ("success_policy", |c| {
            c.success_policy = Some(r#"{"cases":[{"required_indices":[0]}]}"#)
        }),
        ("a node less", |c| {
            let _removed = c.nodes.pop();
        }),
        ("task_index", |c| c.nodes[1].task_index = 2),
        ("node_id", |c| c.nodes[0].node_id = None),
        ("task_name", |c| c.nodes[0].task_name = "fetch_v2"),
        ("input", |c| c.nodes[0].input = r#"{"day":"2026-11-24"}"#),
        ("queue_name", |c| c.nodes[0].queue_name = "slow"),
        ("priority", |c| c.nodes[0].priority = 99),
        ("dependencies", |c| c.nodes[1].dependencies = "[]"),
        ("args_from", |c| c.nodes[1].args_from = None),
        ("workflow_ctx_from", |c| c.nodes[1].workflow_ctx_from = None),
        ("allow_failed_deps", |c| c.nodes[1].allow_failed_deps = true),
        ("join_type", |c| c.nodes[1].join_type = "any"),
        ("min_success", |c| c.nodes[1].min_success = Some(1)),
        ("task_options", |c| c.nodes[0].task_options = None),
        ("retention_class_key", |c| {
            c.nodes[0].retention_class_key = Some("standard_7d")
        }),
        ("is_subworkflow", |c| c.nodes[0].is_subworkflow = true),
        ("sub_definition_key", |c| {
            c.nodes[0].sub_definition_key = Some("tests.start_once.child.v1")
        }),
        ("sub_workflow_params", |c| {
            c.nodes[0].sub_workflow_params = Some("GIVEN")
        }),
    ];
    let original = command().fingerprint();
    for (element, change) in changes {
        let mut changed = command();
        change(&mut changed);
        assert_ne!(changed.fingerprint(), original, "{element}");
    }
}
