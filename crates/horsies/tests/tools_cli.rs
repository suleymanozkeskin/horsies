//! The shipped `horsies` binary: the help text, the commands of the app's
//! own binary refused by name, and `skills install`.

use std::path::Path;
use std::process::{Command, Output};

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_horsies"))
        .args(arguments)
        .output()
        .expect("horsies process")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("UTF-8 output")
}

#[test]
fn help_lists_the_tools_and_names_the_runner_of_the_app_binary() {
    let output = run(&["--help"]);
    assert_eq!(output.status.code(), Some(0));
    let help = text(&output.stdout);
    for command in ["web", "get-docs", "skills"] {
        assert!(help.contains(command), "{help}");
    }
    assert!(help.contains("horsies::cli::run(app)"), "{help}");
    for removed in ["cutover", "transcode"] {
        assert!(!help.contains(removed), "{help}");
    }
}

#[test]
fn worker_scheduler_and_check_are_refused_with_where_they_run() {
    for command in ["worker", "scheduler", "check"] {
        let output = run(&[command]);
        assert_eq!(output.status.code(), Some(2), "{command}");
        let error = text(&output.stderr);
        assert!(error.contains("horsies::cli::run(app)"), "{error}");
        assert!(
            error.contains(&format!("`<your binary> {command}`")),
            "{error}"
        );
    }
}

#[test]
fn removed_commands_are_usage_errors() {
    for command in ["cutover", "transcode"] {
        assert_eq!(run(&[command]).status.code(), Some(2), "{command}");
    }
}

#[test]
fn skills_install_writes_the_folder_and_keeps_a_changed_copy_without_force() {
    let target = tempfile::tempdir().expect("a temporary folder");
    let target_text = target.path().to_str().expect("a UTF-8 temporary path");
    let destination = target.path().join("horsies");

    let installed = run(&["skills", "install", "--target", target_text]);
    assert_eq!(
        installed.status.code(),
        Some(0),
        "{}",
        text(&installed.stderr)
    );
    assert!(text(&installed.stdout).starts_with("installed "));
    assert!(destination.join("SKILL.md").is_file());

    let current = run(&["skills", "install", "--target", target_text]);
    assert_eq!(current.status.code(), Some(0));
    assert!(text(&current.stdout).ends_with("is already current\n"));

    std::fs::write(destination.join("SKILL.md"), "edited").expect("the copy is edited");
    let refused = run(&["skills", "install", "--target", target_text]);
    assert_eq!(refused.status.code(), Some(1));
    let error = text(&refused.stderr);
    assert!(error.contains("SKILL.md"), "{error}");
    assert!(error.contains("--force"), "{error}");
    assert_eq!(read(&destination.join("SKILL.md")), "edited");

    let forced = run(&["skills", "install", "--target", target_text, "--force"]);
    assert_eq!(forced.status.code(), Some(0), "{}", text(&forced.stderr));
    assert_ne!(read(&destination.join("SKILL.md")), "edited");
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).expect("a readable file")
}
