use std::process::{Command, Output};

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_horsies"))
        .args(arguments)
        .output()
        .expect("horsies process")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("UTF-8 stderr")
}

#[test]
fn invalid_invocation_exits_two() {
    let missing_source = run(&["web"]);
    assert_eq!(missing_source.status.code(), Some(2));
    assert!(stderr(&missing_source).contains("required"));

    let incomplete_pool_posture = run(&[
        "web",
        "--database-url",
        "postgresql://pool.example/horsies",
        "--pgbouncer-transaction-mode",
    ]);
    assert_eq!(incomplete_pool_posture.status.code(), Some(2));
    assert!(stderr(&incomplete_pool_posture).contains("--session-database-url"));
}

#[cfg(not(feature = "web"))]
#[test]
fn missing_web_feature_exits_one() {
    let output = run(&["web", "--database-url", "postgresql://localhost/horsies"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        stderr(&output),
        "horsies web requires the `web` cargo feature\n"
    );
}

#[cfg(feature = "web")]
#[test]
fn auth_none_refuses_non_loopback_before_connecting() {
    let output = run(&[
        "web",
        "--database-url",
        "postgresql://127.0.0.1:1/none",
        "--host",
        "0.0.0.0",
        "--auth",
        "none",
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        stderr(&output),
        "horsies web: error: --auth none is only allowed on a loopback host; \"0.0.0.0\" is reachable from the network. Use --auth trusted-header with a proxy-set header.\n"
    );

    let empty_header = run(&[
        "web",
        "--database-url",
        "postgresql://127.0.0.1:1/none",
        "--auth",
        "trusted-header",
        "--trusted-header",
        "",
    ]);
    assert_eq!(empty_header.status.code(), Some(2));
    let error = stderr(&empty_header);
    assert!(error.starts_with("horsies web: error: invalid trusted header name:"));
    assert!(!error.contains("SECURITY:"));
}

#[cfg(feature = "web")]
#[test]
fn trusted_header_warns_before_database_validation() {
    let output = run(&[
        "web",
        "--database-url",
        "not-a-postgresql-url",
        "--auth",
        "trusted-header",
        "--trusted-header",
        "X-Operator",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let error = stderr(&output);
    assert!(error.starts_with(
        "SECURITY: the reverse proxy in front of this server MUST strip or overwrite the X-Operator header on incoming requests. A proxy that forwards a client-supplied header makes this mode trivially spoofable, and horsies cannot detect that.\n"
    ));
    assert!(error.contains("invalid application configuration:"));
}
