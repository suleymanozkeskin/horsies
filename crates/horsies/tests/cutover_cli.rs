use std::process::{Command, Output};
use std::str::FromStr;

use horsies::{expected_schema_version, PostgresBroker};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, Executor, PgConnection};
use uuid::Uuid;

const DATABASE_PREFIX: &str = "horsies_p9_cli_";

fn authority_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join(".env").exists());
    let password = root
        .and_then(|path| std::fs::read_to_string(path.join(".env")).ok())
        .and_then(|content| {
            content
                .lines()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(key, _)| key.trim() == "DB_PASSWORD")
                .map(|(_, value)| value.trim().to_owned())
        })
        .unwrap_or_else(|| "W0rklane".to_owned());
    format!("postgresql://postgres:{password}@localhost:5432/postgres")
}

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_horsies"))
        .args(arguments)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

fn ladder(seconds: &str) -> Output {
    run(&[
        "cutover",
        "ladder-evaluate",
        "--relocation-seconds-per-million",
        "120",
        "--fixed-seconds",
        "30",
        "--preparation-seconds-per-million",
        "0",
        "--rung",
        "one-million",
        "--measured-seconds",
        seconds,
        "--measured-fixed-seconds",
        "30",
        "--measured-preparation-seconds",
        "0",
        "--commit",
        "250000:30",
        "--commit",
        "500000:60",
        "--commit",
        "750000:90",
        "--commit",
        "1000000:120",
    ])
}

#[test]
fn ladder_and_preconnection_refusals_have_typed_process_exit_postures() {
    for seconds in ["105", "187.5"] {
        let output = ladder(seconds);
        assert!(output.status.success(), "{}", stderr(&output));
        assert!(stdout(&output).starts_with("ladder rung one-million passed:"));
        assert!(stderr(&output).is_empty());
    }
    let over = ladder("187.500001");
    assert!(!over.status.success());
    assert!(stdout(&over).contains("busted the ceiling"));
    assert!(stderr(&over).contains("measured time exceeded the planning ceiling"));
    let under = ladder("104.999999");
    assert!(!under.status.success());
    assert!(stdout(&under).contains("disproved the estimate from below"));
    assert!(stderr(&under).contains("measured time fell below the prediction floor"));

    for arguments in [
        vec![
            "cutover",
            "preflight",
            "--relocation-seconds-per-million",
            "1",
            "--fixed-seconds",
            "1",
            "--preparation-seconds-per-million",
            "1",
        ],
        vec!["cutover", "drain"],
        vec!["cutover", "install-programs"],
        vec!["cutover", "prepare"],
        vec!["cutover", "relocate"],
        vec![
            "cutover",
            "tighten",
            "--backup-label",
            "b",
            "--operator-confirmation",
            "point-of-no-return: b",
        ],
        vec!["cutover", "validate"],
        vec!["cutover", "rollback-programs"],
        vec!["cutover", "status"],
    ] {
        let output = run(&arguments);
        assert!(!output.status.success(), "{arguments:?}");
        assert_eq!(
            stderr(&output),
            "cutover failed: --database-url is required for this cutover command\n"
        );
    }
    let missing = run(&[
        "cutover",
        "run",
        "--relocation-seconds-per-million",
        "1",
        "--fixed-seconds",
        "1",
        "--preparation-seconds-per-million",
        "1",
        "--backup-label",
        "b",
        "--operator-confirmation",
        "point-of-no-return: b",
        "--confirm-stage",
        "drain",
    ]);
    assert!(!missing.status.success());
    assert_eq!(
        stderr(&missing),
        "cutover failed: run is missing explicit confirmations for: normalize-identity, install-programs, prepare, relocate, tighten, validate\n"
    );
    assert!(!stderr(&missing).contains("NormalizeIdentity"));
    assert!(!stderr(&missing).contains("InstallPrograms"));
}

#[tokio::test]
async fn database_commands_print_facts_and_refuse_invalid_postures() {
    let base = PgConnectOptions::from_str(&authority_url()).unwrap();
    let mut admin = PgConnection::connect_with(&base.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_p9_cli_setup'))")
        .execute(&mut admin)
        .await
        .unwrap();
    let stale: Vec<String> = sqlx::query_scalar(
        "SELECT datname FROM pg_database
         WHERE left(datname, length('horsies_p9_cli_')) = 'horsies_p9_cli_'
           AND NOT EXISTS (
               SELECT 1 FROM pg_stat_activity WHERE datname = pg_database.datname
           )
         ORDER BY datname",
    )
    .fetch_all(&mut admin)
    .await
    .unwrap();
    for stale_name in stale {
        let suffix = stale_name.strip_prefix(DATABASE_PREFIX).unwrap();
        assert!(
            suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "refuse to drop non-generated P9 CLI database {stale_name:?}"
        );
        admin
            .execute(format!("DROP DATABASE \"{stale_name}\"").as_str())
            .await
            .unwrap();
    }
    let name = format!("{DATABASE_PREFIX}{}", Uuid::new_v4().simple());
    admin
        .execute(format!("CREATE DATABASE \"{name}\"").as_str())
        .await
        .unwrap();
    let options = base.clone().database(&name);
    let url = options.to_url_lossy().to_string();
    let anchor = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_with(options.clone())
        .await
        .unwrap();
    let unlocked: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext('horsies_p9_cli_setup'))")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(unlocked);
    let broker = PostgresBroker::connect(&url).await.unwrap();
    broker.ensure_schema_initialized().await.unwrap();

    let preflight = run(&[
        "cutover",
        "--database-url",
        &url,
        "preflight",
        "--relocation-seconds-per-million",
        "1",
        "--fixed-seconds",
        "1",
        "--preparation-seconds-per-million",
        "1",
    ]);
    assert!(preflight.status.success(), "{}", stderr(&preflight));
    assert!(stdout(&preflight)
        .contains("unfingerprinted=0, unprepared=0, unclassified=0 (0 bytes), class-days=0"));
    let status = run(&["cutover", "--database-url", &url, "status"]);
    assert!(status.status.success(), "{}", stderr(&status));
    let expected_status = format!("stored-schema={}, attested=true", expected_schema_version());
    assert!(stdout(&status).contains(&expected_status));
    assert!(!stdout(&status).contains("CutoverStatus"));
    let prepare = run(&["cutover", "--database-url", &url, "prepare"]);
    assert!(prepare.status.success(), "{}", stderr(&prepare));
    assert_eq!(
        stdout(&prepare),
        "preparation complete: rows=0, live=0, batches=0, inline=0, over-bound=0, policy-declined=0, decode-failed=0\n"
    );
    let relocate = run(&["cutover", "--database-url", &url, "relocate"]);
    assert!(relocate.status.success(), "{}", stderr(&relocate));
    assert_eq!(
        stdout(&relocate),
        "relocation complete: rows=0, batches=0, legacy-kind=0\n"
    );
    let valid = run(&["cutover", "--database-url", &url, "validate"]);
    assert!(valid.status.success(), "{}", stderr(&valid));
    assert!(stdout(&valid).contains("validation passed and attested"));
    let rollback = run(&["cutover", "--database-url", &url, "rollback-programs"]);
    assert!(!rollback.status.success());
    assert!(stderr(&rollback).contains("after tighten only a named backup restore is valid"));

    let task_id = broker
        .enqueue(
            "p9.cli",
            Some("[]"),
            Some("{}"),
            "default",
            50,
            None,
            None,
            None,
            Some("{}"),
            &"c".repeat(64),
            None,
            None,
            None,
            Some("forever"),
            Some(false),
        )
        .await
        .unwrap();
    assert_eq!(
        broker.claim("default", 1, "p9-cli", None).await.unwrap()[0].id,
        task_id
    );
    broker.close().await;
    drop(broker);

    let drain = run(&["cutover", "--database-url", &url, "drain"]);
    assert!(!drain.status.success());
    assert!(stdout(&drain).contains("drain blocked: claimed=1, running=0"));
    assert!(stderr(&drain).contains(
        "cutover stage drain refused: claimed=1, running=0, finalizing=0, recent_heartbeats=0"
    ));
    assert!(!stderr(&drain).contains("Blocked {"));
    let install = run(&["cutover", "--database-url", &url, "install-programs"]);
    assert!(!install.status.success());
    assert!(stdout(&install).contains("drain blocked: claimed=1, running=0"));
    assert!(!stderr(&install).contains("Blocked {"));
    let tighten = run(&[
        "cutover",
        "--database-url",
        &url,
        "tighten",
        "--backup-label",
        "p9-cli.dump",
        "--operator-confirmation",
        "wrong",
    ]);
    assert!(!tighten.status.success());
    assert!(stdout(&tighten).contains("tighten refused: operator confirmation"));
    assert!(stdout(&tighten).contains("rows are in flight"));

    let pool = PgPoolOptions::new().connect_with(options).await.unwrap();
    sqlx::query("ALTER TABLE horsies_tasks DROP CONSTRAINT horsies_tasks_live_status_only")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let invalid = run(&["cutover", "--database-url", &url, "validate"]);
    assert!(!invalid.status.success());
    assert_eq!(
        stdout(&invalid),
        "validation failed: the live-only status domain is absent\n"
    );

    anchor.close().await;
    let active: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = $1")
            .bind(&name)
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(active, 0);
    admin
        .execute(format!("DROP DATABASE \"{name}\"").as_str())
        .await
        .unwrap();
}
