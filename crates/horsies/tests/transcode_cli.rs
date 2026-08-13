use std::process::{Command, Output};
use std::str::FromStr;

use horsies::run_horsies_migrations;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool};
use uuid::Uuid;

const DATABASE_PREFIX: &str = "horsies_p10_cli_";

fn test_db_url() -> String {
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

struct CliDatabase {
    name: String,
    url: String,
    pool: PgPool,
    base_options: PgConnectOptions,
}

impl CliDatabase {
    async fn create() -> Self {
        let base_options = PgConnectOptions::from_str(&test_db_url()).unwrap();
        let mut admin = PgConnection::connect_with(&base_options.clone().database("postgres"))
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_p10_cli_setup'))")
            .execute(&mut admin)
            .await
            .unwrap();
        let stale: Vec<String> = sqlx::query_scalar(
            "SELECT d.datname FROM pg_database AS d
             WHERE left(d.datname, length('horsies_p10_cli_')) = 'horsies_p10_cli_'
               AND NOT EXISTS (
                   SELECT 1 FROM pg_stat_activity AS a WHERE a.datname = d.datname
               )
             ORDER BY d.datname",
        )
        .fetch_all(&mut admin)
        .await
        .unwrap();
        for database in stale {
            let suffix = database.strip_prefix(DATABASE_PREFIX).unwrap();
            assert!(
                suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "refuse to drop non-generated P10 CLI database {database:?}"
            );
            sqlx::query(&format!("DROP DATABASE \"{database}\""))
                .execute(&mut admin)
                .await
                .unwrap();
        }
        let name = format!("{DATABASE_PREFIX}{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut admin)
            .await
            .unwrap();
        let options = base_options.clone().database(&name);
        let url = options.to_url_lossy().to_string();
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(2)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with(options)
            .await
            .unwrap();
        let unlocked: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext('horsies_p10_cli_setup'))")
                .fetch_one(&mut admin)
                .await
                .unwrap();
        assert!(unlocked);
        run_horsies_migrations(&pool).await.unwrap();
        Self {
            name,
            url,
            pool,
            base_options,
        }
    }

    async fn destroy(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.base_options.database("postgres"))
            .await
            .unwrap();
        let active: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = $1")
                .bind(&self.name)
                .fetch_one(&mut admin)
                .await
                .unwrap();
        assert_eq!(active, 0);
        sqlx::query(&format!("DROP DATABASE \"{}\"", self.name))
            .execute(&mut admin)
            .await
            .unwrap();
    }
}

fn invoke(database_url: Option<&str>, arguments: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_horsies"));
    command.arg("transcode");
    if let Some(database_url) = database_url {
        command.args(["--database-url", database_url]);
    }
    command.args(arguments).output().unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

fn assert_success(output: Output, fact: &str) -> String {
    let out = stdout(&output);
    assert!(
        output.status.success(),
        "stdout={out}\nstderr={}",
        stderr(&output)
    );
    assert!(out.contains(fact), "missing {fact:?} in {out:?}");
    assert!(!out.contains("Planned("));
    assert!(!out.contains("Transcode"));
    out
}

#[tokio::test]
async fn every_transcode_command_has_factual_output_and_typed_exit_posture() {
    let missing = invoke(
        None,
        &["status", "--job-id", "00000000-0000-0000-0000-000000000000"],
    );
    assert!(!missing.status.success());
    assert!(stderr(&missing).contains("--database-url is required"));

    let database = CliDatabase::create().await;
    let session = Uuid::new_v4().to_string();
    let job = Uuid::new_v4().to_string();
    assert_success(
        invoke(Some(&database.url), &["begin", "--session-id", &session]),
        "archive maintenance active: session=",
    );
    assert_success(
        invoke(
            Some(&database.url),
            &[
                "plan",
                "--job-id",
                &job,
                "--component",
                "result",
                "--source-version",
                "1",
                "--target-version",
                "2",
                "--source-codec",
                "json-utf8",
                "--target-codec",
                "framed-v2",
            ],
        ),
        "transcode planned: job=",
    );
    assert_success(
        invoke(Some(&database.url), &["status", "--job-id", &job]),
        "state=PLANNED",
    );
    let early_finish = invoke(Some(&database.url), &["finish", "--session-id", &session]);
    assert!(!early_finish.status.success());
    assert!(stderr(&early_finish).contains("unfinished replacement job"));
    assert_success(
        invoke(
            Some(&database.url),
            &["copy", "--job-id", &job, "--batch-size", "2"],
        ),
        "transcode copy complete: job=",
    );
    assert_success(
        invoke(Some(&database.url), &["verify", "--job-id", &job]),
        "verified=true",
    );
    assert_success(
        invoke(Some(&database.url), &["swap", "--job-id", &job]),
        "transcode swap complete: job=",
    );
    assert_success(
        invoke(Some(&database.url), &["finalize", "--job-id", &job]),
        "decoder-retirement-ready=true",
    );
    assert_success(
        invoke(Some(&database.url), &["finish", "--session-id", &session]),
        "archive maintenance complete: session=",
    );

    let run_session = Uuid::new_v4().to_string();
    let run_job = Uuid::new_v4().to_string();
    let run = assert_success(
        invoke(
            Some(&database.url),
            &[
                "run",
                "--session-id",
                &run_session,
                "--job-id",
                &run_job,
                "--component",
                "attempts",
                "--source-version",
                "1",
                "--target-version",
                "2",
                "--source-codec",
                "json-utf8",
                "--target-codec",
                "framed-v2",
                "--batch-size",
                "10",
            ],
        ),
        "transcode finalized: job=",
    );
    for fact in [
        "archive maintenance active:",
        "transcode planned:",
        "transcode copy complete:",
        "transcode verification:",
        "transcode swap complete:",
        "archive maintenance complete:",
    ] {
        assert!(run.contains(fact), "missing run fact {fact:?}");
    }
    database.destroy().await;
}
