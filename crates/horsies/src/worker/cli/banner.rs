//! Startup banner for the horsies worker.
//!
//! Prints a rich startup banner with ASCII art, version, configuration
//! summary, and registered tasks list -- matching the Python library's
//! `core/banner.py` output.

use std::fmt::Write as FmtWrite;
use std::io::{self, Write};

use owo_colors::OwoColorize;

use crate::core::config::app::AppConfig;
use crate::core::config::queue::QueueMode;
use crate::core::registry::task::TaskRegistry;

use crate::worker::config::WorkerConfig;

// ---------------------------------------------------------------------------
// ASCII art constants
// ---------------------------------------------------------------------------

/// Braille art of a galloping horse (matches Python's HORSE_BRAILLE).
const HORSE_BRAILLE: &str = r"
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣶⣾⣿⣿⣿⣿⣷⣯⡀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⣿⣆⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣿⣿⣿⡟⠹⣿⣿⣿⣿⣿⣦⡀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣠⣴⣾⣿⣿⣿⣶⣶⣶⣤⣤⣤⣤⣤⣶⣾⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠈⠉⠛⠻⡿⣿⣿⠂⠀
⠀⠀⢀⣀⠀⢀⣀⣠⣶⣿⣿⠟⢛⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⡐⠀⠀⠀⠀⠀⠀⠈⠋⡿⠁⠀⠀
⠀⠀⠀⢹⣿⣿⣿⣿⣿⡿⠁⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠛⠻⠿⠛⠉⠀⠀⠀⠈⢯⡻⣿⣿⣿⣿⣿⢿⣿⣿⣿⣿⣿⣿⡿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣷⣿⣿⣿⡟⠀⠙⠻⠿⠿⣿⣿⠃⣿⣿⣿⣿⣿⣿⣿⣿⡁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⡿⠁⢀⠀⠀⠀⠀⠀⠂⠀⢿⣿⣿⣿⡍⠈⢁⣙⣿⢦⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⠏⠀⠀⣼⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣷⠀⠀⠀⠀⠁⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣧⣀⢄⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣧⠀⠀⢀⣼⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣀⠀⠀⠀⢀⡀⠀⠀⠀⡀⠀⠀⠀⣀⠛⣿⣷⣍⠛⣦⡀⠀⠂⢠⣷⣶⣾⡿⠟⠛⢃⠀⢠⣾⡟⠀⠀⠀⠀⡀⠀⠀⠀⡀⠀⠀⠀
⠄⡤⠦⠤⠤⢤⡤⠡⠤⠤⢤⠬⠤⠤⠤⢤⠅⠀⠤⣿⣷⠄⠎⢻⣤⡦⠄⠀⠤⢵⠄⠠⠤⠬⣾⠏⠁⠀⠥⡤⠄⠠⠬⢦⡤⠀⠠⠵⢤⠠
⠚⠒⠒⠒⡶⠚⠒⠒⠒⡰⠓⠒⠒⠒⢲⠓⠒⠒⠒⢻⠿⠀⠀⠚⢿⡷⠐⠒⠒⠚⡆⠒⠒⠒⠚⣖⠒⠒⠒⠚⢶⠒⠒⠒⠚⢶⠂⠐⠒⠛";

/// Figlet-style "horsies" logo with version and tagline placeholders.
const LOGO_TEXT: &str = r"
    __                    _
   / /_  ____  __________(_)__  _____
  / __ \/ __ \/ ___/ ___/ / _ \/ ___/      {version}
 / / / / /_/ / /  (__  ) /  __(__  )       distributed task queue
/_/ /_/\____/_/  /____/_/\___/____/        and workflow engine
";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Information needed to render the startup banner.
///
/// This is deliberately decoupled from `Worker` so the banner can be printed
/// before the worker is fully constructed (e.g. from CLI orchestration code).
pub struct BannerInfo<'a> {
    pub worker_id: &'a str,
    pub worker_config: &'a WorkerConfig,
    pub app_config: &'a AppConfig,
    pub task_registry: &'a TaskRegistry,
    pub role: &'a str,
}

/// Print the full startup banner to stderr.
///
/// Shows ASCII art, version, config summary, broker URL (password masked),
/// recovery settings, and the full list of registered tasks.
pub fn print_banner(info: &BannerInfo<'_>) {
    let text = format_banner(info);
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(text.as_bytes());
    let _ = stderr.flush();
}

/// Format the complete banner string (with ANSI colors).
pub fn format_banner(info: &BannerInfo<'_>) -> String {
    let mut buf = String::with_capacity(2048);

    // --- ASCII art ---
    buf.push_str(&HORSE_BRAILLE.cyan().to_string());
    buf.push('\n');

    // --- Logo with version ---
    let version = env!("CARGO_PKG_VERSION");
    let logo = LOGO_TEXT.replace("{version}", &format!("v{}", version));
    buf.push_str(&logo.bright_yellow().bold().to_string());
    buf.push('\n');

    // --- [config] section ---
    write_section_header(&mut buf, "config");

    write_kv(&mut buf, "role", info.role);
    write_kv(&mut buf, "worker_id", info.worker_id);
    write_kv(
        &mut buf,
        "queue_mode",
        &format_queue_mode(info.app_config.queue_mode),
    );

    // Queues
    let queues_str = info.worker_config.queues.join(", ");
    write_kv(&mut buf, "queues", &queues_str);

    // Concurrency
    write_kv(
        &mut buf,
        "concurrency",
        &info.worker_config.concurrency.to_string(),
    );

    // Max claim batch
    write_kv(
        &mut buf,
        "max_claim_batch",
        &info.worker_config.max_claim_batch.to_string(),
    );

    // Max claim per worker
    let max_claim_display = if info.worker_config.max_claim_per_worker == 0 {
        format!("{} (auto)", info.worker_config.concurrency)
    } else {
        info.worker_config.max_claim_per_worker.to_string()
    };
    write_kv(&mut buf, "max_claim_per_worker", &max_claim_display);

    // Broker URL (password masked)
    let masked_url = mask_broker_url(&info.app_config.broker.database_url);
    write_kv(&mut buf, "broker", &masked_url);

    // Cluster-wide cap
    if let Some(cap) = info.app_config.cluster_wide_cap {
        write_kv(&mut buf, "cluster_cap", &format!("{} (cluster-wide)", cap));
    }

    // Prefetch
    if info.app_config.prefetch_buffer > 0 {
        write_kv(
            &mut buf,
            "prefetch",
            &info.app_config.prefetch_buffer.to_string(),
        );
    }

    buf.push('\n');

    // --- [recovery] section ---
    write_section_header(&mut buf, "recovery");

    let recovery = &info.app_config.recovery;
    write_kv(
        &mut buf,
        "requeue_stale_claimed",
        &format_bool(recovery.auto_requeue_stale_claimed),
    );
    write_kv(
        &mut buf,
        "fail_stale_running",
        &format_bool(recovery.auto_fail_stale_running),
    );
    write_kv(
        &mut buf,
        "check_interval",
        &format_ms(recovery.check_interval_ms),
    );
    write_kv(
        &mut buf,
        "heartbeat_interval",
        &format_ms(recovery.runner_heartbeat_interval_ms),
    );

    buf.push('\n');

    // --- [tasks] section ---
    let mut task_names: Vec<&str> = info.task_registry.task_names().collect();
    task_names.sort();

    let header = format!(
        "[{}] ({} registered)",
        "tasks".bright_cyan().bold(),
        task_names.len()
    );
    let _ = writeln!(buf, "{}", header);

    for name in &task_names {
        let _ = writeln!(buf, "  {} {}", ".".dimmed(), name.white());
    }

    buf.push('\n');
    buf
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Write a "[section]" header line.
fn write_section_header(buf: &mut String, name: &str) {
    let _ = writeln!(buf, "[{}]", name.bright_cyan().bold());
}

/// Write a "  .> key:  value" line with consistent alignment.
fn write_kv(buf: &mut String, key: &str, value: &str) {
    // Pad the key *before* applying colors so ANSI escape codes don't
    // interfere with the alignment width.
    let padded_key = format!("{:<22}", format!("{}:", key));
    let _ = writeln!(
        buf,
        "  {} {} {}",
        ".>".dimmed(),
        padded_key.white().bold(),
        value.bright_white()
    );
}

/// Format a `QueueMode` for display.
fn format_queue_mode(mode: QueueMode) -> String {
    match mode {
        QueueMode::Default => "default".to_owned(),
        QueueMode::Custom => "custom".to_owned(),
    }
}

/// Format a boolean for display.
fn format_bool(val: bool) -> String {
    if val {
        "yes".to_owned()
    } else {
        "no".to_owned()
    }
}

/// Format milliseconds into a human-readable string.
fn format_ms(ms: u64) -> String {
    if ms >= 60_000 {
        let mins = ms / 60_000;
        let remainder_s = (ms % 60_000) / 1_000;
        if remainder_s > 0 {
            format!("{}m{}s", mins, remainder_s)
        } else {
            format!("{}m", mins)
        }
    } else if ms >= 1_000 {
        let secs = ms / 1_000;
        let remainder_ms = ms % 1_000;
        if remainder_ms > 0 {
            format!("{}.{}s", secs, remainder_ms / 100)
        } else {
            format!("{}s", secs)
        }
    } else {
        format!("{}ms", ms)
    }
}

/// Mask the password in a PostgreSQL connection URL.
///
/// Transforms `postgresql://user:secret@host/db` into
/// `postgresql://user:****@host/db`.
fn mask_broker_url(url: &str) -> String {
    // Find the `@` that separates credentials from host.
    let Some(at_pos) = url.find('@') else {
        return url.to_owned(); // No credentials in URL.
    };

    let pre_at = &url[..at_pos];

    // Find the last `:` before `@` -- that separates user from password.
    // But we need to skip the `://` in the scheme.
    let scheme_end = url.find("://").map(|i| i + 3).unwrap_or(0);
    let userinfo = &pre_at[scheme_end..];

    match userinfo.rfind(':') {
        Some(colon_in_userinfo) => {
            let colon_abs = scheme_end + colon_in_userinfo;
            format!("{}:****{}", &url[..colon_abs], &url[at_pos..])
        }
        None => url.to_owned(), // No password portion.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_url_with_password() {
        let url = "postgresql://user:secret@localhost:5432/mydb";
        assert_eq!(
            mask_broker_url(url),
            "postgresql://user:****@localhost:5432/mydb"
        );
    }

    #[test]
    fn mask_url_with_special_chars_in_password() {
        // Password with special chars but no @ (properly URL-encoded passwords).
        let url = "postgres://admin:s3cr3t%21@db.example.com/prod";
        assert_eq!(
            mask_broker_url(url),
            "postgres://admin:****@db.example.com/prod"
        );
    }

    #[test]
    fn mask_url_no_password() {
        let url = "postgresql://localhost/mydb";
        assert_eq!(mask_broker_url(url), "postgresql://localhost/mydb");
    }

    #[test]
    fn mask_url_user_no_password() {
        let url = "postgresql://user@localhost/mydb";
        assert_eq!(mask_broker_url(url), "postgresql://user@localhost/mydb");
    }

    #[test]
    fn format_ms_seconds() {
        assert_eq!(format_ms(30_000), "30s");
        assert_eq!(format_ms(1_500), "1.5s");
        assert_eq!(format_ms(500), "500ms");
    }

    #[test]
    fn format_ms_minutes() {
        assert_eq!(format_ms(120_000), "2m");
        assert_eq!(format_ms(90_000), "1m30s");
    }

    #[test]
    fn format_bool_values() {
        assert_eq!(format_bool(true), "yes");
        assert_eq!(format_bool(false), "no");
    }

    #[test]
    fn format_queue_modes() {
        assert_eq!(format_queue_mode(QueueMode::Default), "default");
        assert_eq!(format_queue_mode(QueueMode::Custom), "custom");
    }

    #[test]
    fn banner_contains_version() {
        use crate::core::config::broker::PostgresConfig;
        use crate::core::config::recovery::RecoveryConfig;

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig::from_url("postgresql://localhost/test"),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: Default::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let worker_config = WorkerConfig::default();
        let registry = TaskRegistry::new();

        let info = BannerInfo {
            worker_id: "test-worker-id",
            worker_config: &worker_config,
            app_config: &app_config,
            task_registry: &registry,
            role: "worker",
        };

        let output = format_banner(&info);
        // The version string should appear (without ANSI codes interfering
        // with the content check -- the raw version text is embedded).
        assert!(output.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn banner_masks_broker_password() {
        use crate::core::config::broker::PostgresConfig;
        use crate::core::config::recovery::RecoveryConfig;

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig::from_url("postgresql://user:secret@localhost/test"),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: Default::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let worker_config = WorkerConfig::default();
        let registry = TaskRegistry::new();

        let info = BannerInfo {
            worker_id: "test-worker-id",
            worker_config: &worker_config,
            app_config: &app_config,
            task_registry: &registry,
            role: "worker",
        };

        let output = format_banner(&info);
        assert!(!output.contains("secret"), "password should be masked");
        assert!(output.contains("****"), "masked placeholder should appear");
    }

    #[test]
    fn banner_shows_tasks_sorted() {
        use crate::core::config::broker::PostgresConfig;
        use crate::core::config::recovery::RecoveryConfig;
        use crate::core::task::fn_trait::{
            BlockingTaskFn, RawTaskResult, RegisteredTask, TaskMeta,
        };
        use std::sync::Arc;

        struct Dummy;
        impl BlockingTaskFn for Dummy {
            fn execute(&self, args: &[u8]) -> RawTaskResult {
                crate::core::task::result::TaskResult::Ok(args.to_vec())
            }
        }

        let mut registry = TaskRegistry::new();
        registry
            .register(
                "zeta_task",
                RegisteredTask::Blocking {
                    task: Arc::new(Dummy),
                    meta: TaskMeta::default(),
                },
            )
            .unwrap();
        registry
            .register(
                "alpha_task",
                RegisteredTask::Blocking {
                    task: Arc::new(Dummy),
                    meta: TaskMeta::default(),
                },
            )
            .unwrap();

        let app_config = AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig::from_url("postgresql://localhost/test"),
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: Default::default(),
            schedule: None,
            resend_on_transient_err: false,
        };
        let worker_config = WorkerConfig::default();

        let info = BannerInfo {
            worker_id: "test-id",
            worker_config: &worker_config,
            app_config: &app_config,
            task_registry: &registry,
            role: "worker",
        };

        let output = format_banner(&info);
        // Both tasks should appear, and alpha before zeta.
        let alpha_pos = output.find("alpha_task").expect("alpha_task missing");
        let zeta_pos = output.find("zeta_task").expect("zeta_task missing");
        assert!(
            alpha_pos < zeta_pos,
            "tasks should be sorted alphabetically"
        );
        assert!(output.contains("2 registered"));
    }
}
