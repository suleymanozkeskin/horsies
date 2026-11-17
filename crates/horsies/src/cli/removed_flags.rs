//! The flags of the `0.1.0-alpha` worker and scheduler commands, refused by
//! name with what replaces each (Python `REMOVED_WORKER_FLAGS`). The runner
//! exits 2, as for any argument error.

use std::ffi::OsString;

/// A removed flag of one command, and its replacement.
struct RemovedFlag {
    command: &'static str,
    names: &'static [&'static str],
    replacement: &'static str,
}

const REMOVED_FLAGS: &[RemovedFlag] = &[
    RemovedFlag {
        command: "worker",
        names: &["-q", "--queues"],
        replacement: "a worker serves every queue of the app: set the queues with \
                      AppConfig.queue_mode and AppConfig.custom_queues",
    },
    RemovedFlag {
        command: "worker",
        names: &["-c", "--concurrency"],
        replacement: "set the children in a profile of AppConfig.worker_profiles \
                      (WorkerProfile.isolated, shared_async, shared_thread) and start the \
                      worker with --profile <name>",
    },
    RemovedFlag {
        command: "worker",
        names: &["--max-claim-batch"],
        replacement: "set WorkerProfile.claim.per_queue_per_pass in a profile of \
                      AppConfig.worker_profiles and start the worker with --profile <name>",
    },
    RemovedFlag {
        command: "worker",
        names: &["--max-claim-per-worker"],
        replacement: "set WorkerProfile.claim.claimed_per_worker in a profile of \
                      AppConfig.worker_profiles and start the worker with --profile <name>",
    },
    RemovedFlag {
        command: "worker",
        names: &["--coalesce-notifies"],
        replacement: "it has no replacement",
    },
    RemovedFlag {
        command: "scheduler",
        names: &["--check-interval"],
        replacement: "set AppConfig.schedule.check_interval_seconds",
    },
    RemovedFlag {
        command: "scheduler",
        names: &["--dry-run"],
        replacement: "run the check command to check the schedules without a start",
    },
];

/// The refusal of the first removed flag of `arguments` (the program name
/// first), or `None`. A flag counts in its own form and in its `=value`
/// form, only after its own command. Pure.
pub(super) fn refusal(arguments: &[OsString]) -> Option<String> {
    let texts: Vec<String> = arguments
        .iter()
        .skip(1)
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect();
    let command = texts.first()?;
    texts.iter().skip(1).find_map(|argument| {
        let name = argument
            .split_once('=')
            .map_or(argument.as_str(), |(name, _)| name);
        REMOVED_FLAGS
            .iter()
            .find(|flag| flag.command == command && flag.names.contains(&name))
            .map(|flag| format!("{name} was removed in 0.8.0: {}", flag.replacement))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(texts: &[&str]) -> Vec<OsString> {
        texts.iter().map(OsString::from).collect()
    }

    #[test]
    fn each_removed_worker_flag_is_refused_by_name() {
        for flag in [
            "-q",
            "--queues",
            "-c",
            "--concurrency",
            "--max-claim-batch",
            "--max-claim-per-worker",
            "--coalesce-notifies",
        ] {
            let refused = refusal(&arguments(&["app", "worker", flag, "4"])).unwrap();
            assert!(
                refused.starts_with(&format!("{flag} was removed")),
                "{refused}"
            );
        }
    }

    #[test]
    fn a_flag_with_its_value_after_an_equals_sign_is_refused() {
        let refused = refusal(&arguments(&["app", "worker", "--concurrency=8"])).unwrap();
        assert!(refused.contains("WorkerProfile.isolated"));
    }

    #[test]
    fn the_scheduler_flags_are_refused_for_the_scheduler_only() {
        assert!(refusal(&arguments(&["app", "scheduler", "--dry-run"])).is_some());
        assert!(refusal(&arguments(&["app", "scheduler", "--check-interval", "5"])).is_some());
        assert!(refusal(&arguments(&["app", "worker", "--dry-run"])).is_none());
    }

    #[test]
    fn current_flags_and_no_command_pass() {
        assert!(refusal(&arguments(&["app", "worker", "--profile", "web"])).is_none());
        assert!(refusal(&arguments(&["app"])).is_none());
        assert!(refusal(&arguments(&["app", "check", "--live"])).is_none());
    }
}
