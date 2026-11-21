//! A worker or scheduler process of a test (Python
//! `clients/python/tests/e2e/helpers/worker.py`), or a command that ends on
//! its own, such as `check --live`.
//!
//! The process starts in a process group of its own, with stdin closed and
//! stdout and stderr drained into a bounded buffer of lines, so a full pipe
//! never blocks it and a failure report can show its last lines. A stop
//! sends `SIGTERM` to the process, which drains its children, and kills the
//! whole group after [`STOP_WAIT`]. Dropping the value stops the process
//! and kills whatever is left of its group.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The most output lines kept per process; older lines are dropped.
pub const LOG_LINE_LIMIT: usize = 5_000;

/// The lines of a failure report.
pub const REPORT_LINES: usize = 200;

/// The longest wait for the exit after `SIGTERM`: a test profile drains in
/// 5 s, and the stop steps after the drain are bounded.
pub const STOP_WAIT: Duration = Duration::from_secs(60);

/// The longest wait for the exit after `SIGKILL`.
pub const KILL_WAIT: Duration = Duration::from_secs(10);

/// The interval of the exit and readiness polls.
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

type LogLines = Arc<Mutex<VecDeque<String>>>;

/// How a stop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopEnd {
    /// The process had exited before the stop.
    AlreadyExited(ExitStatus),
    /// The process exited after `SIGTERM`, within [`STOP_WAIT`].
    Exited(ExitStatus),
    /// The process did not exit within [`STOP_WAIT`]; its group was killed.
    /// `None` when it did not exit within [`KILL_WAIT`] either.
    KilledAfterStopWait(Option<ExitStatus>),
}

/// A process of a test, stopped when dropped.
pub struct TestProcess {
    label: String,
    child: Child,
    logs: LogLines,
    /// The drain threads of stdout and stderr; detached at drop.
    drains: Vec<JoinHandle<()>>,
}

impl TestProcess {
    /// Start `binary` with `args` and the extra environment `env`, in a new
    /// process group. Panics when the start fails.
    pub fn spawn<A, K, V>(label: &str, binary: &Path, args: A, env: &[(K, V)]) -> TestProcess
    where
        A: IntoIterator,
        A::Item: AsRef<OsStr>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        use std::os::unix::process::CommandExt;

        let mut command = Command::new(binary);
        let _configured = command
            .args(args)
            .envs(env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("{label}: start {}: {error}", binary.display()));
        let logs: LogLines = Arc::new(Mutex::new(VecDeque::with_capacity(LOG_LINE_LIMIT)));
        let mut drains = Vec::with_capacity(2);
        if let Some(stdout) = child.stdout.take() {
            drains.push(drain(stdout, Arc::clone(&logs)));
        }
        if let Some(stderr) = child.stderr.take() {
            drains.push(drain(stderr, Arc::clone(&logs)));
        }
        TestProcess {
            label: format!("{label}[pid={}]", child.id()),
            child,
            logs,
            drains,
        }
    }

    /// The process id; also the id of its process group.
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The label of failure messages: the name and the pid.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The exit status, when the process has exited.
    pub fn exit_status(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    /// Every kept output line, oldest first.
    pub fn logs(&self) -> Vec<String> {
        lock(&self.logs).iter().cloned().collect()
    }

    /// Whether a kept output line contains `needle`.
    pub fn logs_contain(&self, needle: &str) -> bool {
        lock(&self.logs).iter().any(|line| line.contains(needle))
    }

    /// The number of kept output lines that contain `needle`.
    pub fn log_count(&self, needle: &str) -> usize {
        lock(&self.logs)
            .iter()
            .filter(|line| line.contains(needle))
            .count()
    }

    /// The label and the last [`REPORT_LINES`] output lines, for a failure
    /// message.
    pub fn report(&self) -> String {
        let logs = lock(&self.logs);
        let skip = logs.len().saturating_sub(REPORT_LINES);
        let tail: Vec<&str> = logs.iter().skip(skip).map(String::as_str).collect();
        format!(
            "{} output (last {REPORT_LINES} lines):\n{}",
            self.label,
            tail.join("\n")
        )
    }

    /// Wait until `ready` answers true, polling every [`POLL_INTERVAL`].
    /// Panics with the report when the process exits first or `limit`
    /// passes.
    pub async fn wait_until(
        &mut self,
        limit: Duration,
        subject: &str,
        mut ready: impl AsyncFnMut() -> bool,
    ) {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(status) = self.exit_status() {
                panic!(
                    "{subject}: the process exited first ({status})\n{}",
                    self.report()
                );
            }
            if ready().await {
                return;
            }
            if Instant::now() >= deadline {
                panic!("{subject}: not reached within {limit:?}\n{}", self.report());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Wait until an output line contains `needle`. Panics as
    /// [`Self::wait_until`].
    pub async fn wait_for_log(&mut self, needle: &str, limit: Duration) {
        let logs = Arc::clone(&self.logs);
        self.wait_until(limit, &format!("the log line {needle:?}"), async || {
            lock(&logs).iter().any(|line| line.contains(needle))
        })
        .await;
    }

    /// Wait until the process exits on its own, at most `limit`. `None`
    /// when it still runs.
    pub async fn wait_exit(&mut self, limit: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(status) = self.exit_status() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Wait until the process exits on its own and its stdout and stderr
    /// are read to their end, at most `limit`: [`Self::logs`] then holds
    /// every line that the process wrote. `None` when `limit` passes first:
    /// the process still runs, or a process that it started holds its
    /// stdout or stderr open.
    pub async fn wait_exit_with_output(&mut self, limit: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + limit;
        loop {
            let output_read = self.drains.iter().all(JoinHandle::is_finished);
            match (self.exit_status(), output_read) {
                (Some(status), true) => return Some(status),
                (Some(_), false) | (None, _) => {}
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Send `signal` to the process (not its group).
    pub fn signal(&self, signal: libc::c_int) {
        // The pid is a child of this process that has not been reaped (the
        // value holds it), so the id is not reused.
        let _sent = unsafe { libc::kill(self.pid_t(), signal) };
    }

    /// Stop the process: `SIGTERM`, then the exit within [`STOP_WAIT`], else
    /// `SIGKILL` to the group. The test runtime keeps running while it
    /// waits.
    pub async fn stop(&mut self) -> StopEnd {
        if let Some(status) = self.exit_status() {
            return StopEnd::AlreadyExited(status);
        }
        self.signal(libc::SIGTERM);
        match self.wait_exit(STOP_WAIT).await {
            Some(status) => StopEnd::Exited(status),
            None => {
                self.kill_group();
                StopEnd::KilledAfterStopWait(self.wait_exit(KILL_WAIT).await)
            }
        }
    }

    /// Kill the process and every process of its group with `SIGKILL`: the
    /// crash of a worker with its children. Does not wait.
    pub fn kill_group(&self) {
        // The group id is the pid of the unreaped leader (process_group(0)).
        let _sent = unsafe { libc::killpg(self.pid_t(), libc::SIGKILL) };
    }

    /// Kill the process alone with `SIGKILL`; its children see the parent
    /// end. Does not wait.
    pub fn kill_parent(&self) {
        self.signal(libc::SIGKILL);
    }

    /// The ids of the direct children of the process (`pgrep -P`). Empty
    /// when it has none or `pgrep` fails.
    pub fn child_pids(&self) -> Vec<u32> {
        let output = Command::new("pgrep")
            .arg("-P")
            .arg(self.pid().to_string())
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output();
        match output {
            Ok(output) => String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn pid_t(&self) -> libc::pid_t {
        libc::pid_t::try_from(self.pid()).expect("a process id fits pid_t")
    }

    /// The blocking stop of `Drop`.
    fn stop_blocking(&mut self) {
        if self.exit_status().is_none() {
            self.signal(libc::SIGTERM);
            let deadline = Instant::now() + STOP_WAIT;
            while self.exit_status().is_none() && Instant::now() < deadline {
                std::thread::sleep(POLL_INTERVAL);
            }
        }
        // Children that outlived the worker, or a worker that did not stop.
        self.kill_group();
        let deadline = Instant::now() + KILL_WAIT;
        while self.exit_status().is_none() && Instant::now() < deadline {
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for TestProcess {
    /// Stop the process and kill its group. The drain threads are not
    /// joined: a grandchild that left the group can hold the pipes open;
    /// each thread ends at the end of its pipe.
    fn drop(&mut self) {
        self.stop_blocking();
    }
}

impl std::fmt::Debug for TestProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestProcess")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

/// Read `stream` to its end, one line at a time, into `logs`. A line that is
/// not UTF-8 is kept with replacement characters.
fn drain(stream: impl Read + Send + 'static, logs: LogLines) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stream);
        let mut line = Vec::new();
        loop {
            line.clear();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&line).trim_end().to_owned();
                    let mut kept = lock(&logs);
                    if kept.len() >= LOG_LINE_LIMIT {
                        let _dropped = kept.pop_front();
                    }
                    kept.push_back(text);
                }
            }
        }
    })
}

fn lock(logs: &LogLines) -> MutexGuard<'_, VecDeque<String>> {
    // No code that holds the lock can panic: it pushes, pops or reads.
    logs.lock()
        .expect("the log buffer lock is never held across a panic")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHELL: &str = "/bin/sh";

    const NO_ENV: [(&str, &str); 0] = [];

    #[tokio::test]
    async fn wait_exit_with_output_gives_the_status_and_every_line_of_both_streams() {
        let mut process = TestProcess::spawn(
            "shell",
            Path::new(SHELL),
            ["-c", "echo out; echo err >&2; exit 3"],
            &NO_ENV,
        );

        let status = process
            .wait_exit_with_output(Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("the shell did not end\n{}", process.report()));

        assert_eq!(status.code(), Some(3));
        let mut lines = process.logs();
        lines.sort();
        assert_eq!(lines, vec!["err".to_owned(), "out".to_owned()]);
    }

    #[tokio::test]
    async fn wait_exit_with_output_is_none_while_the_process_runs() {
        let mut process =
            TestProcess::spawn("sleep", Path::new(SHELL), ["-c", "sleep 30"], &NO_ENV);

        let ended = process
            .wait_exit_with_output(Duration::from_millis(300))
            .await;

        assert_eq!(ended, None);
        assert_eq!(process.exit_status(), None);
    }
}
