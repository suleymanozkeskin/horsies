//! The OS threads of an idle producer process of the Rust client after its
//! first send (the Python check of the same count:
//! `tests/extension/test_async_calls.py`,
//! `test_an_idle_producer_after_its_first_send_keeps_the_core_threads_only`).
//!
//! The count is read in a process of its own: the test starts this test
//! binary again with [`CHILD_URL_VARIABLE`] set, and that process runs the
//! ignored test [`child_counts_the_threads_of_a_first_send`] alone. The
//! counts of the child are differences, so the threads of the test harness
//! do not count. The database is a copy of the core's test template
//! (`crates/horsies-core/src/test_support/database.rs`); its URL names the
//! server by IP address, so the connect needs no host name lookup, which
//! runs on a thread of the runtime's blocking pool.

#![cfg(all(
    feature = "tokio-postgres",
    any(target_os = "linux", target_os = "macos")
))]

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::process::{Command, Output};
use std::time::Duration;

use horsies::{task, AppConfig, Horsies, TaskError};
use horsies_core::process_runtime::PRODUCER_WORKER_THREADS;
use horsies_core::PostgresBroker;

/// The test databases of the core. This binary creates copies of the
/// template only.
#[allow(dead_code)]
#[path = "../../horsies-core/src/test_support/database.rs"]
mod test_database;

use test_database::TestDatabase;

/// Installs the schema into a test template through the public API, with
/// the Rust client family.
async fn install_template(url: &str) {
    let broker = PostgresBroker::connect(url)
        .await
        .expect("connect the template broker");
    broker
        .migrate()
        .await
        .expect("install the schema into the test template");
    broker.close().await;
}

/// The variable that makes a run of this binary the child: the URL of its
/// database.
const CHILD_URL_VARIABLE: &str = "HORSIES_PRODUCER_THREADS_URL";

/// The name of the child test, for `--exact`.
const CHILD_TEST: &str = "child_counts_the_threads_of_a_first_send";

/// The start of the line in which the child prints its counts.
const REPORT_PREFIX: &str = "producer-threads:";

/// How long the child makes no call after its first send before its last
/// count (Python `PRODUCER_IDLE_S`).
const PRODUCER_IDLE: Duration = Duration::from_secs(2);

/// The port of a URL that names none.
const DEFAULT_POSTGRES_PORT: u16 = 5432;

#[task("producer_threads_echo")]
async fn echo(value: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(value)
}

/// The number of OS threads of this process (`/proc/self/task`).
#[cfg(target_os = "linux")]
fn os_thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read /proc/self/task")
        .count()
}

/// The number of OS threads of this process: the lines of `ps -M` after
/// its header, one per thread. `Command::output` starts no thread.
#[cfg(target_os = "macos")]
fn os_thread_count() -> usize {
    let listed = Command::new("ps")
        .args(["-M", "-p", &std::process::id().to_string()])
        .output()
        .expect("run ps -M for this process");
    assert!(listed.status.success(), "ps -M failed: {listed:?}");
    let text = String::from_utf8(listed.stdout).expect("the output of ps -M is UTF-8");
    text.lines()
        .count()
        .checked_sub(1)
        .expect("ps -M printed its header line")
}

/// `url` with its host replaced by one IP address of the host, IPv4 first.
/// A URL whose host is an IP address already is returned as it is.
///
/// # Panics
///
/// When the URL has no host or its host has no address: the test cannot
/// run.
fn with_ip_address(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("the test URL has a scheme");
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    let (user_info, host_and_port) = match authority.rsplit_once('@') {
        Some((user_info, host_and_port)) => (format!("{user_info}@"), host_and_port),
        None => (String::new(), authority),
    };
    if host_and_port.starts_with('[') {
        // A bracketed IPv6 address.
        return url.to_owned();
    }
    let (host, port) = match host_and_port.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>()
                .unwrap_or_else(|error| panic!("the port of the test URL, {port}: {error}")),
        ),
        None => (host_and_port, DEFAULT_POSTGRES_PORT),
    };
    assert!(!host.is_empty(), "the test URL names no host");
    if host.parse::<IpAddr>().is_ok() {
        return url.to_owned();
    }
    let addresses: Vec<SocketAddr> = (host, port)
        .to_socket_addrs()
        .unwrap_or_else(|error| panic!("look up the test host {host}: {error}"))
        .collect();
    let address = addresses
        .iter()
        .find(|address| address.is_ipv4())
        .or_else(|| addresses.first())
        .unwrap_or_else(|| panic!("the test host {host} has no address"));
    let shown = match address.ip() {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    format!("{scheme}://{user_info}{shown}:{port}{remainder}")
}

/// The thread counts of the child, in the order it read them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ThreadCounts {
    /// Before the app is built.
    before_app: usize,
    /// After `Horsies::new` and the registration.
    after_app: usize,
    /// After the first blocking send returned.
    after_send: usize,
    /// After [`PRODUCER_IDLE`] with no call.
    idle: usize,
}

impl ThreadCounts {
    /// The report line of the child.
    fn line(&self) -> String {
        format!(
            "{REPORT_PREFIX} before_app={} after_app={} after_send={} idle={}",
            self.before_app, self.after_app, self.after_send, self.idle
        )
    }

    /// The counts of the report line in `stdout`.
    ///
    /// # Panics
    ///
    /// When `stdout` has no report line, or the line is malformed.
    fn parse(stdout: &str) -> ThreadCounts {
        let line = stdout
            .lines()
            .find_map(|line| line.strip_prefix(REPORT_PREFIX))
            .unwrap_or_else(|| panic!("the child printed no report line:\n{stdout}"));
        let mut counts = [None; 4];
        for pair in line.split_whitespace() {
            let (key, value) = pair
                .split_once('=')
                .unwrap_or_else(|| panic!("a malformed pair {pair:?} in {line:?}"));
            let position = match key {
                "before_app" => 0,
                "after_app" => 1,
                "after_send" => 2,
                "idle" => 3,
                other => panic!("an unknown key {other:?} in {line:?}"),
            };
            let count = value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("the count of {key} in {line:?}: {error}"));
            counts[position] = Some(count);
        }
        let read = |position: usize| {
            counts[position].unwrap_or_else(|| panic!("a count is missing in {line:?}"))
        };
        ThreadCounts {
            before_app: read(0),
            after_app: read(1),
            after_send: read(2),
            idle: read(3),
        }
    }
}

/// The child: a producer app on the database of [`CHILD_URL_VARIABLE`]
/// sends once with the blocking API, then makes no call. Prints its counts.
#[test]
#[ignore = "the child process of an_idle_producer_after_its_first_send_keeps_the_runtime_workers_only"]
fn child_counts_the_threads_of_a_first_send() {
    let url = std::env::var(CHILD_URL_VARIABLE)
        .unwrap_or_else(|error| panic!("{CHILD_URL_VARIABLE} names the database: {error}"));
    let before_app = os_thread_count();
    let mut app = Horsies::new(AppConfig::for_database_url(url)).expect("the producer app");
    let echo_function = echo::register(&mut app).expect("register the task");
    let after_app = os_thread_count();
    let sent = echo_function.blocking().send(echo::Input { value: 1 });
    let after_send = os_thread_count();
    let _handle = sent.expect("the first send");
    std::thread::sleep(PRODUCER_IDLE);
    let idle = os_thread_count();
    let counts = ThreadCounts {
        before_app,
        after_app,
        after_send,
        idle,
    };
    println!("{}", counts.line());
}

/// The app build starts no thread; the first send starts the workers of the
/// process runtime and no other thread; the idle process keeps them.
#[tokio::test]
async fn an_idle_producer_after_its_first_send_keeps_the_runtime_workers_only() {
    let database = TestDatabase::create().await;
    let url = with_ip_address(database.url());
    let program = std::env::current_exe().expect("the path of this test binary");
    let ran: Output = tokio::task::spawn_blocking(move || {
        Command::new(program)
            .args([
                CHILD_TEST,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_URL_VARIABLE, url)
            .output()
    })
    .await
    .expect("the child run joins")
    .expect("the child process starts");
    let stdout = String::from_utf8_lossy(&ran.stdout);
    assert!(
        ran.status.success(),
        "the child failed ({}):\n{stdout}\n{}",
        ran.status,
        String::from_utf8_lossy(&ran.stderr)
    );
    let counts = ThreadCounts::parse(&stdout);

    assert_eq!(counts.after_app, counts.before_app, "{counts:?}");
    assert_eq!(
        counts.after_send,
        counts.before_app + PRODUCER_WORKER_THREADS.get(),
        "{counts:?}"
    );
    assert_eq!(counts.idle, counts.after_send, "{counts:?}");
}
