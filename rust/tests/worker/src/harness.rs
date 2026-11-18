//! One test of the suite: a fresh database, the app of an instance in the
//! test process, a marker directory, and the worker and scheduler
//! processes of the test binary (Python `tests/e2e/conftest.py`,
//! `helpers/worker.py`).
//!
//! The test process is a producer: it sends and reads through the client
//! API. Task bodies run only in the children of the worker processes, which
//! build the same app from the environment that [`Suite::process_env`]
//! gives them.

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::time::Duration;

use horsies::{
    Horsies, TaskFunction, TaskFunctionNotFound, TaskHandle, TaskOutcome, TaskRuntime,
    UserErrorCode, WaitLimit,
};
use horsies_test_support::database::{TestDatabase, Topology};
use horsies_test_support::process::TestProcess;
use horsies_test_support::rows;
use serde::de::DeserializeOwned;

use crate::instance::{Connection, Instance, Profile, ReadyClass, INSTANCE_ENV};
use crate::markers::{MarkerDir, MARKER_DIR_ENV};
use crate::tasks::{basic, classes, custom, recovery};

/// The longest wait for a worker to run its readiness task: the start of
/// the parent, its children and the first claim.
pub const READY_LIMIT: Duration = Duration::from_secs(60);

/// The line that the core worker logs once its start ended: its children,
/// its startup coverage, the liveness lock and its services (the ping
/// responder among them) run (`horsies-core` `worker/worker.rs`).
pub const WORKER_READY_LINE: &str = "worker ready";

/// The result wait of one readiness probe.
const READY_PROBE: Duration = Duration::from_millis(500);

/// The longest wait for a scheduler to write its first schedule state.
pub const SCHEDULER_READY_LIMIT: Duration = Duration::from_secs(30);

/// The worker log level of the test processes.
const LOG_LEVEL: &str = "INFO";

/// A wait limit of `duration`. Panics above the wait bound of the client.
pub fn within(duration: Duration) -> WaitLimit {
    WaitLimit::parse(Some(duration))
        .unwrap_or_else(|refused| panic!("a test wait limit of {duration:?}: {refused}"))
}

/// The resources of one test. Dropped in field order: the app, the marker
/// directory, then the database.
pub struct Suite {
    instance: Instance,
    binary: PathBuf,
    connection: Connection,
    runtime: TaskRuntime,
    /// The app of the instance in the test process.
    pub app: Horsies,
    /// The marker directory that the worker children see.
    pub markers: MarkerDir,
    /// The database of the test.
    pub db: TestDatabase,
}

impl Suite {
    /// A fresh database, the app of `instance` on it with its schema
    /// installed, and a marker directory. `binary` is the test worker
    /// binary: `env!("CARGO_BIN_EXE_horsies-test-worker")` in a test of this
    /// crate. Panics with the cause when a step fails.
    pub async fn start(binary: &str, instance: Instance) -> Suite {
        let db = TestDatabase::create().await;
        let connection = match db.topology() {
            Topology::Direct => Connection {
                database_url: db.url().to_owned(),
                session_database_url: None,
            },
            Topology::TransactionPool { pool_url } => Connection {
                database_url: pool_url.clone(),
                session_database_url: Some(db.url().to_owned()),
            },
        };
        let app = crate::build_app(instance, &connection)
            .unwrap_or_else(|error| panic!("build the app of {}: {error}", instance.name()));
        // The first operation installs the schema, before any worker starts.
        let _ping = app
            .ping_database()
            .await
            .unwrap_or_else(|error| panic!("install the schema in {}: {error}", db.name()));
        Suite {
            instance,
            binary: PathBuf::from(binary),
            connection,
            runtime: app.task_runtime(),
            app,
            markers: MarkerDir::create(),
            db,
        }
    }

    /// The instance of the suite.
    pub fn instance(&self) -> Instance {
        self.instance
    }

    /// The configuration of the suite's app: a second app of a test, on
    /// the same database, starts from it.
    pub fn config(&self) -> horsies::AppConfig {
        self.instance.config(&self.connection)
    }

    /// The typed function of a task of the app, through its generated
    /// `handle`: `suite.function(basic::simple::handle)`.
    pub fn function<A, T, E>(
        &self,
        handle: fn(&TaskRuntime) -> Result<TaskFunction<A, T, E>, TaskFunctionNotFound>,
    ) -> TaskFunction<A, T, E> {
        handle(&self.runtime).unwrap_or_else(|missing| panic!("{missing}"))
    }

    /// A direct connection to the database of the test.
    pub async fn client(&self) -> horsies_test_support::tokio_postgres::Client {
        self.db.connect().await
    }

    /// The environment of a process of the test binary.
    pub fn process_env(&self) -> Vec<(&'static str, String)> {
        let mut env = self.connection.env();
        env.push((INSTANCE_ENV, self.instance.name().to_owned()));
        env.push((MARKER_DIR_ENV, self.markers.path().display().to_string()));
        env.push(("RUST_BACKTRACE", "1".to_owned()));
        env
    }

    /// Start the test binary with `args`. Not waited for.
    pub fn spawn(&self, label: &str, args: &[&str]) -> TestProcess {
        TestProcess::spawn(label, Path::new(&self.binary), args, &self.process_env())
    }

    /// Start a worker of `profile` without waiting for it.
    pub fn spawn_worker(&self, profile: Profile) -> TestProcess {
        self.spawn(
            &format!("worker {}", profile.name()),
            &[
                "worker",
                "--profile",
                profile.name(),
                "--loglevel",
                LOG_LEVEL,
            ],
        )
    }

    /// Start a worker of `profile` and wait until it logged
    /// [`WORKER_READY_LINE`] and a readiness task of the profile's class ran
    /// (Python `run_worker` with its ready check). The log line is this
    /// worker's own: with two workers of one profile, the first can run the
    /// readiness task of the second before the second answers pings.
    pub async fn worker(&self, profile: Profile) -> TestProcess {
        let mut worker = self.spawn_worker(profile);
        worker.wait_for_log(WORKER_READY_LINE, READY_LIMIT).await;
        self.wait_ready(&mut worker, profile).await;
        worker
    }

    /// Wait until `worker`, of `profile`, ran a readiness task. Panics with
    /// the worker's output when it exits first or [`READY_LIMIT`] passes.
    pub async fn wait_ready(&self, worker: &mut TestProcess, profile: Profile) {
        let ready = self.ready_function(profile.ready_class());
        let handle = ready
            .send(())
            .await
            .unwrap_or_else(|error| panic!("send the readiness task: {error}"));
        worker
            .wait_until(
                READY_LIMIT,
                "the worker runs its readiness task",
                async || {
                    matches!(
                        handle.get(within(READY_PROBE)).await,
                        Ok(TaskOutcome::Succeeded(_))
                    )
                },
            )
            .await;
    }

    fn ready_function(&self, class: ReadyClass) -> TaskFunction<(), String, Infallible> {
        match (self.instance, class) {
            (Instance::CustomQueues, ReadyClass::Isolated) => {
                self.function(custom::custom_healthcheck::handle)
            }
            (Instance::CustomQueues, ReadyClass::SharedAsync | ReadyClass::SharedThread) => {
                panic!("the custom queue instance has no shared readiness task")
            }
            (Instance::RecoveryQueues, ReadyClass::Isolated) => {
                self.function(recovery::recovery_healthcheck::handle)
            }
            (Instance::RecoveryQueues, ReadyClass::SharedAsync | ReadyClass::SharedThread) => {
                panic!("the recovery queue instance has no shared readiness task")
            }
            (_, ReadyClass::Isolated) => self.function(basic::healthcheck::handle),
            (_, ReadyClass::SharedAsync) => self.function(classes::shared_healthcheck::handle),
            (_, ReadyClass::SharedThread) => self.function(classes::thread_healthcheck::handle),
        }
    }

    /// Start the scheduler and wait until it wrote a schedule state row.
    pub async fn scheduler(&self) -> TestProcess {
        let mut scheduler = self.spawn("scheduler", &["scheduler", "--loglevel", LOG_LEVEL]);
        let client = self.client().await;
        scheduler
            .wait_until(
                SCHEDULER_READY_LIMIT,
                "the scheduler writes its state",
                async || {
                    rows::count(&client, "SELECT count(*) FROM horsies_schedule_state", &[]).await
                        > 0
                },
            )
            .await;
        scheduler
    }
}

/// The outcome of `handle` within `limit`. Panics when the read fails.
pub async fn outcome<T, E>(handle: &TaskHandle<T, E>, limit: Duration) -> TaskOutcome<T, E>
where
    T: DeserializeOwned + Clone,
    E: UserErrorCode,
{
    handle
        .get(within(limit))
        .await
        .unwrap_or_else(|error| panic!("read the outcome of task {}: {error}", handle.task_id()))
}

/// The value of a task that succeeded within `limit`. Panics on a failed
/// task with its error.
pub async fn succeeded<T, E>(handle: &TaskHandle<T, E>, limit: Duration) -> T
where
    T: DeserializeOwned + Clone,
    E: UserErrorCode + std::fmt::Debug,
{
    match outcome(handle, limit).await {
        TaskOutcome::Succeeded(value) => value,
        TaskOutcome::Failed(error) => {
            panic!("task {} failed: {error:?}", handle.task_id())
        }
    }
}

/// The error code text of a task that failed within `limit`. Panics on a
/// task that succeeded.
pub async fn failed_code<T, E>(handle: &TaskHandle<T, E>, limit: Duration) -> String
where
    T: DeserializeOwned + Clone + std::fmt::Debug,
    E: UserErrorCode,
{
    match outcome(handle, limit).await {
        TaskOutcome::Failed(error) => error.code().text(),
        TaskOutcome::Succeeded(value) => {
            panic!("task {} succeeded with {value:?}", handle.task_id())
        }
    }
}
