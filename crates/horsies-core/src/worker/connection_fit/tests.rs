//! The worker connection check. Python specification:
//! `tests/integration/test_worker_connection_check.py` (the two live tests),
//! `tests/unit/test_worker_profile_fit.py` (`TestReadServerConnectionLimit`,
//! `TestConnectionNeed`, `TestCheckLiveWorkerConnections`) and
//! `python/horsies/core/worker/profile_fit.py` (the terms and the texts). The
//! listener term is the core's one connection per process.

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use serde_json::json;
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::*;
use crate::broker::terminalization_matrix::{migrated_database_url, migrated_pool};
use crate::core::config::bounded::BoundedU32;
use crate::core::config::child_sizing::{
    parent_pool_connections, shared_async_pool_connections, shared_thread_pool_connections,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS, PARENT_POOL_CONNECTIONS_FLOOR,
};
use crate::core::config::worker_profile::{
    IsolatedChildren, ParentPool, PoolSizeSetting, SharedAsyncChildren, SharedPool,
    SharedThreadChildren, MAX_CHILD_POOL_CONNECTIONS,
};
use crate::worker::children::{
    isolated_pool_config, shared_async_pool_config, shared_thread_pool_config, ChildLaunch,
    PoolFacts,
};
use crate::worker::executor::placement::CpuPlacement;
use crate::worker::executor::protocol::{
    ChildConnection, ChildKind, ChildLogLevel, ChildProgram, DatabaseUrl,
};
use crate::worker::executor::slots::ExitNotSeen;
use crate::worker::executor::supervisor::ChildCommand;

/// Seats of the shared thread children of the live test (Python
/// `THREAD_SEATS`).
const THREAD_SEATS: u32 = 8;

/// The task-send pool of each child kind when the profile does not set one.
const DEFAULT_TASK_SEND_POOL: u32 = 2;

fn bounded<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
    BoundedU32::try_from(value).expect("a test value inside its bounds")
}

fn isolated(children: u32) -> IsolatedSetting {
    IsolatedSetting::IsolatedChildren(IsolatedChildren::new(bounded(children)))
}

fn shared_async(children: u32, seats: u32, max_size: PoolSizeSetting) -> SharedAsyncSetting {
    SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren {
        pool: SharedPool {
            max_size,
            ..SharedPool::default()
        },
        ..SharedAsyncChildren::new(bounded(children), bounded(seats))
    })
}

fn shared_thread(children: u32, seats: u32) -> SharedThreadSetting {
    SharedThreadSetting::SharedThreadChildren(SharedThreadChildren::new(
        bounded(children),
        bounded(seats),
    ))
}

fn set_pool_size(connections: u32) -> PoolSizeSetting {
    PoolSizeSetting::PoolSize {
        connections: bounded(connections),
    }
}

fn isolated_only(children: u32) -> WorkerProfile {
    WorkerProfile {
        isolated: isolated(children),
        ..WorkerProfile::default()
    }
}

/// A broker config whose session work uses the worker pool (no separate
/// session URL).
fn direct() -> PostgresConfig {
    PostgresConfig::from_url("postgresql://postgres:none@127.0.0.1:1/none")
}

fn limit(max_connections: i32, superuser_reserved: i32, reserved: i32) -> ServerConnectionLimit {
    server_connection_limit(max_connections, superuser_reserved, reserved)
        .expect("a limit with no negative setting")
}

/// The resolved pool `max_size` of each shared child of the profile's mode.
fn shared_async_pool_of(profile: &WorkerProfile) -> u32 {
    match &profile.shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => {
            children.resolved_max_pool_size().get()
        }
        SharedAsyncSetting::NoSharedAsyncChildren {} => {
            panic!("the profile has shared async children")
        }
    }
}

fn shared_thread_pool_of(profile: &WorkerProfile) -> u32 {
    match &profile.shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => {
            children.resolved_max_pool_size().get()
        }
        SharedThreadSetting::NoSharedThreadChildren {} => {
            panic!("the profile has shared thread children")
        }
    }
}

/// A broker whose pool cannot connect: a read through it fails within
/// 250 ms.
fn unreachable_broker() -> PostgresBroker {
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(250))
        .connect_lazy("postgresql://postgres:none@127.0.0.1:1/none")
        .expect("a lazy pool of an unreachable server");
    PostgresBroker::from_pool(pool)
}

/// The usable slots of the test server, read here and not through the code
/// under test.
async fn usable_slots_of_the_test_server() -> u32 {
    let usable: i32 = sqlx::query_scalar(
        "SELECT current_setting('max_connections')::int
                - current_setting('superuser_reserved_connections')::int
                - COALESCE(current_setting('reserved_connections', true), '0')::int",
    )
    .fetch_one(&migrated_pool().await)
    .await
    .expect("read the usable slots of the test server");
    let usable = u32::try_from(usable).expect("the test server has usable slots");
    assert!(usable >= 1, "the test server has usable slots");
    usable
}

fn task_sends_note(terms: &str, sends: u64, total: u64) -> String {
    format!(
        "not counted: task code that sends opens a pool of the task_send_pool of its child \
         kind in its child, for each event loop that it sends from; with one such loop in \
         each child, {terms} = {sends} more, {} in all",
        total + sends
    )
}

// --- the terms --------------------------------------------------------------

#[test]
fn the_formula_names_each_mode_and_the_one_listener_connection() {
    let profile = WorkerProfile {
        isolated: isolated(2),
        shared_async: shared_async(3, 10, PoolSizeSetting::PoolSizeFromSeats {}),
        shared_thread: shared_thread(1, THREAD_SEATS),
        ..WorkerProfile::default()
    };
    let parent = profile.resolved_parent_pool().get();
    let async_pool = shared_async_pool_of(&profile);
    let thread_pool = shared_thread_pool_of(&profile);
    let total = u64::from(parent) + 1 + 1 + 2 + 3 * u64::from(async_pool) + u64::from(thread_pool);

    let need = worker_connection_need(&profile, &direct());

    assert_eq!(need.listener, 1);
    assert_eq!(need.runner_heartbeat, 1);
    assert_eq!(need.total(), total);
    assert_eq!(
        need.formula(),
        format!(
            "parent pool max_size {parent} + listener 1 + runner heartbeat 1 \
             + 2 isolated children x isolated.pool max_size 1 \
             + 3 shared async children x shared_async.pool max_size {async_pool} \
             + 1 shared thread children x shared_thread.pool max_size {thread_pool} = {total}"
        )
    );
    assert_eq!(
        need.task_sends_note(),
        task_sends_note(
            "2 isolated children x isolated.task_send_pool 2 \
             + 3 shared async children x shared_async.task_send_pool 2 \
             + 1 shared thread children x shared_thread.task_send_pool 2",
            12,
            total
        )
    );
}

#[test]
fn a_mode_without_children_has_no_term() {
    let profile = isolated_only(1);
    let parent = profile.resolved_parent_pool().get();

    let need = worker_connection_need(&profile, &direct());

    assert_eq!(
        need.formula(),
        format!(
            "parent pool max_size {parent} + listener 1 + runner heartbeat 1 \
             + 1 isolated children x isolated.pool max_size 1 = {}",
            u64::from(parent) + 3
        )
    );
}

/// The core's worker broker builds a pool of its own on a session URL that
/// differs from `database_url`, sized as the worker pool; the sum counts it
/// right after the worker pool. Python does not count its session engine.
#[test]
fn a_separate_session_url_adds_a_session_pool_sized_as_the_worker_pool() {
    let profile = isolated_only(1);
    let parent = profile.resolved_parent_pool().get();
    let mut separate = direct();
    separate.session_database_url =
        Some("postgresql://postgres:none@127.0.0.1:1/none?application_name=session".to_owned());
    let mut same_url = direct();
    same_url.session_database_url = Some(same_url.database_url.clone());

    let need = worker_connection_need(&profile, &separate);

    assert_eq!(
        need.session_pool,
        SessionPoolTerm::Separate { max_size: parent }
    );
    assert_eq!(
        need.formula(),
        format!(
            "parent pool max_size {parent} + session pool max_size {parent} + listener 1 \
             + runner heartbeat 1 + 1 isolated children x isolated.pool max_size 1 = {}",
            2 * u64::from(parent) + 3
        )
    );
    assert_eq!(
        worker_connection_need(&profile, &same_url).session_pool,
        SessionPoolTerm::SharedWithWorkerPool
    );
}

#[test]
fn usable_slots_leave_out_both_reserved_settings() {
    let server = limit(100, 3, 2);
    assert_eq!(server.max_connections(), 100);
    assert_eq!(server.reserved(), 5);
    assert_eq!(server.usable(), 95);
    assert_eq!(limit(5, 3, 4).usable(), 0);
}

#[test]
fn a_negative_setting_makes_the_limit_unreadable_by_name() {
    for (settings, named) in [
        ((-1, 3, 0), "max_connections"),
        ((100, -1, 0), "superuser_reserved_connections"),
        ((100, 3, -1), "reserved_connections"),
    ] {
        let (max_connections, superuser_reserved, reserved) = settings;
        match server_connection_limit(max_connections, superuser_reserved, reserved) {
            Err(ConnectionLimitUnreadable::NegativeSetting { setting, value }) => {
                assert_eq!(setting, named);
                assert_eq!(value, -1);
            }
            other => panic!("{named}: expected a refused setting, got {other:?}"),
        }
    }
}

#[test]
fn a_sum_equal_to_the_usable_slots_fits_and_one_more_does_not() {
    let profile = isolated_only(1);
    let total = worker_connection_need(&profile, &direct()).total();
    let at_total = i32::try_from(total).expect("a small total");

    assert!(matches!(
        compare_connections(&profile, &direct(), limit(at_total, 0, 0)),
        ConnectionFit::Fits { .. }
    ));
    assert!(matches!(
        compare_connections(&profile, &direct(), limit(at_total - 1, 0, 0)),
        ConnectionFit::ExceedsServer { .. }
    ));
}

/// Python `ConnectionsFit.describe`: the line a worker's boot log states for
/// a profile that fits.
#[test]
fn a_fitting_profile_is_described_with_its_task_sends() {
    let profile = isolated_only(1);
    let fit = compare_connections(&profile, &direct(), limit(100, 3, 0));
    let ConnectionFit::Fits { need, .. } = &fit else {
        panic!("a small profile fits: {fit:?}");
    };

    assert_eq!(
        fit.describe(),
        format!(
            "one worker opens up to {} Postgres connections itself, of 97 usable on the \
             server; {}",
            need.total(),
            need.task_sends_note()
        )
    );
}

#[test]
fn a_profile_above_the_usable_slots_is_refused_with_the_formula_and_the_task_sends() {
    let big = isolated_only(40);
    let profiles = BTreeMap::from([
        ("big".to_owned(), big),
        ("small".to_owned(), isolated_only(1)),
    ]);
    let need = worker_connection_need(&big, &direct());

    let errors = worker_connection_errors(&profiles, &direct(), limit(30, 3, 2));

    let [error] = errors.as_slice() else {
        panic!("expected one error, got {errors:?}");
    };
    assert_eq!(error.code, Some(ErrorCode::CheckWorkerConnections));
    assert_eq!(
        error.message,
        format!(
            "worker_profiles['big']: one worker opens up to {} Postgres connections itself, \
             not counting task code; the server has 25 usable slots \
             (max_connections 30 - reserved 5)",
            need.total()
        )
    );
    assert_eq!(error.notes, vec![need.formula(), need.task_sends_note()]);
    assert_eq!(error.help_text.as_deref(), Some(CONNECTIONS_HELP));
}

#[test]
fn every_profile_that_does_not_fit_is_reported_in_name_order() {
    let profiles = BTreeMap::from([
        ("zeta".to_owned(), isolated_only(50)),
        ("alpha".to_owned(), isolated_only(60)),
    ]);

    let errors = worker_connection_errors(&profiles, &direct(), limit(20, 0, 0));

    let paths: Vec<bool> = errors
        .iter()
        .zip(["worker_profiles['alpha']:", "worker_profiles['zeta']:"])
        .map(|(error, path)| error.message.starts_with(path))
        .collect();
    assert_eq!(paths, vec![true, true], "{errors:?}");
}

// --- the check without a read -----------------------------------------------

/// Python `_check_worker_connections`: with no profiles, or through a
/// transaction pooler, nothing is read. The broker cannot connect, so a read
/// would be an error.
#[tokio::test]
async fn no_profile_or_transaction_mode_reads_nothing() {
    let broker = unreachable_broker();
    let mut config = AppConfig::for_database_url("postgresql://postgres:none@127.0.0.1:1/none");

    assert!(check_worker_connections(&config, &broker).await.is_empty());

    config
        .worker_profiles
        .insert("default".to_owned(), isolated_only(1));
    config.broker.pgbouncer_transaction_mode = true;
    config.broker.session_database_url =
        Some("postgresql://postgres:none@127.0.0.1:2/none".to_owned());
    assert!(check_worker_connections(&config, &broker).await.is_empty());
}

#[tokio::test]
async fn an_unreadable_limit_is_one_error_and_no_profile_is_compared() {
    let broker = unreachable_broker();
    let mut config = AppConfig::for_database_url("postgresql://postgres:none@127.0.0.1:1/none");
    config
        .worker_profiles
        .insert("a".to_owned(), isolated_only(1));
    config
        .worker_profiles
        .insert("b".to_owned(), isolated_only(1));

    let errors = check_worker_connections(&config, &broker).await;

    let [error] = errors.as_slice() else {
        panic!("expected one error, got {errors:?}");
    };
    assert_eq!(error.code, Some(ErrorCode::CheckWorkerConnections));
    assert_eq!(
        error.message,
        "the server connection limit could not be read"
    );
    let [note] = error.notes.as_slice() else {
        panic!("expected one note, got {:?}", error.notes);
    };
    assert!(
        note.starts_with("reading the server connection limit failed"),
        "{note}"
    );
    assert_eq!(error.help_text.as_deref(), Some(LIMIT_UNREADABLE_HELP));
}

// --- the live check against the test server ----------------------------------

/// Python `test_worker_connection_check.py::test_each_child_kind_counts_its_own_pool_against_the_server`:
/// the check reads the server's limit and refuses `big`, whose shared async
/// pools alone pass it; the formula names the pool of each child kind, and
/// the second note the task-send pool of each. `small` fits.
#[tokio::test]
#[serial]
async fn each_child_kind_counts_its_own_pool_against_the_server() {
    let usable = usable_slots_of_the_test_server().await;
    let per_async_child = usable.min(MAX_CHILD_POOL_CONNECTIONS);
    let async_children = usable / per_async_child + 1;
    let big = WorkerProfile {
        isolated: isolated(1),
        shared_async: shared_async(async_children, 4, set_pool_size(per_async_child)),
        shared_thread: shared_thread(1, THREAD_SEATS),
        ..WorkerProfile::default()
    };
    let mut config = AppConfig::for_database_url(migrated_database_url().await);
    config.worker_profiles = BTreeMap::from([
        ("big".to_owned(), big),
        ("small".to_owned(), isolated_only(1)),
    ]);
    let broker = PostgresBroker::from_pool(migrated_pool().await);

    let errors = check_worker_connections(&config, &broker).await;

    let parent_pool = u64::from(big.resolved_parent_pool().get());
    let thread_pool = shared_thread_pool_of(&big);
    let total = parent_pool
        + 1
        + 1
        + 1
        + u64::from(async_children) * u64::from(per_async_child)
        + u64::from(thread_pool);
    assert!(total > u64::from(usable));
    let [error] = errors.as_slice() else {
        panic!("expected one error, got {errors:?}");
    };
    assert_eq!(error.code, Some(ErrorCode::CheckWorkerConnections));
    assert!(
        error.message.starts_with(&format!(
            "worker_profiles['big']: one worker opens up to {total} Postgres connections itself"
        )),
        "{}",
        error.message
    );
    assert_eq!(
        error.notes,
        vec![
            format!(
                "parent pool max_size {parent_pool} + listener 1 + runner heartbeat 1 \
                 + 1 isolated children x isolated.pool max_size 1 \
                 + {async_children} shared async children x shared_async.pool max_size \
                 {per_async_child} + 1 shared thread children x shared_thread.pool max_size \
                 {thread_pool} = {total}"
            ),
            task_sends_note(
                &format!(
                    "1 isolated children x isolated.task_send_pool {DEFAULT_TASK_SEND_POOL} \
                     + {async_children} shared async children x shared_async.task_send_pool \
                     {DEFAULT_TASK_SEND_POOL} + 1 shared thread children x \
                     shared_thread.task_send_pool {DEFAULT_TASK_SEND_POOL}"
                ),
                u64::from(DEFAULT_TASK_SEND_POOL) * (1 + u64::from(async_children) + 1),
                total
            ),
        ]
    );
}

/// Python `test_worker_connection_check.py::test_the_parent_pool_of_the_profile_is_counted_against_the_server`:
/// a parent pool above the usable slots alone fails the check, and the
/// formula names it. The pool of `PostgresConfig` plays no part.
#[tokio::test]
#[serial]
async fn the_parent_pool_of_the_profile_is_counted_against_the_server() {
    let usable = usable_slots_of_the_test_server().await;
    let parent_pool = (usable + 1).min(MAX_CHILD_POOL_CONNECTIONS);
    let profile = WorkerProfile {
        isolated: isolated(1),
        pool: ParentPool {
            max_size: set_pool_size(parent_pool),
        },
        ..WorkerProfile::default()
    };
    let mut config = AppConfig::for_database_url(migrated_database_url().await);
    config.broker.pool_size = 1;
    config.broker.max_overflow = 0;
    config.worker_profiles = BTreeMap::from([("parent".to_owned(), profile)]);
    let broker = PostgresBroker::from_pool(migrated_pool().await);

    let errors = check_worker_connections(&config, &broker).await;

    let total = u64::from(parent_pool) + 1 + 1 + 1;
    assert!(total > u64::from(usable));
    let [error] = errors.as_slice() else {
        panic!("expected one error, got {errors:?}");
    };
    assert_eq!(error.code, Some(ErrorCode::CheckWorkerConnections));
    assert_eq!(
        error.notes.first().map(String::as_str),
        Some(
            format!(
                "parent pool max_size {parent_pool} + listener 1 + runner heartbeat 1 \
                 + 1 isolated children x isolated.pool max_size 1 = {total}"
            )
            .as_str()
        )
    );
}

// --- the fit at worker boot ---------------------------------------------------
//
// Python `tests/unit/test_cli.py::TestBootConnectionFit` and
// `python/horsies/core/cli.py:691-745`: the worker reads the limit before it starts,
// logs the fit of its profile, and starts in every case.

/// `test_a_profile_above_the_limit_warns_and_the_worker_starts`, with the
/// core's one listener connection: 5 + 1 + 1 + 20 = 27 against 26 usable.
#[test]
fn a_profile_above_the_limit_is_one_warning_with_the_formula_and_the_help() {
    let profile = isolated_only(20);
    let fit =
        BootConnectionFit::Compared(compare_connections(&profile, &direct(), limit(29, 3, 0)));

    let record = boot_connection_log("default", &fit);

    assert_eq!(record.level, BootLogLevel::Warning);
    assert!(
        record.message.starts_with(
            "Worker profile 'default': one worker opens up to 27 Postgres connections itself, \
             not counting task code; the server has 26 usable slots"
        ),
        "{}",
        record.message
    );
    assert!(
        record
            .message
            .contains("20 isolated children x isolated.task_send_pool 2 = 40 more, 67 in all"),
        "{}",
        record.message
    );
    assert!(
        record
            .message
            .ends_with(&format!("To fix: {CONNECTIONS_HELP}")),
        "{}",
        record.message
    );
}

/// `test_a_profile_within_the_limit_does_not_warn`.
#[test]
fn a_profile_within_the_limit_is_info() {
    let profile = isolated_only(1);
    let compared = compare_connections(&profile, &direct(), limit(100, 3, 0));
    let expected = format!("Worker profile 'default': {}", compared.describe());

    let record = boot_connection_log("default", &BootConnectionFit::Compared(compared));

    assert_eq!(record.level, BootLogLevel::Info);
    assert_eq!(record.message, expected);
}

/// Through a transaction pooler the boot reads nothing: the broker cannot
/// connect, so a read would be the unreadable outcome.
#[tokio::test]
async fn boot_through_a_transaction_pooler_reads_nothing() {
    let broker = unreachable_broker();
    let mut config = direct();
    config.pgbouncer_transaction_mode = true;
    config.session_database_url = Some("postgresql://postgres:none@127.0.0.1:2/none".to_owned());

    let fit = boot_connection_fit(&isolated_only(1), &config, &broker).await;

    assert!(
        matches!(fit, BootConnectionFit::NotComparedThroughPooler),
        "{fit:?}"
    );
    assert_eq!(
        boot_connection_log("default", &fit),
        BootConnectionLog {
            level: BootLogLevel::Info,
            message: "Worker profile 'default': connections go through a transaction pooler; \
                      the server connection limit is not compared"
                .to_owned(),
        }
    );
}

/// `test_an_unreadable_limit_warns_and_the_worker_starts`.
#[tokio::test]
async fn boot_with_an_unreadable_limit_is_one_warning() {
    let broker = unreachable_broker();

    let fit = boot_connection_fit(&isolated_only(1), &direct(), &broker).await;

    assert!(
        matches!(fit, BootConnectionFit::LimitUnreadable(_)),
        "{fit:?}"
    );
    let record = boot_connection_log("batch", &fit);
    assert_eq!(record.level, BootLogLevel::Warning);
    assert!(
        record.message.starts_with(
            "Worker profile 'batch': the server connection limit could not be read \
             (reading the server connection limit failed: "
        ),
        "{}",
        record.message
    );
    assert!(
        record
            .message
            .ends_with("; the connections of this worker were not compared with it"),
        "{}",
        record.message
    );
}

#[test]
fn a_name_is_quoted_as_python_s_repr() {
    assert_eq!(quoted_profile_name("default"), "'default'");
    assert_eq!(quoted_profile_name(r"a\b"), r"'a\\b'");
}

// --- the read of the server limit --------------------------------------------
//
// Python `tests/unit/test_worker_profile_fit.py::TestReadServerConnectionLimit`.

/// `test_reads_max_and_both_reserved_settings`: the read returns the
/// server's `max_connections`, and as reserved the sum of
/// `superuser_reserved_connections` and `reserved_connections` (no such
/// setting before PostgreSQL 16: 0). The settings are read here from
/// `pg_settings`, not through the code under test.
#[tokio::test]
#[serial]
async fn the_read_returns_max_connections_and_both_reserved_settings() {
    let pool = migrated_pool().await;
    let (max_connections, superuser_reserved, reserved): (i32, i32, i32) = sqlx::query_as(
        "SELECT (SELECT setting::int FROM pg_settings WHERE name = 'max_connections'),
                (SELECT setting::int FROM pg_settings
                  WHERE name = 'superuser_reserved_connections'),
                COALESCE((SELECT setting::int FROM pg_settings
                           WHERE name = 'reserved_connections'), 0)",
    )
    .fetch_one(&pool)
    .await
    .expect("read the settings of the test server");
    let broker = PostgresBroker::from_pool(pool);

    let read = read_server_connection_limit(&broker)
        .await
        .expect("the test server answers the read");

    assert_eq!(
        i64::from(read.max_connections()),
        i64::from(max_connections)
    );
    assert_eq!(
        i64::from(read.reserved()),
        i64::from(superuser_reserved) + i64::from(reserved)
    );
}

/// `test_database_error_is_unreadable`: the server refuses the session (its
/// database does not exist), and the limit is unreadable with the server's
/// error.
#[tokio::test]
#[serial]
async fn a_server_error_makes_the_limit_unreadable() {
    let url = migrated_database_url().await;
    let missing = PgConnectOptions::from_str(&url)
        .expect("the test database url parses")
        .database(&format!(
            "horsies_missing_{}",
            uuid::Uuid::new_v4().simple()
        ));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_lazy_with(missing);
    let broker = PostgresBroker::from_pool(pool);

    let read = read_server_connection_limit(&broker).await;

    match read {
        Err(ConnectionLimitUnreadable::Database(BrokerError::Statement(
            crate::db::StatementFailure::NotSent {
                cause:
                    crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::ConnectFailed {
                        cause:
                            crate::db::ConnectError {
                                cause: crate::db::ConnectFailure::ServerRefused(error),
                                ..
                            },
                        ..
                    }),
                ..
            },
        ))) => {
            assert_eq!(error.code().code(), "3D000", "{error}");
        }
        other => panic!("expected the server's refusal, got {other:?}"),
    }
}

/// `test_os_error_is_unreadable`: no connection can be made (nothing
/// listens on the port), and the limit is unreadable with the driver's
/// error.
#[tokio::test]
async fn a_connection_that_cannot_be_made_makes_the_limit_unreadable() {
    let read = read_server_connection_limit(&unreachable_broker()).await;

    assert!(
        matches!(read, Err(ConnectionLimitUnreadable::Database(_))),
        "{read:?}"
    );
}

/// `test_no_answer_within_the_bound_is_unreadable`: a server that takes the
/// connection and never answers. The clock is paused, so the bound of the
/// read passes with no real wait; the connect bound of the pool (the URL's
/// `connect_timeout`) is longer, so the bound of the read is what ends it.
#[tokio::test(start_paused = true)]
async fn no_answer_within_the_bound_makes_the_limit_unreadable() {
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let port = silent.local_addr().expect("the bound address").port();
    // The accepted stream stays open in the output of this task until the
    // handle is dropped; the task never writes to it.
    let held = tokio::spawn(async move { silent.accept().await });
    let connect_bound_seconds = CONNECTION_LIMIT_READ_TIMEOUT.as_secs() * 6;
    let handle = crate::test_support::core_handle(&format!(
        "postgresql://postgres:none@127.0.0.1:{port}/none?connect_timeout={connect_bound_seconds}"
    ));
    let open = handle.pools().expect("the pools of a silent server build");

    let read = read_connection_limit_on(open.main(), StatementLog::Off).await;

    held.abort();
    match read {
        Err(ConnectionLimitUnreadable::TimedOut { bound }) => {
            assert_eq!(bound, CONNECTION_LIMIT_READ_TIMEOUT);
        }
        other => panic!("expected the read to time out, got {other:?}"),
    }
}

// --- the terms of a profile ----------------------------------------------------
//
// Python `tests/unit/test_worker_profile_fit.py::TestConnectionNeed`, with the
// core's one listener connection.

fn parsed(document: serde_json::Value) -> WorkerProfile {
    serde_json::from_value(document).expect("a valid test profile")
}

fn seats(count: u32) -> NonZeroU32 {
    NonZeroU32::new(count).expect("test seats above 0")
}

/// The task-send pool that the init frame of the one child kind of
/// `profile` gives each child of that kind.
fn task_send_pool_of_the_init_frame(profile: &WorkerProfile) -> u64 {
    let launch = ChildLaunch {
        command: ChildCommand {
            program: PathBuf::from("/bin/sh"),
            args: Vec::new(),
        },
        preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u:p@localhost/db".to_owned()),
        connection: ChildConnection::default(),
    };
    let facts = PoolFacts {
        worker_id: "worker-1".to_owned(),
        pending_limit: NonZeroUsize::new(8).expect("a test pending limit above 0"),
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        placement: CpuPlacement::SharedCpus,
    };
    let kinds = [
        isolated_pool_config(profile, &launch, &facts).map(|config| config.init.child_kind),
        shared_async_pool_config(profile, &launch, &facts).map(|config| config.init.child_kind),
        shared_thread_pool_config(profile, &launch, &facts).map(|config| config.init.child_kind),
    ];
    let sends: Vec<u64> = kinds
        .into_iter()
        .flatten()
        .map(|kind| match kind {
            ChildKind::Isolated(child) => child.task_send_pool.get(),
            ChildKind::SharedAsync(child) => child.task_send_pool.get(),
            ChildKind::SharedThread(child) => child.task_send_pool.get(),
        })
        .collect();
    let [send] = sends.as_slice() else {
        panic!("the profile has one kind of child: {sends:?}");
    };
    *send
}

/// `test_parent_pool_listener_heartbeat_and_child_pools`: 8 isolated
/// children need the worker pool floor, the listener, the runner heartbeat
/// and the pool of each child: 5 + 1 + 1 + 8 = 15.
#[test]
fn the_need_is_the_worker_pool_the_listener_the_heartbeat_and_each_child_pool() {
    let need = worker_connection_need(&isolated_only(8), &direct());

    assert_eq!(
        need,
        WorkerConnectionNeed {
            parent_pool: PARENT_POOL_CONNECTIONS_FLOOR,
            session_pool: SessionPoolTerm::SharedWithWorkerPool,
            listener: LISTENER_CONNECTIONS_PER_WORKER,
            runner_heartbeat: RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER,
            child_pools: vec![ModePools {
                mode: ChildMode::Isolated,
                children: 8,
                per_child: 1,
                task_send_pool: DEFAULT_TASK_SEND_POOL_CONNECTIONS,
            }],
        }
    );
    assert_eq!(LISTENER_CONNECTIONS_PER_WORKER, 1);
    assert_eq!(RUNNER_HEARTBEAT_CONNECTIONS_PER_WORKER, 1);
    assert_eq!(need.total(), 15);
}

/// `test_task_sends_add_the_task_send_pool_of_each_kind`: each kind counts
/// its own task-send pool in what task code that sends adds; the pool of the
/// broker config plays no part.
#[test]
fn task_sends_add_the_task_send_pool_of_each_kind() {
    let profile = parsed(json!({
        "isolated": {"kind": "isolated_children", "count": 2,
                     "task_send_pool": {"connections": 3}},
        "shared_async": {"kind": "shared_async_children", "count": 8, "seats": 50,
                         "task_send_pool": {"connections": 7}},
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 4}
    }));
    let mut large_broker_pool = direct();
    large_broker_pool.pool_size = 30;
    large_broker_pool.max_overflow = 30;

    let need = worker_connection_need(&profile, &direct());

    let sends: Vec<u32> = need
        .child_pools
        .iter()
        .map(|pools| pools.task_send_pool)
        .collect();
    assert_eq!(sends, vec![3, 7, 2]);
    assert_eq!(need.task_sends(), 2 * 3 + 8 * 7 + 2);
    assert_eq!(need.with_task_sends(), need.total() + 64);
    assert_eq!(
        need.task_sends_note(),
        task_sends_note(
            "2 isolated children x isolated.task_send_pool 3 \
             + 8 shared async children x shared_async.task_send_pool 7 \
             + 1 shared thread children x shared_thread.task_send_pool 2",
            64,
            need.total()
        )
    );
    assert_eq!(worker_connection_need(&profile, &large_broker_pool), need);
}

/// `test_the_task_send_term_is_the_pool_of_the_broker_of_a_child`: the init
/// frame gives each child the task-send pool of its kind, and task code in
/// the child sends through a pool of exactly that size with no overflow
/// ([`PostgresConfig::with_exact_pool`]), whatever pool the broker config
/// names. The check counts that same size for the kind.
#[test]
fn the_task_send_term_is_the_pool_that_a_child_of_its_kind_opens() {
    let mut broker = direct();
    broker.pool_size = 30;
    broker.max_overflow = 30;
    let isolated = json!({
        "isolated": {"kind": "isolated_children", "count": 1,
                     "task_send_pool": {"connections": 4}}
    });
    let shared_async = json!({
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 10,
                         "task_send_pool": {"connections": 6}}
    });
    let shared_thread = json!({
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 10}
    });
    for profile in [isolated, shared_async, shared_thread].map(parsed) {
        let need = worker_connection_need(&profile, &broker);
        let [pools] = need.child_pools.as_slice() else {
            panic!("the profile has one kind of child: {need:?}");
        };
        let sent = task_send_pool_of_the_init_frame(&profile);
        let child_pool = NonZeroU32::new(pools.task_send_pool).expect("a task-send pool above 0");

        let child_broker = broker.with_exact_pool(child_pool);

        assert_eq!(sent, u64::from(pools.task_send_pool), "{:?}", pools.mode);
        assert_eq!(
            (child_broker.pool_size, child_broker.max_overflow),
            (pools.task_send_pool, 0),
            "{:?}",
            pools.mode
        );
    }
}

/// `test_each_mode_counts_its_own_resolved_pool_max`: each child counts the
/// resolved pool `max_size` of its mode, the shared pools from their seats,
/// and the worker pool follows every seat.
#[test]
fn each_mode_counts_its_own_resolved_pool_max() {
    let profile = parsed(json!({
        "isolated": {"kind": "isolated_children", "count": 2},
        "shared_async": {"kind": "shared_async_children", "count": 3, "seats": 200},
        "shared_thread": {"kind": "shared_thread_children", "count": 4, "seats": 64}
    }));
    let async_pool = shared_async_pool_connections(seats(200)).get();
    let thread_pool = shared_thread_pool_connections(seats(64)).get();
    let send = DEFAULT_TASK_SEND_POOL_CONNECTIONS;

    let need = worker_connection_need(&profile, &direct());

    assert_eq!(
        need.child_pools,
        vec![
            ModePools {
                mode: ChildMode::Isolated,
                children: 2,
                per_child: 1,
                task_send_pool: send,
            },
            ModePools {
                mode: ChildMode::SharedAsync,
                children: 3,
                per_child: async_pool,
                task_send_pool: send,
            },
            ModePools {
                mode: ChildMode::SharedThread,
                children: 4,
                per_child: thread_pool,
                task_send_pool: send,
            },
        ]
    );
    assert_eq!(
        need.child_pools
            .iter()
            .map(|pools| pools.children)
            .sum::<u32>(),
        9
    );
    assert_eq!(
        need.parent_pool,
        parent_pool_connections(2 + 3 * 200 + 4 * 64).get()
    );
    assert_eq!(
        need.total(),
        u64::from(need.parent_pool)
            + 1
            + 1
            + 2
            + 3 * u64::from(async_pool)
            + 4 * u64::from(thread_pool)
    );
}

/// `test_a_set_pool_size_is_counted_as_set`: a pool `max_size` that the
/// profile sets is the term of its mode, whatever the seats derive.
#[test]
fn a_set_child_pool_size_is_counted_as_set() {
    let profile = parsed(json!({
        "isolated": {"kind": "isolated_children", "count": 2,
                     "pool": {"max_size": {"kind": "pool_size", "connections": 5}}},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 200,
                         "pool": {"max_size": {"kind": "pool_size", "connections": 7}}},
        "shared_thread": {"kind": "shared_thread_children", "count": 3, "seats": 1,
                          "pool": {"max_size": {"kind": "pool_size", "connections": 1}}}
    }));

    let need = worker_connection_need(&profile, &direct());

    let per_child: Vec<(ChildMode, u32)> = need
        .child_pools
        .iter()
        .map(|pools| (pools.mode, pools.per_child))
        .collect();
    assert_eq!(
        per_child,
        vec![
            (ChildMode::Isolated, 5),
            (ChildMode::SharedAsync, 7),
            (ChildMode::SharedThread, 1),
        ]
    );
    let parent_pool = u64::from(parent_pool_connections(2 + 200 + 3).get());
    assert_eq!(need.total(), parent_pool + 1 + 1 + 2 * 5 + 7 + 3);
}

/// `test_the_parent_pool_derived_from_the_seats_is_counted`: one shared
/// async child of 1000 seats derives a worker pool of 20, and the check
/// counts all of it.
#[test]
fn the_worker_pool_derived_from_the_seats_is_counted() {
    let profile = parsed(json!({
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 1000}
    }));

    let need = worker_connection_need(&profile, &direct());

    assert_eq!(need.parent_pool, 20);
    assert_eq!(
        need.total(),
        20 + 1 + 1 + u64::from(shared_async_pool_connections(seats(1000)).get())
    );
    assert!(
        need.formula()
            .starts_with("parent pool max_size 20 + listener 1"),
        "{}",
        need.formula()
    );
}

/// `test_a_set_parent_pool_is_counted_as_set`: a worker pool that the
/// profile sets is counted as set, not as the seats derive it.
#[test]
fn a_set_worker_pool_is_counted_as_set() {
    let profile = parsed(json!({
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 1000},
        "pool": {"max_size": {"kind": "pool_size", "connections": 40}}
    }));

    let need = worker_connection_need(&profile, &direct());

    assert_eq!(need.parent_pool, 40);
    assert_eq!(
        need.total(),
        40 + 1 + 1 + u64::from(shared_async_pool_connections(seats(1000)).get())
    );
}

// --- the live check of every profile -------------------------------------------
//
// Python `tests/unit/test_worker_profile_fit.py::TestCheckLiveWorkerConnections`.

/// `test_profiles_that_fit_have_no_error`: the check reads the limit of the
/// test server, and a profile whose worker fits in the usable slots gives no
/// error.
#[tokio::test]
#[serial]
async fn profiles_that_fit_have_no_error() {
    let usable = usable_slots_of_the_test_server().await;
    let mut config = AppConfig::for_database_url(migrated_database_url().await);
    config.worker_profiles = BTreeMap::from([("small".to_owned(), isolated_only(1))]);
    let need = worker_connection_need(&isolated_only(1), &config.broker).total();
    assert!(
        need <= u64::from(usable),
        "the test server has {usable} usable slots, fewer than the {need} of one small worker"
    );
    let broker = PostgresBroker::from_pool(migrated_pool().await);

    let errors = check_worker_connections(&config, &broker).await;

    assert!(errors.is_empty(), "{errors:?}");
}
