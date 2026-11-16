//! The worker connection check. Python specification:
//! `tests/integration/test_worker_connection_check.py` (the two live tests)
//! and `horsies/core/worker/profile_fit.py` (the terms and the texts). The
//! listener term is the core's one connection per process.

use std::collections::BTreeMap;
use std::time::Duration;

use serial_test::serial;
use sqlx::postgres::PgPoolOptions;

use super::*;
use crate::broker::terminalization_matrix::{migrated_database_url, migrated_pool};
use crate::core::config::bounded::BoundedU32;
use crate::core::config::worker_profile::{
    IsolatedChildren, ParentPool, PoolSizeSetting, SharedAsyncChildren, SharedPool,
    SharedThreadChildren, MAX_CHILD_POOL_CONNECTIONS,
};

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
// `horsies/core/cli.py:691-745`: the worker reads the limit before it starts,
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
