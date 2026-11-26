//! One worker profile with the three execution classes: isolated children,
//! shared async children and shared thread children. Each task declares its
//! class; a shared class needs `timeout_ms`.
//!
//! ```text
//! cargo run -p horsies-examples --example worker_profiles -- worker --profile mixed
//! cargo run -p horsies-examples --example worker_profiles -- demo
//! ```

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::process::ExitCode;
use std::time::Duration;

use horsies::{
    blocking_task, task, AppConfig, ChildPreload, Horsies, HorsiesError, IsolatedChildCount,
    IsolatedChildren, IsolatedSetting, SharedAsyncChildCount, SharedAsyncChildren,
    SharedAsyncSeats, SharedAsyncSetting, SharedThreadChildCount, SharedThreadChildren,
    SharedThreadSeats, SharedThreadSetting, TaskError, TaskOutcome, TaskRuntime, UserErrorCode,
    WaitLimit, WorkerProfile,
};
use horsies_examples::{app_refused, database_url, main_of, DemoResult};

/// The profile that `worker --profile mixed` runs.
const PROFILE: &str = "mixed";

const ISOLATED_CHILDREN: IsolatedChildCount = IsolatedChildCount::from_constant(1);
const ASYNC_CHILDREN: SharedAsyncChildCount = SharedAsyncChildCount::from_constant(1);
const ASYNC_SEATS: SharedAsyncSeats = SharedAsyncSeats::from_constant(20);
const THREAD_CHILDREN: SharedThreadChildCount = SharedThreadChildCount::from_constant(1);
const THREAD_SEATS: SharedThreadSeats = SharedThreadSeats::from_constant(4);

/// The codes of `fetch_page`'s own errors.
#[derive(Debug, Clone, PartialEq, UserErrorCode)]
enum FetchCode {
    Interrupted,
}

/// One task at a time in its own process: for code that may crash or leak.
#[blocking_task("checksum", execution = isolated)]
fn checksum(data: Vec<u8>) -> Result<u64, TaskError<Infallible>> {
    Ok(data.into_iter().map(u64::from).sum())
}

/// Many runs on one runtime of a child. At `timeout_ms` the run gets the
/// cancel signal, then `cancel_grace_ms` to end.
#[task(
    "fetch_page",
    execution = shared_async,
    timeout_ms = 10_000,
    cancel_grace_ms = 500
)]
async fn fetch_page(rt: TaskRuntime, path: String) -> Result<usize, TaskError<FetchCode>> {
    tokio::select! {
        () = tokio::time::sleep(Duration::from_millis(100)) => Ok(path.len()),
        () = rt.cancelled() => Err(TaskError::user(FetchCode::Interrupted, "cancelled at the deadline")),
    }
}

/// Many runs on the thread seats of a child.
#[blocking_task("resize", execution = shared_thread, timeout_ms = 10_000)]
fn resize(width: u32, height: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(width / 2 * (height / 2))
}

/// One child of each class.
fn mixed_profile() -> WorkerProfile {
    WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(ISOLATED_CHILDREN)),
        shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
            ASYNC_CHILDREN,
            ASYNC_SEATS,
        )),
        shared_thread: SharedThreadSetting::SharedThreadChildren(SharedThreadChildren::new(
            THREAD_CHILDREN,
            THREAD_SEATS,
        )),
        preload: ChildPreload::NoPreload,
        ..WorkerProfile::default()
    }
}

fn build_app() -> Result<Horsies, HorsiesError> {
    let mut config = AppConfig::for_database_url(database_url());
    config.worker_profiles = BTreeMap::from([(PROFILE.to_owned(), mixed_profile())]);
    let mut app = Horsies::new(config)?;
    let _checksum = checksum::register(&mut app)?;
    let _fetch_page = fetch_page::register(&mut app)?;
    let _resize = resize::register(&mut app)?;
    Ok(app)
}

async fn demo(app: Horsies) -> DemoResult {
    let limit = WaitLimit::Within(Duration::from_secs(30));
    let sum = checksum::send(&app, vec![1, 2, 3]).await?;
    let mut pages = Vec::new();
    for path in ["/", "/docs", "/docs/rust"] {
        pages.push(fetch_page::send(&app, path.to_owned()).await?);
    }
    let thumbnail = resize::send(&app, 1920, 1080).await?;

    match sum.get(limit).await? {
        TaskOutcome::Succeeded(value) => println!("checksum: {value}"),
        TaskOutcome::Failed(error) => println!("checksum failed: {error}"),
    }
    for page in pages {
        match page.get(limit).await? {
            TaskOutcome::Succeeded(length) => println!("fetch_page: {length}"),
            TaskOutcome::Failed(error) => println!("fetch_page failed: {error}"),
        }
    }
    match thumbnail.get(limit).await? {
        TaskOutcome::Succeeded(pixels) => println!("resize: {pixels} pixels"),
        TaskOutcome::Failed(error) => println!("resize failed: {error}"),
    }
    Ok(())
}

fn main() -> ExitCode {
    match build_app() {
        Ok(app) => main_of(app, demo),
        Err(error) => app_refused(&error),
    }
}
