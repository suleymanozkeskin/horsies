// Each execution class on the function kind that runs it.
use std::convert::Infallible;

use horsies::{blocking_task, task, TaskError};

#[task("isolated_by_default")]
async fn isolated_by_default(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[task("isolated_async", execution = isolated, timeout_ms = 1_000)]
async fn isolated_async(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[task("shared_async_default_grace", execution = shared_async, timeout_ms = 30_000)]
async fn shared_async_default_grace(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[task(
    "shared_async_with_grace",
    execution = shared_async,
    timeout_ms = 30_000,
    cancel_grace_ms = 10_000
)]
async fn shared_async_with_grace(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[blocking_task("isolated_blocking", execution = isolated)]
fn isolated_blocking(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[task(
    "isolated_with_grace",
    execution = isolated,
    timeout_ms = 30_000,
    cancel_grace_ms = 2_000
)]
async fn isolated_with_grace(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[blocking_task("isolated_blocking_with_grace", cancel_grace_ms = 500)]
fn isolated_blocking_with_grace(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

#[blocking_task("shared_thread", execution = shared_thread, timeout_ms = 60_000)]
fn shared_thread(count: u32) -> Result<u32, TaskError<Infallible>> {
    Ok(count)
}

fn main() {
    let _ = isolated_by_default::register as fn(&mut horsies::Horsies) -> _;
    let _ = isolated_async::register as fn(&mut horsies::Horsies) -> _;
    let _ = shared_async_default_grace::register as fn(&mut horsies::Horsies) -> _;
    let _ = shared_async_with_grace::register as fn(&mut horsies::Horsies) -> _;
    let _ = isolated_blocking::register as fn(&mut horsies::Horsies) -> _;
    let _ = shared_thread::register as fn(&mut horsies::Horsies) -> _;
    let _ = isolated_with_grace::register as fn(&mut horsies::Horsies) -> _;
    let _ = isolated_blocking_with_grace::register as fn(&mut horsies::Horsies) -> _;
}
