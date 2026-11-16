---
title: Retry Policy
summary: Automatic retry behavior with fixed or exponential backoff.
related: [defining-tasks, ../../concepts/task-lifecycle]
tags: [tasks, retry, backoff]
---

## Basic Usage

```rust
#[task(
    "flaky_task",
    auto_retry_for = ["TRANSIENT_ERROR"],
    retry_policy = RetryPolicy::fixed(vec![60, 300, 900], true)?
)]
async fn flaky_task() -> Result<String, TaskError> {
    // Will retry up to 3 times with delays: 1min, 5min, 15min
    // ...
}
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | `u32` | 3 | Number of retry attempts (1–20) |
| `intervals` | `Vec<u32>` | [60, 300, 900] | Delay intervals in seconds (each 1–86400) |
| `backoff_strategy` | `BackoffStrategy` | `Fixed` | `Fixed` or `Exponential` |
| `jitter` | `bool` | `true` | Add random variation to delays |

Auto-retry triggers are configured separately via the `auto_retry_for` macro attribute.

## Backoff Strategies

### Fixed Backoff

Uses exact intervals from the list. The list length must match `max_retries`.

```rust
// Retry 3 times: wait 1min, then 5min, then 15min
RetryPolicy::fixed(vec![60, 300, 900], true)?

// Equivalent to:
RetryPolicy {
    max_retries: 3,
    intervals: vec![60, 300, 900],
    backoff_strategy: BackoffStrategy::Fixed,
    jitter: true,
}
```

### Exponential Backoff

Uses a base interval multiplied by 2^(attempt-1).

```rust
// Base 30s: retry at 30s, 60s, 120s, 240s, 480s
RetryPolicy::exponential(30, 5, true)?

// Equivalent to:
RetryPolicy {
    max_retries: 5,
    intervals: vec![30],  // Single base interval
    backoff_strategy: BackoffStrategy::Exponential,
    jitter: true,
}
```

## Jitter

When `jitter = true` (default), delays are randomized by +/-25%:

- 60 second base -> 45-75 seconds actual
- Prevents thundering herd when many tasks retry simultaneously

```rust
// Disable jitter for predictable delays
RetryPolicy::fixed(vec![60, 300, 900], false)?
```

## Auto-Retry Triggers

Retries only happen when specific conditions are met. Configure via `auto_retry_for` on the `#[task]` macro:

```rust
#[task(
    "api_call",
    auto_retry_for = ["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
    retry_policy = RetryPolicy::fixed(vec![30, 60, 120], true)?
)]
async fn api_call() -> Result<ApiResponse, TaskError> {
    // ...
}
```

`auto_retry_for` accepts user-defined error code strings. These must match the codes returned in your `TaskError::new(code, message)` calls.

## How Retries Work

1. Task fails with matching error code
2. Worker checks `retry_count < max_retries`
3. If retries remaining, task status set to Pending
4. `next_retry_at` calculated from retry policy
5. Task not claimable until `next_retry_at` passes
6. Worker sends delayed notification to trigger claiming

Each execution writes an attempt row to `horsies_task_attempts`. A retried
failure has `will_retry=true` and `outcome=Failed`. The final attempt has
`will_retry=false`. Terminalization archives the full attempt snapshot in the
history row and removes the live attempt rows in the same transaction.

Use `handle.info(false, false, true)` to inspect the full attempt timeline.

## Retry Count Tracking

| Field | Description |
|-------|-------------|
| `retry_count` | Current number of retry attempts |
| `max_retries` | Maximum attempts allowed |
| `next_retry_at` | When task becomes claimable again |

## Validation

The policy validates consistency:

```rust
// This returns Err:
RetryPolicy {
    max_retries: 3,
    intervals: vec![60, 300],  // Only 2 intervals for 3 retries
    backoff_strategy: BackoffStrategy::Fixed,
    jitter: true,
}
// Fixed backoff requires intervals.len() == max_retries

// This also returns Err:
RetryPolicy {
    max_retries: 3,
    intervals: vec![60, 300, 900],  // Multiple intervals
    backoff_strategy: BackoffStrategy::Exponential,  // Needs exactly 1
    jitter: true,
}
```

## Examples

### API with Rate Limiting

```rust
#[task(
    "call_external_api",
    auto_retry_for = ["RATE_LIMITED", "SERVICE_UNAVAILABLE"],
    retry_policy = RetryPolicy::exponential(60, 5, true)?
)]
async fn call_external_api(input: ApiInput) -> Result<ApiResponse, TaskError> {
    match reqwest::get(&input.url).await {
        Ok(response) if response.status() == 429 => {
            Err(TaskError::new("RATE_LIMITED", "Rate limited"))
        }
        Ok(response) => {
            let data = response.json().await.map_err(|e| {
                TaskError::new("PARSE_ERROR", e.to_string())
            })?;
            Ok(data)
        }
        Err(e) if e.is_timeout() => {
            Err(TaskError::new("SERVICE_UNAVAILABLE", "Timeout"))
        }
        Err(e) => Err(TaskError::new("API_ERROR", e.to_string())),
    }
}
```

### Database Transaction with Deadlock Retry

```rust
#[task(
    "update_inventory",
    auto_retry_for = ["DEADLOCK"],
    retry_policy = RetryPolicy::fixed(vec![1, 2, 5], true)?  // Quick retries
)]
async fn update_inventory(input: InventoryUpdate) -> Result<(), TaskError> {
    match db_update_stock(input.item_id, input.delta).await {
        Ok(()) => Ok(()),
        Err(e) if e.is_deadlock() => {
            Err(TaskError::new("DEADLOCK", "Deadlock detected"))
        }
        Err(e) => Err(TaskError::new("DB_ERROR", e.to_string())),
    }
}
```
