---
title: Retrieving Results
summary: Getting task outcomes via TaskHandle.get().
related: [sending-tasks, ../../concepts/result-handling]
tags: [tasks, results, TaskHandle, errors]
---

# Retrieving Results

Retrieve task outcomes through `TaskHandle::get()`. For error handling
strategy, see [Error Handling](../error-handling) and [Result
Handling](../../concepts/result-handling).

## How To

### Basic Retrieval

```rust
use std::time::Duration;
use horsies::TaskResult;

let handle = match my_task::send(input).await {
    Ok(h) => h,
    Err(err) => {
        println!("Send failed: {}", err.message);
        return;
    }
};

// Block until complete (or timeout/error)
let result: TaskResult<MyOutput> = handle.get(None).await;

match result {
    TaskResult::Ok(value) => println!("Success: {:?}", value),
    TaskResult::Err(err) => {
        if err.is_transient() {
            println!("Transient error, task may complete later");
        } else {
            println!("Error: {:?} - {:?}", err.error_code, err.message);
        }
    }
}
```

### Timeouts

Specify maximum wait time as a `Duration`:

```rust
use std::time::Duration;

// Wait up to 5 seconds
let result = handle.get(Some(Duration::from_secs(5))).await;

if result.is_err() {
    let err = result.unwrap_err();
    // Check if it was a timeout (task may still be running)
    if err.is_transient() {
        println!("Timed out, task may still complete later");
    }
}
```

### Result Caching

Results are cached on the handle after the first terminal resolution:

```rust
let handle = match my_task::send(input).await {
    Ok(h) => h,
    Err(err) => {
        println!("Send failed: {}", err.message);
        return;
    }
};

// First call fetches from database
let result1 = handle.get(None).await;

// Subsequent calls return cached result
let result2 = handle.get(None).await; // No database query
```

### Checking Result State

```rust
let result = handle.get(None).await;

// Check state
if result.is_ok() {
    let value = result.unwrap();
} else {
    let error = result.unwrap_err();
}

// Or use pattern matching
match result {
    TaskResult::Ok(value) => { /* use value */ }
    TaskResult::Err(err) => { /* handle error */ }
}
```

### Task Metadata and Attempt History

Use `handle.info()` to fetch task metadata without waiting for completion:

```rust
let info_result = handle.info(true, true, false).await;
if let Ok(Some(task_info)) = info_result {
    println!("Status: {:?}, Retries: {}", task_info.status, task_info.retry_count);
    println!("Error code: {:?}", task_info.error_code);
}
```

To inspect per-attempt execution history, pass `include_attempts = true`:

```rust
let info_result = handle.info(false, false, true).await;
if let Ok(Some(task_info)) = info_result {
    if let Some(attempts) = task_info.attempts {
        for attempt in &attempts {
            println!(
                "Attempt {}: {:?} (retry={}, error={:?})",
                attempt.attempt, attempt.outcome, attempt.will_retry, attempt.error_code
            );
        }
    }
}
```

`info()` reads both live and history storage. A terminal task's attempt list is
decoded from its history snapshot. Corrupt result or attempt envelopes return
an error. They are not guessed.

## Rerun a retained terminal task

`rerun_task` creates a new task from retained source input. It never changes
the source record. The new task stores its direct source and its rerun root.

```rust
use horsies::{
    rerun_task, RerunEnqueuePolicy, RerunOutcome, RerunTask,
};

let source_id = handle.task_id();
let broker = app.get_broker().await?;

let outcome = rerun_task(
    &broker,
    RerunTask::new(
        source_id,
        None,
        Some("manual-rerun-4182".to_owned()),
    ),
    RerunEnqueuePolicy::standard(true),
)
.await?;

match outcome {
    RerunOutcome::Enqueued { new_task_id, .. } => {
        println!("rerun task: {new_task_id}");
    }
    RerunOutcome::KeyReplay { existing_task_id } => {
        println!("existing rerun: {existing_task_id}");
    }
    RerunOutcome::SourceLive { task_id } => {
        println!("source is live: {task_id}");
    }
    RerunOutcome::SourceAbsent { task_id, .. } => {
        println!("source is absent: {task_id}");
    }
    RerunOutcome::NotEligible { task_id, reason } => {
        println!("source is not eligible: {task_id} {reason:?}");
    }
    RerunOutcome::InputUnavailable { task_id, .. } => {
        println!("source input is unavailable: {task_id}");
    }
    RerunOutcome::InputCorrupt { task_id, detail } => {
        println!("source input is corrupt: {task_id} {detail}");
    }
    RerunOutcome::KeyConflict { task_id, .. } => {
        println!("rerun key conflicts: {task_id}");
    }
}
```

The source must be a retained terminal non-workflow task. A completed source is
not eligible. A workflow backing task is not eligible.

The source input must have an `INLINE` rerun envelope. Set
`PostgresConfig.retain_rerun_input_default = true` before enqueue when later
rerun is required. The default is `false`.

The policy selects the new retention class. It also selects whether the new
task retains its own rerun input. `RerunEnqueuePolicy::standard(true)` uses
`standard_30d` and a 24-hour key reservation window.

`RerunOutcome` is exhaustive:

- `Enqueued`
- `SourceLive`
- `SourceAbsent`
- `NotEligible`
- `InputUnavailable`
- `InputCorrupt`
- `KeyConflict`
- `KeyReplay`

Use a caller key when an operator or API request may be submitted more than
once. The rerun lineage is part of the command fingerprint. A source enqueue
key cannot silently replay a rerun command.

## Things to Avoid

**Don't silently ignore errors by only logging them.**

```rust
// Wrong - logs error but takes no action
let result = handle.get(None).await;
if result.is_err() {
    println!("{:?}", result.unwrap_err().message);
}
// Code continues as if nothing happened...

// Correct - handle or propagate errors
let result = handle.get(None).await;
match result {
    TaskResult::Err(err) => {
        handle_error(&err);
        return Err(err.into());
    }
    TaskResult::Ok(value) => {
        println!("Success: {:?}", value);
    }
}
```

For detailed error handling patterns, see [Error Handling](../error-handling).
