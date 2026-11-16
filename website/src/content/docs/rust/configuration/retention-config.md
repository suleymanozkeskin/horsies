---
title: Retention Config
summary: Set record lifetime, partition coverage, and cleanup policy.
related: [recovery-config, app-config, ../../tasks/sending-tasks]
tags: [configuration, retention, partitions]
---

## Overview

`AppConfig.retention` controls data lifetime. It also controls partition
coverage and cleanup work. Crash detection stays in `RecoveryConfig`.

```rust
use std::collections::HashMap;

use chrono::Duration;
use horsies::{AppConfig, RetentionClassConfig, RetentionConfig};

let config = AppConfig {
    retention: RetentionConfig {
        queue_retention: HashMap::from([
            ("emails".to_owned(), Some(Duration::days(7))),
            ("audit".to_owned(), None),
        ]),
        retention_classes: vec![RetentionClassConfig {
            key: "audit_1y".to_owned(),
            duration: Duration::days(365),
        }],
        terminal_record_retention_hours: Some(24 * 90),
        ..RetentionConfig::default()
    },
    ..AppConfig::for_database_url("postgresql://...")
};
```

## Fields

| Field | Type | Default | Bounds or meaning |
|---|---|---|---|
| `worker_state_retention_hours` | `Option<u32>` | `Some(168)` | Worker-state rows. `None` disables deletion. Range: 1–8,760 hours |
| `terminal_record_retention_hours` | `Option<u32>` | `Some(720)` | Terminal workflow and workflow-task rows. `None` disables deletion. Range: 1–43,800 hours |
| `paused_workflow_auto_cancel_after` | `Option<chrono::Duration>` | `None` | Expire paused workflows after this age. The value must be positive |
| `history_leaf_horizon_days` | `u32` | `3` | Future daily history leaves. Range: 2–14 days |
| `heartbeat_leaf_horizon_hours` | `u32` | `6` | Future hourly heartbeat leaves and the heartbeat class window. Range: 2–48 hours |
| `retention_classes` | `Vec<RetentionClassConfig>` | empty | Extra finite task-history classes |
| `queue_retention` | `HashMap<String, Option<Duration>>` | empty | Per-queue task-history policy. `None` means forever |
| `partition_maintenance_interval_s` | `u64` | `900` | Partition coverage and pruning interval. Range: 60–3,600 seconds |
| `retention_sweep_interval_s` | `u64` | `300` | Row cleanup interval. Range: 30–86,400 seconds |
| `retention_delete_batch_size` | `u32` | `500` | Rows per cleanup batch. Range: 50–10,000 |

Retention validation uses `CONFIG_INVALID_RETENTION` (`HRS-216`). Invalid
fields are reported together.

## Per-queue retention

`queue_retention` maps each queue to a duration or to `None`. A duration
creates a finite class. `None` keeps terminal task records forever.

```rust
use std::collections::HashMap;
use chrono::Duration;

let retention = RetentionConfig {
    queue_retention: HashMap::from([
        ("emails".to_owned(), Some(Duration::days(7))),
        ("reports".to_owned(), Some(Duration::days(90))),
        ("audit".to_owned(), None),
    ]),
    ..RetentionConfig::default()
};
```

A queue without a mapping uses `standard_30d`. A mapped queue gets a derived
class such as `q_emails_7d`. The class is fixed when the task is sent.
Changing a mapping creates a new class. Existing tasks keep the old class.

The precedence is:

1. The explicit send choice.
2. The queue mapping.
3. `standard_30d`.

An explicit `standard_30d` choice overrides a queue mapping. The same rules
apply to immediate sends, delayed sends, schedule fires, and workflow task
enqueues.

Each mapping must name a configured queue. A typo fails config validation.
Each queue and duration pair owns a class. Two queues with the same duration
still own separate classes.

The `q_` prefix is reserved for queue-derived classes. Do not use it in
`retention_classes`.

## Declared classes

Use `retention_classes` for named finite policies.

```rust
let retention = RetentionConfig {
    retention_classes: vec![
        RetentionClassConfig {
            key: "audit_1y".to_owned(),
            duration: Duration::days(365),
        },
        RetentionClassConfig {
            key: "transient_2d".to_owned(),
            duration: Duration::days(2),
        },
    ],
    ..RetentionConfig::default()
};
```

A class key must be a safe identifier. Its maximum length is 18 characters.
The limit keeps every derived PostgreSQL name within 63 bytes. The keys
`standard_30d`, `forever`, and `heartbeats` are reserved. Durations must be
positive. Class keys must be unique.

The worker registers configured classes at startup. It repeats registration
during maintenance. A class duration is immutable after registration.

A duration is a minimum. History leaves span one UTC day. A finite record can
survive for up to one extra day before its whole leaf is old enough to drop.

Validation is local to each process. Every process that sends into a class
must declare that class.

## Per-send retention

Use `TaskSendOptions::retention_class(...)` for a finite class. Use
`TaskSendOptions::retain_forever()` for the explicit forever choice.

```rust
use horsies::TaskSendOptions;

let audit = audit_task::with_options(
    TaskSendOptions::new().retention_class("audit_1y"),
)
.send(input)
.await?;

let forever = audit_task::with_options(
    TaskSendOptions::new().retain_forever(),
)
.send(other_input)
.await?;
```

The stored `retention_class_key` is `forever` for the explicit forever
choice. An unknown class fails before the enqueue writes a row.

## Cleanup mechanisms

| Data | Mechanism | Policy |
|---|---|---|
| Terminal task records | Drop daily history partitions | Task retention class |
| Heartbeats | Drop hourly partitions | `heartbeat_leaf_horizon_hours` |
| Terminal workflows and workflow tasks | Batched row delete | `terminal_record_retention_hours` |
| Worker-state snapshots | Batched row delete | `worker_state_retention_hours` |

Partition drop returns the partition files at once. Row delete leaves dead
tuples for autovacuum.

The worker maintains future coverage before writes need it. The worker role
needs `CREATE` on the history and heartbeat partition parents. Use an external
coverage job if the worker role does not hold that privilege.

A blocked detach uses a statement timeout. The worker reports the refusal and
continues with other classes. The next maintenance pass retries the leaf.

## Paused workflow expiry

Set `paused_workflow_auto_cancel_after` to bound a paused workflow's age.
The default is `None`. `None` leaves paused workflows unchanged.

An expired workflow gets status `EXPIRED`. Its error uses
`WORKFLOW_EXPIRED`. A child workflow expiry propagates to its parent like a
cancellation.

## Moved, new, and removed recovery fields

These fields moved from the alpha.25 `RecoveryConfig` to
`AppConfig.retention`:

- `terminal_record_retention_hours`
- `worker_state_retention_hours`
- `retention_sweep_interval_s`
- `retention_delete_batch_size`

These `AppConfig.retention` fields are new in alpha.26:

- `retention_classes`
- `history_leaf_horizon_days`
- `heartbeat_leaf_horizon_hours`
- `partition_maintenance_interval_s`
- `paused_workflow_auto_cancel_after`

`RecoveryConfig` refuses all nine names above. Each error names
`AppConfig.retention.<field>` as the successor.

`queue_terminal_record_retention_hours` was removed. Use
`AppConfig.retention.queue_retention`.

`heartbeat_retention_hours` was removed. Use partitioned heartbeat storage and
`AppConfig.retention.heartbeat_leaf_horizon_hours`.
