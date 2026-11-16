---
title: Web UI Overview
summary: Browser monitoring for live and retained Horsies data.
related: [./web-ui-deployment, ./action-semantics]
tags: [monitoring, web, dashboard, history]
---

Horsies includes an optional browser dashboard. Enable the `web` Cargo feature
to serve it with axum.

The dashboard reads live state and retained task history. It does not move
terminal rows back into the live table. It does not change retention rules.

## Enable the web feature

```bash
cargo add horsies --features web
```

The transport-free `horsies::monitoring` module is always available. The
`horsies::web` router and embedded dashboard require the `web` feature.

## Views

### Tasks

The task view merges two sources:

- `horsies_tasks` supplies `PENDING`, `CLAIMED`, and `RUNNING` tasks.
- `horsies_task_history` supplies retained terminal tasks.

The default history window is the last 24 hours. The maximum accepted window
is 30 days. Horsies refuses larger windows instead of reducing them.

The task view supports status, queue, task name, worker, and error filters. It
also supports search and ordered pages. Null sort values appear last in both
sort directions.

The total for an unfiltered task list can use a planner estimate. Filtered
totals are exact. Facet counts sum live and retained rows before ranking the
groups.

Task detail verifies the retained history digest before decoding the result or
attempt snapshot. A corrupt retained row returns an error. It is never shown as
a valid task.

### Workflows

The workflow list shows current workflow state and progress. The detail view
shows the node graph, node status, task links, and stored errors.

Workflow state includes `EXPIRED`. A paused workflow can expire when
`paused_workflow_auto_cancel_after` is configured.

### Workers and schedules

The worker view shows worker state and recent activity. The schedule view shows
the persisted scheduler state.

Worker and workflow reads use the current operational tables. Task aggregates
combine live tasks with the selected retained-history window.

## Refresh model

The dashboard performs an initial fetch for each view. It then listens for
server-sent invalidation events.

The server listens to the task-status, workflow-status, and worker-state
PostgreSQL channels. It debounces events for 250 milliseconds. One event can
carry at most 100 IDs. An overflow emits the same topic with an empty ID list.
The empty list causes a broader refetch.

The event listener owns one dedicated PostgreSQL session. It is separate from
task-result listeners.

A listener failure emits the `degraded` topic and closes the stream. The
browser then falls back to polling while it reconnects.

Terminal task moves emit `task_done`. The dashboard does not subscribe to that
channel. It converges on a later subscribed event or client refetch.

## Actions

The dashboard exposes four actions when the deployment enables them:

| Resource | Action |
| --- | --- |
| Task | Cancel |
| Workflow | Pause |
| Workflow | Resume |
| Workflow | Cancel |

Task retry is not a dashboard action. Retained tasks can be rerun through the
Rust API. A rerun creates a new task with lineage to the source task.

Every action requires authorization and `X-Horsies-Intent: action`. Every
action also requires an exact schema match. Reads can remain available when
the schema version does not match.

See [Action Semantics](./action-semantics/) for state rules and response codes.

## Query bounds

| Bound | Rule |
| --- | --- |
| Default history window | 24 hours |
| Maximum history window | 30 days |
| Task list `limit` | 1 through 200 |
| Workflow list `limit` | 1 through 200 |
| Task page reach | `offset + limit <= 500` |
| Worker history `limit` | 1 through 1000 |

Time bounds accept timezone-aware ISO-8601 timestamps. The server normalizes
them to UTC. `since` must be earlier than `until`.

## Schema states

The web server probes schema state without running migrations or cutover DDL.
The result is cached for 60 seconds.

| State | Reads | Actions |
| --- | --- | --- |
| `MATCH` | Available | Can be enabled |
| `MISMATCH` | Available where the stored schema can answer the query | Disabled |
| `CUTOVER_REQUIRED` | Available where the stored schema can answer the query | Disabled |
| `ABSENT` | Dashboard shell only | Disabled |
| `UNKNOWN` | Dashboard shell only | Disabled |

`SCHEMA_INCOMPATIBLE` reports a mismatch, missing schema, or incomplete
cutover. `SCHEMA_UNKNOWN` reports a failed schema probe with no cached result.

## Deployment choices

Use `horsies web` for a standalone server. Mount
`create_monitoring_router` in an existing axum service when the host must own
authentication or task registration.

See [Deployment and Authentication](./web-ui-deployment/) for both forms.
