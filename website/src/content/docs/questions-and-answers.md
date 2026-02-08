---
title: Questions & Answers
summary: Common questions about design trade-offs, scaling, and failure behavior.
related: [concepts/architecture, workers/concurrency, workers/heartbeats-recovery, tasks/retry-policy, internals/serialization]
tags: [faq, design]
---

## Why horsies?

Horsies is aiming to provide couple of things that traditional python libraries don't. Shortly but not exhaustively:

- Strict typing
- Errors as values
- Defensive approach to writing tasks

## Why errors as values?

While *"It's easier to ask for forgiveness than permission"* approach is helpful for prototyping, it is terrible for production. And sadly this idea has infected many codebases.

That approach leads developers to focus on happy path and completely ignore what could go wrong.

In contrast, a task function in horsies **must** return a `TaskResult` type with either:

- a generic Ok value
- a mandatory and structured TaskError with a uniform way to propagate errors

Horsies' approach leads the developer to actually think on the error cases on both definition and call site. Same applies for coding agents. Horsies literally will not start if api is not correctly designed. Which will eventually lead to a better feedback loop for both agents and developers.

See [error handling](../tasks/error-handling) for more.

## Why PostgreSQL only?

**Because postgres is a powerful database which can cover the needs of most applications.**

PostgreSQL handles task storage, LISTEN/NOTIFY for real-time dispatch, advisory locks for coordination, and heartbeat tracking. All in a single database with single source of truth.

The trade-off is clear: this architecture is not built for high-throughput, fan-out-heavy workloads. If you have throughput levels which your postgres instance can't handle, use a dedicated broker. If you need moderate throughput with one less thing to operate, horsies fits.

Here `moderate` and `high` throughput is also relative to your postgres instance ( e.g. you will not get the same performance from a PlanetScale Postgres vs Heroku Postgres )

## Is it ergonomic for devs?

Yes, it has strict validations, supports automatic Pydantic, dataclass, datetime serialisation and rehydration on the call site.

## How does it handle retries?

It has a clear retry policy which can be set by developers.

Every retry policy requires an explicit list of error codes to retry on. This gives you fine grained control. Read [retry policy](../tasks/retry-policy) for details.

## How does horsies handle unhandled exceptions?

They don't crash the worker and they don't disappear.

Any unhandled exception inside a task is caught, wrapped into a `TaskError` with `error_code=UNHANDLED_EXCEPTION`, and stored as a normal error result. The worker continues processing other tasks.

For finer control, the exception mapper lets you assign specific error codes to specific exception classes  without try/except boilerplate in your task functions. You can set it per-task or globally on the app. The mapper uses exact class matching only; map each class you need explicitly.

## What validation happens before the app starts?

A lot. Horsies runs multi-phase validation at startup and refuses to start if anything is misconfigured.

This includes: missing or invalid return type annotations on tasks, queue names that don't match the app config, duplicate task names, invalid retry policies, malformed error codes, workflow DAG cycles, unresolvable dependencies, and invalid exception mapper entries.

Errors are displayed in a structured format with error codes, source locations, and help text. The goal is to catch as many potential issues as possible before deployment.
See [check command](../cli#horsies-check)

## Does it have a scheduler?

Yes. Runs a separate process alongside workers with `horsies scheduler` command. It supports intervals with human readable models, not cron expressions. See [scheduler](../scheduling/scheduler-overview)

## I have various needs with data pipelines, does horsies support worker side orchestration and execution?

Yes, horsies provide DAG workflows. Stack your tasks as nodes in the workflow, decide the policy by filling `TaskNode` details.
E.g. `join: [all, any, quorum]`, `waits_for` ( which nodes must be completed prior to this step in the pipeline )
See [workflows](../concepts/workflows/workflow-api)

## Does it have monitoring?

There is a terminal-based TUI called syce, capable of displaying the status of your workers, tasks and workflows in detail.
See [syce](../monitoring/syce-overview)

## Does it support queue based concurrency control in the same deployed instance?

Yes. You do not need to waste a separate instance for each queue.
Deploy workers only when you need more capacity, not when you want to have separate queue limitations.

## How does execution work ?

You can have as many workers as you like.

They can consume the tasks from the specified database.

Worker and its processes send heartbeats through the lifecycle of a task.

Library utilises these heartbeats to keep track of health and take action.

## Is it production-ready?

Horsies is in alpha. The API may change between releases.
Fundamentals will likely remain the same.

Note: I as the author, personally tested this library on a Celery dependent large codebase with great results.

## Is it optimised for linux?

Not yet, but planned.

## Where are the tests?

Tests are work in progress, will be a part of releases in the future.
