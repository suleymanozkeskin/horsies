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

Horsies' approach leads the developer to actually think on the error cases on both definition and call site. Same applies for coding agents. Horsies literally will not start if any of your task functions don't return a `TaskResult` type. Which will eventually lead to a better feedback loop for both agents and developers.

See [error handling](../tasks/error-handling) for more.

## Why PostgreSQL only?

Correctness and performance.

### Correctness:

Every guarantee horsies makes is a Postgres primitive:

- **Claiming**: one server-side function under `FOR UPDATE SKIP LOCKED`; a claim-generation fence rejects stale attempts, including a worker re-claiming its own requeued task. Double execution and phantom retries are impossible states, not tuned-away ones.
- **Finalization**: row lock → immutable attempt-history append → state transition, one transaction.
- **Recovery**: all state is rows and timestamps; the reaper reconstructs and repairs after a worker dies mid-flight — nothing in-flight exists only in a broker's memory.
- **Workflows**: fan-in resolution, completion checks, subworkflow cascades, and orphan self-heal are multi-row transitions under a documented lock order.
- **Dispatch**: LISTEN/NOTIFY push — no polling loop between enqueue and execution.
- **Inspection**: task history is plain tables — SQL, `EXPLAIN`, your existing backups. See [operational indexes](../internals/operational-indexes) for query-shape guidance.

A message broker can approximate the first three with visibility timeouts and acks; it cannot express them as invariants. That is the reason for the Postgres requirement, operating one less service is absolutely not a selling point. In fact, we strongly recommend running a dedicated Postgres instance for your worker.

### Postgres is performant:

It scales with your Postgres instance (a PlanetScale Postgres and a Heroku Postgres will not perform the same); even with a cross-machine deployment, app server and managed Postgres in the same region, holds per-statement p99 in the low single-digit milliseconds across the claim/dispatch/finalize hot path.

Measured numbers: [performance](../internals/performance).

## Is it ergonomic for devs?

Yes, it has strict validations, supports automatic Pydantic, dataclass, datetime serialisation and rehydration on the call site.

## How does it handle retries?

It has a clear retry policy which can be set by developers.

Every retry policy requires an explicit list of error codes to retry on. This gives you fine grained control. Read [retry policy](../tasks/retry-policy) for details.

## How does horsies handle unhandled exceptions?

They don't crash the worker and they don't disappear. You still get your `TaskError`.

Any unhandled exception inside a task is caught, wrapped into a `TaskError` with `error_code=UNHANDLED_EXCEPTION`, and stored as a normal error result. The worker continues processing other tasks.

For finer control, the exception mapper lets you assign specific error codes to specific exception classes  without try/except boilerplate in your task functions. You can set it per-task or globally on the app. The mapper uses exact class matching only; map each class you need explicitly.

## What validation happens before the app starts?

A lot. Horsies runs multi-phase validation at startup and refuses to start if anything is misconfigured.

This includes: missing or invalid return type annotations on tasks, queue names that don't match the app config, duplicate task names, invalid retry policies, malformed error codes, workflow DAG cycles, unresolvable dependencies, and invalid exception mapper entries.

Errors are displayed in a structured format with error codes, source locations, and help text. The goal is to catch as many potential issues as possible before deployment.
See [check command](../cli#horsies-check)

## Does it have a scheduler?

Yes. Runs a separate process alongside workers with `horsies scheduler` command. It supports intervals with human readable models, not cron expressions. See [scheduler](../scheduling/scheduler-overview)

## Does horsies support worker side orchestration and execution of data pipelines?

Yes, horsies provide DAG workflows. Stack your tasks as nodes in the workflow, decide the policy by filling `TaskNode` details. You can even use workflows within workflows, a node itself can be a workflow.
E.g. `join: [all, any, quorum]`, `waits_for` ( which nodes must be completed prior to this step in the pipeline )
See [workflows](../concepts/workflows/workflow-api) and [subworkflows](../concepts/workflows/subworkflows)

## Does it have monitoring?

There is a terminal-based TUI called syce, capable of displaying the status of your workers, tasks and workflows in detail.
See [syce](../monitoring/syce-overview)

A web based ui is in the roadmap.

## Does horsies provide guidance files for coding agents?

Yes. In source checkouts, horsies includes markdown skill files under:

`horsies/.agents/skills/`

These cover:

- quick routing (`SKILL.md`)
- tasks (`tasks.md`)
- workflows (`workflows.md`)
- configuration and operations (`configs.md`)

They are best-practice references for agents and developers, and complement the
public docs plus `llms.txt`.

## Does it support queue based concurrency control in the same deployed instance?

Yes. You do not need to waste a separate instance for each queue.
Deploy workers only when you need more capacity, not when you want to have separate queue limitations.

## Is it production-ready?

Yes — and read the version number as an API-contract statement, not an engine-maturity statement. Pre-1.0 means breaking API changes may still land in minor releases, each documented in the [changelog](../changelog). It does not describe the engine's correctness discipline:

- 3,200+ test functions across unit, integration (real Postgres), e2e (real worker processes), and PgBouncer contract suites — a 2.2:1 test-to-source line ratio.
- Failure-path-first testing: crash recovery, claim fencing, cancel/completion races, rolling-upgrade behavior, and query-plan pinning (`EXPLAIN ANALYZE` assertions on the hot-path statements).
- The claim/finalize semantics are cross-validated against an independent Rust reimplementation of the engine.
- A codebase with ~300 task definitions and recurring schedules, migrated off Celery, runs its full background workload on horsies.

## Throughput expectations and comparisons

Compared with Redis/RabbitMQ-backed systems such as Celery, Dramatiq, RQ, Huey, or arq, Horsies chooses transactional execution semantics over raw broker throughput. 
Those systems should generally win fire-and-forget message benchmarks. Horsies is aimed at workloads where exactly-once-shaped ownership, durable task state, typed results, retries, deadlines, workflow DAGs, worker health, and operational inspection are worth the extra database cost.
Measured per-statement latencies for this trade: [performance](../internals/performance).

The closest comparison is Procrastinate, because it is also PostgreSQL-backed. 
Horsies is heavier than a plain Postgres task queue because it adds strict serialization, typed TaskResult handling, workflow state, subworkflows, success policies, and monitoring state. Benchmark against Procrastinate if plain Postgres task throughput is the main buying criterion.


With the right deployment shape, it is still performant for typical SaaS background workloads where Horsies' additional features will prove its worth. 

### How to get the most out of it:
Use these defaults for throughput-sensitive deployments:

- Co-locate workers and PostgreSQL. Game changer.
- Use direct database connections when the connection budget allows it.
- Use a transaction pooler when managed PostgreSQL connection limits require multiple workers.
- Enable prefetch for high-RTT deployments with prefetch_buffer and claim_lease_ms.
