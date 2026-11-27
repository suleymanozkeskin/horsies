// The sample of the generated worker manifest: its two `bigint` fields hold the largest values of their bounds, and a
// task is named `__proto__`. The unit test of the generated writer compares its text with Python's; the native test
// of the manifest samples has the core parse it.

import { PAUSED_AGE_MICROS_MAX, PAYLOAD_BYTES_MAX } from "../../../../horsies/src/_catalog/worker_manifest.ts";
import type { WorkerManifest, WorkerProfileManifest } from "../../../../horsies/src/_catalog/worker_manifest.ts";

export const PROFILE: WorkerProfileManifest = {
  isolated: {
    kind: "CHILDREN",
    count: 4,
    recycle: { after: { kind: "TASKS", tasks: 1000 }, above: { kind: "MEGABYTES", mb: 512 } },
    pool: { min_size: 0, max_size: 2, pre_ping: true },
    task_send_pool: 2,
  },
  shared_async: {
    kind: "CHILDREN",
    count: 1,
    seats: 50,
    recycle: { after_per_seat: { kind: "NO_LIMIT" }, above: { kind: "NO_LIMIT" } },
    pool: { min_size: 1, max_size: { kind: "FROM_SEATS" }, pre_ping: false },
    executor: { kind: "THREADS", threads: 8 },
    task_send_pool: 4,
  },
  shared_thread: { kind: "NO_CHILDREN" },
  pool: { max_size: { kind: "CONNECTIONS", connections: 10 } },
  claim: {
    per_queue_per_pass: { kind: "TASKS", tasks: 100 },
    claimed_per_worker: { kind: "NO_LIMIT" },
  },
  drain_timeout_ms: 30_000,
  preload: "NO_PRELOAD",
  parent_cpu: "SHARED",
};

export const MANIFEST: WorkerManifest = {
  manifest_version: 1,
  database: {
    database_url: "postgresql://app@127.0.0.1:55432/app",
    session_database: { kind: "SAME_AS_DATABASE_URL" },
    pgbouncer_transaction_mode: false,
    pool_pre_ping: true,
    worker_pool_pre_ping: false,
    pool_timeout_seconds: 30,
    pool_recycle: { kind: "NEVER" },
    echo: false,
    tcp_keepalives: { kind: "OFF" },
    tcp_user_timeout: { kind: "SYSTEM_DEFAULT" },
  },
  send: { retain_rerun_input_default: true, idempotency_reservation_window: 2_592_000_000_000 },
  profiles: new Map([["default", PROFILE]]),
  profile: { kind: "NAMED", name: "default" },
  metrics_endpoint: { kind: "LISTEN_ADDRESS", host: "127.0.0.1", port: 9464 },
  claim: {
    queues: {
      kind: "CUSTOM",
      queues: [
        { name: "urgent", priority: 1, max_concurrency: { kind: "TASKS", tasks: 10 } },
        { name: "bulk", priority: 100, max_concurrency: { kind: "NO_LIMIT" } },
      ],
    },
    prefetch_buffer: 0,
    claim_lease: { kind: "WORKER_DEFAULT" },
    max_claim_renew_age_ms: 180_000,
    cluster_wide_cap: { kind: "TASKS", tasks: 500 },
  },
  recovery: {
    requeue_stale_claimed: true,
    claimed_stale_threshold_ms: 120_000,
    terminate_orphaned_workflow_tasks: true,
    fail_stale_running: true,
    running_stale_threshold_ms: 300_000,
    finalizing_stale_threshold_ms: 300_000,
    crashed_worker_recovery_grace_ms: 0,
    finalization_quarantine_after_attempts: 3,
    workflow_recovery_failure_limit: 10,
    check_interval_ms: 30_000,
    orphan_task_audit_interval_ms: 600_000,
    runner_heartbeat_interval_ms: 30_000,
    claimer_heartbeat_interval_ms: 30_000,
    worker_state_snapshot_interval_ms: 5000,
  },
  retention: {
    worker_state_retention: { kind: "HOURS", hours: 168 },
    terminal_record_retention: { kind: "NO_PRUNING" },
    paused_workflow_cancel_after: { kind: "AFTER", micros: PAUSED_AGE_MICROS_MAX },
    history_leaf_horizon_days: 7,
    heartbeat_leaf_horizon_hours: 24,
    retention_periods: [7, 30],
    queue_retention: new Map([
      ["urgent", { kind: "DAYS", days: 7 }],
      ["bulk", { kind: "FOREVER" }],
    ]),
    partition_maintenance_interval_seconds: 600,
    retention_sweep_interval_seconds: 3600,
    retention_delete_batch_size: 1000,
  },
  resilience: {
    database_retry_initial_ms: 500,
    database_retry_max_ms: 30_000,
    database_retry_max_attempts: { kind: "NO_LIMIT" },
    notify_poll_interval_ms: 5000,
  },
  payload: {
    warn_bytes: { kind: "BYTES", bytes: 1_048_576n },
    reject_bytes: { kind: "BYTES", bytes: PAYLOAD_BYTES_MAX },
  },
  tasks: new Map([
    ["reports.nightly", { execution: "ISOLATED", error_codes: ["REPORT_EMPTY"] }],
    ["__proto__", { execution: "SHARED_ASYNC", error_codes: [] }],
  ]),
  handlers: { kind: "HANDLERS", handlers: [{ name: "audit", event_type: "AttemptEnded" }] },
  worker_ended_spans: "MAKE_SPANS",
  child_launch: {
    command: { program: "/usr/local/bin/node", args: ["--enable-source-maps", "dist/worker.js"] },
    preload: { kind: "NO_PRELOAD_PROGRAM" },
    program: { kind: "RUST" },
    log_level: "INFO",
  },
};
