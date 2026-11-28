import { describe, expect, test } from "bun:test";

import { workerManifestToWire } from "../../../horsies/src/_catalog/worker_manifest.ts";
import type { WorkerManifest } from "../../../horsies/src/_catalog/worker_manifest.ts";
import { MANIFEST, PROFILE } from "./support/worker-manifest-sample.ts";
import { wireText } from "./support/wire-text.ts";

// The expected text is the text of Python's `dump_worker_manifest` for the
// same manifest. Its two `bigint` fields hold the largest values of their
// bounds, and a task is named `__proto__`.

const MANIFEST_TEXT = [
  String.raw`{"manifest_version":1,`,
  String.raw`"database":{"database_url":"postgresql://app@127.0.0.1:55432/app",`,
  String.raw`"session_database":{"kind":"SAME_AS_DATABASE_URL"},`,
  String.raw`"pgbouncer_transaction_mode":false,`,
  String.raw`"pool_pre_ping":true,`,
  String.raw`"worker_pool_pre_ping":false,`,
  String.raw`"pool_timeout_seconds":30,`,
  String.raw`"pool_recycle":{"kind":"NEVER"},`,
  String.raw`"echo":false,`,
  String.raw`"tcp_keepalives":{"kind":"OFF"},`,
  String.raw`"tcp_user_timeout":{"kind":"SYSTEM_DEFAULT"}},`,
  String.raw`"send":{"retain_rerun_input_default":true,"idempotency_reservation_window":2592000000000},`,
  String.raw`"profiles":{"default":{"isolated":{"kind":"CHILDREN",`,
  String.raw`"count":4,`,
  String.raw`"recycle":{"after":{"kind":"TASKS","tasks":1000},"above":{"kind":"MEGABYTES","mb":512}},`,
  String.raw`"pool":{"min_size":0,"max_size":2,"pre_ping":true},`,
  String.raw`"task_send_pool":2},`,
  String.raw`"shared_async":{"kind":"CHILDREN",`,
  String.raw`"count":1,`,
  String.raw`"seats":50,`,
  String.raw`"recycle":{"after_per_seat":{"kind":"NO_LIMIT"},"above":{"kind":"NO_LIMIT"}},`,
  String.raw`"pool":{"min_size":1,"max_size":{"kind":"FROM_SEATS"},"pre_ping":false},`,
  String.raw`"executor":{"kind":"THREADS","threads":8},`,
  String.raw`"task_send_pool":4},`,
  String.raw`"shared_thread":{"kind":"NO_CHILDREN"},`,
  String.raw`"pool":{"max_size":{"kind":"CONNECTIONS","connections":10}},`,
  String.raw`"claim":{"per_queue_per_pass":{"kind":"TASKS","tasks":100},`,
  String.raw`"claimed_per_worker":{"kind":"NO_LIMIT"}},`,
  String.raw`"drain_timeout_ms":30000,`,
  String.raw`"preload":"NO_PRELOAD",`,
  String.raw`"parent_cpu":"SHARED"}},`,
  String.raw`"profile":{"kind":"NAMED","name":"default"},`,
  String.raw`"metrics_endpoint":{"kind":"LISTEN_ADDRESS","host":"127.0.0.1","port":9464},`,
  String.raw`"claim":{"queues":{"kind":"CUSTOM",`,
  String.raw`"queues":[{"name":"urgent","priority":1,"max_concurrency":{"kind":"TASKS","tasks":10}},`,
  String.raw`{"name":"bulk","priority":100,"max_concurrency":{"kind":"NO_LIMIT"}}]},`,
  String.raw`"prefetch_buffer":0,`,
  String.raw`"claim_lease":{"kind":"WORKER_DEFAULT"},`,
  String.raw`"max_claim_renew_age_ms":180000,`,
  String.raw`"cluster_wide_cap":{"kind":"TASKS","tasks":500}},`,
  String.raw`"recovery":{"requeue_stale_claimed":true,`,
  String.raw`"claimed_stale_threshold_ms":120000,`,
  String.raw`"terminate_orphaned_workflow_tasks":true,`,
  String.raw`"fail_stale_running":true,`,
  String.raw`"running_stale_threshold_ms":300000,`,
  String.raw`"finalizing_stale_threshold_ms":300000,`,
  String.raw`"crashed_worker_recovery_grace_ms":0,`,
  String.raw`"finalization_quarantine_after_attempts":3,`,
  String.raw`"workflow_recovery_failure_limit":10,`,
  String.raw`"check_interval_ms":30000,`,
  String.raw`"orphan_task_audit_interval_ms":600000,`,
  String.raw`"runner_heartbeat_interval_ms":30000,`,
  String.raw`"claimer_heartbeat_interval_ms":30000,`,
  String.raw`"worker_state_snapshot_interval_ms":5000},`,
  String.raw`"retention":{"worker_state_retention":{"kind":"HOURS","hours":168},`,
  String.raw`"terminal_record_retention":{"kind":"NO_PRUNING"},`,
  String.raw`"paused_workflow_cancel_after":{"kind":"AFTER","micros":31536000000000000},`,
  String.raw`"history_leaf_horizon_days":7,`,
  String.raw`"heartbeat_leaf_horizon_hours":24,`,
  String.raw`"retention_periods":[7,30],`,
  String.raw`"queue_retention":{"urgent":{"kind":"DAYS","days":7},"bulk":{"kind":"FOREVER"}},`,
  String.raw`"partition_maintenance_interval_seconds":600,`,
  String.raw`"retention_sweep_interval_seconds":3600,`,
  String.raw`"retention_delete_batch_size":1000},`,
  String.raw`"resilience":{"database_retry_initial_ms":500,`,
  String.raw`"database_retry_max_ms":30000,`,
  String.raw`"database_retry_max_attempts":{"kind":"NO_LIMIT"},`,
  String.raw`"notify_poll_interval_ms":5000},`,
  String.raw`"payload":{"warn_bytes":{"kind":"BYTES","bytes":1048576},`,
  String.raw`"reject_bytes":{"kind":"BYTES","bytes":18446744073709551615}},`,
  String.raw`"tasks":{"reports.nightly":{"execution":"ISOLATED","error_codes":["REPORT_EMPTY"]},`,
  String.raw`"__proto__":{"execution":"SHARED_ASYNC","error_codes":[]}},`,
  String.raw`"handlers":{"kind":"HANDLERS","handlers":[{"name":"audit","event_type":"AttemptEnded"}]},`,
  String.raw`"worker_ended_spans":"MAKE_SPANS",`,
  String.raw`"child_launch":{"command":{"program":"/usr/local/bin/node",`,
  String.raw`"args":["--enable-source-maps","dist/worker.js"]},`,
  String.raw`"preload":{"kind":"NO_PRELOAD_PROGRAM"},`,
  String.raw`"program":{"kind":"HOST"},`,
  String.raw`"log_level":"INFO"}}`,
].join("");

describe("workerManifestToWire", () => {
  test("gives Python's text, with every digit of the bigint fields and the map order kept", () => {
    expect(wireText(workerManifestToWire(MANIFEST))).toBe(MANIFEST_TEXT);
  });

  test("throws a defect for a number that is not a safe integer, with its field", () => {
    const manifest: WorkerManifest = {
      ...MANIFEST,
      profiles: new Map([["default", { ...PROFILE, drain_timeout_ms: 2 ** 53 }]]),
    };

    expect(() => workerManifestToWire(manifest)).toThrow(
      "integerToWire: WorkerProfileManifest.drain_timeout_ms is 9007199254740992, not a safe integer",
    );
  });
});
