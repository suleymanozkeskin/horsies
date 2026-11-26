// The core's config text of a parsed config: the serde form of `AppConfig` (`core/src/core/config/*.rs`), and the
// queues of the config.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  type AppConfig,
  type AppConfigInput,
  coreAppConfigJson,
  parseAppConfig,
  queueSetup,
  resendOnTransientErr,
} from "../../../horsies/src/config/app-config.ts";

const DATABASE_URL = "postgresql://horsies@127.0.0.1:55432/horsies";
const SESSION_URL = "postgresql://horsies@127.0.0.1:55433/horsies";

/** A parsed config of an input that the parse must accept. The environment holds `SESSION_URL`. */
function parsed(input: AppConfigInput): AppConfig {
  const outcome = parseAppConfig(input, (name) => (name === "SESSION_URL" ? SESSION_URL : undefined));

  assert.equal(outcome.ok, true, outcome.ok ? "" : JSON.stringify(outcome.error));

  if (!outcome.ok) {
    throw new Error("the config must be accepted");
  }

  return outcome.value;
}

const FULL_INPUT: AppConfigInput = {
  queueMode: "custom",
  customQueues: [
    { name: "urgent", priority: 1, maxConcurrency: { kind: "cap", tasks: 10 } },
    { name: "bulk", priority: 90, maxConcurrency: { kind: "noCap" } },
    { name: "standard", priority: 50 },
  ],
  broker: {
    databaseUrl: { kind: "value", value: DATABASE_URL },
    connection: { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "fromEnv", name: "SESSION_URL" } },
    poolPrePing: false,
    workerPoolPrePing: true,
    poolSize: 8,
    maxOverflow: 2,
    retainRerunInputDefault: "keep",
    poolTimeoutSeconds: 15,
    poolRecycle: { kind: "afterSeconds", seconds: 600 },
    echo: true,
    tcpKeepalives: { kind: "on", idleSeconds: 60, intervalSeconds: 5, probes: 4 },
    tcpUserTimeout: { kind: "after", milliseconds: 20_000 },
  },
  clusterWideCap: { kind: "cap", tasks: 200 },
  claim: { kind: "softCap", prefetchBuffer: 4, leaseMs: 30_000 },
  maxClaimRenewAgeMs: 600_000,
  payload: { warnBytes: { kind: "noBound" }, rejectBytes: { kind: "bytes", bytes: 4_194_304 } },
  recovery: {
    autoRequeueStaleClaimed: false,
    claimedStaleThresholdMs: 120_000,
    autoFailStaleRunning: true,
    autoTerminateOrphanedWorkflowTasks: false,
    orphanTaskAuditIntervalMs: 3_600_000,
    runningStaleThresholdMs: 300_000,
    finalizingStaleThresholdMs: 300_000,
    crashedWorkerRecoveryGraceMs: 10_000,
    phase2QuarantineAfterAttempts: 5,
    workflowRecoveryFailureLimit: 20,
    checkIntervalMs: 30_000,
    runnerHeartbeatIntervalMs: 15_000,
    claimerHeartbeatIntervalMs: 20_000,
    workerStateSnapshotIntervalMs: 5000,
  },
  retention: {
    workerStateRetention: { kind: "hours", hours: 48 },
    terminalRecordRetention: { kind: "noPruning" },
    pausedWorkflowAutoCancelAfter: { kind: "after", age: Temporal.Duration.from({ days: 2, microseconds: 1500 }) },
    historyLeafHorizonDays: 4,
    heartbeatLeafHorizonHours: 12,
    retentionPeriods: [{ days: 7 }, { days: 90 }],
    queueRetention: { urgent: { kind: "forever" }, bulk: { kind: "days", days: 14 } },
    partitionMaintenanceIntervalSeconds: 600,
    retentionSweepIntervalSeconds: 120,
    retentionDeleteBatchSize: 1000,
  },
  resilience: {
    dbRetryInitialMs: 200,
    dbRetryMaxMs: 10_000,
    dbRetryMaxAttempts: { kind: "noLimit" },
    notifyPollIntervalMs: 2000,
  },
  workerProfiles: { default: { isolated: { count: 2 } } },
  resendOnTransientErr: true,
};

/** The serde form of `FULL_INPUT`. */
const FULL_CORE_OBJECT = {
  queue_mode: "custom",
  custom_queues: [
    { name: "urgent", priority: 1, max_concurrency: 10 },
    { name: "bulk", priority: 90, max_concurrency: null },
    { name: "standard", priority: 50 },
  ],
  broker: {
    database_url: DATABASE_URL,
    session_database_url: SESSION_URL,
    pgbouncer_transaction_mode: true,
    pool_pre_ping: false,
    worker_pool_pre_ping: true,
    pool_size: 8,
    max_overflow: 2,
    retain_rerun_input_default: true,
    pool_timeout: 15,
    pool_recycle: { kind: "AFTER_SECONDS", seconds: 600 },
    echo: true,
    tcp_keepalives: { kind: "ON", idle_seconds: 60, interval_seconds: 5, probes: 4 },
    tcp_user_timeout: { kind: "AFTER", milliseconds: 20_000 },
  },
  cluster_wide_cap: 200,
  prefetch_buffer: 4,
  claim_lease_ms: 30_000,
  max_claim_renew_age_ms: 600_000,
  payload: { warn_bytes: null, reject_bytes: 4_194_304 },
  recovery: {
    auto_requeue_stale_claimed: false,
    claimed_stale_threshold_ms: 120_000,
    auto_fail_stale_running: true,
    auto_terminate_orphaned_workflow_tasks: false,
    orphan_task_audit_interval_ms: 3_600_000,
    running_stale_threshold_ms: 300_000,
    finalizing_stale_threshold_ms: 300_000,
    crashed_worker_recovery_grace_ms: 10_000,
    phase2_quarantine_after_attempts: 5,
    workflow_recovery_failure_limit: 20,
    check_interval_ms: 30_000,
    runner_heartbeat_interval_ms: 15_000,
    claimer_heartbeat_interval_ms: 20_000,
    worker_state_snapshot_interval_ms: 5000,
  },
  retention: {
    worker_state_retention_hours: 48,
    terminal_record_retention_hours: null,
    paused_workflow_auto_cancel_after: [172_800, 1_500_000],
    history_leaf_horizon_days: 4,
    heartbeat_leaf_horizon_hours: 12,
    retention_periods: [
      [604_800, 0],
      [7_776_000, 0],
    ],
    queue_retention: { urgent: null, bulk: [1_209_600, 0] },
    partition_maintenance_interval_s: 600,
    retention_sweep_interval_s: 120,
    retention_delete_batch_size: 1000,
  },
  resilience: {
    db_retry_initial_ms: 200,
    db_retry_max_ms: 10_000,
    db_retry_max_attempts: 0,
    notify_poll_interval_ms: 2000,
  },
  worker_profiles: { default: { isolated: { kind: "isolated_children", count: 2 } } },
  resend_on_transient_err: true,
};

describe("coreAppConfigJson", () => {
  test("a config with every field gives the serde form of the core's AppConfig", () => {
    assert.deepEqual(JSON.parse(coreAppConfigJson(parsed(FULL_INPUT))), FULL_CORE_OBJECT);
  });

  test("a config with only a database URL gives the URL and the resend flag; the core fills every other default", () => {
    const config = parsed({ broker: { databaseUrl: { kind: "value", value: DATABASE_URL } } });

    assert.equal(
      coreAppConfigJson(config),
      `{"broker":{"database_url":"${DATABASE_URL}"},"resend_on_transient_err":false}`,
    );
  });

  test("a direct connection whose session uses the database URL leaves the session URL out", () => {
    const config = parsed({
      broker: {
        databaseUrl: { kind: "value", value: DATABASE_URL },
        connection: { kind: "direct", sessionDatabase: { kind: "sameAsDatabaseUrl" } },
      },
    });

    assert.deepEqual(JSON.parse(coreAppConfigJson(config)).broker, {
      database_url: DATABASE_URL,
      pgbouncer_transaction_mode: false,
    });
  });

  test("a direct connection with a separate session database gives the session URL and no PgBouncer mode", () => {
    const config = parsed({
      broker: {
        databaseUrl: { kind: "value", value: DATABASE_URL },
        connection: { kind: "direct", sessionDatabase: { kind: "separate", url: { kind: "fromEnv", name: "SESSION_URL" } } },
      },
    });

    assert.deepEqual(JSON.parse(coreAppConfigJson(config)).broker, {
      database_url: DATABASE_URL,
      session_database_url: SESSION_URL,
      pgbouncer_transaction_mode: false,
    });
  });

  test("the hard cap mode gives a prefetch buffer of 0, and its lease only when the config gives one", () => {
    const withoutLease = parsed({ broker: { databaseUrl: { kind: "value", value: DATABASE_URL } }, claim: { kind: "hardCap" } });

    const withLease = parsed({
      broker: { databaseUrl: { kind: "value", value: DATABASE_URL } },
      claim: { kind: "hardCap", leaseMs: 90_000 },
    });

    assert.equal(
      coreAppConfigJson(withoutLease),
      `{"broker":{"database_url":"${DATABASE_URL}"},"prefetch_buffer":0,"resend_on_transient_err":false}`,
    );
    assert.equal(
      coreAppConfigJson(withLease),
      `{"broker":{"database_url":"${DATABASE_URL}"},"prefetch_buffer":0,"claim_lease_ms":90000,"resend_on_transient_err":false}`,
    );
  });

  test("a custom queue that names no priority has no priority member: the core's default applies", () => {
    const config = parsed({
      broker: { databaseUrl: { kind: "value", value: DATABASE_URL } },
      queueMode: "custom",
      customQueues: [{ name: "bulk" }],
    });

    assert.equal(coreAppConfigJson(config).includes('"custom_queues":[{"name":"bulk"}]'), true);
  });

  test("a queue named __proto__ in the queue retention is a member of the text", () => {
    const config = parsed({
      broker: { databaseUrl: { kind: "value", value: DATABASE_URL } },
      retention: { queueRetention: { ["__proto__"]: { kind: "forever" } } },
    });

    assert.equal(coreAppConfigJson(config).includes('"queue_retention":{"__proto__":null}'), true);
  });
});

describe("queueSetup", () => {
  test("a config with no queue mode has the default queue", () => {
    assert.deepEqual(queueSetup(parsed({ broker: { databaseUrl: { kind: "value", value: DATABASE_URL } } })), {
      kind: "default",
    });
  });

  test("a custom config has each queue by name, and a queue with no cap of its own has the core's default", () => {
    const setup = queueSetup(parsed(FULL_INPUT));

    assert.equal(setup.kind, "custom");

    if (setup.kind !== "custom") {
      return;
    }

    assert.deepEqual([...setup.queues.keys()], ["urgent", "bulk", "standard"]);
    assert.deepEqual(setup.queues.get("urgent"), {
      name: "urgent",
      maxConcurrency: { kind: "cap", tasks: 10 },
    });
    assert.deepEqual(setup.queues.get("standard")?.maxConcurrency, { kind: "coreDefault" });
  });
});

describe("resendOnTransientErr", () => {
  test("the flag is the config's, and off when the config leaves it out", () => {
    assert.equal(resendOnTransientErr(parsed(FULL_INPUT)), true);
    assert.equal(resendOnTransientErr(parsed({ broker: { databaseUrl: { kind: "value", value: DATABASE_URL } } })), false);
  });
});
