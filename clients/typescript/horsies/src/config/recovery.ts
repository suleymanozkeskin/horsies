// The recovery settings of the config: stale claims and runs, orphaned workflow tasks, heartbeats and snapshots (the
// core's `RecoveryConfig`, `core/src/core/config/recovery.rs`).

import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  type ConfigJson,
  type IntegerBound,
  memberPath,
  optionalEntry,
  U32_FIELD,
  U64_FIELD,
} from "./checked.ts";

/** The recovery settings. Each field left out has the core's default; the core checks each range. */
export interface RecoveryConfigInput {
  readonly autoRequeueStaleClaimed?: boolean;
  readonly claimedStaleThresholdMs?: number;
  readonly autoFailStaleRunning?: boolean;
  readonly autoTerminateOrphanedWorkflowTasks?: boolean;
  readonly orphanTaskAuditIntervalMs?: number;
  readonly runningStaleThresholdMs?: number;
  readonly finalizingStaleThresholdMs?: number;
  readonly crashedWorkerRecoveryGraceMs?: number;
  readonly phase2QuarantineAfterAttempts?: number;
  readonly workflowRecoveryFailureLimit?: number;
  readonly checkIntervalMs?: number;
  readonly runnerHeartbeatIntervalMs?: number;
  readonly claimerHeartbeatIntervalMs?: number;
  readonly workerStateSnapshotIntervalMs?: number;
}

/** The core's `recovery` object of `input`. Pure. */
export function recoveryJson(input: RecoveryConfigInput, path: string): Checked<ConfigJson> {
  const integer =
    (name: string, bound: IntegerBound) =>
    (value: number): Checked<ConfigJson> =>
      checkedInteger(value, bound, memberPath(path, name));

  return checkedObject([
    optionalEntry("auto_requeue_stale_claimed", input.autoRequeueStaleClaimed, accepted),
    optionalEntry(
      "claimed_stale_threshold_ms",
      input.claimedStaleThresholdMs,
      integer("claimedStaleThresholdMs", U64_FIELD),
    ),
    optionalEntry("auto_fail_stale_running", input.autoFailStaleRunning, accepted),
    optionalEntry("auto_terminate_orphaned_workflow_tasks", input.autoTerminateOrphanedWorkflowTasks, accepted),
    optionalEntry(
      "orphan_task_audit_interval_ms",
      input.orphanTaskAuditIntervalMs,
      integer("orphanTaskAuditIntervalMs", U64_FIELD),
    ),
    optionalEntry(
      "running_stale_threshold_ms",
      input.runningStaleThresholdMs,
      integer("runningStaleThresholdMs", U64_FIELD),
    ),
    optionalEntry(
      "finalizing_stale_threshold_ms",
      input.finalizingStaleThresholdMs,
      integer("finalizingStaleThresholdMs", U64_FIELD),
    ),
    optionalEntry(
      "crashed_worker_recovery_grace_ms",
      input.crashedWorkerRecoveryGraceMs,
      integer("crashedWorkerRecoveryGraceMs", U64_FIELD),
    ),
    optionalEntry(
      "phase2_quarantine_after_attempts",
      input.phase2QuarantineAfterAttempts,
      integer("phase2QuarantineAfterAttempts", U32_FIELD),
    ),
    optionalEntry(
      "workflow_recovery_failure_limit",
      input.workflowRecoveryFailureLimit,
      integer("workflowRecoveryFailureLimit", U32_FIELD),
    ),
    optionalEntry("check_interval_ms", input.checkIntervalMs, integer("checkIntervalMs", U64_FIELD)),
    optionalEntry(
      "runner_heartbeat_interval_ms",
      input.runnerHeartbeatIntervalMs,
      integer("runnerHeartbeatIntervalMs", U64_FIELD),
    ),
    optionalEntry(
      "claimer_heartbeat_interval_ms",
      input.claimerHeartbeatIntervalMs,
      integer("claimerHeartbeatIntervalMs", U64_FIELD),
    ),
    optionalEntry(
      "worker_state_snapshot_interval_ms",
      input.workerStateSnapshotIntervalMs,
      integer("workerStateSnapshotIntervalMs", U64_FIELD),
    ),
  ]);
}
