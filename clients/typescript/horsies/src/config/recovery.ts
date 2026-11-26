// The recovery settings of the config: stale claims and runs, orphaned workflow tasks, heartbeats and snapshots (the
// core's `RecoveryConfig`, `core/src/core/config/recovery.rs`). A refusal of a recovery field has the code of the
// recovery config (`HRS-204`).

import { ErrorCode } from "../_catalog/error_codes.ts";
import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  type ConfigJson,
  type IntegerBound,
  memberPath,
  optionalEntry,
  u32Field,
  u64Field,
} from "./checked.ts";

/** The recovery settings. Each field left out has the core's default; the core checks each range. */
export interface RecoveryConfigInput {
  readonly requeueStaleClaimed?: boolean;
  readonly claimedStaleThresholdMs?: number;
  readonly failStaleRunning?: boolean;
  readonly terminateOrphanedWorkflowTasks?: boolean;
  readonly orphanTaskAuditIntervalMs?: number;
  readonly runningStaleThresholdMs?: number;
  readonly finalizingStaleThresholdMs?: number;
  readonly crashedWorkerRecoveryGraceMs?: number;
  readonly finalizationQuarantineAfterAttempts?: number;
  readonly workflowRecoveryFailureLimit?: number;
  readonly checkIntervalMs?: number;
  readonly runnerHeartbeatIntervalMs?: number;
  readonly claimerHeartbeatIntervalMs?: number;
  readonly workerStateSnapshotIntervalMs?: number;
}

/** A `u32` field of the recovery config. */
const RECOVERY_U32 = u32Field(ErrorCode.CONFIG_INVALID_RECOVERY);

/** A `u64` field of the recovery config. */
const RECOVERY_U64 = u64Field(ErrorCode.CONFIG_INVALID_RECOVERY);

/** The core's `recovery` object of `input`. Pure. */
export function recoveryJson(input: RecoveryConfigInput, path: string): Checked<ConfigJson> {
  const integer =
    (name: string, bound: IntegerBound) =>
    (value: number): Checked<ConfigJson> =>
      checkedInteger(value, bound, memberPath(path, name));

  return checkedObject([
    optionalEntry("auto_requeue_stale_claimed", input.requeueStaleClaimed, accepted),
    optionalEntry(
      "claimed_stale_threshold_ms",
      input.claimedStaleThresholdMs,
      integer("claimedStaleThresholdMs", RECOVERY_U64),
    ),
    optionalEntry("auto_fail_stale_running", input.failStaleRunning, accepted),
    optionalEntry("auto_terminate_orphaned_workflow_tasks", input.terminateOrphanedWorkflowTasks, accepted),
    optionalEntry(
      "orphan_task_audit_interval_ms",
      input.orphanTaskAuditIntervalMs,
      integer("orphanTaskAuditIntervalMs", RECOVERY_U64),
    ),
    optionalEntry(
      "running_stale_threshold_ms",
      input.runningStaleThresholdMs,
      integer("runningStaleThresholdMs", RECOVERY_U64),
    ),
    optionalEntry(
      "finalizing_stale_threshold_ms",
      input.finalizingStaleThresholdMs,
      integer("finalizingStaleThresholdMs", RECOVERY_U64),
    ),
    optionalEntry(
      "crashed_worker_recovery_grace_ms",
      input.crashedWorkerRecoveryGraceMs,
      integer("crashedWorkerRecoveryGraceMs", RECOVERY_U64),
    ),
    optionalEntry(
      "phase2_quarantine_after_attempts",
      input.finalizationQuarantineAfterAttempts,
      integer("finalizationQuarantineAfterAttempts", RECOVERY_U32),
    ),
    optionalEntry(
      "workflow_recovery_failure_limit",
      input.workflowRecoveryFailureLimit,
      integer("workflowRecoveryFailureLimit", RECOVERY_U32),
    ),
    optionalEntry("check_interval_ms", input.checkIntervalMs, integer("checkIntervalMs", RECOVERY_U64)),
    optionalEntry(
      "runner_heartbeat_interval_ms",
      input.runnerHeartbeatIntervalMs,
      integer("runnerHeartbeatIntervalMs", RECOVERY_U64),
    ),
    optionalEntry(
      "claimer_heartbeat_interval_ms",
      input.claimerHeartbeatIntervalMs,
      integer("claimerHeartbeatIntervalMs", RECOVERY_U64),
    ),
    optionalEntry(
      "worker_state_snapshot_interval_ms",
      input.workerStateSnapshotIntervalMs,
      integer("workerStateSnapshotIntervalMs", RECOVERY_U64),
    ),
  ]);
}
