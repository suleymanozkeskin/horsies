// The config of an app: the core's `AppConfig` fields in camelCase (`core/src/core/config/app.rs`), parsed at the
// boundary and written as the core's config text.
//
// - A field that the config leaves out is left out of the text, so the core's default applies: each default has one
//   source.
// - The parse checks only what the core's config reader needs before it can read the text (`checked.ts`); the core
//   checks every rule of the config (the bridge's `checkAppConfig`).
// - A refusal of the cluster cap has the code `HRS-201`; a refusal of a claim field has the code of the prefetch
//   setting (`HRS-202`).

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import type { HorsiesError } from "../app/refusal.ts";
import type { TracingConfigInput } from "../telemetry/tracing-config.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type DatabaseConfigInput, databaseJson } from "./database.ts";
import {
  accepted,
  type Checked,
  checkedEntries,
  type ConfigJson,
  checkedInteger,
  checkedObject,
  type ConfigObject,
  entry,
  type IntegerBound,
  type JsonEntry,
  memberPath,
  optionalEntry,
  U32_MAX,
  u32Field,
} from "./checked.ts";
import { type PayloadPolicyInput, payloadJson } from "./payload.ts";
import { type QueueConfigInput, queueEntries, type QueueSetup, queueSetupOf } from "./queues.ts";
import { type RecoveryConfigInput, recoveryJson } from "./recovery.ts";
import { type ResilienceConfigInput, resilienceJson } from "./resilience.ts";
import { type RetentionConfigInput, retentionJson } from "./retention.ts";
import type { EnvironmentRead } from "./secret.ts";
import { type WorkerProfileInput, workerProfilesJson } from "./worker-profiles.ts";

/**
 * How a worker claims tasks:
 * - `hardCap`: a worker claims no task past its free seats. `leaseMs`: how long a claim holds before it expires, in
 *   milliseconds; left out, the core's default.
 * - `softCap`: a worker claims up to `prefetchBuffer` tasks (at least 1) past its free seats, and each claim holds for
 *   `leaseMs` milliseconds.
 */
export type ClaimMode =
  | { readonly kind: "hardCap"; readonly leaseMs?: number }
  | { readonly kind: "softCap"; readonly prefetchBuffer: number; readonly leaseMs: number };

/**
 * The cluster-wide cap of `RUNNING` tasks: at most `tasks` tasks (1 or more) run at once across every queue of the
 * cluster, or no cluster-wide cap (only the queue and worker limits apply).
 */
export type ClusterCapSetting =
  | { readonly kind: "clusterCap"; readonly tasks: number }
  | { readonly kind: "noClusterCap" };

/**
 * The config of an app, without its task and workflow lists. Only `database.databaseUrl` is required; every field left
 * out has the core's default.
 */
export type AppConfigInput = QueueConfigInput & {
  readonly database: DatabaseConfigInput;
  /** The most tasks that run at once in the whole cluster, or no cap. Left out: `{ kind: "noClusterCap" }`. */
  readonly clusterWideCap?: ClusterCapSetting;
  readonly claim?: ClaimMode;
  /** The age of a claimed task past which a heartbeat no longer renews it, in milliseconds. */
  readonly maxClaimRenewAgeMs?: number;
  readonly payload?: PayloadPolicyInput;
  readonly recovery?: RecoveryConfigInput;
  readonly retention?: RetentionConfigInput;
  readonly resilience?: ResilienceConfigInput;
  /** The settings of each worker, by name: a worker runs the profile of its name. */
  readonly workerProfiles?: Readonly<Record<string, WorkerProfileInput>>;
  /** Send again, up to 3 times, after a send fails with a transient database error. Left out: no resend. */
  readonly resendOnTransientError?: boolean;
  /** The spans and producer metrics of the app. Left out: `{ kind: "noTracing" }`. `Horsies.create` reads it; the core does not. */
  readonly tracing?: TracingConfigInput;
};

/** A parsed config. The core's config text is not enumerable, so a log of the value never shows a secret. */
export type AppConfig = {
  readonly queues: QueueSetup;
  readonly resendOnTransientError: boolean;
  /** The JSON text of the core's `AppConfig`. It holds the database URLs. */
  readonly coreConfigJson: string;
};

/** A `u32` field of the cluster cap. */
const CLUSTER_CAP_U32 = u32Field(HorsiesErrorCode.CONFIG_INVALID_CLUSTER_CAP);

/** A `u32` field of the claim settings. */
const PREFETCH_U32 = u32Field(HorsiesErrorCode.CONFIG_INVALID_PREFETCH);

/** The core's value of `prefetch_buffer` in the hard cap mode. */
const HARD_CAP_PREFETCH_BUFFER = 0;

/**
 * A prefetch buffer of the soft cap mode: at least 1, because the core reads `0` as the hard cap mode (`ClaimMode`
 * gives that mode its own variant).
 */
const SOFT_CAP_PREFETCH_BUFFER: IntegerBound = Object.freeze({
  min: 1,
  max: U32_MAX,
  code: HorsiesErrorCode.CONFIG_INVALID_PREFETCH,
});

/**
 * The parsed config of `input`, or every refusal in field order. Reads each `fromEnv` secret through `environment`.
 * The parse checks the shape that the core's config reader needs; the bridge's `checkAppConfig` checks the rules.
 */
export function parseAppConfig(input: AppConfigInput, environment: EnvironmentRead): Result<AppConfig, readonly HorsiesError[]> {
  const parsed = coreConfigObject(input, environment);

  switch (parsed.kind) {
    case "refused":
      return err(parsed.refusals);
    case "accepted":
      return ok(appConfigOf(input, parsed.value));
    default:
      return unreachable(parsed, "parseAppConfig");
  }
}

/** The JSON text of the core's `AppConfig` of a parsed config. Pure. */
export function coreAppConfigJson(config: AppConfig): string {
  return config.coreConfigJson;
}

/** The queues of a parsed config. Pure. */
export function queueSetup(config: AppConfig): QueueSetup {
  return config.queues;
}

/** Whether a send of the app resends after a transient database error. Pure. */
export function resendOnTransientError(config: AppConfig): boolean {
  return config.resendOnTransientError;
}

/** The core's `AppConfig` object of `input`, in the order of the core's fields. */
function coreConfigObject(input: AppConfigInput, environment: EnvironmentRead): Checked<ConfigObject> {
  return checkedObject([
    queueEntries(input),
    entry("database", databaseJson(input.database, environment, "database")),
    optionalEntry("cluster_wide_cap", input.clusterWideCap, (cap) => clusterCapJson(cap, "clusterWideCap")),
    claimEntries(input.claim, "claim"),
    optionalEntry("max_claim_renew_age_ms", input.maxClaimRenewAgeMs, (value) =>
      checkedInteger(value, PREFETCH_U32, "maxClaimRenewAgeMs"),
    ),
    optionalEntry("payload", input.payload, (payload) => payloadJson(payload, "payload")),
    optionalEntry("recovery", input.recovery, (recovery) => recoveryJson(recovery, "recovery")),
    optionalEntry("retention", input.retention, (retention) => retentionJson(retention, "retention")),
    optionalEntry("resilience", input.resilience, (resilience) => resilienceJson(resilience, "resilience")),
    optionalEntry("worker_profiles", input.workerProfiles, (profiles) =>
      workerProfilesJson(profiles, "workerProfiles"),
    ),
    entry("resend_on_transient_error", accepted(input.resendOnTransientError === true)),
  ]);
}

/** The core's tagged `cluster_wide_cap` value of a cap setting. Pure. */
function clusterCapJson(cap: ClusterCapSetting, path: string): Checked<ConfigJson> {
  switch (cap.kind) {
    case "clusterCap":
      return checkedObject([
        accepted([["kind", "cluster_cap"]]),
        entry("tasks", checkedInteger(cap.tasks, CLUSTER_CAP_U32, memberPath(path, "tasks"))),
      ]);
    case "noClusterCap":
      return accepted({ kind: "no_cluster_cap" });
    default:
      return unreachable(cap, "clusterCapJson");
  }
}

/**
 * The core's members `prefetch_buffer` and `claim_lease_ms` of a claim mode; none when the config leaves the mode out.
 * Pure.
 */
function claimEntries(claim: ClaimMode | undefined, path: string): Checked<readonly JsonEntry[]> {
  if (claim === undefined) {
    return accepted([]);
  }

  switch (claim.kind) {
    case "hardCap":
      return checkedEntries([
        accepted([["prefetch_buffer", HARD_CAP_PREFETCH_BUFFER]]),
        optionalEntry("claim_lease_ms", claim.leaseMs, (value) =>
          checkedInteger(value, PREFETCH_U32, memberPath(path, "leaseMs")),
        ),
      ]);
    case "softCap":
      return checkedEntries([
        entry(
          "prefetch_buffer",
          checkedInteger(claim.prefetchBuffer, SOFT_CAP_PREFETCH_BUFFER, memberPath(path, "prefetchBuffer")),
        ),
        entry("claim_lease_ms", checkedInteger(claim.leaseMs, PREFETCH_U32, memberPath(path, "leaseMs"))),
      ]);
    default:
      return unreachable(claim, "claimEntries");
  }
}

/** The parsed config of an accepted input and its core object. Pure. */
function appConfigOf(input: AppConfigInput, core: ConfigObject): AppConfig {
  const config = {
    queues: queueSetupOf(input),
    resendOnTransientError: input.resendOnTransientError === true,
    coreConfigJson: JSON.stringify(core),
  };

  return Object.freeze(Object.defineProperty(config, "coreConfigJson", { enumerable: false }));
}
