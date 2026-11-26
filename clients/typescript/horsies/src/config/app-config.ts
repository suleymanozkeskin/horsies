// The config of an app: the core's `AppConfig` fields in camelCase (`core/src/core/config/app.rs`), parsed at the
// boundary and written as the core's config text.
//
// - A field that the config leaves out is left out of the text, so the core's default applies: each default has one
//   source.
// - The parse checks only what the core's config reader needs before it can read the text (`checked.ts`); the core
//   checks every rule of the config (the bridge's `checkAppConfig`).

import type { Refusal } from "../app/refusal.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type BrokerConfigInput, brokerJson } from "./broker.ts";
import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  type ConfigObject,
  entry,
  optionalEntry,
  U32_FIELD,
} from "./checked.ts";
import { type PayloadPolicyInput, payloadJson } from "./payload.ts";
import { type QueueConfigInput, queueEntries, type QueueSetup, queueSetupOf, type TaskCap, taskCapJson } from "./queues.ts";
import { type RecoveryConfigInput, recoveryJson } from "./recovery.ts";
import { type ResilienceConfigInput, resilienceJson } from "./resilience.ts";
import { type RetentionConfigInput, retentionJson } from "./retention.ts";
import type { EnvironmentRead } from "./secret.ts";
import { type WorkerProfileInput, workerProfilesJson } from "./worker-profiles.ts";

/**
 * The config of an app, without its task and workflow lists. Only `broker.databaseUrl` is required; every field left
 * out has the core's default.
 */
export type AppConfigInput = QueueConfigInput & {
  readonly broker: BrokerConfigInput;
  /** The most tasks that run at once in the whole cluster, or no cap. */
  readonly clusterWideCap?: TaskCap;
  /** Tasks that a worker claims past its free seats: 0 is the hard cap mode; above 0 needs `claimLeaseMs`. */
  readonly prefetchBuffer?: number;
  /** How long a claim holds before it expires, in milliseconds. */
  readonly claimLeaseMs?: number;
  /** The age of a claimed task past which a heartbeat no longer renews it, in milliseconds. */
  readonly maxClaimRenewAgeMs?: number;
  readonly payload?: PayloadPolicyInput;
  readonly recovery?: RecoveryConfigInput;
  readonly retention?: RetentionConfigInput;
  readonly resilience?: ResilienceConfigInput;
  /** The settings of each worker, by name: a worker runs the profile of its name. */
  readonly workerProfiles?: Readonly<Record<string, WorkerProfileInput>>;
  /** Send again, up to 3 times, after a send fails with a transient database error. Left out: no resend. */
  readonly resendOnTransientErr?: boolean;
};

/** A parsed config. The core's config text is not enumerable, so a log of the value never shows a secret. */
export type AppConfig = {
  readonly queues: QueueSetup;
  readonly resendOnTransientErr: boolean;
  /** The JSON text of the core's `AppConfig`. It holds the database URLs. */
  readonly coreConfigJson: string;
};

/**
 * The parsed config of `input`, or every refusal in field order. Reads each `fromEnv` secret through `environment`.
 * The parse checks the shape that the core's config reader needs; the bridge's `checkAppConfig` checks the rules.
 */
export function parseAppConfig(input: AppConfigInput, environment: EnvironmentRead): Result<AppConfig, readonly Refusal[]> {
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
export function resendOnTransientErr(config: AppConfig): boolean {
  return config.resendOnTransientErr;
}

/** The core's `AppConfig` object of `input`, in the order of the core's fields. */
function coreConfigObject(input: AppConfigInput, environment: EnvironmentRead): Checked<ConfigObject> {
  return checkedObject([
    queueEntries(input),
    entry("broker", brokerJson(input.broker, environment, "broker")),
    optionalEntry("cluster_wide_cap", input.clusterWideCap, (cap) => taskCapJson(cap, "clusterWideCap.tasks")),
    optionalEntry("prefetch_buffer", input.prefetchBuffer, (value) =>
      checkedInteger(value, U32_FIELD, "prefetchBuffer"),
    ),
    optionalEntry("claim_lease_ms", input.claimLeaseMs, (value) => checkedInteger(value, U32_FIELD, "claimLeaseMs")),
    optionalEntry("max_claim_renew_age_ms", input.maxClaimRenewAgeMs, (value) =>
      checkedInteger(value, U32_FIELD, "maxClaimRenewAgeMs"),
    ),
    optionalEntry("payload", input.payload, (payload) => payloadJson(payload, "payload")),
    optionalEntry("recovery", input.recovery, (recovery) => recoveryJson(recovery, "recovery")),
    optionalEntry("retention", input.retention, (retention) => retentionJson(retention, "retention")),
    optionalEntry("resilience", input.resilience, (resilience) => resilienceJson(resilience, "resilience")),
    optionalEntry("worker_profiles", input.workerProfiles, (profiles) =>
      workerProfilesJson(profiles, "workerProfiles"),
    ),
    entry("resend_on_transient_err", accepted(input.resendOnTransientErr === true)),
  ]);
}

/** The parsed config of an accepted input and its core object. Pure. */
function appConfigOf(input: AppConfigInput, core: ConfigObject): AppConfig {
  const config = {
    queues: queueSetupOf(input),
    resendOnTransientErr: input.resendOnTransientErr === true,
    coreConfigJson: JSON.stringify(core),
  };

  return Object.freeze(Object.defineProperty(config, "coreConfigJson", { enumerable: false }));
}
