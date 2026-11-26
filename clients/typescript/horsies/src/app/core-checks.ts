// The checks of `Horsies.create` that the core makes, through the addon's pure check functions (main thread only),
// and the facts that they take. One source per rule: the core's.

import type { ErrorCode } from "../_catalog/error_codes.ts";
import { ErrorCode as ErrorCodes } from "../_catalog/error_codes.ts";
import type { ContractRecord, MillisecondsSetting, RetrySetting } from "../contract/definition.ts";
import type { InstallCoverageArg } from "../native/handle/addon.ts";
import type {
  AppConfigCheck,
  CoreRefusal,
  DeclaredCodesCheck,
  MillisecondsFact,
  NativeSendRules,
  NativeTaskOptions,
  RetryFacts,
  TaskOptionsCheck,
  TaskOptionsFacts,
} from "../native/sends/addon.ts";
import type { QueueRoute } from "../send/send.ts";
import { refusal, type Refusal, type RefusalCode } from "./refusal.ts";

/** The core's pure check functions. On a main thread the addon gives them; a test gives a fake. */
export interface CoreChecks {
  checkAppConfig(appConfigJson: string): AppConfigCheck;
  parseDeclaredErrorCodes(codes: readonly string[]): DeclaredCodesCheck;
  checkTaskOptions(facts: TaskOptionsFacts): TaskOptionsCheck;
}

/** The facts of an accepted config that the app keeps. */
export type AcceptedConfig = {
  readonly rules: NativeSendRules;
  readonly manifestJson: string;
  readonly installCoverage: InstallCoverageArg;
};

/** The outcome of a core check: its value, or its refusals. */
export type CoreChecked<T> = { readonly kind: "accepted"; readonly value: T } | { readonly kind: "refused"; readonly refusals: readonly Refusal[] };

const CATALOG_CODES: ReadonlySet<string> = new Set(Object.values(ErrorCodes));

/** The core check of the config text. Refusals have the path `config`. */
export function checkConfigWithCore(checks: CoreChecks, appConfigJson: string): CoreChecked<AcceptedConfig> {
  const checked = checks.checkAppConfig(appConfigJson);

  switch (checked.kind) {
    case "accepted":
      return {
        kind: "accepted",
        value: { rules: checked.rules, manifestJson: checked.manifestJson, installCoverage: checked.installCoverage },
      };
    case "refused":
      return { kind: "refused", refusals: checked.refusals.map((refused) => refusalOfCore("config", refused)) };
    default: {
      const unexpected: never = checked;

      throw new TypeError(`checkConfigWithCore: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The core checks of one contract: its declared codes, then its options. Refusals have the path `path`. */
export function checkTaskWithCore(
  checks: CoreChecks,
  record: ContractRecord,
  route: QueueRoute,
  path: string,
): CoreChecked<NativeTaskOptions> {
  const codes = codeRefusals(checks, record, path);
  const options = checks.checkTaskOptions(taskOptionsFacts(record, route));

  switch (options.kind) {
    case "accepted":
      return codes.length === 0 ? { kind: "accepted", value: options.options } : { kind: "refused", refusals: codes };
    case "refused":
      return { kind: "refused", refusals: [...codes, ...options.refusals.map((refused) => refusalOfCore(path, refused))] };
    default: {
      const unexpected: never = options;

      throw new TypeError(`checkTaskWithCore: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

function codeRefusals(checks: CoreChecks, record: ContractRecord, path: string): readonly Refusal[] {
  switch (record.codes.kind) {
    case "open":
      return [];
    case "declared": {
      const checked = checks.parseDeclaredErrorCodes(record.codes.codes);

      return checked.kind === "accepted" ? [] : checked.refusals.map((refused) => refusalOfCore(path, refused));
    }

    default: {
      const unexpected: never = record.codes;

      throw new TypeError(`checkTaskWithCore: codes outside their type: ${String(unexpected)}`);
    }
  }
}

/** The facts of a contract that its stored options text holds. Pure. */
export function taskOptionsFacts(record: ContractRecord, route: QueueRoute): TaskOptionsFacts {
  return {
    taskName: record.name,
    queueName: route.queueName,
    execution: record.execution,
    timeout: millisecondsFact(record.timeout),
    cancelGrace: millisecondsFact(record.cancelGrace),
    retry: retryFacts(record.retry),
    declaredCodes: record.codes.kind === "open" ? { kind: "open" } : { kind: "declared", codes: [...record.codes.codes] },
  };
}

function millisecondsFact(setting: MillisecondsSetting): MillisecondsFact {
  return setting.kind === "set" ? { kind: "set", milliseconds: setting.milliseconds } : { kind: "notSet" };
}

function retryFacts(retry: RetrySetting): RetryFacts {
  switch (retry.kind) {
    case "noRetry":
      return { kind: "noRetry" };
    case "fixed":
      return {
        kind: "fixed",
        intervalsSeconds: [...retry.intervalsSeconds],
        jitter: retry.jitter,
        autoRetryFor: [...retry.autoRetryFor],
      };
    case "exponential":
      return {
        kind: "exponential",
        baseSeconds: retry.baseSeconds,
        maxRetries: retry.maxRetries,
        jitter: retry.jitter,
        maxDelay: retry.maxDelay.kind === "seconds" ? { kind: "seconds", seconds: retry.maxDelay.seconds } : { kind: "noCap" },
        autoRetryFor: [...retry.autoRetryFor],
      };
    default: {
      const unexpected: never = retry;

      throw new TypeError(`taskOptionsFacts: a retry policy outside its type: ${String(unexpected)}`);
    }
  }
}

/** The refusal of a core refusal at `path`. A code that the catalog does not hold is a defect: `TypeError`. Pure. */
export function refusalOfCore(path: string, refused: CoreRefusal): Refusal {
  return refusal(refusalCode(refused), path, [refused.message, ...refused.notes].join("; "), refused.help);
}

function refusalCode(refused: CoreRefusal): RefusalCode {
  switch (refused.code.kind) {
    case "noCode":
      return "FIELD_BOUND";
    case "catalog":
      return catalogCode(refused.code.code);
    default: {
      const unexpected: never = refused.code;

      throw new TypeError(`refusalOfCore: a code outside its type: ${String(unexpected)}`);
    }
  }
}

function catalogCode(code: string): ErrorCode {
  if (!CATALOG_CODES.has(code)) {
    throw new TypeError(`refusalOfCore: the core gave the code ${code}, which the generated catalog does not hold`);
  }

  // SAFETY: the set of the generated catalog holds the code, checked on the line above.
  return code as ErrorCode;
}
