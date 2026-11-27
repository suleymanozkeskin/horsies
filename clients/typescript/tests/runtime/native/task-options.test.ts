// The stored task options text through the contract mapping and the addon: for each vector of
// `core/tests/fixtures/stored_forms/task_options.json`, a contract built with `defineTask` from the vector's facts,
// mapped with `taskOptionsFacts`, checked by `checkTaskOptions`, gives the vector's text: `storedText()` for a task
// with no deadline, `prepareSendFacts` with the deadline for a send or a node. Every call is pure: the core of the
// process does not start.
//
// Runs only where a built addon exists, with `HORSIES_NATIVE_ADDON_PATH` naming the file.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { DEFAULT_QUEUE_ROUTE } from "../../../horsies/src/app/contract-list.ts";
import { taskOptionsFacts } from "../../../horsies/src/app/core-checks.ts";
import { parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import { contractRecord } from "../../../horsies/src/contract/definition.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { Execution } from "../../../horsies/src/monitoring/public.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import type { NativeSendRules, NativeTaskOptions } from "../../../horsies/src/native/sends/addon.ts";
import {
  checkAppConfig,
  checkTaskOptions,
  prepareSendFacts,
  storedOptionsText,
} from "../../../horsies/src/native/sends/sends.ts";
import { coreState } from "../../../horsies/src/native/process-core/process-core.ts";
import { epochMicrosTowardPast } from "../../../horsies/src/send/options.ts";
import { h } from "../../../horsies/src/wire/public.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The task options vectors of the core. */
const VECTOR_FILE = fileURLToPath(
  new URL("../../../../../core/tests/fixtures/stored_forms/task_options.json", import.meta.url),
);

/** A database URL of the config: the checks are pure and open no connection. */
const UNUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The retry policy of a vector, as Python's `RetryPolicy` stores it. */
type RetryNotation = {
  readonly max_retries: number;
  readonly intervals: readonly number[];
  readonly backoff_strategy: "fixed" | "exponential";
  readonly jitter: boolean;
  readonly max_delay_seconds: number | null;
  readonly auto_retry_for: readonly string[];
};

/** One vector: the facts of the options, the deadline of a send or a node, and Python's text. */
type OptionsVector = {
  readonly name: string;
  readonly kind: "task" | "send" | "node";
  readonly options: {
    readonly retry_policy: RetryNotation | null;
    readonly timeout_ms: number | null;
    readonly execution: Execution;
    readonly cancel_grace_ms: number | null;
  };
  readonly deadline: string | null;
  readonly text: string;
};

function vectors(): readonly OptionsVector[] {
  // SAFETY: the vector file of the core holds these fields (its README, "Rows"); a changed file fails the asserts.
  return (JSON.parse(readFileSync(VECTOR_FILE, "utf8")) as { readonly vectors: readonly OptionsVector[] }).vectors;
}

/** The `retryPolicy` field of `defineTask` of a vector's policy. */
function retryOf(policy: RetryNotation): object {
  const [base] = policy.intervals;

  switch (policy.backoff_strategy) {
    case "fixed":
      return {
        kind: "fixed",
        intervalsSeconds: policy.intervals,
        jitter: policy.jitter,
        autoRetryFor: policy.auto_retry_for,
      };
    case "exponential":
      return {
        kind: "exponential",
        baseSeconds: base,
        maxRetries: policy.max_retries,
        jitter: policy.jitter,
        ...(policy.max_delay_seconds === null ? {} : { maxDelaySeconds: policy.max_delay_seconds }),
        autoRetryFor: policy.auto_retry_for,
      };
    default: {
      const unexpected: never = policy.backoff_strategy;

      throw new TypeError(`a backoff strategy outside the vector notation: ${String(unexpected)}`);
    }
  }
}

/** The checked options of the contract of a vector. */
function checkedOptions(vector: OptionsVector): NativeTaskOptions {
  const { options } = vector;

  const fields = {
    name: `task_options_${vector.name}`,
    input: {},
    output: h.null(),
    execution: options.execution,
    ...(options.timeout_ms === null ? {} : { timeoutMs: options.timeout_ms }),
    ...(options.cancel_grace_ms === null ? {} : { cancelGraceMs: options.cancel_grace_ms }),
    ...(options.retry_policy === null ? {} : { retryPolicy: retryOf(options.retry_policy) }),
  };

  // SAFETY: the fields come from a vector at run time, so their static types are wide; `defineTask` keeps them as
  // they are, and the core's check below judges them.
  const contract = defineTask(fields as never);
  const checked = checkTaskOptions(taskOptionsFacts(contractRecord(contract), DEFAULT_QUEUE_ROUTE));

  if (checked.kind !== "accepted") {
    assert.fail(`${vector.name}: the core refuses the options: ${JSON.stringify(checked.refusals)}`);
  }

  return checked.options;
}

/** The send rules of a config with only a database URL. */
function sendRules(): NativeSendRules {
  const config = parseAppConfig({ database: { databaseUrl: { kind: "value", value: UNUSED_DATABASE_URL } } }, () => undefined);

  if (!config.ok) {
    assert.fail(`the minimal config is refused: ${JSON.stringify(config.error)}`);
  }

  const checked = checkAppConfig(config.value.coreConfigJson);

  if (checked.kind !== "accepted") {
    assert.fail(`the core refuses the minimal config: ${JSON.stringify(checked.refusals)}`);
  }

  return checked.rules;
}

/** The options text of a send or a node of a vector with a deadline. */
function deadlineText(vector: OptionsVector, deadline: string, options: NativeTaskOptions): string {
  const prepared = prepareSendFacts(
    sendRules(),
    {
      retention: { kind: "queueOrDefault" },
      deadline: { kind: "until", epochMicros: epochMicrosTowardPast(Temporal.Instant.from(deadline)) },
      inputBytes: 2,
    },
    options,
  );

  if (prepared.kind !== "prepared") {
    assert.fail(`${vector.name}: the send is refused: ${prepared.error.class}`);
  }

  return prepared.taskOptions;
}

describe("the stored task options text through the contract mapping and the addon", () => {
  test("each vector gives Python's text", () => {
    const all = vectors();

    assert.equal(all.length, 12);

    for (const vector of all) {
      const options = checkedOptions(vector);
      const text = vector.deadline === null ? storedOptionsText(options) : deadlineText(vector, vector.deadline, options);

      assert.equal(text, vector.text, vector.name);
    }
  });

  test("each kind of vector is covered: a task, a send and a node", () => {
    assert.deepEqual([...new Set(vectors().map((vector) => vector.kind))].toSorted(), ["node", "send", "task"]);
  });

  test("the checks and the prepare step start no core", () => {
    for (const vector of vectors()) {
      const options = checkedOptions(vector);

      if (vector.deadline !== null) {
        deadlineText(vector, vector.deadline, options);
      }
    }

    assert.equal(coreState(), "not_started");
  });
});
