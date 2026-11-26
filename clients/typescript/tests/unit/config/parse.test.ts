// The outcomes of `parseAppConfig`: an accepted config, each refusal kind, and several refusals in field order.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { Refusal } from "../../../horsies/src/app/refusal.ts";
import { type AppConfigInput, parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import type { EnvironmentRead } from "../../../horsies/src/config/secret.ts";

const DATABASE_URL = "postgresql://horsies:secret@127.0.0.1:55432/horsies";

const NO_ENVIRONMENT: EnvironmentRead = () => undefined;

/** A config with only a database URL value and the given fields. */
function configWith(fields: Partial<AppConfigInput>): AppConfigInput {
  return { broker: { databaseUrl: { kind: "value", value: DATABASE_URL } }, ...fields };
}

/** The refusals of a config that the parse must refuse. */
function refusalsOf(input: AppConfigInput, environment: EnvironmentRead = NO_ENVIRONMENT): readonly Refusal[] {
  const parsed = parseAppConfig(input, environment);

  assert.equal(parsed.ok, false, "the config must be refused");

  return parsed.ok ? [] : parsed.error;
}

/** The core's config object of a config that the parse must accept. */
function coreObjectOf(input: AppConfigInput, environment: EnvironmentRead = NO_ENVIRONMENT): object {
  const parsed = parseAppConfig(input, environment);

  assert.equal(parsed.ok, true, parsed.ok ? "" : JSON.stringify(parsed.error));

  return parsed.ok ? JSON.parse(parsed.value.coreConfigJson) : {};
}

describe("parseAppConfig", () => {
  test("a config with a URL value is accepted, and the text holds only the given fields", () => {
    assert.deepEqual(coreObjectOf(configWith({})), {
      broker: { database_url: DATABASE_URL },
      resend_on_transient_err: false,
    });
  });

  test("a URL from the environment is read at the parse", () => {
    const input: AppConfigInput = { broker: { databaseUrl: { kind: "fromEnv", name: "APP_DATABASE_URL" } } };
    const environment: EnvironmentRead = (name) => (name === "APP_DATABASE_URL" ? DATABASE_URL : undefined);

    assert.deepEqual(coreObjectOf(input, environment), {
      broker: { database_url: DATABASE_URL },
      resend_on_transient_err: false,
    });
  });

  test("an environment variable that is not set is refused with HRS-203, and the message names the variable", () => {
    const refusals = refusalsOf({ broker: { databaseUrl: { kind: "fromEnv", name: "APP_DATABASE_URL" } } });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-203", "broker.databaseUrl"]],
    );
    assert.match(refusals[0]?.message ?? "", /APP_DATABASE_URL is not set/);
  });

  test("a number that is not a whole number in the range of its field is refused with the code of its section", () => {
    for (const poolSize of [1.5, -1, 2 ** 32, Number.NaN]) {
      const refusals = refusalsOf(configWith({ broker: { databaseUrl: { kind: "value", value: DATABASE_URL }, poolSize } }));

      assert.deepEqual(
        refusals.map((one) => [one.code, one.path]),
        [["HRS-213", "broker.poolSize"]],
      );
      assert.match(refusals[0]?.message ?? "", /from 0 to 4294967295/);
    }
  });

  test("a number outside the range of its field has the catalog code of its config section", () => {
    const cases: readonly (readonly [Partial<AppConfigInput>, string, string])[] = [
      [{ customQueues: [{ name: "urgent", priority: -1 }], queueMode: "custom" }, "HRS-200", "customQueues[0].priority"],
      [{ clusterWideCap: { kind: "cap", tasks: -1 } }, "HRS-201", "clusterWideCap.tasks"],
      [{ claim: { kind: "hardCap", leaseMs: -1 } }, "HRS-202", "claim.leaseMs"],
      [{ maxClaimRenewAgeMs: -1 }, "HRS-202", "maxClaimRenewAgeMs"],
      [{ recovery: { checkIntervalMs: -1 } }, "HRS-204", "recovery.checkIntervalMs"],
      [{ resilience: { dbRetryMaxMs: -1 } }, "HRS-208", "resilience.dbRetryMaxMs"],
      [
        { broker: { databaseUrl: { kind: "value", value: DATABASE_URL }, poolRecycle: { kind: "afterSeconds", seconds: -1 } } },
        "HRS-213",
        "broker.poolRecycle.seconds",
      ],
      [{ retention: { retentionPeriods: [{ days: -1 }] } }, "HRS-216", "retention.retentionPeriods[0].days"],
      [{ payload: { warnBytes: { kind: "bytes", bytes: -1 } } }, "HRS-227", "payload.warnBytes.bytes"],
    ];

    for (const [fields, code, path] of cases) {
      assert.deepEqual(
        refusalsOf(configWith(fields)).map((one) => [one.code, one.path]),
        [[code, path]],
      );
    }
  });

  test("a text with a lone surrogate is refused with the code of its section, and the message holds no part of it", () => {
    const refusals = refusalsOf({
      queueMode: "custom",
      customQueues: [{ name: "urgent\uD800", priority: 1 }],
      broker: { databaseUrl: { kind: "value", value: "postgresql://\uD800" } },
      retention: { queueRetention: { ["bulk\uD800"]: { kind: "forever" } } },
    });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-200", "customQueues[0].name"],
        ["HRS-203", "broker.databaseUrl"],
        ["HRS-216", 'retention.queueRetention["bulk\\ud800"]'],
      ],
    );
    assert.equal(refusals[1]?.message.includes("postgresql"), false);
  });

  test("a session URL with a lone surrogate is refused with HRS-203 at the path of its connection mode", () => {
    const refusals = refusalsOf(
      configWith({
        broker: {
          databaseUrl: { kind: "value", value: DATABASE_URL },
          connection: { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "value", value: "postgresql://\uD800" } },
        },
      }),
    );

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-203", "broker.connection.sessionDatabaseUrl"]],
    );
  });

  test("a soft cap prefetch buffer of 0 is refused with HRS-202: the core reads 0 as the hard cap mode", () => {
    const refusals = refusalsOf(configWith({ claim: { kind: "softCap", prefetchBuffer: 0, leaseMs: 30_000 } }));

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-202", "claim.prefetchBuffer"]],
    );
    assert.match(refusals[0]?.message ?? "", /from 1 to 4294967295/);
  });

  test("a keepalive outside the bounds of the core's socket settings is refused with HRS-215", () => {
    const refusals = refusalsOf(
      configWith({
        broker: {
          databaseUrl: { kind: "value", value: DATABASE_URL },
          tcpKeepalives: { kind: "on", idleSeconds: 0, intervalSeconds: 10, probes: 128 },
        },
      }),
    );

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-215", "broker.tcpKeepalives.idleSeconds"],
        ["HRS-215", "broker.tcpKeepalives.probes"],
      ],
    );
  });

  test("a worker profile count outside the bounds of its type is refused with HRS-218", () => {
    const refusals = refusalsOf(configWith({ workerProfiles: { io: { sharedAsync: { count: 0, seats: 1001 } } } }));

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-218", 'workerProfiles["io"].sharedAsync.count'],
        ["HRS-218", 'workerProfiles["io"].sharedAsync.seats'],
      ],
    );
  });

  test("a paused age that is not the core's paused age is refused with HRS-216", () => {
    const ages = [
      Temporal.Duration.from({ hours: 1, nanoseconds: 1 }),
      Temporal.Duration.from({ seconds: 0 }),
      Temporal.Duration.from({ months: 1 }),
      Temporal.Duration.from({ days: 365_001 }),
    ];

    for (const age of ages) {
      const refusals = refusalsOf(configWith({ retention: { pausedWorkflowAutoCancelAfter: { kind: "after", age } } }));

      assert.deepEqual(
        refusals.map((one) => [one.code, one.path]),
        [["HRS-216", "retention.pausedWorkflowAutoCancelAfter.age"]],
        age.toString(),
      );
    }
  });

  test("a retry attempt count of 0 is refused with HRS-208: the core reads 0 as no limit", () => {
    const refusals = refusalsOf(configWith({ resilience: { dbRetryMaxAttempts: { kind: "attempts", attempts: 0 } } }));

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-208", "resilience.dbRetryMaxAttempts.attempts"]],
    );
  });

  test("several refusals come in field order", () => {
    const refusals = refusalsOf({
      queueMode: "custom",
      customQueues: [{ name: "urgent", priority: 1.5 }],
      broker: { databaseUrl: { kind: "fromEnv", name: "UNSET_URL" }, maxOverflow: -1 },
      claim: { kind: "softCap", prefetchBuffer: 0.5, leaseMs: 30_000 },
      workerProfiles: { cpu: { drainTimeoutMs: 0 } },
    });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-200", "customQueues[0].priority"],
        ["HRS-203", "broker.databaseUrl"],
        ["HRS-213", "broker.maxOverflow"],
        ["HRS-202", "claim.prefetchBuffer"],
        ["HRS-218", 'workerProfiles["cpu"].drainTimeoutMs'],
      ],
    );
  });

  test("a parsed config is frozen, and neither its keys nor its JSON hold the database URL", () => {
    const parsed = parseAppConfig(configWith({}), NO_ENVIRONMENT);

    assert.equal(parsed.ok, true);

    if (!parsed.ok) {
      return;
    }

    assert.equal(Object.isFrozen(parsed.value), true);
    assert.deepEqual(Object.keys(parsed.value), ["queues", "resendOnTransientErr"]);
    assert.equal(JSON.stringify(parsed.value).includes("secret"), false);
    assert.match(parsed.value.coreConfigJson, /secret/);
  });
});
