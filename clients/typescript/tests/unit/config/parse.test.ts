// The outcomes of `parseAppConfig`: an accepted config, each refusal kind, and several refusals in field order.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { HorsiesError } from "../../../horsies/src/app/refusal.ts";
import { type AppConfigInput, parseAppConfig } from "../../../horsies/src/config/app-config.ts";
import type { EnvironmentRead } from "../../../horsies/src/config/secret.ts";
import type { LogFormat, SchemaMode } from "../../../horsies/src/config/process-settings.ts";
import type { WorkerProfileInput } from "../../../horsies/src/config/worker-profiles.ts";

const DATABASE_URL = "postgresql://horsies:secret@127.0.0.1:55432/horsies";

const NO_ENVIRONMENT: EnvironmentRead = () => undefined;

/** A config with only a database URL value and the given fields. */
function configWith(fields: Partial<AppConfigInput>): AppConfigInput {
  return { database: { databaseUrl: { kind: "value", value: DATABASE_URL } }, ...fields };
}

/** The refusals of a config that the parse must refuse. */
function refusalsOf(input: AppConfigInput, environment: EnvironmentRead = NO_ENVIRONMENT): readonly HorsiesError[] {
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
      database: { database_url: DATABASE_URL },
      resend_on_transient_error: false,
    });
  });

  test("a URL from the environment is read at the parse", () => {
    const input: AppConfigInput = { database: { databaseUrl: { kind: "fromEnv", name: "APP_DATABASE_URL" } } };
    const environment: EnvironmentRead = (name) => (name === "APP_DATABASE_URL" ? DATABASE_URL : undefined);

    assert.deepEqual(coreObjectOf(input, environment), {
      database: { database_url: DATABASE_URL },
      resend_on_transient_error: false,
    });
  });

  test("an environment variable that is not set is refused with HRS-203, and the message names the variable", () => {
    const refusals = refusalsOf({ database: { databaseUrl: { kind: "fromEnv", name: "APP_DATABASE_URL" } } });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-203", "database.databaseUrl"]],
    );
    assert.match(refusals[0]?.message ?? "", /APP_DATABASE_URL is not set/);
  });

  test("a number that is not a whole number in the range of its field is refused with the code of its section", () => {
    for (const poolSize of [1.5, -1, 2 ** 32, Number.NaN]) {
      const refusals = refusalsOf(configWith({ database: { databaseUrl: { kind: "value", value: DATABASE_URL }, poolSize } }));

      assert.deepEqual(
        refusals.map((one) => [one.code, one.path]),
        [["HRS-213", "database.poolSize"]],
      );
      assert.match(refusals[0]?.message ?? "", /from 0 to 4294967295/);
    }
  });

  test("a number outside the range of its field has the catalog code of its config section", () => {
    const cases: readonly (readonly [Partial<AppConfigInput>, string, string])[] = [
      [{ customQueues: [{ name: "urgent", priority: -1 }], queueMode: "custom" }, "HRS-200", "customQueues[0].priority"],
      [{ clusterWideCap: { kind: "clusterCap", tasks: -1 } }, "HRS-201", "clusterWideCap.tasks"],
      [{ claim: { kind: "hardCap", leaseMs: -1 } }, "HRS-202", "claim.leaseMs"],
      [{ maxClaimRenewAgeMs: -1 }, "HRS-202", "maxClaimRenewAgeMs"],
      [{ recovery: { checkIntervalMs: -1 } }, "HRS-204", "recovery.checkIntervalMs"],
      [{ resilience: { databaseRetryMaxMs: -1 } }, "HRS-208", "resilience.databaseRetryMaxMs"],
      [
        { database: { databaseUrl: { kind: "value", value: DATABASE_URL }, poolRecycle: { kind: "after", seconds: -1 } } },
        "HRS-213",
        "database.poolRecycle.seconds",
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
      database: { databaseUrl: { kind: "value", value: "postgresql://\uD800" } },
      retention: { queueRetention: { ["bulk\uD800"]: { kind: "forever" } } },
    });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-200", "customQueues[0].name"],
        ["HRS-203", "database.databaseUrl"],
        ["HRS-216", 'retention.queueRetention["bulk\\ud800"]'],
      ],
    );
    assert.equal(refusals[1]?.message.includes("postgresql"), false);
  });

  test("a session URL with a lone surrogate is refused with HRS-203 at the path of its connection mode", () => {
    const refusals = refusalsOf(
      configWith({
        database: {
          databaseUrl: { kind: "value", value: DATABASE_URL },
          connection: { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "value", value: "postgresql://\uD800" } },
        },
      }),
    );

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-203", "database.connection.sessionDatabaseUrl"]],
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
        database: {
          databaseUrl: { kind: "value", value: DATABASE_URL },
          tcpKeepalives: { kind: "on", idleSeconds: 0, intervalSeconds: 10, probes: 128 },
        },
      }),
    );

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-215", "database.tcpKeepalives.idleSeconds"],
        ["HRS-215", "database.tcpKeepalives.probes"],
      ],
    );
  });

  test("a worker profile count outside the bounds of its type is refused with HRS-218", () => {
    const refusals = refusalsOf(configWith({ workerProfiles: { io: { sharedAsync: { kind: "sharedAsyncChildren", count: 0, seats: 1001 } } } }));

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-218", 'workerProfiles["io"].sharedAsync.count'],
        ["HRS-218", 'workerProfiles["io"].sharedAsync.seats'],
      ],
    );
  });

  // One case per bounded profile field: a value outside the bound of the field's generated constants, and the path
  // that the refusal names.
  const outOfBoundProfiles: readonly (readonly [path: string, profile: WorkerProfileInput])[] = [
    ["claim.perQueuePerPass.tasks", { claim: { perQueuePerPass: { kind: "taskLimit", tasks: 0 } } }],
    ["claim.claimedPerWorker.tasks", { claim: { claimedPerWorker: { kind: "taskLimit", tasks: 2_147_483_648 } } }],
    ["pool.maxSize.connections", { pool: { maxSize: { kind: "connections", connections: 0 } } }],
    ["drainTimeoutMs", { drainTimeoutMs: 86_400_001 }],
    ["isolated.pool.minSize", { isolated: { kind: "isolatedChildren", count: 1, pool: { minSize: 1001 } } }],
    [
      "isolated.pool.maxSize.connections",
      { isolated: { kind: "isolatedChildren", count: 1, pool: { maxSize: { kind: "connections", connections: 1001 } } } },
    ],
    [
      "isolated.taskSendPool.connections",
      { isolated: { kind: "isolatedChildren", count: 1, taskSendPool: { kind: "connections", connections: 0 } } },
    ],
    ["sharedAsync.pool.minSize", { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 1, pool: { minSize: -1 } } }],
    [
      "sharedAsync.pool.maxSize.connections",
      { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 1, pool: { maxSize: { kind: "connections", connections: 0 } } } },
    ],
    [
      "sharedAsync.executor.threads",
      { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 1, executor: { kind: "threads", threads: 257 } } },
    ],
    [
      "sharedAsync.taskSendPool.connections",
      { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 1, taskSendPool: { kind: "connections", connections: 1001 } } },
    ],
  ];

  for (const [path, profile] of outOfBoundProfiles) {
    test(`a worker profile ${path} outside its bounds is refused with HRS-218 at its path`, () => {
      const refusals = refusalsOf(configWith({ workerProfiles: { io: profile } }));

      assert.deepEqual(
        refusals.map((one) => [one.code, one.path]),
        [["HRS-218", `workerProfiles["io"].${path}`]],
      );
    });
  }

  test("runtime threads of 0 and 1 to 256: 0 is refused, the bounds are accepted", () => {
    const withThreads = (threads: number): AppConfigInput =>
      configWith({ workerProfiles: { io: { sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 1, executor: { kind: "threads", threads } } } } });

    assert.deepEqual(
      refusalsOf(withThreads(0)).map((one) => [one.code, one.path]),
      [["HRS-218", 'workerProfiles["io"].sharedAsync.executor.threads']],
    );
    assert.equal(parseAppConfig(withThreads(1), NO_ENVIRONMENT).ok, true);
    assert.equal(parseAppConfig(withThreads(256), NO_ENVIRONMENT).ok, true);
  });

  test("a paused age that is not the core's paused age is refused with HRS-216", () => {
    const ages = [
      Temporal.Duration.from({ hours: 1, nanoseconds: 1 }),
      Temporal.Duration.from({ seconds: 0 }),
      Temporal.Duration.from({ months: 1 }),
      Temporal.Duration.from({ days: 365_001 }),
    ];

    for (const age of ages) {
      const refusals = refusalsOf(configWith({ retention: { pausedWorkflowCancelAfter: { kind: "after", age } } }));

      assert.deepEqual(
        refusals.map((one) => [one.code, one.path]),
        [["HRS-216", "retention.pausedWorkflowCancelAfter.age"]],
        age.toString(),
      );
    }
  });

  test("a retry attempt count of 0 is refused with HRS-208: the core reads 0 as no limit", () => {
    const refusals = refusalsOf(configWith({ resilience: { databaseRetryMaxAttempts: { kind: "attempts", attempts: 0 } } }));

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [["HRS-208", "resilience.databaseRetryMaxAttempts.attempts"]],
    );
  });

  test("several refusals come in field order", () => {
    const refusals = refusalsOf({
      queueMode: "custom",
      customQueues: [{ name: "urgent", priority: 1.5 }],
      database: { databaseUrl: { kind: "fromEnv", name: "UNSET_URL" }, maxOverflow: -1 },
      claim: { kind: "softCap", prefetchBuffer: 0.5, leaseMs: 30_000 },
      workerProfiles: { cpu: { drainTimeoutMs: 0 } },
    });

    assert.deepEqual(
      refusals.map((one) => [one.code, one.path]),
      [
        ["HRS-200", "customQueues[0].priority"],
        ["HRS-203", "database.databaseUrl"],
        ["HRS-213", "database.maxOverflow"],
        ["HRS-202", "claim.prefetchBuffer"],
        ["HRS-218", 'workerProfiles["cpu"].drainTimeoutMs'],
      ],
    );
  });

  test("the schema mode and the log format have their defaults, and stay out of the core's text", () => {
    const parsed = parseAppConfig(configWith({}), NO_ENVIRONMENT);

    assert.ok(parsed.ok);
    assert.equal(parsed.value.schemaMode, "MIGRATE_AND_VALIDATE");
    assert.deepEqual(parsed.value.logFormat, { kind: "textLogs" });
    assert.deepEqual(Object.keys(coreObjectOf(configWith({ schemaMode: "OBSERVE_ONLY", logFormat: { kind: "jsonLogs" } }))), [
      "database",
      "resend_on_transient_error",
    ]);
  });

  test("each schema mode and each log format is kept", () => {
    for (const schemaMode of ["MIGRATE_AND_VALIDATE", "OBSERVE_ONLY", "ASSUME_INITIALIZED"] as const) {
      const parsed = parseAppConfig(configWith({ schemaMode }), NO_ENVIRONMENT);

      assert.equal(parsed.ok ? parsed.value.schemaMode : "", schemaMode);
    }

    for (const kind of ["textLogs", "jsonLogs"] as const) {
      const parsed = parseAppConfig(configWith({ logFormat: { kind } }), NO_ENVIRONMENT);

      assert.deepEqual(parsed.ok ? parsed.value.logFormat : {}, { kind });
    }
  });

  test("a schema mode or a log format outside its union (a caller with no type check) is a TypeError", () => {
    // A JavaScript caller can pass values that the types refuse.
    const schemaMode: SchemaMode = Object("MIGRATE");
    const logFormat: LogFormat = Object({ kind: "xmlLogs" });

    assert.throws(() => parseAppConfig(configWith({ schemaMode }), NO_ENVIRONMENT), TypeError);
    assert.throws(() => parseAppConfig(configWith({ logFormat }), NO_ENVIRONMENT), TypeError);
  });

  test("a parsed config is frozen, and neither its keys nor its JSON hold the database URL", () => {
    const parsed = parseAppConfig(configWith({}), NO_ENVIRONMENT);

    assert.equal(parsed.ok, true);

    if (!parsed.ok) {
      return;
    }

    assert.equal(Object.isFrozen(parsed.value), true);
    assert.deepEqual(Object.keys(parsed.value), ["queues", "resendOnTransientError", "schemaMode", "logFormat"]);
    assert.equal(JSON.stringify(parsed.value).includes("secret"), false);
    assert.match(parsed.value.coreConfigJson, /secret/);
  });
});
