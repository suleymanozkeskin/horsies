// The checks of `app.schedules`: each check of a schedule, of the section and of the whole declaration lands in the
// schedules refusal list with its code and its place; the first failing check of a schedule wins; a disabled
// schedule needs no listed task; the worker start skips the list and `check` reads it; a worker thread runs no core
// check.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { appInternals } from "../../../horsies/src/app/internals.ts";
import { areaScopes, staticCheck } from "../../../horsies/src/check/static-check.ts";
import { scheduleDeclaration } from "../../../horsies/src/schedules/app-member.ts";
import { zoneReading } from "../../../horsies/src/schedules/zone.ts";
import { fakeChecks } from "../app/support.ts";
import {
  customModeApp,
  declare,
  defaultModeApp,
  FAKE_SCHEDULER,
  fakeCore,
  HOURLY,
  ping,
  report,
  scheduleRefusals,
  sync,
  unlisted,
  upstreamOnly,
} from "./support.ts";

describe("app.schedules: each check lands in the schedules refusal list", () => {
  test("a contract that is not in the tasks list", () => {
    const context = declare(defaultModeApp(), [{ name: "ghost", task: unlisted, input: {}, pattern: HOURLY }], {
      kind: "core",
      calls: fakeCore(),
    });

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-300", "schedules[0] ghost", "schedules[0] ghost: the task unlisted is not in the tasks list of this app"],
    ]);
  });

  test("a queue in the default queue mode", () => {
    const context = declare(
      defaultModeApp(),
      [{ name: "nightly", task: ping, input: {}, pattern: HOURLY, queue: "default" }],
      { kind: "core", calls: fakeCore() },
    );

    assert.deepEqual(scheduleRefusals(context), [
      [
        "HRS-205",
        "schedules[0] nightly",
        'schedules[0] nightly: queue: the schedule names the queue "default", and the default queue mode takes no queue',
      ],
    ]);
  });

  test("a queue that is not a custom queue", () => {
    const context = declare(
      customModeApp(),
      [{ name: "nightly", task: sync, input: { region: "eu" }, pattern: HOURLY, queue: "bulk" }],
      { kind: "core", calls: fakeCore() },
    );

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules[0] nightly", 'schedules[0] nightly: queue: "bulk" is not a custom queue'],
    ]);
  });

  test("an input that does not fit the task's input types", () => {
    // A JavaScript caller can pass an input that the types refuse.
    const input: { readonly format: string } = Object({ format: 3 });

    const context = declare(defaultModeApp(), [{ name: "nightly", task: report, input, pattern: HOURLY }], {
      kind: "core",
      calls: fakeCore(),
    });

    const [refused] = scheduleRefusals(context);

    assert.equal(refused?.[0], "HRS-205");
    assert.match(refused?.[2] ?? "", /^schedules\[0\] nightly: input does not fit the task: \$\.format: /);
  });

  test("a task that only a workflow node can run", () => {
    const context = declare(
      defaultModeApp(),
      [{ name: "nightly", task: upstreamOnly, input: Object({}), pattern: HOURLY }],
      { kind: "core", calls: fakeCore() },
    );

    assert.deepEqual(scheduleRefusals(context), [
      [
        "HRS-205",
        "schedules[0] nightly",
        "schedules[0] nightly: the task takes an upstream result (upstream); only a workflow node can run it",
      ],
    ]);
  });

  test("an input over the payload reject bound; an input at the bound passes", () => {
    const input = { format: "x".repeat(10) };
    const atBound = Buffer.byteLength('{"format":"xxxxxxxxxx"}', "utf8");

    const over = declare(defaultModeApp(), [{ name: "big", task: report, input, pattern: HOURLY }], {
      kind: "core",
      calls: fakeCore({ rejectBound: { kind: "bytes", bytes: BigInt(atBound - 1) } }),
    });

    assert.deepEqual(scheduleRefusals(over), [
      [
        "HRS-205",
        "schedules[0] big",
        `schedules[0] big: input is ${String(atBound)} bytes, over payload.rejectBytes=${String(atBound - 1)}; every slot sends the same input, so every send is refused`,
      ],
    ]);

    const at = declare(defaultModeApp(), [{ name: "big", task: report, input, pattern: HOURLY }], {
      kind: "core",
      calls: fakeCore({ rejectBound: { kind: "bytes", bytes: BigInt(atBound) } }),
    });

    assert.deepEqual(scheduleRefusals(at), []);
  });

  test("a time zone that the core's zone data does not hold, worded from the runtime's reading", () => {
    const core = fakeCore();

    const context = declare(
      defaultModeApp(),
      [
        { name: "a", task: ping, input: {}, pattern: HOURLY, timezone: "Nope/Zone" },
        { name: "b", task: ping, input: {}, pattern: HOURLY, timezone: "+05:00" },
        { name: "c", task: ping, input: {}, pattern: HOURLY, timezone: "europe/berlin" },
        { name: "d", task: ping, input: {}, pattern: HOURLY, timezone: "2020-01-01T00:00+01:00" },
        { name: "e", task: ping, input: {}, pattern: HOURLY, timezone: "America/Sao_Paulo" },
      ],
      { kind: "core", calls: core },
    );

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules[0] a", 'schedules[0] a: timezone: "Nope/Zone" is not an IANA zone name of the core\'s zone data'],
      ["HRS-205", "schedules[1] b", 'schedules[1] b: timezone: "+05:00" is a UTC offset, not a zone name'],
      ["HRS-205", "schedules[2] c", 'schedules[2] c: timezone: the zone data writes "europe/berlin" as "Europe/Berlin"'],
      ["HRS-205", "schedules[3] d", 'schedules[3] d: timezone: "2020-01-01T00:00+01:00" is a UTC offset, not a zone name'],
      [
        "HRS-205",
        "schedules[4] e",
        'schedules[4] e: timezone: "America/Sao_Paulo" is not an IANA zone name of the core\'s zone data',
      ],
    ]);
    assert.ok(core.zones.includes("Europe/Berlin"), "the hint's spelling is checked with the core's zone data");
  });

  test("a hint whose spelling the core does not hold is no hint", () => {
    const context = declare(
      defaultModeApp(),
      [{ name: "a", task: ping, input: {}, pattern: HOURLY, timezone: "utc" }],
      { kind: "core", calls: fakeCore({ zones: [] }) },
    );

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules[0] a", 'schedules[0] a: timezone: "utc" is not an IANA zone name of the core\'s zone data'],
    ]);
  });

  test("the core's zone data decides: the four zones of the North American rules pass on every runtime", () => {
    const names = ["EST5EDT", "CST6CDT", "MST7MDT", "PST8PDT"];
    const core = fakeCore();

    const context = declare(
      defaultModeApp(),
      names.map((name) => ({ name, task: ping, input: {}, pattern: HOURLY, timezone: name })),
      { kind: "core", calls: core },
    );

    assert.deepEqual(scheduleRefusals(context), []);
    assert.deepEqual(core.zones, names);
  });

  test("a pattern that the stored form cannot hold", () => {
    const context = declare(
      defaultModeApp(),
      [{ name: "a", task: ping, input: {}, pattern: { kind: "hourly", minute: 1.5 } }],
      { kind: "core", calls: fakeCore() },
    );

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules[0] a", "schedules[0] a: pattern.minute: 1.5 is not a whole number from -9007199254740991 to 9007199254740991"],
    ]);
  });

  test("a pattern that breaks a rule of the core", () => {
    const core = fakeCore({ pattern: { kind: "refused", cause: "the pattern breaks its rules: minute 60" } });

    const context = declare(defaultModeApp(), [{ name: "a", task: ping, input: {}, pattern: { kind: "hourly", minute: 60 } }], {
      kind: "core",
      calls: core,
    });

    assert.deepEqual(core.patterns, ['{"type":"hourly","minute":60,"second":0}']);
    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules[0] a", "schedules[0] a: pattern: the pattern breaks its rules: minute 60"],
    ]);
  });

  test("a catch-up count and a check interval that are not whole numbers inside their bounds", () => {
    const context = declare(
      defaultModeApp(),
      [
        { name: "a", task: ping, input: {}, pattern: HOURLY, catchUpMissed: true, maxCatchUpRuns: 2.5 },
        { name: "b", task: ping, input: {}, pattern: HOURLY, maxCatchUpRuns: 0 },
        { name: "c", task: ping, input: {}, pattern: HOURLY, maxCatchUpRuns: 10_001, enabled: false },
        { name: "d", task: ping, input: {}, pattern: HOURLY, maxCatchUpRuns: 10_000 },
      ],
      { kind: "core", calls: fakeCore() },
      { checkIntervalSeconds: 0.5 },
    );

    assert.deepEqual(scheduleRefusals(context), [
      ["HRS-205", "schedules section", "schedules section: checkIntervalSeconds: 0.5 is not a whole number from 1 to 60"],
      ["HRS-205", "schedules[0] a", "schedules[0] a: maxCatchUpRuns: 2.5 is not a whole number from 1 to 10000"],
      ["HRS-205", "schedules[1] b", "schedules[1] b: maxCatchUpRuns: 0 is not a whole number from 1 to 10000"],
      ["HRS-205", "schedules[2] c", "schedules[2] c: maxCatchUpRuns: 10001 is not a whole number from 1 to 10000"],
    ]);
  });

  test("a check interval above its bound; the bounds pass", () => {
    const over = declare(defaultModeApp(), [], { kind: "core", calls: fakeCore() }, { checkIntervalSeconds: 61 });

    assert.deepEqual(scheduleRefusals(over), [
      ["HRS-205", "schedules section", "schedules section: checkIntervalSeconds: 61 is not a whole number from 1 to 60"],
    ]);

    for (const checkIntervalSeconds of [1, 60]) {
      assert.deepEqual(scheduleRefusals(declare(defaultModeApp(), [], { kind: "core", calls: fakeCore() }, { checkIntervalSeconds })), []);
    }
  });

  test("a repeated name, at the schedule that repeats it, in a worker thread too", () => {
    const entries = [
      { name: "a", task: ping, input: {}, pattern: HOURLY },
      { name: "b", task: ping, input: {}, pattern: HOURLY, enabled: false },
      { name: "a", task: ping, input: {}, pattern: HOURLY, timezone: "Nope/Zone" },
      { name: "b", task: ping, input: {}, pattern: HOURLY },
    ] as const;

    const expected = [
      ["HRS-205", "schedules[2] a", 'schedules[2] a: name: schedules[0] has the name "a"'],
      ["HRS-205", "schedules[3] b", 'schedules[3] b: name: schedules[1] has the name "b"'],
    ];

    assert.deepEqual(scheduleRefusals(declare(defaultModeApp(), entries, { kind: "core", calls: fakeCore() })), expected);
    assert.deepEqual(scheduleRefusals(declare(defaultModeApp(), entries, { kind: "noCore" })), expected);
  });

  test("a refusal of the core's manifest parser, the backstop of the checks above", () => {
    const core = fakeCore({
      preparation: { kind: "manifestRefused", fieldPath: "schedules[1].activation", cause: "a rule of the parser" },
    });

    const context = declare(
      defaultModeApp(),
      [
        { name: "a", task: ping, input: {}, pattern: HOURLY },
        { name: "b", task: ping, input: {}, pattern: HOURLY },
      ],
      { kind: "core", calls: core },
    );

    assert.deepEqual(scheduleRefusals(context), [
      [
        "HRS-205",
        "schedules",
        "schedules: the core's scheduler manifest parser refuses schedules[1].activation: a rule of the parser",
      ],
    ]);
    assert.deepEqual(scheduleDeclaration(context), {
      kind: "declared",
      section: { enabled: true, checkIntervalSeconds: 1 },
      scheduler: { kind: "refused" },
    });
  });

  test("a second app.schedules call; the first declaration stays", () => {
    const app = defaultModeApp();
    const first = declare(app, [{ name: "a", task: ping, input: {}, pattern: HOURLY }], { kind: "core", calls: fakeCore() });
    const kept = scheduleDeclaration(first);

    declare(app, [{ name: "b", task: ping, input: {}, pattern: HOURLY }], { kind: "core", calls: fakeCore() });

    assert.deepEqual(scheduleRefusals(first), [
      ["HRS-205", "app.schedules", "app.schedules: app.schedules is called a second time; the schedules of the first call stay"],
    ]);
    assert.equal(scheduleDeclaration(first), kept);
  });
});

describe("app.schedules: the order, the disabled schedules and the threads", () => {
  test("the first failing check of a schedule wins, in Python's order; every schedule is checked", () => {
    const context = declare(
      defaultModeApp(),
      [
        { name: "a", task: unlisted, input: {}, pattern: HOURLY, timezone: "Nope/Zone" },
        { name: "b", task: unlisted, input: {}, pattern: HOURLY },
        { name: "c", task: ping, input: {}, pattern: { kind: "monthly", day: 0.5, at: Temporal.PlainTime.from("08:00") }, timezone: "Nope/Zone" },
      ],
      { kind: "core", calls: fakeCore() },
    );

    assert.deepEqual(
      scheduleRefusals(context).map(([code, path, message]) => [code, path, message.replace(/^[^:]+: /, "").split(":")[0]]),
      [
        ["HRS-205", "schedules[0] a", "timezone"],
        ["HRS-300", "schedules[1] b", "the task unlisted is not in the tasks list of this app"],
        ["HRS-205", "schedules[2] c", "pattern.day"],
      ],
    );
  });

  test("a disabled schedule needs no listed task, queue or input; its pattern is checked", () => {
    const core = fakeCore();

    const context = declare(
      defaultModeApp(),
      [{ name: "paused", task: unlisted, input: Object({ any: 1 }), pattern: HOURLY, queue: "none", timezone: "Nope/Zone", enabled: false }],
      { kind: "core", calls: core },
    );

    assert.deepEqual(scheduleRefusals(context), []);
    assert.deepEqual(core.patterns, ['{"type":"hourly","minute":0,"second":0}']);
    assert.deepEqual(scheduleDeclaration(context), {
      kind: "declared",
      section: { enabled: true, checkIntervalSeconds: 1 },
      scheduler: { kind: "prepared", scheduler: FAKE_SCHEDULER },
    });
  });

  test("a passing declaration asks the facts of every admitted queue and prepares the scheduler", () => {
    const core = fakeCore();

    const context = declare(customModeApp(), [{ name: "nightly", task: sync, input: { region: "eu" }, pattern: HOURLY }], {
      kind: "core",
      calls: core,
    });

    assert.deepEqual(scheduleRefusals(context), []);
    assert.deepEqual(core.queueNames, [["standard", "urgent"]]);
    assert.equal(core.manifests.length, 1);
    assert.equal(scheduleDeclaration(context).kind, "declared");
  });

  test("in a worker thread no core check runs: no pattern rule, no zone, no payload bound, no scheduler", () => {
    const context = declare(
      defaultModeApp(),
      [
        {
          name: "nightly",
          task: report,
          input: { format: "x".repeat(10_000) },
          pattern: { kind: "hourly", minute: 60 },
          timezone: "Nope/Zone",
        },
      ],
      { kind: "noCore" },
    );

    assert.deepEqual(scheduleRefusals(context), []);
    assert.deepEqual(scheduleDeclaration(context), {
      kind: "declared",
      section: { enabled: true, checkIntervalSeconds: 1 },
      scheduler: { kind: "notPrepared" },
    });
  });

  test("an app with no app.schedules call has no declaration; an empty list is a declaration", () => {
    assert.deepEqual(scheduleDeclaration(appInternals(defaultModeApp())), { kind: "notDeclared" });
    assert.equal(scheduleDeclaration(declare(defaultModeApp(), [], { kind: "noCore" })).kind, "declared");
  });
});

describe("the schedules refusal list and the static check", () => {
  test("the refusals of the schedules area count only with the schedules", () => {
    assert.deepEqual(areaScopes("schedules"), ["withSchedules"]);
  });

  test("check reads the list; the worker start skips it", () => {
    const app = defaultModeApp();

    app.implement(report, (_input, { ok }) => ok(null));
    app.implement(ping, (_input, { ok }) => ok(null));
    app.implement(upstreamOnly, (_input, { ok }) => ok(null));

    const context = declare(app, [{ name: "ghost", task: unlisted, input: {}, pattern: HOURLY }], { kind: "core", calls: fakeCore() });
    const checked = staticCheck(context, fakeChecks(), "withSchedules", "check");

    assert.equal(checked.kind, "refused");
    assert.deepEqual(
      checked.kind === "refused" ? checked.report.refusals.map((refused) => refused.path) : [],
      ["schedules[0] ghost"],
    );
    assert.equal(staticCheck(context, fakeChecks(), "withoutSchedules", "worker-startup").kind, "passed");
  });
});

describe("zoneReading", () => {
  test("an IANA zone name, a link name, an offset, another case and an unknown name", () => {
    assert.deepEqual(zoneReading("UTC"), { kind: "zoneName" });
    assert.deepEqual(zoneReading("Europe/Berlin"), { kind: "zoneName" });
    assert.deepEqual(zoneReading("Asia/Calcutta"), { kind: "zoneName" });
    assert.deepEqual(zoneReading("-03:00"), { kind: "offset" });
    assert.deepEqual(zoneReading("utc"), { kind: "otherSpelling", spelling: "UTC" });
    assert.deepEqual(zoneReading("Nope/Zone"), { kind: "unknown" });
  });

  test("an offset inside a date-time text reads as an offset", () => {
    assert.deepEqual(zoneReading("2020-01-01T00:00+01:00"), { kind: "offset" });
    assert.deepEqual(zoneReading("2020-01-01T00:00-05:00"), { kind: "offset" });
  });
});
