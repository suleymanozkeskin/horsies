import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import storedFile from "./baseline.json" with { type: "json" };
import {
  type BaselineKey,
  currentMachine,
  findBaseline,
  judge,
  type MachineFacts,
  type MachineIdentity,
  machineFrom,
  parseBaseline,
  projectSha256,
  type StoredBaseline,
  type StoredBaselineFile,
  storedBaselineOf,
  withRecordedBaseline,
  writeBaselineFile,
} from "./baseline.ts";
import { createProjectFolder, durationFromMilliseconds, removeProjectFolder } from "./measure.ts";

const machine: MachineIdentity = { platform: "linux", arch: "x64", cpuModel: "Test CPU", logicalCores: 4, bun: "1.4.4+38afee6c8" };
const otherMachine: MachineIdentity = { ...machine, cpuModel: "Other CPU" };
const key: BaselineKey = { machine, projectSha256: "a".repeat(64), runsPerSession: 9, sessions: 3 };
const sessionMedians = [30, 28, 31].map(durationFromMilliseconds);
const stored: StoredBaseline = storedBaselineOf(key, sessionMedians);

function millisecondsOf(duration: Temporal.Duration): number {
  return duration.total("millisecond");
}

describe("machine identity", () => {
  const facts: MachineFacts = { platform: "linux", arch: "x64", cpuModels: ["Test CPU", "Second CPU"], logicalCores: 4, bun: "1.4.4+38afee6c8" };

  test("machineFrom identified: the first CPU model names the CPU", () => {
    expect(machineFrom(facts)).toEqual({ kind: "identified", machine });
  });

  test("machineFrom cpuModelNotReported: the system reports no CPU", () => {
    expect(machineFrom({ ...facts, cpuModels: [] })).toEqual({ kind: "cpuModelNotReported" });
  });

  test("currentMachine: the platform and the Bun binary of this process", () => {
    expect(currentMachine()).toMatchObject({ kind: "identified", machine: { platform: process.platform, arch: process.arch } });
  });
});

describe("projectSha256", () => {
  const files = new Map([
    ["values.ts", "export {};\n"],
    ["tsconfig.json", "{}\n"],
  ]);

  test("does not depend on the order of the files", () => {
    const reordered = new Map([...files].reverse());

    expect(projectSha256(reordered)).toBe(projectSha256(files));
  });

  test("changes when a file changes", () => {
    const changed = new Map([...files, ["values.ts", "export const changed = 1;\n"]]);

    expect(projectSha256(changed)).not.toBe(projectSha256(files));
  });
});

describe("findBaseline", () => {
  const file: StoredBaselineFile = { entries: [storedBaselineOf({ ...key, machine: otherMachine }, sessionMedians), stored] };

  test("found: the entry of the machine, the project, the run count and the session count", () => {
    expect(findBaseline(file, key)).toEqual({ kind: "found", stored });
  });

  test.each([
    ["another machine", { ...key, machine: { ...machine, bun: "1.4.5+0000000aa" } }],
    ["another project", { ...key, projectSha256: "b".repeat(64) }],
    ["another run count", { ...key, runsPerSession: 11 }],
    ["another session count", { ...key, sessions: 5 }],
  ])("noMatch: %s", (_, otherKey: BaselineKey) => {
    expect(findBaseline(file, otherKey)).toEqual({ kind: "noMatch" });
  });
});

describe("parseBaseline", () => {
  test("parsed: the median and the spread of the session medians", () => {
    const parsed = parseBaseline(stored);

    if (parsed.kind !== "parsed") {
      throw new Error(`expected parsed, got ${parsed.kind}`);
    }

    expect([millisecondsOf(parsed.baseline.median), millisecondsOf(parsed.baseline.noise)]).toEqual([30, 3]);
  });

  test("unreadable: a text that is not an ISO 8601 duration", () => {
    const parsed = parseBaseline({ ...stored, sessionMedians: ["PT0.03S", "thirty", "PT0.03S"] });

    expect(parsed).toMatchObject({ kind: "unreadable", text: "thirty" });
  });

  test("unreadable: an even count of session medians", () => {
    const parsed = parseBaseline({ ...stored, sessionMedians: ["PT0.03S", "PT0.03S"] });

    expect(parsed.kind).toBe("unreadable");
  });

  test("each baseline in baseline.json is readable", () => {
    const committed: StoredBaselineFile = storedFile;

    expect(committed.entries.map((entry) => parseBaseline(entry).kind)).toEqual(committed.entries.map(() => "parsed"));
  });
});

describe("judge", () => {
  const baseline = { median: durationFromMilliseconds(30), noise: durationFromMilliseconds(3) };

  test("withinNoise: a median at the baseline median plus the noise", () => {
    expect(judge(baseline, durationFromMilliseconds(33)).kind).toBe("withinNoise");
  });

  test("slower: a median above the baseline median plus the noise", () => {
    expect(judge(baseline, durationFromMilliseconds(33.001)).kind).toBe("slower");
  });
});

describe("recording", () => {
  test("withRecordedBaseline: replaces the entry of the same machine and keeps the others", () => {
    const other = storedBaselineOf({ ...key, machine: otherMachine }, sessionMedians);
    const newer = storedBaselineOf(key, [20, 21, 22].map(durationFromMilliseconds));

    expect(withRecordedBaseline({ entries: [stored, other] }, newer)).toEqual({ entries: [other, newer] });
  });

  test("writeBaselineFile written: the file holds the entries as JSON", () => {
    const folder = createProjectFolder(tmpdir());

    if (folder.kind === "notCreated") {
      throw new Error(`test setup: no folder: ${folder.cause}`);
    }

    const path = join(folder.path, "baseline.json");

    try {
      expect(writeBaselineFile(path, { entries: [stored] })).toEqual({ kind: "written" });
      expect(JSON.parse(readFileSync(path, "utf8"))).toEqual({ entries: [stored] });
    } finally {
      expect(removeProjectFolder(folder.path)).toEqual({ kind: "removed" });
    }
  });

  test("writeBaselineFile notWritten: the folder does not exist", () => {
    const path = join(tmpdir(), "horsies-typecheck-speed-missing-parent", "baseline.json");

    expect(writeBaselineFile(path, { entries: [stored] }).kind).toBe("notWritten");
  });
});
