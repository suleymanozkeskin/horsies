import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import storedFile from "./baseline.json" with { type: "json" };
import {
  type BaselineKey,
  compareProjects,
  currentBun,
  currentMachine,
  findBaseline,
  judge,
  type MachineFacts,
  type MachineIdentity,
  machineFrom,
  parseBaseline,
  type ProjectFingerprints,
  projectSha256,
  type StoredBaseline,
  type StoredBaselineFile,
  storedBaselineOf,
  withRecordedBaseline,
  writeBaselineFile,
} from "./baseline.ts";
import { createProjectFolder, removeProjectFolder } from "./measure.ts";
import type { Ratio } from "./ratio.ts";

const machine: MachineIdentity = { platform: "linux", arch: "x64", cpuModel: "Test CPU", logicalCores: 4 };

const key: BaselineKey = {
  platform: "linux",
  arch: "x64",
  bun: "1.4.4+38afee6c8",
  runsPerSession: 9,
  sessions: 3,
};

const projects: ProjectFingerprints = { controlSha256: "a".repeat(64), appSha256: "c".repeat(64) };

const sessionRatios: Ratio[] = [10, 9.5, 10.5].map((appPerControl) => ({ appPerControl }));
const stored: StoredBaseline = storedBaselineOf(key, projects, machine, sessionRatios);

describe("machine identity", () => {
  const facts: MachineFacts = { platform: "linux", arch: "x64", cpuModels: ["Test CPU", "Second CPU"], logicalCores: 4 };

  test("machineFrom identified: the first CPU model names the CPU", () => {
    expect(machineFrom(facts)).toEqual({ kind: "identified", machine });
  });

  test("machineFrom cpuModelNotReported: the system reports no CPU", () => {
    expect(machineFrom({ ...facts, cpuModels: [] })).toEqual({ kind: "cpuModelNotReported" });
  });

  test("currentMachine: the platform of this process", () => {
    expect(currentMachine()).toMatchObject({ kind: "identified", machine: { platform: process.platform, arch: process.arch } });
  });

  test("currentBun: the version and the revision of this Bun binary", () => {
    expect(currentBun()).toBe(`${Bun.version}+${Bun.revision.slice(0, 9)}`);
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
  const otherBun: StoredBaseline = storedBaselineOf({ ...key, bun: "1.4.5+0000000aa" }, projects, machine, sessionRatios);
  const file: StoredBaselineFile = { entries: [otherBun, stored] };

  test("found: the entry of the platform, the Bun binary, the run count and the session count", () => {
    expect(findBaseline(file, key)).toEqual({ kind: "found", stored });
  });

  test("found: the entry of another CPU of the same platform, because the CPU speed cancels in the ratio", () => {
    const onOtherCpu = storedBaselineOf(key, projects, { ...machine, cpuModel: "Other CPU", logicalCores: 16 }, sessionRatios);

    expect(findBaseline({ entries: [onOtherCpu] }, key)).toEqual({ kind: "found", stored: onOtherCpu });
  });

  test("found: the projects have changed since the recording, because the projects are not in the key", () => {
    const changed: ProjectFingerprints = { controlSha256: "b".repeat(64), appSha256: "d".repeat(64) };

    expect(findBaseline(file, key)).toEqual({ kind: "found", stored });
    expect(compareProjects(stored, changed)).toBe("changed");
    expect(compareProjects(stored, projects)).toBe("same");
  });

  test("the entry stores the fingerprints of the projects that recorded it", () => {
    expect(stored).toMatchObject(projects);
  });

  test.each([
    ["another operating system", { ...key, platform: "darwin" }],
    ["another CPU architecture", { ...key, arch: "arm64" }],
    ["another Bun binary", { ...key, bun: "1.4.6+0000000bb" }],
    ["another run count", { ...key, runsPerSession: 11 }],
    ["another session count", { ...key, sessions: 5 }],
  ])("noMatch: %s", (_, otherKey: BaselineKey) => {
    expect(findBaseline(file, otherKey)).toEqual({ kind: "noMatch" });
  });
});

describe("parseBaseline", () => {
  test("parsed: the median and the spread of the session ratios", () => {
    const parsed = parseBaseline(stored);

    expect(parsed).toEqual({ kind: "parsed", baseline: { median: { appPerControl: 10 }, noise: { appPerControl: 1 } } });
  });

  test.each([
    ["not a number", Number.NaN],
    ["infinite", Number.POSITIVE_INFINITY],
    ["zero", 0],
    ["negative", -1],
  ])("unreadable: a session ratio that is %s", (_, value) => {
    const parsed = parseBaseline({ ...stored, sessionRatios: [10, value, 10] });

    expect(parsed).toMatchObject({ kind: "unreadable", text: String(value) });
  });

  test("unreadable: an even count of session ratios", () => {
    const parsed = parseBaseline({ ...stored, sessionRatios: [10, 10] });

    expect(parsed.kind).toBe("unreadable");
  });

  test("each baseline in baseline.json is readable", () => {
    const committed: StoredBaselineFile = storedFile;

    expect(committed.entries.map((entry) => parseBaseline(entry).kind)).toEqual(committed.entries.map(() => "parsed"));
  });
});

describe("judge", () => {
  const baseline = { median: { appPerControl: 10 }, noise: { appPerControl: 1 } };

  test("withinNoise: a ratio at the baseline median plus the noise", () => {
    expect(judge(baseline, { appPerControl: 11 })).toEqual({ kind: "withinNoise", measured: { appPerControl: 11 }, limit: { appPerControl: 11 } });
  });

  test("slower: a ratio above the baseline median plus the noise", () => {
    expect(judge(baseline, { appPerControl: 11.001 }).kind).toBe("slower");
  });

  test("withinNoise: a ratio below the baseline median", () => {
    expect(judge(baseline, { appPerControl: 5 }).kind).toBe("withinNoise");
  });
});

describe("recording", () => {
  test("withRecordedBaseline: replaces the entry of the same platform and Bun binary and keeps the others", () => {
    const otherBun = storedBaselineOf({ ...key, bun: "1.4.5+0000000aa" }, projects, machine, sessionRatios);
    const otherPlatform = storedBaselineOf(key, projects, { ...machine, platform: "darwin", arch: "arm64" }, sessionRatios);
    const newer = storedBaselineOf(key, projects, machine, [8, 8.5, 9].map((appPerControl) => ({ appPerControl })));

    expect(withRecordedBaseline({ entries: [stored, otherBun, otherPlatform] }, newer)).toEqual({
      entries: [otherBun, otherPlatform, newer],
    });
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
