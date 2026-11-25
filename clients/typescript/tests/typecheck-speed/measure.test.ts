import { describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  bunCheckCommand,
  type CheckCommand,
  type CheckRun,
  checkOutputLimitBytes,
  checkRunTimeout,
  createProjectFolder,
  durationFromMilliseconds,
  medianOf,
  removeProjectFolder,
  runCheckOnce,
  runSession,
  spreadOf,
  warmUpRuns,
  writeProjectFiles,
} from "./measure.ts";

const tinyTsconfig = '{ "compilerOptions": { "strict": true, "noEmit": true, "types": [] }, "include": ["*.ts"] }\n';
const shortBound = Temporal.Duration.from({ milliseconds: 200 });
const missingFolder = join(tmpdir(), "horsies-typecheck-speed-missing-parent", "inner");

function shellCommand(script: string): CheckCommand {
  return { executable: "sh", args: ["-c", script] };
}

function newFolder(): string {
  const folder = createProjectFolder(tmpdir());

  if (folder.kind === "notCreated") {
    throw new Error(`test setup: no folder: ${folder.cause}`);
  }

  return folder.path;
}

function removeFolder(folder: string): void {
  expect(removeProjectFolder(folder)).toEqual({ kind: "removed" });
}

/** Runs `command` in a new folder that holds `files`, then removes the folder. */
function runInProject(files: ReadonlyMap<string, string>, command: CheckCommand, bound: Temporal.Duration): CheckRun {
  const folder = newFolder();
  expect(writeProjectFiles(folder, files)).toEqual({ kind: "written" });
  const run = runCheckOnce(command, folder, bound);
  removeFolder(folder);

  return run;
}

describe("runCheckOnce", () => {
  test("passed: a project with no type error", () => {
    const files = new Map([
      ["tsconfig.json", tinyTsconfig],
      ["index.ts", "export const value: number = 1;\n"],
    ]);

    const run = runInProject(files, bunCheckCommand, checkRunTimeout);

    expect(run.kind).toBe("passed");
  });

  test("failed: a project with a type error", () => {
    const files = new Map([
      ["tsconfig.json", tinyTsconfig],
      ["index.ts", 'export const value: number = "text";\n'],
    ]);

    const run = runInProject(files, bunCheckCommand, checkRunTimeout);

    if (run.kind !== "failed") {
      throw new Error(`expected failed, got ${run.kind}`);
    }

    expect(run.exitCode).toBe(1);
    expect(run.output).toContain("TS2322");
  });

  test("timedOut: a run longer than the bound", () => {
    const run = runInProject(new Map(), shellCommand("sleep 5"), shortBound);

    expect(run).toEqual({ kind: "timedOut", bound: shortBound });
  });

  test("outputTooLarge: output above the limit", () => {
    const run = runInProject(new Map(), shellCommand(`head -c ${checkOutputLimitBytes + 1} /dev/zero`), checkRunTimeout);

    expect(run).toEqual({ kind: "outputTooLarge", limitBytes: checkOutputLimitBytes });
  });

  test("killed: a run stopped by a signal", () => {
    const run = runInProject(new Map(), shellCommand("kill -9 $$"), checkRunTimeout);

    expect(run).toEqual({ kind: "killed", signal: "SIGKILL" });
  });

  test("didNotStart: a program that does not exist", () => {
    const command: CheckCommand = { executable: join(tmpdir(), "horsies-no-such-program"), args: [] };
    const run = runInProject(new Map(), command, checkRunTimeout);

    expect(run.kind).toBe("didNotStart");
  });
});

describe("runSession", () => {
  test("measured: the median of the measured runs; the warm-up run is not measured", () => {
    const folder = newFolder();
    const session = runSession(shellCommand("echo run >> runs.txt"), folder, 3, checkRunTimeout);
    const startedRuns = readFileSync(join(folder, "runs.txt"), "utf8").trim().split("\n");
    removeFolder(folder);

    if (session.kind !== "measured") {
      throw new Error(`expected measured, got ${session.kind}`);
    }

    expect(startedRuns).toHaveLength(warmUpRuns + 3);
    expect(session.runs).toHaveLength(3);
    expect(Temporal.Duration.compare(session.median, medianOf(session.runs))).toBe(0);
  });

  test("runFailed: the first failed run ends the session", () => {
    const folder = newFolder();
    const session = runSession(shellCommand("exit 3"), folder, 3, checkRunTimeout);
    removeFolder(folder);

    expect(session).toMatchObject({ kind: "runFailed", failure: { kind: "failed", exitCode: 3 } });
  });
});

describe("statistics", () => {
  const values = [30, 10, 20].map(durationFromMilliseconds);

  test("medianOf: the middle value", () => {
    expect(medianOf(values).total("millisecond")).toBe(20);
  });

  test("spreadOf: the slowest value minus the fastest value", () => {
    expect(spreadOf(values).total("millisecond")).toBe(20);
  });
});

describe("project folder", () => {
  test("createProjectFolder created: an empty folder", () => {
    const folder = newFolder();

    expect(readdirSync(folder)).toEqual([]);
    removeFolder(folder);
  });

  test("createProjectFolder notCreated: the parent folder does not exist", () => {
    expect(createProjectFolder(missingFolder).kind).toBe("notCreated");
  });

  test("writeProjectFiles written: each file at its path", () => {
    const folder = newFolder();
    const write = writeProjectFiles(folder, new Map([["types/t00.ts", "export {};\n"]]));
    const source = readFileSync(join(folder, "types", "t00.ts"), "utf8");
    removeFolder(folder);

    expect(write).toEqual({ kind: "written" });
    expect(source).toBe("export {};\n");
  });

  test("writeProjectFiles notWritten: a file is in the place of a folder", () => {
    const folder = newFolder();

    const files = new Map([
      ["blocker", "text\n"],
      ["blocker/inner.ts", "export {};\n"],
    ]);

    const write = writeProjectFiles(folder, files);
    removeFolder(folder);

    expect(write).toMatchObject({ kind: "notWritten", file: "blocker/inner.ts" });
  });

  test("removeProjectFolder removed: the folder and its content are gone", () => {
    const folder = newFolder();
    expect(writeProjectFiles(folder, new Map([["a/b.ts", "export {};\n"]]))).toEqual({ kind: "written" });

    expect(removeProjectFolder(folder)).toEqual({ kind: "removed" });
    expect(existsSync(folder)).toBe(false);
  });

  test("removeProjectFolder notRemoved: a path that the file system refuses", () => {
    const refusedPath = join(tmpdir(), "horsies\0refused");

    expect(removeProjectFolder(refusedPath)).toMatchObject({ kind: "notRemoved", path: refusedPath });
  });
});
