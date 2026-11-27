import { describe, expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

import { appProjectFiles } from "./app-project.ts";
import { projectSha256 } from "./baseline.ts";
import { controlProjectFiles } from "./control-project.ts";
import { bunCheckCommand, checkRunTimeout, createProjectFolder, removeProjectFolder, runCheckOnce, writeProjectFiles } from "./measure.ts";

const projectParent = fileURLToPath(new URL(".", import.meta.url));

/** The text of the files of the folder `prefix`, in one string. */
function sourceOf(files: ReadonlyMap<string, string>, prefix: string): string {
  return [...files].filter(([path]) => path.startsWith(prefix)).map(([, source]) => source).join("\n");
}

function countOf(source: string, text: string): number {
  return source.split(text).length - 1;
}

const files = appProjectFiles();
const contracts = sourceOf(files, "contracts/");
const workflows = sourceOf(files, "workflows/");

describe("appProjectFiles", () => {
  test("every call gives the same files", () => {
    expect(projectSha256(appProjectFiles())).toBe(projectSha256(files));
  });

  test("500 contracts: 3 to 8 parameters, codes on 250, retry on 125, a chain parameter on 450", () => {
    expect(countOf(contracts, "defineTask(")).toBe(500);
    expect(countOf(contracts, "errorCodes:")).toBe(250);
    expect(countOf(contracts, "retryPolicy:")).toBe(125);
    expect(countOf(contracts, "prev: h.taskOutcome(")).toBe(450);
  });

  test("all three execution classes are declared", () => {
    expect(countOf(contracts, 'execution: "SHARED_ASYNC"')).toBe(167);
    expect(countOf(contracts, 'execution: "SHARED_THREAD"')).toBe(166);
    expect(countOf(contracts, "execution:")).toBe(333);
  });

  test("500 implementations and 500 sends with a result read", () => {
    expect(countOf(sourceOf(files, "impls/"), "app.implement(")).toBe(500);
    expect(countOf(sourceOf(files, "sends/"), "app.send(")).toBe(500);
    expect(countOf(sourceOf(files, "sends/"), ".get({")).toBe(500);
  });

  test("50 workflows of 10 nodes: 17 with a quorum join, 10 with a success policy", () => {
    expect(countOf(workflows, "defineWorkflow(")).toBe(50);
    expect(countOf(workflows, "w.task(")).toBe(500);
    expect(countOf(workflows, 'kind: "quorum"')).toBe(17);
    expect(countOf(workflows, "successPolicy:")).toBe(10);
  });

  test("50 starts with a typed resultFor", () => {
    const starts = sourceOf(files, "starts/");

    expect(countOf(starts, "app.start(")).toBe(50);
    expect(countOf(starts, ".resultFor(")).toBe(50);
  });

  test("20 schedules", () => {
    expect(countOf(files.get("schedules.ts") ?? "", 'name: "schedule-')).toBe(20);
  });

  test("the control project holds the same 500 tasks", () => {
    const control = sourceOf(controlProjectFiles(), "tasks/");

    for (const index of [0, 249, 499]) {
      expect(control).toContain(`function task${index}(`);
      expect(contracts).toContain(`export const task${index} = defineTask(`);
    }
  });
});

describe("the app project type check", () => {
  /** Writes the app project, and `extra` files, to a new folder next to this file and checks it. */
  function checkWith(extra: ReadonlyMap<string, string>): ReturnType<typeof runCheckOnce> {
    const folder = createProjectFolder(projectParent);

    if (folder.kind === "notCreated") {
      throw new Error(`test setup: no folder: ${folder.cause}`);
    }

    try {
      expect(writeProjectFiles(folder.path, new Map([...files, ...extra]))).toEqual({ kind: "written" });

      return runCheckOnce(bunCheckCommand, folder.path, checkRunTimeout);
    } finally {
      expect(removeProjectFolder(folder.path)).toEqual({ kind: "removed" });
    }
  }

  test("passed: the project type-checks with no error", () => {
    expect(checkWith(new Map()).kind).toBe("passed");
  });

  test("failed: a send with an input that the contract refuses", () => {
    const wrongSend = 'import { app } from "./app.ts";\nimport { task0 } from "./contracts/c00.ts";\nvoid app.send(task0, { p0: 5 });\n';
    const run = checkWith(new Map([["wrong-send.ts", wrongSend]]));

    if (run.kind !== "failed") {
      throw new Error(`expected failed, got ${run.kind}`);
    }

    expect(run.output).toContain("wrong-send.ts");
  });
});
