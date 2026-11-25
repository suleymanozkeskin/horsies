import { describe, expect, test } from "bun:test";
import { tmpdir } from "node:os";

import { projectSha256 } from "./baseline.ts";
import { controlProjectFiles } from "./control-project.ts";
import { bunCheckCommand, checkRunTimeout, createProjectFolder, removeProjectFolder, runCheckOnce, writeProjectFiles } from "./measure.ts";

describe("controlProjectFiles", () => {
  test("every call gives the same files", () => {
    expect(projectSha256(controlProjectFiles())).toBe(projectSha256(controlProjectFiles()));
  });

  test("157 TypeScript files and a tsconfig.json", () => {
    const paths = [...controlProjectFiles().keys()];

    expect(paths.filter((path) => path.endsWith(".ts"))).toHaveLength(157);
    expect(paths.filter((path) => !path.endsWith(".ts"))).toEqual(["tsconfig.json"]);
  });

  test("the project type-checks with no error", () => {
    const folder = createProjectFolder(tmpdir());

    if (folder.kind === "notCreated") {
      throw new Error(`test setup: no folder: ${folder.cause}`);
    }

    const write = writeProjectFiles(folder.path, controlProjectFiles());
    const run = runCheckOnce(bunCheckCommand, folder.path, checkRunTimeout);
    expect(removeProjectFolder(folder.path)).toEqual({ kind: "removed" });

    expect(write).toEqual({ kind: "written" });
    expect(run.kind).toBe("passed");
  });
});
