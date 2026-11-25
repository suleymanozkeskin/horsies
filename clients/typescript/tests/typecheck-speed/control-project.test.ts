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

    try {
      expect(writeProjectFiles(folder.path, controlProjectFiles())).toEqual({ kind: "written" });
      expect(runCheckOnce(bunCheckCommand, folder.path, checkRunTimeout).kind).toBe("passed");
    } finally {
      expect(removeProjectFolder(folder.path)).toEqual({ kind: "removed" });
    }
  });
});
