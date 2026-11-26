// The commands of the `horsies` tool: the command of the arguments, the refusal of a command of the app's entry, the
// flags, the output lines and the exit codes of `skills install` and `get-docs`, and the bounded tool run.

import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, test } from "node:test";

import { COMMAND_NAMES } from "../../../horsies/src/cli/run.ts";
import type { ToolOutput } from "../../../horsies/src/tools/command-line.ts";
import { type GetDocsDeps, runGetDocs, runToolBounded } from "../../../horsies/src/tools/docs-command.ts";
import { DOCS_REPO_PATH } from "../../../horsies/src/tools/docs.ts";
import { APP_ENTRY_COMMANDS, runTool, toolCommandOf } from "../../../horsies/src/tools/run.ts";
import { runSkillsInstall } from "../../../horsies/src/tools/skills-command.ts";
import { PACKAGE_SKILL_FOLDER, SKILL_FILE_NAMES } from "../../../horsies/src/tools/skills.ts";
import { err, ok } from "../../../horsies/src/wire/result.ts";

const folders: string[] = [];

function newFolder(): string {
  const folder = mkdtempSync(join(tmpdir(), "horsies-tool-test-"));

  folders.push(folder);

  return folder;
}

after(() => {
  for (const folder of folders) {
    rmSync(folder, { recursive: true, force: true });
  }
});

/** An output that records its lines. */
function recordedOutput(): ToolOutput & { out: string[]; error: string[] } {
  const out: string[] = [];
  const error: string[] = [];

  return { out, error, writeOut: (line) => out.push(line), writeError: (line) => error.push(line) };
}

/** A package folder with the five skill files and a manifest of `version`. */
function packageWith(version: string): string {
  const folder = newFolder();

  mkdirSync(join(folder, PACKAGE_SKILL_FOLDER), { recursive: true });

  for (const name of SKILL_FILE_NAMES) {
    writeFileSync(join(folder, PACKAGE_SKILL_FOLDER, name), `# ${name}`);
  }

  writeFileSync(join(folder, "package.json"), JSON.stringify({ name: "horsies", version }));

  return folder;
}

describe("toolCommandOf", () => {
  test("the commands of the tool", () => {
    assert.deepEqual(toolCommandOf(["--help"]), { kind: "help" });
    assert.deepEqual(toolCommandOf(["get-docs", "--ref", "main"]), { kind: "getDocs", args: ["--ref", "main"] });
    assert.deepEqual(toolCommandOf(["skills", "install", "--target", "x"]), { kind: "skillsInstall", args: ["--target", "x"] });
  });

  test("no command, an unknown command and a skills command with no subcommand are usage errors", () => {
    for (const args of [[], ["cutover"], ["skills"], ["skills", "remove"]]) {
      assert.equal(toolCommandOf(args).kind, "usageError", args.join(" "));
    }
  });

  test("each command of cli.run(app) is refused by name, with exit code 2 and where it runs", () => {
    assert.deepEqual(APP_ENTRY_COMMANDS, COMMAND_NAMES);

    for (const command of APP_ENTRY_COMMANDS) {
      const output = recordedOutput();

      assert.equal(runTool([command], { output, packageFolder: newFolder(), getDocs: getDocsDeps(output, newFolder(), () => ok(undefined)) }), 2);
      assert.match(output.error[0] ?? "", new RegExp(`^error: \`horsies ${command}\` is not a command of this tool.*cli\\.run\\(app\\).*\`<your entry> ${command}\``, "u"));
    }
  });

  test("the help names both commands and the app's entry, with exit code 0", () => {
    const output = recordedOutput();

    assert.equal(runTool(["-h"], { output, packageFolder: newFolder(), getDocs: getDocsDeps(output, newFolder(), () => ok(undefined)) }), 0);
    assert.match(output.out.join("\n"), /get-docs.*\n.*skills install.*[\s\S]*-V, --version[\s\S]*cli\.run\(app\)/u);
  });

  test("a usage error writes the usage line and the error line, with exit code 2", () => {
    const output = recordedOutput();

    assert.equal(runTool([], { output, packageFolder: newFolder(), getDocs: getDocsDeps(output, newFolder(), () => ok(undefined)) }), 2);
    assert.deepEqual(output.error, ["usage: horsies [-h] [-V] {get-docs,skills} ...", "horsies: error: the following arguments are required: command"]);
  });

  test("--version writes the name and the version of the package with exit code 0; an unreadable version gives 1", () => {
    for (const flag of ["--version", "-V"]) {
      const output = recordedOutput();

      assert.equal(runTool([flag], { output, packageFolder: packageWith("0.8.1"), getDocs: getDocsDeps(output, newFolder(), () => ok(undefined)) }), 0);
      assert.deepEqual(output.out, ["horsies 0.8.1"]);
    }

    const output = recordedOutput();

    assert.equal(runTool(["--version"], { output, packageFolder: newFolder(), getDocs: getDocsDeps(output, newFolder(), () => ok(undefined)) }), 1);
    assert.match(output.error[0] ?? "", /^error: the version of the horsies package cannot be read/u);
  });
});

describe("runSkillsInstall", () => {
  test("installs the five files with one line and exit code 0, then finds the copy current", () => {
    const packageFolder = packageWith("0.8.1");
    const target = join(newFolder(), ".claude", "skills");
    const first = recordedOutput();
    const second = recordedOutput();

    assert.equal(runSkillsInstall(["--target", target], { output: first, packageFolder }), 0);
    assert.deepEqual(first.out, [`installed 5 files to ${join(target, "horsies")}`]);
    assert.equal(runSkillsInstall(["--target", target], { output: second, packageFolder }), 0);
    assert.deepEqual(second.out, [`${join(target, "horsies")} is already current`]);
  });

  test("a changed copy is an error with exit code 1; --force replaces it", () => {
    const packageFolder = packageWith("0.8.1");
    const target = newFolder();
    const output = recordedOutput();

    assert.equal(runSkillsInstall(["--target", target], { output, packageFolder }), 0);
    writeFileSync(join(target, "horsies", "tasks.md"), "edited");
    assert.equal(runSkillsInstall(["--target", target], { output, packageFolder }), 1);
    assert.match(output.error[0] ?? "", /^error: .*differs from the packaged skill \(tasks\.md\)/u);
    assert.equal(runSkillsInstall(["--target", target, "--force"], { output, packageFolder }), 0);
  });

  test("a missing --target, an unknown flag and a positional argument are usage errors with exit code 2", () => {
    for (const args of [[], ["--target", "x", "--dest", "y"], ["x"]]) {
      const output = recordedOutput();

      assert.equal(runSkillsInstall(args, { output, packageFolder: packageWith("0.8.1") }), 2, args.join(" "));
      assert.equal(output.error[0], "usage: horsies skills install [-h] --target TARGET [--force]");
    }
  });

  test("--help prints the help with exit code 0", () => {
    const output = recordedOutput();

    assert.equal(runSkillsInstall(["--help"], { output, packageFolder: newFolder() }), 0);
    assert.match(output.out.join("\n"), /--target TARGET[\s\S]*--force/u);
  });
});

function getDocsDeps(output: ToolOutput, packageFolder: string, runTool: GetDocsDeps["runTool"]): GetDocsDeps {
  return {
    output,
    packageFolder,
    runTool,
    writeStatus: (line) => output.writeError(line),
    makeWorkspace: () => ok(newFolder()),
    removeWorkspace: () => ok(undefined),
  };
}

describe("runGetDocs", () => {
  test("a fetch that succeeds writes the file count with exit code 0", () => {
    const output = recordedOutput();
    const target = join(newFolder(), "docs");

    const deps = getDocsDeps(output, packageWith("0.8.1"), (call) => {
      if (call.name === "git checkout") {
        mkdirSync(join(call.cwd, DOCS_REPO_PATH, "typescript"), { recursive: true });
        writeFileSync(join(call.cwd, DOCS_REPO_PATH, "typescript", "index.mdx"), "---\ntitle: Overview\n---\n");
      }

      return ok(undefined);
    });

    assert.equal(runGetDocs(["--output", target], deps), 0);
    assert.equal(output.error.at(-1), `Done. 1 files written to ${target}/`);
  });

  test("a failed fetch writes the error with exit code 1", () => {
    const output = recordedOutput();
    const deps = getDocsDeps(output, packageWith("0.8.1"), () => err({ kind: "exited", status: 6, stderr: "could not resolve host" }));

    assert.equal(runGetDocs(["--output", join(newFolder(), "docs")], deps), 1);
    assert.equal(output.error.at(-1), "error: curl failed: exit status 6: could not resolve host");
  });

  test("a pre-release version with no --ref is an error with exit code 1, and names --ref main", () => {
    const output = recordedOutput();

    assert.equal(runGetDocs([], getDocsDeps(output, packageWith("0.9.0-rc.1"), () => ok(undefined))), 1);
    assert.match(output.error[0] ?? "", /^error: installed version "0\.9\.0-rc\.1" is not a release version.*--ref main/u);
  });

  test("a --ref other than main and an unknown flag are usage errors with exit code 2", () => {
    for (const args of [["--ref", "dev"], ["--target", "x"]]) {
      const output = recordedOutput();

      assert.equal(runGetDocs(args, getDocsDeps(output, packageWith("0.8.1"), () => ok(undefined))), 2, args.join(" "));
      assert.equal(output.error[0], "usage: horsies get-docs [-h] [--output OUTPUT] [--ref {main}]");
    }
  });

  test("--help prints the help with exit code 0", () => {
    const output = recordedOutput();

    assert.equal(runGetDocs(["-h"], getDocsDeps(output, newFolder(), () => ok(undefined))), 0);
    assert.match(output.out.join("\n"), /--output OUTPUT[\s\S]*--ref \{main\}/u);
  });
});

describe("runToolBounded", () => {
  const node = process.execPath;
  const limits = { timeMs: 10_000, stderrBytes: 65_536 };

  test("a run that exits 0", () => {
    assert.deepEqual(runToolBounded({ name: "node", command: node, args: ["-e", ""], cwd: tmpdir() }, limits), { ok: true, value: undefined });
  });

  test("a run that exits with another status gives the status and the start of its stderr", () => {
    const ran = runToolBounded({ name: "node", command: node, args: ["-e", "process.stderr.write(' bad input \\n'); process.exit(3)"], cwd: tmpdir() }, limits);

    assert.deepEqual(ran, { ok: false, error: { kind: "exited", status: 3, stderr: "bad input" } });
  });

  test("a command that does not exist does not start", () => {
    const ran = runToolBounded({ name: "missing", command: "horsies-no-such-command", args: [], cwd: tmpdir() }, limits);

    assert.ok(!ran.ok && ran.error.kind === "notStarted");
    assert.equal(ran.error.cause.code, "ENOENT");
  });

  test("a run past the time bound is killed", () => {
    const ran = runToolBounded({ name: "node", command: node, args: ["-e", "setTimeout(() => {}, 30000)"], cwd: tmpdir() }, { ...limits, timeMs: 200 });

    assert.deepEqual(ran, { ok: false, error: { kind: "timedOut", boundMs: 200 } });
  });

  test("a run that writes more stderr than the bound is killed", () => {
    const ran = runToolBounded({ name: "node", command: node, args: ["-e", "process.stderr.write('x'.repeat(200000))"], cwd: tmpdir() }, limits);

    assert.deepEqual(ran, { ok: false, error: { kind: "stderrTooLarge", boundBytes: 65_536 } });
  });

  test("a run that a signal ends", () => {
    const ran = runToolBounded({ name: "node", command: node, args: ["-e", "process.kill(process.pid, 'SIGTERM')"], cwd: tmpdir() }, limits);

    assert.deepEqual(ran, { ok: false, error: { kind: "signalled", signal: "SIGTERM" } });
  });
});
