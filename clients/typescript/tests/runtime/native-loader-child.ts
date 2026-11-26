// A child process of `native-loader.test.ts`: one case of the kept load, which needs a process of its
// own (a process loads the addon once). It writes its outcome as one JSON line on stdout.

import { requireDevAddon, loadDevAddon } from "../../horsies/src/native/dev-addon.ts";
import { keptLoad, loadAddon } from "../../horsies/src/native/load.ts";
import type { AddonLoad } from "../../horsies/src/native/load.ts";

/** An absolute path where no file exists. */
const MISSING_BUILD_PATH = "/nonexistent/horsies-loader-check/horsies.node";

/** The outcome of one case: the facts that the test compares. */
type CaseOutcome = Readonly<Record<string, string | number | boolean>>;

/** Run the case that the first argument names. */
function runCase(name: string | undefined): CaseOutcome {
  switch (name) {
    case "kept-load":
      return keptLoadCase();
    case "dev-missing-file":
      return devMissingFile();
    case "package-first":
      return packageFirst();
    case "require-unset":
      return requireUnset();
    default:
      return { unknownCase: String(name) };
  }
}

/** The first `firstLoad` runs once; a later call with another `firstLoad` gives the kept outcome. */
function keptLoadCase(): CaseOutcome {
  let firstCalls = 0;
  let laterCalls = 0;

  const first = keptLoad((): AddonLoad => {
    firstCalls += 1;

    return { kind: "platformUnsupported", host: "check-host" };
  });

  const later = keptLoad((): AddonLoad => {
    laterCalls += 1;

    return { kind: "outsideMainThread", threadId: 0 };
  });

  return { first: first.kind, later: later.kind, same: first === later, firstCalls, laterCalls };
}

/** The dev entry with a file that does not exist: `loadFailed`, and the package loader keeps it. */
function devMissingFile(): CaseOutcome {
  const dev = loadDevAddon(MISSING_BUILD_PATH);
  const kept = loadAddon();

  return {
    dev: dev.kind,
    causeNamesPath: dev.kind === "loadFailed" && dev.cause.includes("horsies-loader-check"),
    kept: kept.kind,
    keptSource: kept.kind === "loadFailed" ? kept.source.kind : "none",
  };
}

/** The package loader runs first: the dev entry then reports the kept load of the other source. */
function packageFirst(): CaseOutcome {
  const kept = loadAddon();
  const dev = loadDevAddon(MISSING_BUILD_PATH);

  return { kept: kept.kind, dev: dev.kind, keptIsSame: dev.kind === "otherLoadKept" && dev.kept === kept };
}

/** `requireDevAddon` with the variable unset throws the text that names the variable. */
function requireUnset(): CaseOutcome {
  try {
    void requireDevAddon();

    return { threw: false };
  } catch (cause) {
    return { threw: true, message: cause instanceof Error ? cause.message : String(cause) };
  }
}

process.stdout.write(`${JSON.stringify(runCase(process.argv[2]))}\n`);
