// The check of the exception codes of the implementations of contracts with open codes: it runs on a main thread for
// `check` and the worker start, and never in `app.implement`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies } from "../../../horsies/src/app.ts";
import type { CoreChecks } from "../../../horsies/src/app/core-checks.ts";
import type { CreateEnvironment } from "../../../horsies/src/app/create.ts";
import { exceptionCodeRefusals } from "../../../horsies/src/app/implement.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { DeclaredCodesCheck } from "../../../horsies/src/native/sends/addon.ts";
import { h } from "../../../horsies/src/wire/public.ts";

class RateLimited extends Error {}

/** Core checks that accept everything but the code lists that hold `BAD`; they count the code checks. */
function checks(): CoreChecks & { readonly codeLists: string[][] } {
  const codeLists: string[][] = [];

  return {
    codeLists,
    checkAppConfig: () => {
      throw new TypeError("not called on a worker thread");
    },
    parseDeclaredErrorCodes: (codes): DeclaredCodesCheck => {
      codeLists.push([...codes]);

      return codes.includes("bad code")
        ? {
            kind: "refused",
            refusals: [{ code: "HRS-112", message: "'bad code' is invalid", notes: [], help: { kind: "noHelp" } }],
          }
        : { kind: "accepted" };
    },
    checkTaskOptions: () => {
      throw new TypeError("not called on a worker thread");
    },
  };
}

/** A worker-thread environment: the create runs no core check, so the test needs no addon. */
const environment: CreateEnvironment = {
  mainThread: false,
  environment: () => undefined,
  processStart: { argumentAfterScript: undefined, seatIsolate: false },
  coreChecks: () => {
    throw new TypeError("not called on a worker thread");
  },
  coreHandle: () => {
    throw new TypeError("not called on a worker thread");
  },
  producer: () => {
    throw new TypeError("not called on a worker thread");
  },
};

const open = defineTask({ name: "open", input: {}, output: h.null() });
const declared = defineTask({ name: "declared", input: {}, output: h.null(), errors: ["RATE_LIMITED"] });

function appWith(...registrations: readonly ((app: ReturnType<typeof created>) => void)[]): ReturnType<typeof created> {
  const app = created();

  for (const register of registrations) {
    register(app);
  }

  return app;
}

function created() {
  const outcome = createHorsies(
    { broker: { databaseUrl: { kind: "value", value: "postgresql://127.0.0.1:55432/none" } }, tasks: [open, declared] },
    environment,
  );

  assert.equal(outcome.ok, true);

  if (!outcome.ok) {
    throw new TypeError("the app was refused");
  }

  return outcome.value;
}

describe("exceptionCodeRefusals", () => {
  test("a code of an open contract that the core refuses is a refusal at its implementation", () => {
    const app = appWith((target) => target.implement(open, () => open.ok(null), { exceptions: [[RateLimited, "bad code"]] }));
    const fake = checks();

    assert.deepEqual(
      exceptionCodeRefusals(appInternals(app), fake).map((refused) => [refused.code, refused.path]),
      [["HRS-112", "implement open"]],
    );
    assert.deepEqual(fake.codeLists, [["bad code"]]);
  });

  test("a code that the core accepts gives no refusal", () => {
    const app = appWith((target) =>
      target.implement(open, () => open.ok(null), { exceptions: [[RateLimited, "RATE_LIMITED"]] }),
    );

    assert.deepEqual(exceptionCodeRefusals(appInternals(app), checks()), []);
  });

  test("a contract with declared codes and an implementation with no exceptions make no core call", () => {
    const app = appWith(
      (target) => target.implement(declared, () => declared.ok(null), { exceptions: [[RateLimited, "RATE_LIMITED"]] }),
      (target) => target.implement(open, () => open.ok(null)),
    );

    const fake = checks();

    assert.deepEqual(exceptionCodeRefusals(appInternals(app), fake), []);
    assert.deepEqual(fake.codeLists, []);
  });
});
