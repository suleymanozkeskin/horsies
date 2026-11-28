// The child members of an app: `provide` returns a key and calls nothing; the members keep their order; a member
// after the serve is a defect. The attempt number parser.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { onChildStartFor, provideFor, servedMembers } from "../../../horsies/src/child/app-members.ts";
import { AttemptNumber } from "../../../horsies/src/child/attempt.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { contextOf, testApp } from "../worker/support.ts";

describe("the child members of an app", () => {
  test("provide returns a key and calls nothing; providers and hooks keep their order and their names", () => {
    const context = contextOf(testApp());
    let calls = 0;

    const pool = provideFor(context, async function openPool() {
      calls += 1;

      return "pool";
    });

    onChildStartFor(context, function warmCache() {
      calls += 1;
    });
    provideFor(context, () => 2);

    const served = servedMembers(context);

    assert.equal(calls, 0);
    assert.equal(pool.name, "provide #1 openPool");
    assert.deepEqual(
      served.providers.map((provider) => provider.name),
      ["provide #1 openPool", "provide #2 (anonymous)"],
    );
    assert.equal(served.providers[0]?.key, pool);
    assert.deepEqual(
      served.hooks.map((hook) => hook.name),
      ["onChildStart #1 warmCache"],
    );
  });

  test("a provider or a hook added after the serve is a defect", () => {
    const context = contextOf(testApp());

    servedMembers(context);
    assert.throws(() => provideFor(context, () => 1), ClientDefect);
    assert.throws(() => {
      onChildStartFor(context, () => undefined);
    }, ClientDefect);
  });
});

describe("the attempt number", () => {
  test("a whole number from 1 to the bound is an attempt; another is refused with the reason", () => {
    const first = AttemptNumber.parse(1);

    assert.equal(first.ok ? first.value : 0, 1);
    assert.equal(AttemptNumber.parse(AttemptNumber.MAX).ok, true);

    for (const value of [0, -1, 1.5, AttemptNumber.MAX + 1, Number.NaN]) {
      const refused = AttemptNumber.parse(value);

      assert.equal(refused.ok, false);
      assert.match(refused.ok ? "" : refused.error.reason, /^an attempt number is a whole number from 1 to /u);
    }
  });
});
