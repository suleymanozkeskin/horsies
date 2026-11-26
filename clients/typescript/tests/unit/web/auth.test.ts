// The shipped access policies and the trusted header rule.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  allowAll,
  HEADER_NAME_RULE,
  trustedHeader,
  trustedHeaderRefusalText,
  viewOnly,
} from "../../../horsies/src/web/auth.ts";
import type { MonitoringAuthPolicy } from "../../../horsies/src/web/auth.ts";

/** A request with the headers `headers`: a record, or name and value pairs that may repeat a name. */
function request(headers: Record<string, string> | [string, string][]): Request {
  return new Request("http://monitoring.test/api/tasks", { headers });
}

/** The two verdicts of `policy` for `incoming`. */
async function verdicts(policy: MonitoringAuthPolicy, incoming: Request): Promise<[boolean, boolean]> {
  return [await policy.canView(incoming), await policy.canAct(incoming)];
}

/** The policy of a `trustedHeader` call that must be accepted. */
function accepted(headerName: string, actions: "enabled" | "disabled"): MonitoringAuthPolicy {
  const built = trustedHeader(headerName, { actions });

  assert.ok(built.ok, `${headerName} is a header name`);

  return built.value;
}

describe("allowAll and viewOnly", () => {
  test("allowAll allows the view and the actions", async () => {
    assert.deepEqual(await verdicts(allowAll(), request({})), [true, true]);
  });

  test("viewOnly allows the view and refuses the actions", async () => {
    assert.deepEqual(await verdicts(viewOnly(), request({})), [true, false]);
  });
});

describe("trustedHeader", () => {
  test("a request with the header may view, and may act when actions are enabled", async () => {
    const withHeader = request({ "X-Forwarded-User": "ops@example.test" });

    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "enabled"), withHeader), [true, true]);
    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "disabled"), withHeader), [true, false]);
  });

  test("a request without the header may do nothing", async () => {
    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "enabled"), request({})), [false, false]);
  });

  test("a request with a blank value may do nothing", async () => {
    const policy = accepted("X-Forwarded-User", "enabled");

    assert.deepEqual(await verdicts(policy, request({ "X-Forwarded-User": "  " })), [false, false]);
  });

  test("a request with the header two times, both blank, may do nothing", async () => {
    const incoming = request([
      ["X-Forwarded-User", " "],
      ["X-Forwarded-User", " "],
    ]);

    assert.equal(incoming.headers.get("X-Forwarded-User"), ", ");
    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "enabled"), incoming), [false, false]);
  });

  test("a request with a blank value before a name may do nothing", async () => {
    const incoming = request([
      ["X-Forwarded-User", " "],
      ["X-Forwarded-User", "alice"],
    ]);

    assert.equal(incoming.headers.get("X-Forwarded-User"), ", alice");
    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "enabled"), incoming), [false, false]);
  });

  test("an identity that holds a comma and no blank member may view and act", async () => {
    const incoming = request({ "X-Forwarded-User": "Doe, Jane" });

    assert.deepEqual(await verdicts(accepted("X-Forwarded-User", "enabled"), incoming), [true, true]);
  });

  test("the header name is read in any case", async () => {
    const policy = accepted("x-forwarded-user", "disabled");

    assert.equal(await policy.canView(request({ "X-FORWARDED-USER": "ops" })), true);
  });

  test("a blank header name is refused", () => {
    assert.deepEqual(trustedHeader("  ", { actions: "enabled" }), {
      ok: false,
      error: { kind: "blankHeaderName" },
    });
  });

  test("a header name that is not an HTTP token is refused with the rule", () => {
    assert.deepEqual(trustedHeader("X Forwarded User", { actions: "enabled" }), {
      ok: false,
      error: { kind: "invalidHeaderName", headerName: "X Forwarded User", rule: HEADER_NAME_RULE },
    });
  });

  test("the refusal texts name the cause", () => {
    assert.equal(trustedHeaderRefusalText({ kind: "blankHeaderName" }), "the trusted header needs a header name");
    assert.match(
      trustedHeaderRefusalText({ kind: "invalidHeaderName", headerName: "a:b", rule: HEADER_NAME_RULE }),
      /"a:b" is not valid: a header name is/u,
    );
  });
});
