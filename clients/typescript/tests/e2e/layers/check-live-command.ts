// `<entry> check --live` as a process (Rust `clients/rust/tests/worker/tests/check_live_command.rs`, Python
// `clients/python/tests/e2e/test_check_live_command.py`).
//
// The worker entry builds the app of the default instance from the database URL of its environment and hands over to
// `cli.run`, as the entry of an app does. Each test gives the process a URL of its own and checks the exit code and
// the output.

import assert from "node:assert/strict";
import { describe } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { DATABASE_URL_ENV, pooledUrl } from "../src/connection.ts";
import { exitText } from "../support/process.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest run of one `check --live` process: the build of the app, one connect and the live reads. */
const CHECK_LIMIT_MS = 60_000;

/** A URL whose port has no server (port 1). It holds a password, so the connect reads no password source. */
const UNREACHABLE_DATABASE_URL = "postgresql://postgres:unused@127.0.0.1:1/horsies";

/** The end of one `check --live` process: its exit code and every output line. */
type CheckEnd = { readonly code: number; readonly output: string };

/** Run `check --live` with `databaseUrl` as the database URL of the app. Fails the test when it does not end in time. */
async function runCheckLive(suite: Suite, databaseUrl: string): Promise<CheckEnd> {
  const check = suite.spawnEntry("check --live", ["check", "--live"], { [DATABASE_URL_ENV]: databaseUrl });
  const exit = await check.waitExit(CHECK_LIMIT_MS);

  if (exit.kind !== "exitedWithCode") {
    assert.fail(`check --live did not exit with a code within ${String(CHECK_LIMIT_MS)} ms (${exitText(exit)})\n${check.report()}`);
  }

  return { code: exit.code, output: check.logLines().join("\n") };
}

/** `url` with the scheme `postgresql+psycopg`: a URL that names a driver. Pure. */
function withDriverScheme(url: string): string {
  const split = url.indexOf("://");

  assert.ok(split > 0, "the database URL of the suite has a scheme");

  return `postgresql+psycopg${url.slice(split)}`;
}

/** `url` with the query parameter `parameter` (`name=value`) added. Pure. */
function withQueryParameter(url: string, parameter: string): string {
  return url.includes("?") ? `${url}&${parameter}` : `${url}?${parameter}`;
}

describe("check --live", () => {
  e2eTest("check --live on the test database passes", "default", async (suite) => {
    const end = await runCheckLive(suite, pooledUrl(suite.db.connection));

    assert.equal(end.code, 0, end.output);
    assert.ok(end.output.includes("ok: all validations passed"), end.output);
    assert.ok(!end.output.includes("error["), end.output);
  });

  e2eTest("a URL that names a driver is refused when the entry builds the app, before any connect", "default", async (suite) => {
    const end = await runCheckLive(suite, withDriverScheme(pooledUrl(suite.db.connection)));

    assert.equal(end.code, 1, end.output);
    assert.ok(end.output.includes("HRS-203"), end.output);
    assert.ok(end.output.includes("postgresql+psycopg"), end.output);
    assert.ok(!end.output.includes("broker connectivity check failed"), end.output);
    assert.ok(!end.output.includes("ok: all validations passed"), end.output);
  });

  e2eTest("a URL setting that the core refuses at its first pool is a connectivity failure that names the setting", "default", async (suite) => {
    const end = await runCheckLive(suite, withQueryParameter(pooledUrl(suite.db.connection), "keepalives=1"));

    assert.equal(end.code, 1, end.output);
    assert.ok(end.output.includes("error[HRS-203]: broker connectivity check failed"), end.output);
    assert.ok(end.output.includes("the parameter keepalives is refused"), end.output);
    assert.ok(!end.output.includes("ok: all validations passed"), end.output);
  });

  e2eTest("a port with no server is a connectivity failure", "default", async (suite) => {
    const end = await runCheckLive(suite, UNREACHABLE_DATABASE_URL);

    assert.equal(end.code, 1, end.output);
    assert.ok(end.output.includes("error[HRS-203]: broker connectivity check failed"), end.output);
    assert.ok(!end.output.includes("ok: all validations passed"), end.output);
  });
});
