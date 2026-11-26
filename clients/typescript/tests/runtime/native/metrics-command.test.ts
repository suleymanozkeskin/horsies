// The fleet metrics server with a built addon and a database: the endpoint answers, a stop request
// ends the run, a second run and an address in use, the core's check of `--listen`, and the
// `metrics` command with its production dependencies.

import assert from "node:assert/strict";
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { before, describe, test } from "node:test";

import { EXIT_USAGE } from "../../../horsies/src/cli/usage.ts";
import { metricsCommandDeps, runMetricsCommand } from "../../../horsies/src/cli/metrics-command.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { envelopeValue, nativeValueAsync } from "../../../horsies/src/native/call/envelope.ts";
import { fleetMetricsAddon } from "../../../horsies/src/native/facades/fleet-metrics.ts";
import type { NativeFleetMetricsServer } from "../../../horsies/src/native/facades/fleet-metrics.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** How many times a test asks the endpoint before it gives up. */
const MAX_ENDPOINT_TRIES = 50;

/** The wait between two tries, in milliseconds. */
const ENDPOINT_TRY_WAIT_MS = 100;

/** A free TCP port of 127.0.0.1: bound, read, and freed. */
async function freePort(): Promise<number> {
  const holder = createServer();
  const listening = Promise.withResolvers<undefined>();

  holder.listen({ host: "127.0.0.1", port: 0 }, () => listening.resolve(undefined));
  await listening.promise;

  // SAFETY: a TCP server that listens on a host and a port has an `AddressInfo` address.
  const port = (holder.address() as AddressInfo).port;
  const closed = Promise.withResolvers<undefined>();

  holder.close(() => closed.resolve(undefined));
  await closed.promise;

  return port;
}

/** The server of `handle` on `listen`. */
function prepared(handle: CoreHandle, listen: string): NativeFleetMetricsServer {
  return envelopeValue(
    fleetMetricsAddon().prepareFleetMetrics(handle.native, { listen, queueNames: ["default"], scheduleNames: [] }),
  );
}

/** The text of `GET /metrics` on `port`, once the endpoint answers. */
async function metricsText(port: number): Promise<string> {
  for (let tries = 0; tries < MAX_ENDPOINT_TRIES; tries += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/metrics`);

      return await response.text();
    } catch {
      await new Promise((resolve) => setTimeout(resolve, ENDPOINT_TRY_WAIT_MS));
    }
  }

  return assert.fail(`the endpoint on port ${port} did not answer`);
}

describe("the fleet metrics server with a built addon and a database", () => {
  let handle: CoreHandle;

  before(async () => {
    handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
    assert.equal((await handle.installSchema()).kind, "installed");
  });

  test("the endpoint answers the fleet metrics, and a stop request ends the run stopped", async () => {
    const port = await freePort();
    const server = prepared(handle, `127.0.0.1:${port}`);
    const ended = nativeValueAsync("FleetMetricsServer.run", () => server.run());

    try {
      assert.match(await metricsText(port), /horsies_/u);
    } finally {
      envelopeValue(server.requestStop());
    }

    assert.deepEqual(await ended, { kind: "stopped" });
  });

  test("a second run throws ClientDefect", async () => {
    const port = await freePort();
    const server = prepared(handle, `127.0.0.1:${port}`);

    envelopeValue(server.requestStop());
    assert.deepEqual(await nativeValueAsync("FleetMetricsServer.run", () => server.run()), { kind: "stopped" });
    await assert.rejects(nativeValueAsync("FleetMetricsServer.run", () => server.run()), { name: "ClientDefect" });
  });

  test("an address in use ends the run failed with the cause", async () => {
    const holder = createServer();
    const listening = Promise.withResolvers<undefined>();

    holder.listen({ host: "127.0.0.1", port: 0 }, () => listening.resolve(undefined));
    await listening.promise;

    try {
      // SAFETY: a TCP server that listens on a host and a port has an `AddressInfo` address.
      const port = (holder.address() as AddressInfo).port;
      const ended = await nativeValueAsync("FleetMetricsServer.run", () => prepared(handle, `127.0.0.1:${port}`).run());

      assert.equal(ended.kind, "failed");
      assert.match(ended.kind === "failed" ? ended.message : "", /could not bind the metrics endpoint/u);
    } finally {
      holder.close();
    }
  });

  test("the core checks a --listen text", () => {
    assert.deepEqual(envelopeValue(fleetMetricsAddon().parseMetricsListenAddress("[::1]:9100")), {
      kind: "address",
      host: "::1",
      port: 9100,
    });

    const refused = envelopeValue(fleetMetricsAddon().parseMetricsListenAddress("127.0.0.1:0"));

    assert.equal(refused.kind, "refused");
  });

  test("the metrics command exits 2 for a --listen that the core refuses", async () => {
    const deps = metricsCommandDeps({ coreHandle: handle, queueNames: ["default"], scheduleNames: [] });

    assert.equal(await runMetricsCommand(["--listen", "no-port"], deps), EXIT_USAGE);
  });
});
