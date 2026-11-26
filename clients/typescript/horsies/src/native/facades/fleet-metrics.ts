// The functions of the fleet metrics server in the `facades` area of the addon, the records they
// carry, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";

/**
 * The answer of the address check: the host (an IPv6 address without brackets) and the port, 1 to
 * 65535; or the refusal, whose `message` names the text and the rule.
 */
export type MetricsListenAddressCheck =
  | { readonly kind: "address"; readonly host: string; readonly port: number }
  | { readonly kind: "refused"; readonly message: string };

/** The settings of a fleet metrics server. */
export interface FleetMetricsSettings {
  /** The `HOST:PORT` address that `parseMetricsListenAddress` accepted. */
  readonly listen: string;
  /** The `queue` label values. */
  readonly queueNames: string[];
  /** The `schedule` label values. */
  readonly scheduleNames: string[];
}

/** How a run ended: at a stop request, or with an address that did not bind. */
export type FleetMetricsRunEnd = { readonly kind: "stopped" } | { readonly kind: "failed"; readonly message: string };

/** A fleet metrics server that runs once. */
export interface NativeFleetMetricsServer {
  /** Bind the address and serve until a stop request. */
  run(): Promise<NativeEnvelope<FleetMetricsRunEnd>>;
  /** Ask the run to stop. Idempotent; returns at once. */
  requestStop(): NativeEnvelope<undefined>;
}

/** The fleet metrics functions of the `facades` area. */
export interface FleetMetricsAddon {
  parseMetricsListenAddress(text: string): NativeEnvelope<MetricsListenAddressCheck>;
  prepareFleetMetrics(handle: NativeCoreHandle, settings: FleetMetricsSettings): NativeEnvelope<NativeFleetMetricsServer>;
}

/** The fleet metrics functions of the addon. Throws as `requireAddon` does. */
export function fleetMetricsAddon(): FleetMetricsAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this part compares this interface with the declarations that the native build writes.
  return requireAddon() as FleetMetricsAddon;
}
