// The minors of `@opentelemetry/api` that the probe of `api-minors-probe.ts` runs on.

/** The minors that the probe runs: the installed API, and the floor of the peer range. */
export const MINOR_NAMES = ["installed", "floor"] as const;

/** A minor of the probe. */
export type MinorName = (typeof MINOR_NAMES)[number];

/** The version of the floor of the peer range, which `@opentelemetry/api-floor` installs. */
export const FLOOR_VERSION = "1.7.0";
