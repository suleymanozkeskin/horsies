// The test-only exports of the addon, and the checks that a published build has none of them.

import type { NativeEnvelope } from "../../horsies/src/native/call/envelope.ts";
import type { CoreState } from "../../horsies/src/native/process-core/addon.ts";

/**
 * The exports that only a build with the cargo feature `test-probes` registers (`bridge/Cargo.toml`,
 * `bridge/src/call/callback_probe.rs`, `bridge/src/facades/child_spec_probe.rs`). A published build
 * has none of them.
 */
export const TEST_ONLY_EXPORTS: readonly string[] = ["probeCallback", "setProbeChildSpecSource"];

/** The test-only exports that the export object of a loaded addon has. Pure. */
export function testOnlyExportsIn(exports: object): readonly string[] {
  return TEST_ONLY_EXPORTS.filter((name) => name in exports);
}

/**
 * The test-only export names that the bytes of an addon file hold. A build registers each export under its name, so a
 * build with the feature holds the name as text. Pure.
 */
export function testOnlyNamesInBytes(bytes: Uint8Array): readonly string[] {
  const buffer = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  return TEST_ONLY_EXPORTS.filter((name) => buffer.includes(name, 0, "latin1"));
}

/** The problem of the core state that a loaded addon reports, or `none` when it is `not_started`. Pure. */
export function coreStateProblem(answer: NativeEnvelope<CoreState>): string | "none" {
  switch (answer.kind) {
    case "value":
      return answer.value === "not_started" ? "none" : `coreState() is ${answer.value}, not not_started`;
    case "thrown":
      return `coreState() threw ${answer.thrown.code}`;
  }
}
