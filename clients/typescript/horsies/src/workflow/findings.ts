// The findings of the spec checks, and the checked spec that a build gives when it has none. The checked spec is plain
// data (no function, no contract object), so a seat isolate can send it to the main thread of its child.

import type { ErrorCode } from "../_catalog/error_codes.ts";
import type { SpecOnError, SpecSubWorkflowParams } from "../_catalog/workflow_spec.ts";
import type { RefusalCode } from "../app/refusal.ts";

/** A finding of the spec checks: the code, the place in the build (`nodes[2] charge`), the rule and the fixes. */
export type SpecFinding = {
  readonly code: RefusalCode;
  readonly place: string;
  readonly detail: string;
  readonly help: readonly string[];
};

/** A frozen finding. Pure. */
export function finding(
  code: ErrorCode | "FIELD_BOUND",
  place: string,
  detail: string,
  help: readonly string[],
): SpecFinding {
  return Object.freeze({ code, place, detail, help: Object.freeze([...help]) });
}

/** The join of a checked node. */
export type CheckedJoin =
  | { readonly kind: "all" }
  | { readonly kind: "any" }
  | { readonly kind: "quorum"; readonly minSuccess: number };

/** The deadline of a task node: none, or an instant in epoch microseconds (cut toward the past). */
export type NodeDeadline = { readonly kind: "noDeadline" } | { readonly kind: "until"; readonly epochMicros: bigint };

/** What a checked node runs. */
export type CheckedBody =
  | {
      readonly kind: "task";
      readonly queue: string;
      readonly priority: number;
      readonly deadline: NodeDeadline;
    }
  | { readonly kind: "subWorkflow"; readonly definitionKey: string; readonly params: SpecSubWorkflowParams };

/** One node of a checked spec. */
export type CheckedNode = {
  readonly index: number;
  readonly nodeId: string;
  /** The task name, or the child's workflow name. */
  readonly name: string;
  /** The stored static members (`{}` for a child that takes no params). */
  readonly staticInput: string;
  /** The parameter sources in the declaration order of the parameters, then `waitsFor`, each node once. */
  readonly dependencies: readonly number[];
  /** The parameter that each upstream node fills, in the declaration order of the parameters. */
  readonly argsFrom: ReadonlyMap<string, number>;
  /** The node ids of the context sources. */
  readonly ctxFrom: readonly string[];
  readonly allowFailedDeps: boolean;
  readonly join: CheckedJoin;
  readonly body: CheckedBody;
};

/** The success policy of a checked spec: none, or the cases and the optional nodes as node indexes. */
export type CheckedPolicy =
  | { readonly kind: "noPolicy" }
  | {
      readonly kind: "cases";
      readonly cases: readonly (readonly number[])[];
      readonly optional: readonly number[];
    };

/** A spec that passed both gates of the checks. Plain data. */
export type CheckedSpec = {
  readonly name: string;
  readonly definitionKey: string;
  readonly onError: SpecOnError;
  readonly nodes: readonly CheckedNode[];
  /** The index of the output node. */
  readonly output: number;
  readonly successPolicy: CheckedPolicy;
};
