// The nodes of a workflow build: the node reference that `w.task` and `w.subWorkflow` give, the node options, the
// input of a node (a value or an upstream node per parameter), and the object that a build returns.
//
// A node reference is a frozen `{ kind: "node", id }`. Its build and its place in the build are kept out of its public
// members, so a reference of another build is a check finding, not a type error (one TypeScript type serves every
// build).

import type { ContextUse } from "../contract/define-task.ts";
import type { IsUpstreamOutcome, ParameterSchemas } from "../contract/schema.ts";
import type { AnyWire, EmptyObject, Flat, ValueOf } from "../wire/infer.ts";
import type { TaskError } from "../wire/task-error.ts";
import type { ParameterSent, SenderMayLeaveOut } from "../zod/infer.ts";

/** Type-only key of the types of a node. It does not exist at run time. */
export declare const nodeTypes: unique symbol;

/** The output type and the code type of a node. */
export type NodeTypes<O, E extends string> = { readonly output: O; readonly code: E };

/** A node of a workflow build. `O` is its output type, `E` the type of its codes, `Id` its node id. */
export interface NodeRef<O, E extends string, Id extends string> {
  readonly kind: "node";
  /** The node id: the key of the node in the stored workflow and in `resultFor`. */
  readonly id: Id;
  readonly [nodeTypes]: NodeTypes<O, E>;
}

/** Any node: the constraint of a generic parameter. */
export type AnyNode = NodeRef<unknown, string, string>;

/** When a node with dependencies runs: all of them ended, any one succeeded, or `minSuccess` of them succeeded. */
export type Join =
  | { readonly kind: "all" }
  | { readonly kind: "any" }
  | { readonly kind: "quorum"; readonly minSuccess: number };

/** The join of a node that names none. */
export type DefaultJoin = { readonly kind: "all" };

/** The text that a required parameter fed by a node under an `any` or `quorum` join gets as its type. */
export type JoinNeedsOptional<K> =
  `horsies: under an "any" or "quorum" join, parameter '${K & string}' must be declared .optional()`;

/** The success value of an upstream outcome parameter: `h.taskOutcome(contract)` gives the contract's output. */
export type UpstreamValue<S> = S extends AnyWire
  ? Extract<ValueOf<S>, { readonly kind: "succeeded" }> extends { readonly value: infer T }
    ? T
    : never
  : never;

/**
 * The custom codes of an upstream outcome parameter: `h.taskOutcome(contract)` gives the codes of a task contract, or
 * any code (`string`) for a workflow contract or a task with no `errorCodes`.
 */
export type UpstreamCode<S> = S extends AnyWire
  ? Extract<ValueOf<S>, { readonly kind: "failed" }> extends { readonly error: TaskError<infer E> }
    ? E
    : never
  : never;

/**
 * The value of one parameter of a node: a value of the parameter's type, or, for a parameter declared
 * `h.taskOutcome(contract)`, a node whose output and codes fit those of the contract. Under an `any` or `quorum` join,
 * such a parameter must be optional: its source may not have ended.
 */
export type NodeParameter<S, J extends Join, K> =
  IsUpstreamOutcome<S> extends true
    ? J extends { readonly kind: "any" } | { readonly kind: "quorum" }
      ? SenderMayLeaveOut<S> extends true
        ? NodeRef<UpstreamValue<S>, UpstreamCode<S>, string>
        : JoinNeedsOptional<K>
      : NodeRef<UpstreamValue<S>, UpstreamCode<S>, string>
    : ParameterSent<S>;

/** The input of a node: one member per parameter; an optional or defaulted one may be left out. */
export type NodeInput<P extends ParameterSchemas, J extends Join> = keyof P extends never
  ? EmptyObject
  : Flat<
      {
        -readonly [K in keyof P as SenderMayLeaveOut<P[K]> extends true ? never : K]: NodeParameter<P[K], J, K>;
      } & {
        -readonly [K in keyof P as SenderMayLeaveOut<P[K]> extends true ? K : never]?: NodeParameter<P[K], J, K>;
      }
    >;

/** The options of every node. */
export interface NodeOptionsBase<Id extends string, J extends Join> {
  /** The node id: `[A-Za-z0-9_\-:.]+`, at most 128 characters, unique in the build. */
  readonly id: Id;
  /** Nodes of this build that this node waits for, besides the sources of its parameters. */
  readonly waitsFor?: readonly AnyNode[];
  /** When the node runs. Left out: `{ kind: "all" }`. */
  readonly join?: J;
  /** Run the node when a dependency failed (its parameter gets the failure). Left out: the node is `SKIPPED`. */
  readonly allowFailedDeps?: boolean;
}

/** The options of a task node. `ctxFrom` exists only for a task that takes the workflow context. */
export type TaskNodeOptions<Id extends string, J extends Join, C extends ContextUse> = NodeOptionsBase<Id, J> & {
  /** A queue that the app admits. Left out: the task's queue. */
  readonly queue?: string;
  /** An integer 1 to 100. Left out: the priority of the queue. */
  readonly priority?: number;
  /** The node's task expires if no worker starts it before this instant. */
  readonly goodUntil?: Temporal.Instant;
} & ([C] extends ["takesContext"]
    ? {
        /** Nodes of `waitsFor` whose results the task's workflow context holds. */
        readonly ctxFrom?: readonly AnyNode[];
      }
    : { readonly ctxFrom?: never });

/** The options of a sub-workflow node. */
export type SubWorkflowNodeOptions<Id extends string, J extends Join> = NodeOptionsBase<Id, J>;

/** One way for a workflow to succeed: every node of `required` completed. */
export interface SuccessCase {
  readonly required: readonly AnyNode[];
}

/**
 * When a workflow succeeds: when every required node of one case completed. `optional` records the nodes that may
 * fail; it has no effect at run time.
 */
export interface SuccessPolicy {
  readonly cases: readonly SuccessCase[];
  readonly optional?: readonly AnyNode[];
}

/** What a build returns: the output node, and a success policy. */
export interface BuildReturn {
  /** The node whose result is the workflow's result. */
  readonly output: AnyNode;
  readonly successPolicy?: SuccessPolicy;
}

/** The output type of a node. */
export type NodeOutput<N> = N extends NodeRef<infer O, string, string> ? O : never;

/** The code type of a node. */
export type NodeCode<N> = N extends NodeRef<unknown, infer E, string> ? E : never;

/** The nodes of a success policy. */
export type PolicyNodes<S> = S extends SuccessPolicy
  ?
      | S["cases"][number]["required"][number]
      | (S extends { readonly optional: infer Q extends readonly AnyNode[] } ? Q[number] : never)
  : never;

/** The nodes in the object that a build returns: the typed set of `resultFor`. */
export type ReturnedNodes<R extends BuildReturn> =
  | R["output"]
  | (R extends { readonly successPolicy: infer S } ? PolicyNodes<S> : never);

/** The node of `N` with the id `Id`. */
export type NodeById<N, Id extends string> =
  N extends NodeRef<unknown, string, infer NodeId> ? (Id extends NodeId ? N : never) : never;

/** The output type of a workflow: the output type of its output node. */
export type WorkflowOutput<R extends BuildReturn> = NodeOutput<R["output"]>;
