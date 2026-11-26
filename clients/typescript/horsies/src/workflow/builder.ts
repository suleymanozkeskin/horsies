// The builder `w` of a workflow build, and one run of a build.
//
// The builder records each node as it is given and checks nothing: the checks of `checks.ts` read the recorded nodes
// after the build returns, so every finding names its node. A node can name only nodes that the build made before it,
// so a build has no cycle and its first node is a root.

import { NODE_INDEX_MAX } from "../_catalog/workflow_spec.ts";
import { isFunction, isObjectLike } from "../codec/values.ts";
import type { ContextUse, ExecutionClass, TaskContract } from "../contract/define-task.ts";
import type { OutputSchema, ParameterSchemas, SchemaValue } from "../contract/schema.ts";
import type { WorkflowContract } from "./define.ts";
import type {
  AnyNode,
  BuildReturn,
  DefaultJoin,
  Join,
  NodeInput,
  NodeRef,
  SubWorkflowNodeOptions,
  TaskNodeOptions,
  WorkflowOutput,
} from "./node.ts";

/** The builder of a workflow build. */
export interface WorkflowBuilder {
  /** A node that runs the task of `contract`. Each parameter takes a value, or a node for an `h.resultOf` one. */
  task<
    P extends ParameterSchemas,
    O extends OutputSchema,
    E extends string,
    X extends ExecutionClass,
    C extends ContextUse,
    const Id extends string,
    const J extends Join = DefaultJoin,
  >(
    contract: TaskContract<P, O, E, X, C>,
    input: NoInfer<NodeInput<P, J>>,
    options: TaskNodeOptions<Id, J, C>,
  ): NodeRef<SchemaValue<O>, E, Id>;
  /** A node that runs the workflow of `workflow` as a child. A child with `params: {}` takes `{}`. */
  subWorkflow<
    P extends ParameterSchemas,
    R extends BuildReturn,
    const Id extends string,
    const J extends Join = DefaultJoin,
  >(
    workflow: WorkflowContract<P, R>,
    params: NoInfer<NodeInput<P, J>>,
    options: SubWorkflowNodeOptions<Id, J>,
  ): NodeRef<WorkflowOutput<R>, string, Id>;
}

/** The options of a node as the build gave them, before the checks. Every node option type is one of these. */
export type RecordedOptions = {
  readonly id: string;
  readonly waitsFor?: readonly AnyNode[];
  readonly join?: Join;
  readonly allowFailedDeps?: boolean;
  readonly queue?: string;
  readonly priority?: number;
  readonly goodUntil?: Temporal.Instant;
  readonly ctxFrom?: readonly AnyNode[];
};

/** One node as the build gave it. The input and the options are the given values, not checked. */
export type RecordedNode =
  | { readonly kind: "task"; readonly contract: object; readonly input: object; readonly options: RecordedOptions }
  | {
      readonly kind: "subWorkflow";
      readonly workflow: object;
      readonly input: object;
      readonly options: RecordedOptions;
    };

/** The nodes of one run of a build, in the order of the calls. */
/** The most nodes of one workflow (the core's `MAX_WORKFLOW_NODES`). */
export const MAX_WORKFLOW_NODES: number = NODE_INDEX_MAX + 1;

export class BuildSession {
  readonly #nodes: RecordedNode[] = [];
  #made = 0;
  #state: "open" | "sealed" = "open";

  /** The recorded nodes: at most `MAX_WORKFLOW_NODES + 1`, so the checks see that a build made too many. */
  nodes(): readonly RecordedNode[] {
    return this.#nodes;
  }

  /** How many nodes the build made, the ones past the record bound too. */
  madeCount(): number {
    return this.#made;
  }

  /**
   * Record a node and give its reference. Past `MAX_WORKFLOW_NODES + 1` nodes the session keeps no more nodes, so a
   * build that makes too many holds no growing record; the checks refuse the build (`HRS-038`) before they read a
   * reference. A call after the build returned is a defect: `TypeError`.
   */
  add(node: RecordedNode): AnyNode {
    if (this.#state === "sealed") {
      throw new TypeError("horsies: a workflow node was added after its build returned; a build is sync");
    }

    const index = this.#made;

    this.#made += 1;

    if (this.#nodes.length <= MAX_WORKFLOW_NODES) {
      this.#nodes.push(node);
    }

    return nodeRefOf(this, index, node.options.id);
  }

  /** End the build: no node is added after it. */
  seal(): void {
    this.#state = "sealed";
  }
}

/** Where a node reference points: a node of a build, or nothing (a value that no builder gave). */
export type NodeSlot =
  | { readonly kind: "node"; readonly session: BuildSession; readonly index: number }
  | { readonly kind: "notANode" };

const slots = new WeakMap<object, { readonly session: BuildSession; readonly index: number }>();

const NOT_A_NODE: NodeSlot = Object.freeze({ kind: "notANode" });

function nodeRefOf(session: BuildSession, index: number, id: string): AnyNode {
  // SAFETY: `nodeTypes` is a type-only key; the node's types are the type parameters of the builder call.
  const ref = Object.freeze({ kind: "node", id }) as AnyNode;

  slots.set(ref, Object.freeze({ session, index }));

  return ref;
}

/** The node that `value` refers to, or `notANode` for an object that no builder gave. Pure. */
export function nodeSlot(value: object): NodeSlot {
  const slot = slots.get(value);

  return slot === undefined ? NOT_A_NODE : Object.freeze({ kind: "node", ...slot });
}

/** The builder that records into `session`. */
function builderOf(session: BuildSession): WorkflowBuilder {
  const builder: WorkflowBuilder = {
    task: (contract, input, options) =>
      // SAFETY: the node's types are the type parameters of this call; `nodeTypes` is a type-only key.
      session.add({ kind: "task", contract, input: Object(input), options }) as never,
    subWorkflow: (workflow, input, options) =>
      // SAFETY: the node's types are the type parameters of this call; `nodeTypes` is a type-only key.
      session.add({ kind: "subWorkflow", workflow, input: Object(input), options }) as never,
  };

  return Object.freeze(builder);
}

/** The end of one run of a build. */
export type BuildRun =
  /** The build returned a value that is no thenable. */
  | { readonly kind: "returned"; readonly session: BuildSession; readonly returned: unknown }
  /** The build threw; `cause` is the thrown value. */
  | { readonly kind: "threw"; readonly cause: unknown }
  /** The build returned a thenable: a build is sync. */
  | { readonly kind: "thenable" };

/**
 * Run `build` with a new builder and `params`. Never throws: a throw is `threw`. A returned thenable is `thenable`; its
 * rejection is handled here, so it is not an unhandled rejection. Effects: the user's build.
 */
export function runBuild(build: (w: WorkflowBuilder, params: never) => unknown, params: object): BuildRun {
  const session = new BuildSession();
  let returned: unknown;

  try {
    // SAFETY: the caller decoded `params` with the params' kind tree of the build's own definition.
    returned = build(builderOf(session), params as never);
  } catch (cause) {
    return { kind: "threw", cause };
  } finally {
    session.seal();
  }

  if (isThenable(returned)) {
    const _handled = Promise.resolve(returned).catch(() => undefined);

    return { kind: "thenable" };
  }

  return { kind: "returned", session, returned };
}

/** Whether a returned value is a thenable. Pure. */
function isThenable(value: unknown): value is PromiseLike<unknown> {
  return isObjectLike(value) && "then" in value && isFunction(value.then);
}
