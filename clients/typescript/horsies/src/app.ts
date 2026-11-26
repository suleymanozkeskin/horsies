// The app: the contract list, the config, and the members that send, read and register implementations. Each area of
// the package adds its members in one block here.

import { implementationOf, type BodyFor, type Implementation, type ImplementOptions } from "./contract/implementation.ts";
import type { ExecutionClass, TaskContract } from "./contract/define-task.ts";
import type { TaskId, WorkflowId } from "./contract/ids.ts";
import type { OutputSchema, ParameterSchemas, SchemaValue, SendableInput } from "./contract/schema.ts";
import { readFormOf, type TaskHandle, taskHandleOf } from "./read/handle.ts";
import type { RetryableSendError, TaskSendError } from "./send/errors.ts";
import type { SendOptions } from "./send/options.ts";
import { retrySend, sendTask } from "./send/send.ts";
import { err, ok, type Result } from "./wire/result.ts";
import type { AppContext } from "./app/context.ts";
import {
  createApp,
  type CreateEnvironment,
  type CreateRefused,
  type HorsiesConfig,
  processEnvironment,
} from "./app/create.ts";
import { registerImplementation } from "./app/implement.ts";
import { recordAppContext } from "./app/internals.ts";
import { createWorkflowArea, type WorkflowArea } from "./workflow/app-area.ts";
import { workflowCoreOf } from "./workflow/core.ts";
import type { WorkflowContract } from "./workflow/define.ts";
import type { WorkflowHandle, WorkflowHandleById } from "./workflow/handle.ts";
import type { BuildReturn } from "./workflow/node.ts";
import { NO_OP_START_OBSERVER, type StartOptions } from "./workflow/start.ts";
import type { StartEnqueueFailed, WorkflowStartError } from "./workflow/start-error.ts";
import { type ChildStartHook, onChildStartFor, provideFor, type ProviderFactory } from "./child/app-members.ts";
import type { StateKey } from "./child/state.ts";

/** The constructor of an app, for `createHorsies` in this module. */
let constructApp: (context: AppContext) => Horsies;

/** A horsies app. `Horsies.create` builds it. */
export class Horsies {
  readonly #context: AppContext;
  readonly #workflows: WorkflowArea;

  static {
    constructApp = (context: AppContext): Horsies => new Horsies(context);
  }

  private constructor(context: AppContext) {
    this.#context = context;
    this.#workflows = createWorkflowArea({
      core: workflowCoreOf(context.core),
      registry: context.workflows,
      observer: NO_OP_START_OBSERVER,
      now: () => Temporal.Now.instant(),
      newWorkflowId: () => crypto.randomUUID(),
    });
    recordAppContext(this, context);
  }

  /**
   * The app of `config`, or every refusal: the config first, then the contracts in list order. On a main thread the
   * core checks the config and each contract (the addon loads; the core does not start); on a worker thread only the
   * checks that are TypeScript code run.
   */
  static create(config: HorsiesConfig): Result<Horsies, CreateRefused> {
    return createHorsies(config, processEnvironment());
  }

  /**
   * The implementation of a listed contract. Never throws and calls no addon function: a contract that the app does
   * not list, or a second implementation, is a refusal that `check` and the worker start report.
   */
  implement<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass>(
    contract: TaskContract<P, O, E, X>,
    body: NoInfer<BodyFor<P, O, E, X>>,
    options: ImplementOptions<NoInfer<E>> = {},
  ): Implementation<P, O, E, X> {
    const implementation = implementationOf(contract, body, options.exceptions ?? []);

    registerImplementation(this.#context, implementation);

    return implementation;
  }

  /**
   * Add a provider of app state and give its key; `runtime.state(key)` gives the value. Calls nothing: the factory
   * runs in each task child, before the child reports ready, in registration order and before the `onChildStart`
   * hooks, for at most 10 s. A factory that throws, rejects or runs past its bound stops the worker with
   * `CHILD_HOOK_FAILED` (exit 173). A call after `cli.run` serves a child is a defect: `ClientDefect`.
   */
  provide<T>(factory: ProviderFactory<T>): StateKey<Awaited<T>> {
    return provideFor(this.#context, factory);
  }

  /**
   * Run `hook` once in each task child, a replacement child included, after the providers and before the child
   * reports ready, for at most 10 s. A hook that throws, rejects or runs past its bound stops the worker with
   * `CHILD_HOOK_FAILED` (exit 173). A call after `cli.run` serves a child is a defect: `ClientDefect`.
   */
  onChildStart(hook: ChildStartHook): void {
    onChildStartFor(this.#context, hook);
  }

  /** Send a task: its handle, or the send error. A delay is the only start option. */
  async send<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass>(
    contract: TaskContract<P, O, E, X>,
    input: NoInfer<SendableInput<P>>,
    options: SendOptions = {},
  ): Promise<Result<TaskHandle<SchemaValue<O>, E>, TaskSendError<SchemaValue<O>, E>>> {
    return sendTask<SchemaValue<O>, E>(this.#context.send, contract, Object(input), options);
  }

  /**
   * Send again the payload of `ENQUEUE_FAILED` or `IDEMPOTENCY_KEY_CONFLICT`, under the same task id. Takes an error
   * of another app instance too, when this app lists its task.
   */
  async retrySend<O, E extends string>(error: RetryableSendError<O, E>): Promise<Result<TaskHandle<O, E>, TaskSendError<O, E>>> {
    return retrySend(this.#context.send, error);
  }

  /**
   * Start a workflow: its handle, or the start error. The same `workflowId` with the same params returns the existing
   * run; other params under the id give `WORKFLOW_COMMAND_CONFLICT`; another workflow under it `WORKFLOW_ID_CONFLICT`.
   */
  async start<P extends ParameterSchemas, R extends BuildReturn>(
    workflow: WorkflowContract<P, R>,
    params: NoInfer<SendableInput<P>>,
    options: StartOptions = {},
  ): Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>> {
    return this.#workflows.start(workflow, params, options);
  }

  /** Send again the spec of `ENQUEUE_FAILED`, under the same workflow id. */
  async retryStart<R extends BuildReturn>(
    error: StartEnqueueFailed<R>,
  ): Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>> {
    return this.#workflows.retryStart(error);
  }

  /** A typed handle of a stored workflow id, with no I/O. `resultFor` takes the node's contract. */
  workflowHandle<P extends ParameterSchemas, R extends BuildReturn>(
    workflow: WorkflowContract<P, R>,
    workflowId: WorkflowId,
  ): WorkflowHandleById<R> {
    return this.#workflows.workflowHandle(workflow, workflowId);
  }

  /** A typed handle of a stored task id, with no I/O. The contract gives the types only; the app need not list it. */
  taskHandle<P extends ParameterSchemas, O extends OutputSchema, E extends string, X extends ExecutionClass>(
    contract: TaskContract<P, O, E, X>,
    taskId: TaskId,
  ): TaskHandle<SchemaValue<O>, E> {
    return taskHandleOf(this.#context.core, readFormOf(contract), taskId);
  }
}

/** The app of `config` in the process facts `environment` (a test gives its own). Not exported from an entry point. */
export function createHorsies(config: HorsiesConfig, environment: CreateEnvironment): Result<Horsies, CreateRefused> {
  const created = createApp(config, environment);

  return created.ok ? ok(constructApp(created.value)) : err(created.error);
}
