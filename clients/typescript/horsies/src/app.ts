// The app: the contract list, the config, and the members that send, read and register implementations. Each area of
// the package adds its members in one block here.

import { implementationOf, type BodyFor, type Implementation, type ImplementOptions } from "./contract/implementation.ts";
import type { ExecutionClass, TaskContract } from "./contract/define-task.ts";
import type { TaskId } from "./contract/ids.ts";
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

/** The constructor of an app, for `createHorsies` in this module. */
let constructApp: (context: AppContext) => Horsies;

/** A horsies app. `Horsies.create` builds it. */
export class Horsies {
  readonly #context: AppContext;

  static {
    constructApp = (context: AppContext): Horsies => new Horsies(context);
  }

  private constructor(context: AppContext) {
    this.#context = context;
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
