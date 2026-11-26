// A fake of the addon calls of child mode: each answer that a test sets, and each call that the child made.

import type { ChildNative } from "../../../horsies/src/child/host.ts";
import type {
  BodySettle,
  ChildArgumentsCheck,
  ChildHostPlan,
  ChildPost,
  ChildServeEnd,
  RunCancelStateRecord,
  SpawnEnd,
  SpawnRegistration,
  StartHookCall,
} from "../../../horsies/src/native/child/addon.ts";
import type { HandlerChildPlan } from "../../../horsies/src/native/handler-child/addon.ts";
import type { AppConfigCheck, NativeTaskError, TaskErrorBuild, TaskErrorParts } from "../../../horsies/src/native/sends/addon.ts";
import type { PanicRule } from "../../../horsies/src/thrown-errors.ts";
import { fakeSendRules } from "../app/support.ts";

/** The task error that the fake builds: the stored text names the parts. */
export class FakeTaskError implements NativeTaskError {
  readonly parts: TaskErrorParts;

  constructor(parts: TaskErrorParts) {
    this.parts = parts;
  }

  storedText(): { readonly kind: "value"; readonly value: string } {
    return { kind: "value", value: JSON.stringify(this.parts) };
  }
}

/** What the fake's serve got. */
export type ServeCall = {
  readonly plan: ChildHostPlan;
  readonly post: (post: ChildPost) => void;
  readonly hook: (callId: bigint, call: StartHookCall) => void;
};

/** A fake of the addon calls of child mode. */
export class FakeChildNative implements ChildNative {
  arguments: ChildArgumentsCheck = { kind: "child" };
  refusedCodes: ReadonlySet<string> = new Set();
  settle: BodySettle = "settled";
  readonly calls: string[] = [];
  readonly values: [bigint, string][] = [];
  readonly errors: [bigint, TaskErrorParts][] = [];
  readonly builds: TaskErrorParts[] = [];
  readonly serves: ServeCall[] = [];
  readonly handlerPlans: HandlerChildPlan[] = [];
  #end: (end: ChildServeEnd) => void = () => undefined;

  /** End the serve with `code`. */
  endServe(code: number): void {
    this.#end({ code, exit: code === 0 ? "STOPPED" : "CHILD_HOOK_FAILED" });
  }

  childArguments(): ChildArgumentsCheck {
    this.calls.push("childArguments");

    return this.arguments;
  }

  fillHandlerChild(plan: HandlerChildPlan): void {
    this.calls.push("fillHandlerChild");
    this.handlerPlans.push(plan);
  }

  async serveChild(
    plan: ChildHostPlan,
    post: (post: ChildPost) => void,
    hook: (callId: bigint, call: StartHookCall) => void,
  ): Promise<ChildServeEnd> {
    this.calls.push("serveChild");
    this.serves.push({ plan, post, hook });

    return new Promise((resolve) => {
      this.#end = resolve;
    });
  }

  settleBodyValue(runKey: bigint, json: string): BodySettle {
    this.values.push([runKey, json]);

    return this.settle;
  }

  settleBodyError(runKey: bigint, error: NativeTaskError): BodySettle {
    this.errors.push([runKey, error instanceof FakeTaskError ? error.parts : JSON.parse("null")]);

    return this.settle;
  }

  buildTaskError(parts: TaskErrorParts): TaskErrorBuild {
    this.builds.push(parts);

    return parts.code.kind === "custom" && this.refusedCodes.has(parts.code.code)
      ? { kind: "refused", cause: "the code breaks a rule" }
      : { kind: "built", error: new FakeTaskError(parts) };
  }

  runCancelStateOf(): RunCancelStateRecord {
    return { kind: "notRequested" };
  }

  registerSpawn(): SpawnRegistration {
    return { kind: "registered", workKey: 90n };
  }

  endSpawn(): SpawnEnd {
    return "ended";
  }

  loopTurned(): void {
    this.calls.push("loopTurned");
  }

  loopHold(taskId: string): void {
    this.calls.push(`loopHold ${taskId}`);
  }

  loopRelease(): void {
    this.calls.push("loopRelease");
  }

  setPanicRule(rule: PanicRule): void {
    this.calls.push(`setPanicRule ${rule}`);
  }

  checkAppConfig(): AppConfigCheck {
    return {
      kind: "accepted",
      rules: fakeSendRules(),
      manifestJson: '{"manifest":"of the app"}',
      installCoverage: { historyHorizonDays: 30, heartbeatHorizonHours: 24, declaredPeriodDays: [] },
    };
  }
}

/** Wait until the queued Promise jobs and the timers of 0 ms ran. */
export async function settled(): Promise<void> {
  for (let turn = 0; turn < 5; turn += 1) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}
