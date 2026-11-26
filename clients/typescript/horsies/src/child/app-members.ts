// The child members of an app: the providers of app state (`app.provide`) and the start hooks of a task child
// (`app.onChildStart`). Each call records the member and calls nothing. Once `cli.run` serves a child, a new member is
// a defect: the start hooks of the child are fixed.
//
// In a task child the providers run first, in registration order, then the `onChildStart` hooks, each as one start
// hook of the core with its own bound of 10 s. A provider or hook that throws, rejects or runs past its bound ends the
// child with 173, runs no later hook, and stops the worker with `CHILD_HOOK_FAILED`.

import type { AppContext } from "../app/context.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { type StateKey, type StateKeyIdentity, stateKeyNamed } from "./state.ts";

/** The factory of one provider: it builds the value in each task child, sync or async. */
export type ProviderFactory<T> = () => T | Promise<T>;

/** A start hook of a task child, sync or async. */
export type ChildStartHook = () => void | Promise<void>;

/** One provider of an app: its key (by identity), the name that its failure gives, and its factory. */
export type ProviderMember = {
  readonly key: StateKeyIdentity;
  readonly name: string;
  readonly factory: ProviderFactory<unknown>;
};

/** One `onChildStart` hook of an app, with the name that its failure gives. */
export type StartHookMember = { readonly name: string; readonly hook: ChildStartHook };

/** The child members of an app. `phase` is `serving` once `cli.run` serves a child. */
type ChildMembers = {
  readonly providers: ProviderMember[];
  readonly hooks: StartHookMember[];
  phase: "open" | "serving";
};

const members = new WeakMap<AppContext, ChildMembers>();

function membersOf(context: AppContext): ChildMembers {
  const kept = members.get(context);

  if (kept !== undefined) {
    return kept;
  }

  const created: ChildMembers = { providers: [], hooks: [], phase: "open" };

  members.set(context, created);

  return created;
}

/** The members of `context`, open for a new member. A new member of a served child is a defect: `ClientDefect`. */
function openMembers(context: AppContext, subject: string): ChildMembers {
  const kept = membersOf(context);

  switch (kept.phase) {
    case "open":
      return kept;
    case "serving":
      throw new ClientDefect({ subject, rule: `${subject} is called before cli.run serves the child` });
    default: {
      const unexpected: never = kept.phase;

      throw new ClientDefect({ subject: "exhaustive switch", rule: `a member phase outside its type: ${String(unexpected)}` });
    }
  }
}

/** The name of a member: its position and the function's name. */
function memberName(kind: string, position: number, function_: { readonly name: string }): string {
  return `${kind} #${String(position)} ${function_.name === "" ? "(anonymous)" : function_.name}`;
}

/** Add a provider to `context` and give its key. Calls nothing. */
export function provideFor<T>(context: AppContext, factory: ProviderFactory<T>): StateKey<Awaited<T>> {
  const open = openMembers(context, "app.provide");
  const name = memberName("provide", open.providers.length + 1, factory);
  const key = stateKeyNamed<Awaited<T>>(name);

  open.providers.push(Object.freeze({ key, name, factory }));

  return key;
}

/** Add a start hook of a task child to `context`. Calls nothing. */
export function onChildStartFor(context: AppContext, hook: ChildStartHook): void {
  const open = openMembers(context, "app.onChildStart");

  open.hooks.push(Object.freeze({ name: memberName("onChildStart", open.hooks.length + 1, hook), hook }));
}

/** The providers and the start hooks of `context`, in registration order. */
export type ChildMembersView = {
  readonly providers: readonly ProviderMember[];
  readonly hooks: readonly StartHookMember[];
};

/** Close the members of `context` for the serve of a child, and give them. A later member is a defect. */
export function servedMembers(context: AppContext): ChildMembersView {
  const kept = membersOf(context);

  kept.phase = "serving";

  return Object.freeze({ providers: Object.freeze([...kept.providers]), hooks: Object.freeze([...kept.hooks]) });
}

/** The providers of `context`, in registration order, with no change of its phase. */
export function providersOf(context: AppContext): readonly ProviderMember[] {
  return Object.freeze([...membersOf(context).providers]);
}
