// The internal view of an app for the other areas of the package (the worker parent, the child process, the events
// and the schedules). It is not exported from an entry point.

import type { AppContext } from "./context.ts";

const contexts = new WeakMap<object, AppContext>();

/** Keep the context of an app. Called once, by the app's constructor. */
export function recordAppContext(app: object, context: AppContext): void {
  contexts.set(app, context);
}

/** The context of an app. A value that `Horsies.create` did not build is a defect: `TypeError`. */
export function appInternals(app: object): AppContext {
  const context = contexts.get(app);

  if (context === undefined) {
    throw new TypeError("horsies: the value is not an app (build it with Horsies.create)");
  }

  return context;
}
