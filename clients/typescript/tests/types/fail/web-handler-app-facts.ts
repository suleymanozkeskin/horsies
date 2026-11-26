// expect: TS2740 at 13; floor: TS2345 at 13
// `createMonitoringHandler` takes an app of `Horsies.create`, whose one core handle holds the app's child spec source.
// It takes no handle and no object of app facts, so a handle with no child spec source cannot reach it.

import { allowAll, createMonitoringHandler } from "../../../horsies/src/index.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";

declare const handle: CoreHandle;

const facts = { coreHandle: handle, queueNames: ["default"], scheduleNames: [], clientVersion: "0.0.0" };

// The facts of an app, with a handle that may hold no child spec source.
export const handler = createMonitoringHandler(facts, { auth: allowAll() });
