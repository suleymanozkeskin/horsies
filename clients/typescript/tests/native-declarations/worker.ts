// The generated declarations of the `worker` area match the hand-kept ones, function by function and method by
// method (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  ConnectivityStep,
  ListenAddressCheck,
  LiveCheckOutcome,
  LogBacklog,
  NativePreparedWorker,
  SeatIsolateMemory,
  WorkerAddon,
  WorkerBootCauseName,
  WorkerFailCauseName,
  WorkerPreparation,
  WorkerRunEnded,
} from "../../horsies/src/native/worker/addon.ts";
import type { ManifestProbeAddon, WorkerManifestProbe } from "../runtime/native/manifest-probe.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const parseListenAddressMatches: SameFunction<
  typeof Generated.parseListenAddress,
  WorkerAddon["parseListenAddress"]
> = true;

export const prepareWorkerMatches: SameFunction<typeof Generated.prepareWorker, WorkerAddon["prepareWorker"]> = true;
export const runWorkerMatches: SameFunction<typeof Generated.runWorker, WorkerAddon["runWorker"]> = true;
export const checkLiveMatches: SameFunction<typeof Generated.checkLive, WorkerAddon["checkLive"]> = true;

export const workerLogBacklogMatches: SameFunction<typeof Generated.workerLogBacklog, WorkerAddon["workerLogBacklog"]> =
  true;

// The class, method by method, and its member set: no method on one side only.
export const requestStopMatches: SameFunction<
  Generated.PreparedWorker["requestStop"],
  NativePreparedWorker["requestStop"]
> = true;

export const preparedWorkerMatches: Mutual<Generated.PreparedWorker, NativePreparedWorker> = true;

export const listenAddressMatches: Mutual<Generated.ListenAddressCheck, ListenAddressCheck> = true;
export const preparationMatches: Mutual<Generated.WorkerPreparation, WorkerPreparation> = true;
export const seatIsolateMemoryMatches: Mutual<Generated.SeatIsolateMemory, SeatIsolateMemory> = true;
export const bootCauseMatches: Mutual<Generated.WorkerBootCauseName, WorkerBootCauseName> = true;
export const failCauseMatches: Mutual<Generated.WorkerFailCauseName, WorkerFailCauseName> = true;
export const runEndedMatches: Mutual<Generated.WorkerRunEnded, WorkerRunEnded> = true;
export const stepMatches: Mutual<Generated.ConnectivityStep, ConnectivityStep> = true;
export const liveCheckMatches: Mutual<Generated.LiveCheckOutcome, LiveCheckOutcome> = true;
export const backlogMatches: Mutual<Generated.LogBacklog, LogBacklog> = true;

// The test export of a build with the cargo feature `test-probes` (`build:dev`).
export const probeParseWorkerManifestMatches: SameFunction<
  typeof Generated.probeParseWorkerManifest,
  ManifestProbeAddon["probeParseWorkerManifest"]
> = true;

export const workerManifestProbeMatches: Mutual<Generated.WorkerManifestProbe, WorkerManifestProbe> = true;
