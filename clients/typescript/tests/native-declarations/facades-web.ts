// The generated declarations of the monitoring web handler and the fleet metrics server of the
// `facades` area match the hand-kept ones, function by function and method by method (arguments
// and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  FleetMetricsAddon,
  FleetMetricsRunEnd,
  FleetMetricsSettings,
  MetricsListenAddressCheck,
  NativeFleetMetricsServer,
} from "../../horsies/src/native/facades/fleet-metrics.ts";
import type {
  AccessGrantArg,
  MonitoringEventStep,
  MonitoringHeaderRecord,
  MonitoringHandlerSettings,
  MonitoringRequestArg,
  MonitoringResponseRecord,
  NativeEventStream,
  NativeMonitoringHandler,
  ResponseFormName,
  RouteAccessRecord,
  RouteNeed,
  TraceLinkCheck,
  TraceLinkRefusalRecord,
  AccessVerdict,
  WebHandlerAddon,
} from "../../horsies/src/native/facades/web-handler.ts";
import type { ActionsSwitch } from "../../horsies/src/web/auth.ts";
import type { WebProbeAddon } from "../runtime/native/web-probe.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

type Handler = Generated.MonitoringHandler;

type Stream = Generated.MonitoringEventStream;

type Server = Generated.FleetMetricsServer;

export const buildMonitoringHandlerMatches: SameFunction<
  typeof Generated.buildMonitoringHandler,
  WebHandlerAddon["buildMonitoringHandler"]
> = true;

export const checkTraceLinkTemplateMatches: SameFunction<
  typeof Generated.checkTraceLinkTemplate,
  WebHandlerAddon["checkTraceLinkTemplate"]
> = true;

export const logMonitoringFailureMatches: SameFunction<
  typeof Generated.logMonitoringFailure,
  WebHandlerAddon["logMonitoringFailure"]
> = true;

export const maxRequestBodyBytesMatches: SameFunction<
  Handler["maxRequestBodyBytes"],
  NativeMonitoringHandler["maxRequestBodyBytes"]
> = true;

export const routeAccessMatches: SameFunction<Handler["routeAccess"], NativeMonitoringHandler["routeAccess"]> = true;
export const handleMatches: SameFunction<Handler["handle"], NativeMonitoringHandler["handle"]> = true;

export const subscribeEventsMatches: SameFunction<
  Handler["subscribeEvents"],
  NativeMonitoringHandler["subscribeEvents"]
> = true;

export const closeEventsMatches: SameFunction<Handler["closeEvents"], NativeMonitoringHandler["closeEvents"]> = true;
export const nextFrameMatches: SameFunction<Stream["nextFrame"], NativeEventStream["nextFrame"]> = true;
export const streamCloseMatches: SameFunction<Stream["close"], NativeEventStream["close"]> = true;

export const parseMetricsListenAddressMatches: SameFunction<
  typeof Generated.parseMetricsListenAddress,
  FleetMetricsAddon["parseMetricsListenAddress"]
> = true;

export const prepareFleetMetricsMatches: SameFunction<
  typeof Generated.prepareFleetMetrics,
  FleetMetricsAddon["prepareFleetMetrics"]
> = true;

// The test export of a build with the cargo feature `test-probes` (`build:dev`).
export const setProbeChildSpecSourceMatches: SameFunction<
  typeof Generated.setProbeChildSpecSource,
  WebProbeAddon["setProbeChildSpecSource"]
> = true;

export const runMatches: SameFunction<Server["run"], NativeFleetMetricsServer["run"]> = true;
export const requestStopMatches: SameFunction<Server["requestStop"], NativeFleetMetricsServer["requestStop"]> = true;

// The member sets: no method on one side only.
export const handlerMatches: Mutual<Handler, NativeMonitoringHandler> = true;
export const streamMatches: Mutual<Stream, NativeEventStream> = true;
export const serverMatches: Mutual<Server, NativeFleetMetricsServer> = true;

export const actionsMatches: Mutual<Generated.ActionsSwitch, ActionsSwitch> = true;
export const needMatches: Mutual<Generated.RouteNeed, RouteNeed> = true;
export const formMatches: Mutual<Generated.ResponseFormName, ResponseFormName> = true;
export const accessMatches: Mutual<Generated.RouteAccessRecord, RouteAccessRecord> = true;
export const verdictMatches: Mutual<Generated.AccessVerdict, AccessVerdict> = true;
export const grantMatches: Mutual<Generated.AccessGrantArg, AccessGrantArg> = true;
export const requestMatches: Mutual<Generated.MonitoringRequestArg, MonitoringRequestArg> = true;
export const headerMatches: Mutual<Generated.MonitoringHeaderRecord, MonitoringHeaderRecord> = true;
export const responseMatches: Mutual<Generated.MonitoringResponseRecord, MonitoringResponseRecord> = true;
export const stepMatches: Mutual<Generated.MonitoringEventStep, MonitoringEventStep> = true;
export const settingsMatches: Mutual<Generated.MonitoringHandlerSettings, MonitoringHandlerSettings> = true;
export const traceLinkCheckMatches: Mutual<Generated.TraceLinkCheck, TraceLinkCheck> = true;
export const traceLinkRefusalMatches: Mutual<Generated.TraceLinkRefusalRecord, TraceLinkRefusalRecord> = true;
export const listenCheckMatches: Mutual<Generated.MetricsListenAddressCheck, MetricsListenAddressCheck> = true;
export const fleetSettingsMatches: Mutual<Generated.FleetMetricsSettings, FleetMetricsSettings> = true;
export const runEndMatches: Mutual<Generated.FleetMetricsRunEnd, FleetMetricsRunEnd> = true;
