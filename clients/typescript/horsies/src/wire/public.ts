// The public names of the wire types and the codec. `src/index.ts` exports this module.

export type {
  CodecRefused,
  DecodeRefusalKind,
  DecodeRefused,
  EncodeRefusalKind,
  EncodeRefused,
  EnvelopeRefused,
  EnvelopeRule,
  ValueRefused,
} from "../codec/refusal.ts";

export type { DecimalRefused } from "../codec/decimal-text.ts";

export type { JsonValue } from "../codec/json-value.ts";

export { h, type WireBuilders } from "./builders.ts";

export { Decimal } from "./decimal.ts";

export type { EmptyObject } from "./infer.ts";

export type { Err, Ok, Result } from "./result.ts";

export type { ResultOfWire } from "./result-of.ts";

export type {
  ExceptionRecord,
  LibraryTaskCode,
  SubWorkflowFailure,
  SubWorkflowSummary,
  TaskError,
  TaskErrorCode,
  TaskOutcome,
  WorkflowStatus,
} from "./task-error.ts";

export type { Presence, Role, Wire } from "./wire-type.ts";
