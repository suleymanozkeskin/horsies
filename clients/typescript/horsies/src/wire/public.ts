// The public names of the wire types and the codec. `src/index.ts` exports this module. Every type that a public
// declaration names is exported here (the public API gate refuses a type that it cannot import).

export { BUILT_IN_TASK_CODES } from "../_catalog/task_code.ts";

export type {
  CodecRefused,
  DecodeRefusalKind,
  DecodeRefused,
  EncodeRefusalKind,
  EncodeRefused,
  EnvelopeRefused,
  EnvelopeRule,
  ReadBackChanged,
  ReadBackRefused,
  ValueRefused,
} from "../codec/refusal.ts";

export type { DecimalRefused } from "../codec/decimal-text.ts";

export type { JsonValue } from "../codec/json-value.ts";

export { h, type NonEmpty, type ObjectWire, type WireBuilders } from "./builders.ts";

export { Decimal } from "./decimal.ts";

export type {
  AnyWire,
  EmptyObject,
  Flat,
  ObjectSent,
  ObjectValue,
  PresenceOf,
  RequiredWire,
  SentOf,
  TupleSent,
  TupleValue,
  ValueOf,
  ValueWire,
  WireMembers,
} from "./infer.ts";

export type { LiteralMember } from "./kinds.ts";

export type { Err, Ok, Result } from "./result.ts";

export type { TaskOutcomeWire } from "./task-outcome.ts";

export type {
  BuiltInTaskCode,
  ExceptionRecord,
  SubWorkflowFailure,
  SubWorkflowSummary,
  TaskError,
  TaskOutcome,
} from "./task-error.ts";

export { WORKFLOW_STATUSES, WorkflowStatus } from "./task-error.ts";

export type { Presence, Role, Wire } from "./wire-type.ts";
