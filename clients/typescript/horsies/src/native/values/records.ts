// The values that addon operations give: outcomes, catalog error records, causes, and the trace
// argument of a call. Declarations of the `values` area of the bridge.

/** The outcome of one operation: its record, or its expected failure as a raw catalog record. */
export type NativeOutcome<T> =
  | { readonly kind: "done"; readonly record: T }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/**
 * A value of a core error catalog: its generated class and its instance slots, in the class's slot
 * order. The generated converter of the catalog builds the public value from it.
 */
export interface RawCatalogError {
  readonly class: string;
  readonly slots: RawSlot[];
}

/** One instance slot of a catalog value. */
export interface RawSlot {
  readonly name: string;
  readonly value: RawSlotValue;
}

/** The value of one instance slot, by the type of the slot. */
export type RawSlotValue =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "flag"; readonly value: boolean }
  | { readonly kind: "integer"; readonly value: bigint }
  | { readonly kind: "uuid"; readonly text: string }
  | { readonly kind: "codeMember"; readonly catalog: string; readonly value: string }
  | { readonly kind: "sendPayload"; readonly fields: RawRecordField[] }
  | { readonly kind: "exception"; readonly exception: ExceptionRecord };

/** One field of a generated record class, with its name. */
export interface RawRecordField {
  readonly name: string;
  readonly value: RawRecordValue;
}

/** The value of one record field. An optional field with no value is `absent`. */
export type RawRecordValue =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "integer"; readonly value: bigint }
  | { readonly kind: "instant"; readonly epochMicros: bigint }
  | { readonly kind: "absent" };

/** The exception of an `exception` slot. */
export type ExceptionRecord =
  | { readonly kind: "coreFailure"; readonly cause: CauseRecord }
  | { readonly kind: "generated"; readonly class: string; readonly text: string }
  | { readonly kind: "clientClass"; readonly class: ClientClassName; readonly text: string }
  | { readonly kind: "noCause" };

/** A class of exception that the client defines. */
export type ClientClassName = "valueError" | "serializationError";

/** The cause of a failed core operation, with the operation as its subject. */
export type CauseRecord =
  | {
      readonly kind: "server";
      readonly subject: string;
      readonly sqlstate: string;
      readonly message: string;
      readonly retryable: boolean;
    }
  | {
      readonly kind: "connection";
      readonly subject: string;
      readonly message: string;
      readonly retryable: boolean;
    }
  | { readonly kind: "contractViolation"; readonly subject: string; readonly message: string }
  | {
      readonly kind: "preconditionFailed";
      readonly subject: string;
      readonly message: string;
      readonly retryable: boolean;
    };

/**
 * The trace argument of a send, a workflow start or a rerun: no carrier, or the map that the
 * propagator injected into and the 16-byte trace id of the propagated span.
 */
export type TraceArgument =
  | { readonly kind: "noCarrier" }
  | {
      readonly kind: "carrier";
      readonly carrier: Readonly<Record<string, string>>;
      readonly traceId: Uint8Array;
    };
