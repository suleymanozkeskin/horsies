// The cancel cells of a `SHARED_THREAD` child: one `Int32Array` over a `SharedArrayBuffer`, one cell per seat, that
// the main thread writes and the seat isolates read with `Atomics`.
//
// - A cell holds the cancel state of the run of its seat: 0 not requested, 1 deadline, 2 operator (the core's
//   spellings of `CancelCause`).
// - The main thread clears the cell of a seat before it posts a run there, and writes the cause when the run's cancel
//   signal fires. The first cause stays.
// - A sync body reads the cell through `runtime.cancelState()` while it holds its isolate: the read needs no turn of
//   the isolate's event loop.

import type { CancelCauseName } from "../native/child/addon.ts";
import { ClientDefect } from "../thrown-errors.ts";
import type { CancelState } from "../child/runtime.ts";
import { NOT_REQUESTED, requestedState } from "../child/runtime.ts";

/** The value of a cell with no cancel. */
const NOT_REQUESTED_CELL = 0;

/** The value of a cell whose run's signal fired at the deadline. */
const DEADLINE_CELL = 1;

/** The value of a cell whose run an operator cancelled. */
const OPERATOR_CELL = 2;

/** The bytes of one cell. */
const CELL_BYTES: number = Int32Array.BYTES_PER_ELEMENT;

/** The cells of `seats` seats, each not requested. Allocates the shared memory. */
export function newCancelCells(seats: number): Int32Array {
  return new Int32Array(new SharedArrayBuffer(seats * CELL_BYTES));
}

/** The cell value of `cause`. Pure. */
function cellOf(cause: CancelCauseName): number {
  switch (cause) {
    case "DEADLINE":
      return DEADLINE_CELL;
    case "OPERATOR":
      return OPERATOR_CELL;
    default: {
      const unexpected: never = cause;

      throw new ClientDefect({ subject: "cancel cell", rule: `a cause outside its type: ${String(unexpected)}` });
    }
  }
}

/** Clear the cell of `seat`: its next run has no cancel. */
export function clearCancel(cells: Int32Array, seat: number): void {
  Atomics.store(cells, seat, NOT_REQUESTED_CELL);
}

/** Write `cause` into the cell of `seat`, unless a cause is there already: the first cause stays. */
export function requestCancel(cells: Int32Array, seat: number, cause: CancelCauseName): void {
  // The exchange fails when a cause is there already; that cause stays.
  void Atomics.compareExchange(cells, seat, NOT_REQUESTED_CELL, cellOf(cause));
}

/** The cancel state in the cell of `seat`. A value that no writer writes is a defect: `ClientDefect`. */
export function cancelStateOf(cells: Int32Array, seat: number): CancelState {
  const value = Atomics.load(cells, seat);

  switch (value) {
    case NOT_REQUESTED_CELL:
      return NOT_REQUESTED;
    case DEADLINE_CELL:
      return requestedState("DEADLINE");
    case OPERATOR_CELL:
      return requestedState("OPERATOR");
    default:
      throw new ClientDefect({ subject: "cancel cell", rule: `the cell of seat ${String(seat)} holds ${String(value)}` });
  }
}
