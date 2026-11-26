// The name and the message of a thrown value, read with no throw.

/** The name and the message of a thrown value. */
export interface ThrownFacts {
  readonly name: string;
  readonly message: string;
}

/** The name of a thrown value that is not an `Error`. */
const NOT_AN_ERROR = "NotAnError";

/** The text of a fact that cannot be read. */
const NO_TEXT = "a thrown value with no readable text";

/**
 * The facts of `cause`. A value that is not an `Error` has the name `NotAnError`. Each fact is read
 * apart: a fact that cannot be read (a getter that throws, a revoked `Proxy`) has a fixed text, and
 * a fact that is not a string is its text. Never throws.
 */
export function thrownFacts(cause: unknown): ThrownFacts {
  let isError: boolean;

  try {
    isError = cause instanceof Error;
  } catch {
    return { name: NOT_AN_ERROR, message: NO_TEXT };
  }

  if (!isError) {
    return { name: NOT_AN_ERROR, message: factText(() => cause) };
  }

  // SAFETY: `instanceof Error` held just above.
  const error = cause as Error;

  return { name: factText(() => error.name), message: factText(() => error.message) };
}

/** The text of one fact; the fixed text when its read or its conversion throws. */
function factText(read: () => unknown): string {
  try {
    return String(read());
  } catch {
    return NO_TEXT;
  }
}
