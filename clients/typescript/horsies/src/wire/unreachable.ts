/**
 * The last arm of a `switch` that covers every member of a closed union: the type check fails when a member has no
 * case. Reaching it at run time is a defect: it throws a `TypeError` with the subject.
 */
export function unreachable(value: never, subject: string): never {
  throw new TypeError(`${subject}: no case for ${String(value)}`);
}
