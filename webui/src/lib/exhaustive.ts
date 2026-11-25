/**
 * The `default` arm of an exhaustive switch over a closed union. The type
 * checker rejects a call while a member of the union has no arm. At run time
 * it throws: a value outside the union is a defect of the server contract,
 * not a state the UI can draw.
 */
export function assertNever(value: never, subject: string): never {
  throw new Error(`${subject}: value outside the union: ${JSON.stringify(value)}`);
}
