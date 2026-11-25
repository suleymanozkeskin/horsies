// The end of an exhaustive `switch`.

/**
 * Throws for a value that the types make impossible. Reaching it is a defect of the package: the
 * native addon gave a value that its declarations do not name.
 */
export function unreachable(value: never): never {
  throw new Error(`a value outside its declared type reached an exhaustive switch: ${JSON.stringify(value)}`);
}
