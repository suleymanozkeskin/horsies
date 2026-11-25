// The checks of the declaration tests: the generated declaration of the addon and the hand-kept one
// name the same values.

/** `true` when each type is assignable to the other. */
export type Mutual<Generated, Kept> = [Generated] extends [Kept]
  ? [Kept] extends [Generated]
    ? true
    : false
  : false;

/** The tuple of the arguments of a function type. */
type ArgumentsOf<Function> = Function extends (...args: infer Arguments) => unknown ? Arguments : never;

/** The result of a function type. */
type ResultOf<Function> = Function extends (...args: never) => infer Result ? Result : never;

/**
 * `true` when the two functions have the same argument tuple and the same result. A method member
 * alone is checked in both directions of its arguments (method bivariance), so a hand-kept argument
 * wider than the generated one passes `Mutual` of the two objects; the tuples catch it.
 */
export type SameFunction<Generated, Kept> =
  Mutual<ArgumentsOf<Generated>, ArgumentsOf<Kept>> extends true ? Mutual<ResultOf<Generated>, ResultOf<Kept>> : false;
