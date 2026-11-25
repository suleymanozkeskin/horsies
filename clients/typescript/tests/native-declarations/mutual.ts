// `true` when each type is assignable to the other: the generated declaration of the addon and the
// hand-kept one name the same values.

export type Mutual<Generated, Kept> = [Generated] extends [Kept]
  ? [Kept] extends [Generated]
    ? true
    : false
  : false;
