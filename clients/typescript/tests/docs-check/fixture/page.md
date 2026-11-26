# A fixture page of the docs check

`check.test.ts` runs the docs check on this page with each compiler of the two lanes. Each block below shows one rule.

A passing block. It has no marker, so it is chained: the chained blocks below it see its names.

```ts
import { defineTask, h } from "horsies";

const ValidatedOrder = h.object({ orderId: h.uuid(), total: h.decimal() });

export const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid() },
  output: ValidatedOrder,
  errorCodes: ["EMPTY_ORDER"],
});

type Order = h.Infer<typeof ValidatedOrder>;
```

A must-fail block. It shows wrong code on purpose: the check passes when each compiler gives exactly the listed
diagnostics. Line 1 is the first line under the fence. The block sees the names of the chained blocks above it, and
the chain does not hold it.

<!-- ts-check: expect TS2322 at 1 -->
```ts
const order: Order = { orderId: "5f0c3a0e-2b4e-4c1d-9a51-0f6b8f4c2d17", total: 12 };
```

A fragment. It uses a name that no block of this page defines, so it does not compile.

<!-- ts-check: skip partial -- `app` is defined on the app config page -->
```ts
const sent = await app.send(validateOrder, { orderId: crypto.randomUUID() });
```

A failing block. It is chained, and the check reports its error at the page line of the error. It repeats the import
of `h`: in a chain, an import that a block above makes already is dropped.

```ts
import { h } from "horsies";

export const orderTotal: number = h.decimal();
```
