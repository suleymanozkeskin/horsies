// The name and the definition key of a workflow of the end-to-end app.

/** The name `e2e_<name>` and the definition key `tests.e2e.<name>.v1` of the workflow `name`. Pure. */
export function named(name: string): { readonly name: string; readonly definitionKey: string } {
  return { name: `e2e_${name}`, definitionKey: `tests.e2e.${name}.v1` };
}
