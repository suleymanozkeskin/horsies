// A secret of the config (a database URL): the name of an environment variable that holds it, or the value itself.

import { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { refusal } from "../app/refusal.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type Checked, checkedText, refused } from "./checked.ts";

/** A secret: the environment variable that holds it, read at `Horsies.create`, or the value. */
export type Secret = { readonly kind: "fromEnv"; readonly name: string } | { readonly kind: "value"; readonly value: string };

/** How the parse reads an environment variable: its value, or `undefined` when it is not set. */
export type EnvironmentRead = (name: string) => string | undefined;

/**
 * The text of a secret at `path`. An environment variable that is not set, and a text with a lone surrogate, are
 * refusals with the code of an invalid broker URL (`HRS-203`); a message names the variable, never a value. Reads the
 * environment through `environment`.
 */
export function resolvedSecret(secret: Secret, environment: EnvironmentRead, path: string): Checked<string> {
  switch (secret.kind) {
    case "value":
      return checkedText(secret.value, HorsiesErrorCode.DATABASE_INVALID_URL, path);
    case "fromEnv":
      return secretFromEnvironment(secret.name, environment, path);
    default:
      return unreachable(secret, "resolvedSecret");
  }
}

function secretFromEnvironment(name: string, environment: EnvironmentRead, path: string): Checked<string> {
  const value = environment(name);

  if (value === undefined) {
    return refused(
      refusal(HorsiesErrorCode.DATABASE_INVALID_URL, path, `the environment variable ${name} is not set`, [
        `set ${name} to the database URL, or give the URL as { kind: "value", value }`,
      ]),
    );
  }

  return checkedText(value, HorsiesErrorCode.DATABASE_INVALID_URL, path);
}
