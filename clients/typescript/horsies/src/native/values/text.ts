// The text rule of the addon boundary.
//
// Node-API replaces a lone surrogate of a JavaScript string with U+FFFD and gives no error, so a
// stored text could differ from the caller's value. Every call that takes user text checks it here
// first.

/** The outcome of the text check of one argument. */
export type TextCheck =
  | { readonly kind: "wellFormed"; readonly text: string }
  | { readonly kind: "loneSurrogate"; readonly argument: string };

/** Whether `text` has no lone surrogate, so it crosses the boundary unchanged. Pure. */
export function checkText(argument: string, text: string): TextCheck {
  return text.isWellFormed() ? { kind: "wellFormed", text } : { kind: "loneSurrogate", argument };
}
