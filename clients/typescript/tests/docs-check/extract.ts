// The TypeScript code blocks of one docs page and their check markers. Pure: it reads the text of a page. The Rust
// twin is `clients/rust/tests/docs-check/extract.rs`; the Python twin is `clients/python/tests/docs_examples/extract.py`.
//
// A TypeScript block is a fenced block whose language is `ts` or `typescript`; an info string can follow the language
// (```ts title="app.ts"). The marker on the line directly above the opening fence (blank lines can stand between them)
// gives its mode:
//
// - No marker: the block is chained. The chained blocks of a page compile in one file, in page order, so a block sees
//   the names of the chained blocks above it. A chained block can repeat an import of a block above it (`program.ts`).
// - `ts-check: alone`: the block compiles in a file of its own and sees no other block.
// - `ts-check: expect TS2322 at 3`: a must-fail block, wrong code that the page shows on purpose. It compiles in a file
//   of its own after the chained blocks above it, and it passes when each compiler gives exactly the listed
//   diagnostics in the block. Line 1 is the first line under the fence. The list is the list of the type tests' expect line
//   (`../types/cases.ts`): `TS2322 at 3, TS2353 at 4`, and `TS2322 at 3; floor: TS2345 at 3` when the floor lane gives
//   other codes. The chain does not hold the block.
// - `ts-check: skip KIND -- reason`: the block does not compile. The kind and the reason are required:
//   - `elided`: the block shortens on purpose (pseudo-code, a signature shown for reference);
//   - `partial`: a fragment: the block is correct but uses names that the page does not define;
//   - `defect`: the block is wrong for the current API: a known docs defect, to fix.
//
// A marker is `<!-- ts-check: ... -->` in a `.md` page and `{/* ts-check: ... */}` in a `.mdx` page. These stop the
// check: an unknown marker (a marker in the comment form of the other page kind too), a skip with no kind or no reason,
// an expect list that does not read, a marker with no TypeScript fence under it, and a fence that is not closed.

import type { Result } from "../../horsies/src/wire/result.ts";
import { type CaseExpectation, expectationOf } from "../types/cases.ts";

/** The kind of a page, from its file extension. The kind sets the comment form of a marker. */
export type PageKind = "md" | "mdx";

/** Why a block does not compile. */
export type SkipKind = "elided" | "partial" | "defect";

export const SKIP_KINDS: readonly SkipKind[] = ["elided", "partial", "defect"];

/** How a block compiles. */
export type BlockMode =
  | { readonly kind: "chained" }
  | { readonly kind: "alone" }
  | { readonly kind: "mustFail"; readonly expectation: CaseExpectation }
  | { readonly kind: "skipped"; readonly skip: SkipKind; readonly reason: string };

/** A TypeScript block of a page: the line of its opening fence (from 1), its mode and its code lines. */
export type Block = { readonly fenceLine: number; readonly mode: BlockMode; readonly code: readonly string[] };

/** Why the reading of a page stops; `line` is the page line (from 1) of the marker or the fence. */
export type ExtractError =
  | { readonly kind: "UnknownMarker"; readonly line: number; readonly text: string }
  | { readonly kind: "SkipWithoutKind"; readonly line: number }
  | { readonly kind: "SkipWithoutReason"; readonly line: number }
  | { readonly kind: "ExpectListRefused"; readonly line: number; readonly item: string }
  | { readonly kind: "MarkerWithoutFence"; readonly line: number }
  | { readonly kind: "UnclosedFence"; readonly line: number };

const MARKER_WORD = "ts-check";

const TYPESCRIPT_LANGUAGES: readonly string[] = ["ts", "typescript"];

const COMMENT_FORMS: Readonly<Record<PageKind, { readonly open: string; readonly close: string }>> = {
  md: { open: "<!--", close: "-->" },
  mdx: { open: "{/*", close: "*/}" },
};

const PAGE_KINDS: readonly PageKind[] = ["md", "mdx"];

/** An opening fence: its indent, its run of backticks or tildes, and the first word of its info string. */
const FENCE_OPEN = /^(\s*)(`{3,}|~{3,})\s*([^\s{`]*)/;

const FENCE_CLOSE = /^\s*(`{3,}|~{3,})\s*$/;

/** The list grammar of the type tests reads an expect line; a marker's list is read as one. */
const EXPECT_LINE_PREFIX = "// expect: ";

type Fence = { readonly indent: number; readonly run: string; readonly language: string };

type LineKind =
  | { readonly kind: "fence"; readonly fence: Fence }
  | { readonly kind: "marker"; readonly body: string }
  | { readonly kind: "malformedMarker" }
  | { readonly kind: "blank" }
  | { readonly kind: "text" };

/** A marker line: well formed for `pageKind`, malformed, or no marker (`text`). */
function markerLineKind(pageKind: PageKind, trimmed: string): LineKind {
  for (const formKind of PAGE_KINDS) {
    const form = COMMENT_FORMS[formKind];

    if (!trimmed.startsWith(form.open) || !trimmed.slice(form.open.length).trimStart().startsWith(MARKER_WORD)) {
      continue;
    }

    const afterWord = trimmed.slice(form.open.length).trimStart().slice(MARKER_WORD.length);
    const wellFormed = formKind === pageKind && afterWord.startsWith(":") && afterWord.endsWith(form.close);

    return wellFormed ? { kind: "marker", body: afterWord.slice(1, -form.close.length) } : { kind: "malformedMarker" };
  }

  return { kind: "text" };
}

function lineKind(pageKind: PageKind, line: string): LineKind {
  const fence = FENCE_OPEN.exec(line);
  const [, indent, run, language] = fence ?? [];

  if (indent !== undefined && run !== undefined && language !== undefined) {
    return { kind: "fence", fence: { indent: indent.length, run, language } };
  }

  const trimmed = line.trim();

  return trimmed === "" ? { kind: "blank" } : markerLineKind(pageKind, trimmed);
}

/** The mode of `skip KIND -- reason`, from the text after `skip`. */
function skipMode(rest: string, line: number, text: string): Result<BlockMode, ExtractError> {
  const separator = rest.indexOf("--");
  const head = separator === -1 ? rest : rest.slice(0, separator);
  const reason = separator === -1 ? "" : rest.slice(separator + 2).trim();
  const words = head.split(/\s+/).filter((word) => word !== "");
  const [kind] = words;

  if (kind === undefined) {
    return { ok: false, error: { kind: "SkipWithoutKind", line } };
  }

  if (words.length === 1 && reason === "") {
    return { ok: false, error: { kind: "SkipWithoutReason", line } };
  }

  const skip = SKIP_KINDS.find((known) => known === kind);

  if (words.length !== 1 || skip === undefined) {
    return { ok: false, error: { kind: "UnknownMarker", line, text } };
  }

  return { ok: true, value: { kind: "skipped", skip, reason } };
}

/** The mode of `expect LIST`, from the text after `expect`. */
function mustFailMode(rest: string, line: number): Result<BlockMode, ExtractError> {
  const expectation = expectationOf(`${EXPECT_LINE_PREFIX}${rest.trim()}`);

  if (expectation.ok) {
    return { ok: true, value: { kind: "mustFail", expectation: expectation.value } };
  }

  switch (expectation.error.kind) {
    case "BadItem":
      return { ok: false, error: { kind: "ExpectListRefused", line, item: expectation.error.item } };
    case "NoHeader":
      throw new Error(`the expect line of the marker at line ${line} does not start with "${EXPECT_LINE_PREFIX}"`);
  }
}

/** The mode of a marker body (the text between `ts-check:` and the end of the comment). */
function markerMode(body: string, line: number, text: string): Result<BlockMode, ExtractError> {
  const [, verb = "", rest = ""] = /^\s*(\S*)(.*)$/.exec(body) ?? [];

  switch (verb) {
    case "alone":
      return rest.trim() === "" ? { ok: true, value: { kind: "alone" } } : { ok: false, error: { kind: "UnknownMarker", line, text } };
    case "skip":
      return skipMode(rest, line, text);
    case "expect":
      return mustFailMode(rest, line);
    default:
      return { ok: false, error: { kind: "UnknownMarker", line, text } };
  }
}

/** The index of the line that closes `fence`, from the line index `from`; `unclosed` when the page ends first. */
function closingFenceIndex(lines: readonly string[], from: number, fence: Fence): number | "unclosed" {
  for (let index = from; index < lines.length; index += 1) {
    const [, run] = FENCE_CLOSE.exec(lines[index] ?? "") ?? [];

    if (run !== undefined && run[0] === fence.run[0] && run.length >= fence.run.length) {
      return index;
    }
  }

  return "unclosed";
}

/** `lines` without the first `indent` whitespace characters of each line (a fence in a list item is indented). */
function dedent(lines: readonly string[], indent: number): readonly string[] {
  return lines.map((line) => line.slice(Math.min(indent, line.length - line.trimStart().length)));
}

type PendingMarker = { readonly line: number; readonly mode: BlockMode };

/** The TypeScript blocks of a page of `pageKind`, with their modes. Stops at the first marker or fence error. Pure. */
export function readBlocks(pageKind: PageKind, text: string): Result<readonly Block[], ExtractError> {
  const lines = text.split(/\r?\n/);
  const blocks: Block[] = [];
  let pending: PendingMarker | "none" = "none";
  let index = 0;

  while (index < lines.length) {
    const line = lines[index] ?? "";
    const number = index + 1;
    const classified = lineKind(pageKind, line);

    if (pending !== "none" && (classified.kind === "marker" || classified.kind === "text")) {
      return { ok: false, error: { kind: "MarkerWithoutFence", line: pending.line } };
    }

    switch (classified.kind) {
      case "malformedMarker":
        return { ok: false, error: { kind: "UnknownMarker", line: number, text: line.trim() } };
      case "marker": {
        const mode = markerMode(classified.body, number, line.trim());

        if (!mode.ok) {
          return mode;
        }

        pending = { line: number, mode: mode.value };
        index += 1;
        break;
      }

      case "fence": {
        const end = closingFenceIndex(lines, index + 1, classified.fence);

        if (end === "unclosed") {
          return { ok: false, error: { kind: "UnclosedFence", line: number } };
        }

        const typeScript = TYPESCRIPT_LANGUAGES.includes(classified.fence.language);

        if (!typeScript && pending !== "none") {
          return { ok: false, error: { kind: "MarkerWithoutFence", line: pending.line } };
        }

        if (typeScript) {
          const mode: BlockMode = pending === "none" ? { kind: "chained" } : pending.mode;

          blocks.push({ fenceLine: number, mode, code: dedent(lines.slice(index + 1, end), classified.fence.indent) });
        }

        pending = "none";
        index = end + 1;
        break;
      }

      case "blank":
      case "text":
        index += 1;
        break;
    }
  }

  return pending === "none" ? { ok: true, value: blocks } : { ok: false, error: { kind: "MarkerWithoutFence", line: pending.line } };
}

/** The text of an extraction error of `page`, for a message: `page:line: what`. */
export function extractErrorText(page: string, error: ExtractError): string {
  const place = `${page}:${error.line}`;

  switch (error.kind) {
    case "UnknownMarker":
      return `${place}: unknown check marker: ${error.text}`;
    case "SkipWithoutKind":
      return `${place}: "${MARKER_WORD}: skip" needs a kind: "${MARKER_WORD}: skip elided|partial|defect -- reason"`;
    case "SkipWithoutReason":
      return `${place}: "${MARKER_WORD}: skip" needs a reason: "${MARKER_WORD}: skip KIND -- reason"`;
    case "ExpectListRefused":
      return `${place}: the item "${error.item}" of "${MARKER_WORD}: expect" is not "TSnnnn at <block line>"`;
    case "MarkerWithoutFence":
      return `${place}: the check marker is not directly above a TypeScript fence`;
    case "UnclosedFence":
      return `${place}: the fence has no closing fence`;
  }
}

/** The page kind of a file name, or `notAPage`. Pure. */
export function pageKindOf(fileName: string): PageKind | "notAPage" {
  return PAGE_KINDS.find((kind) => fileName.endsWith(`.${kind}`)) ?? "notAPage";
}
