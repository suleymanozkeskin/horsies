// URL search-parameter wire format.
//
// The router's default codec JSON-encodes every value, which turns a
// multi-select filter into
// `?status=%5B%22PENDING%22%2C%22COMPLETED%22%5D` — unreadable, and unusable to
// anyone assembling a link by hand. This pair writes the URL in the shape a
// person would: `?status=PENDING,COMPLETED&view=flat&page=2`.
//
// Each value is encoded on its own, so a comma inside a value becomes `%2C` and
// can never be read as the separator between values. Parsing mirrors that:
// split on the commas that are still raw, then decode each piece.
//
// Parsed values are always text (or a list of text). Coercion to numbers and
// booleans belongs to the validators in `search.ts`, which know each
// parameter's type — a codec that guessed would turn a workflow run named
// "2024" into the number 2024 and then drop it as the wrong type.

/** A scalar a search parameter may hold. Route search shapes are flat by design. */
type SearchScalar = string | number | boolean;

const encodeScalar = (value: SearchScalar): string =>
  encodeURIComponent(String(value));

/**
 * Serialize one key/value pair, or null when the value carries no information.
 *
 * Absent, null, empty-string and empty-array values are omitted: an unset
 * filter has no place in the URL. Parameters left at their default are already
 * dropped upstream — the validators never emit them and a patch clears one by
 * setting it to `undefined`.
 */
function encodePair(key: string, value: unknown): string | null {
  const name = encodeURIComponent(key);
  if (value === undefined || value === null) {
    return null;
  }
  if (Array.isArray(value)) {
    const items: SearchScalar[] = value.filter(
      (item): item is SearchScalar => item !== undefined && item !== null
    );
    return items.length === 0
      ? null
      : `${name}=${items.map(encodeScalar).join(',')}`;
  }
  switch (typeof value) {
    case 'string':
      return value === '' ? null : `${name}=${encodeScalar(value)}`;
    case 'number':
    case 'boolean':
      return `${name}=${encodeScalar(value)}`;
    default: {
      // Unreachable for the route search shapes, which are flat. Kept explicit
      // because `String(value)` would reduce an object to "[object Object]" and
      // lose its contents without a trace.
      const json = JSON.stringify(value);
      return json === undefined ? null : `${name}=${encodeURIComponent(json)}`;
    }
  }
}

/** Serialize a search object, including the leading `?`; empty yields ''. */
export function stringifySearch(search: Record<string, unknown>): string {
  const pairs: string[] = [];
  for (const [key, value] of Object.entries(search)) {
    const pair = encodePair(key, value);
    if (pair !== null) {
      pairs.push(pair);
    }
  }
  return pairs.length === 0 ? '' : `?${pairs.join('&')}`;
}

/**
 * Decode one component, leaving malformed escapes as the literal text.
 *
 * `decodeURIComponent` throws on input like `%ZZ` or a lone surrogate escape,
 * and no cheap predicate covers both. The URL is user-editable, so a bad escape
 * degrades to what was typed instead of breaking navigation.
 */
function decodeComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

/**
 * Read a value the previous JSON codec wrote, so existing bookmarks still work.
 *
 * That codec quoted strings and bracketed arrays; numbers and booleans it wrote
 * bare, which is already the current format. Its commas were percent-encoded,
 * so a legacy array arrives as one undivided piece.
 *
 * Returns null for anything else, including text that merely opens with `[` or
 * `"` without being valid JSON — that is a plain value and stays one.
 */
function legacyValue(decoded: string): string | string[] | null {
  const first = decoded.slice(0, 1);
  if (first !== '[' && first !== '"') {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(decoded);
    if (Array.isArray(parsed)) {
      return parsed.map(item => String(item));
    }
    return typeof parsed === 'string' ? parsed : null;
  } catch {
    return null;
  }
}

function parseValue(raw: string): string | string[] {
  const pieces = raw.split(',');
  if (pieces.length === 1) {
    const decoded = decodeComponent(raw);
    return legacyValue(decoded) ?? decoded;
  }
  return pieces.map(decodeComponent);
}

const asList = (value: string | string[]): string[] =>
  Array.isArray(value) ? value : [value];

/** Parse a query string (with or without its leading `?`) into search values. */
export function parseSearch(
  searchStr: string
): Record<string, string | string[]> {
  const query = searchStr.startsWith('?') ? searchStr.slice(1) : searchStr;
  const parsed: Record<string, string | string[]> = {};
  if (query === '') {
    return parsed;
  }
  for (const pair of query.split('&')) {
    if (pair === '') {
      continue;
    }
    const separator = pair.indexOf('=');
    const key = decodeComponent(
      separator === -1 ? pair : pair.slice(0, separator)
    );
    const value = parseValue(separator === -1 ? '' : pair.slice(separator + 1));
    const existing = parsed[key];
    // A repeated key is the shape the HTTP API itself uses, and the one a
    // person hand-writing a link is likeliest to reach for. Merge rather than
    // letting the last occurrence win.
    parsed[key] =
      existing === undefined ? value : [...asList(existing), ...asList(value)];
  }
  return parsed;
}
