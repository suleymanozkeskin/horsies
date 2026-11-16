#!/usr/bin/env node
/**
 * Token-lint gate — fails when *added / changed* lines introduce hardcoded
 * colors or off-scale font sizes instead of design tokens (src/styles/tokens.css).
 *
 * Deliberately diff-scoped ("changed lines only"): it ratchets, so new/edited
 * code must be clean while untouched lines are ignored.
 *
 * Usage:
 *   node scripts/check-tokens.mjs --base <ref>   # diff <ref>...HEAD (CI)
 *   node scripts/check-tokens.mjs --staged       # staged changes (pre-commit)
 *   node scripts/check-tokens.mjs                # defaults to --base origin/main
 *
 * Only added lines in .tsx/.ts/.jsx/.js are checked. CSS is skipped: tokens.css
 * is the palette source and app.css maps tokens onto Tailwind utilities.
 */

import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/** Files never checked (token sources). */
const ALLOWLIST = [
  /(^|\/)styles\/tokens\.css$/,
  /(^|\/)styles\/app\.css$/,
];

/** Extensions we scan (class strings + inline styles live here). */
const SCAN_EXT = /\.(tsx|ts|jsx|js)$/;

const PALETTE =
  'gray|slate|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose';
const COLOR_UTIL =
  'bg|text|border|ring|ring-offset|from|to|via|divide|fill|stroke|shadow|outline|placeholder|accent|decoration|caret';

/**
 * Each rule: { id, re, hint }. `re` is tested against the added line content.
 * Kept intentionally class-string-shaped to minimise false positives.
 */
export const RULES = [
  {
    id: 'palette-shade',
    re: new RegExp(`\\b(?:${COLOR_UTIL})-(?:${PALETTE})-\\d{2,3}\\b`),
    hint: 'hardcoded Tailwind palette color — use a semantic token (bg-muted, text-foreground, …)',
  },
  {
    id: 'black-white',
    re: new RegExp(`\\b(?:${COLOR_UTIL})-(?:white|black)\\b`),
    hint: 'raw black/white — use a token (bg-card, text-foreground, border-border, …)',
  },
  {
    id: 'arbitrary-color',
    re: /-\[#[0-9a-fA-F]{3,8}\]|-\[(?:rgb|rgba|hsl|hsla|oklch|oklab|lab|lch)\(/,
    hint: 'arbitrary color value — add/use a token in tokens.css instead',
  },
  {
    id: 'inline-style-color',
    re: /\b(?:color|fill|stroke|background|backgroundColor|borderColor|outlineColor)\s*:\s*['"`]?(?:#[0-9a-fA-F]{3,8}|rgb\(|rgba\(|hsl\(|oklch\()/,
    hint: 'hardcoded color in inline style — use a token / className',
  },
  {
    id: 'opacity-modifier',
    // `(?<!\d)` excludes Tailwind fraction utilities (e.g. `left-1/2`,
    // `slide-in-from-left-1/2`): an opacity modifier's `/` always follows a
    // color-token letter, never a digit the way a fraction numerator does.
    re: new RegExp(
      `\\b(?:${COLOR_UTIL})-[a-z][a-z0-9-]*(?<!\\d)\\/\\d{1,3}\\b`
    ),
    hint: 'opacity modifier (/N) on a color class is disallowed — use a solid token from the ramp',
  },
  {
    id: 'arbitrary-font-size',
    re: /\btext-\[\d+px\]/,
    hint: 'arbitrary font size — use a text-N token (text-9 … text-13) or a Tailwind size class',
  },
];

function parseArgs(argv) {
  const args = { mode: 'base', base: 'origin/main' };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--staged') args.mode = 'staged';
    else if (argv[i] === '--base') {
      args.mode = 'base';
      args.base = argv[++i];
    }
  }
  return args;
}

function getDiff(args) {
  const common = ['diff', '--relative', '--unified=0', '--no-color'];
  const cmd =
    args.mode === 'staged'
      ? [...common, '--cached']
      : [...common, `${args.base}...HEAD`];
  return execFileSync('git', cmd, {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
}

function isAllowlisted(file) {
  return ALLOWLIST.some(re => re.test(file));
}

/** Parse a `--unified=0 --relative` diff into { file, line, content } added rows. */
export function addedLines(diff) {
  const rows = [];
  let file = null;
  let scan = false;
  let newLine = 0;
  for (const raw of diff.split('\n')) {
    if (raw.startsWith('+++ ')) {
      const parsed = raw.slice(4).replace(/^b\//, '').trim();
      file = parsed === '/dev/null' ? null : parsed;
      scan = !!file && SCAN_EXT.test(file) && !isAllowlisted(file);
      continue;
    }
    if (raw.startsWith('--- ')) continue;
    if (raw.startsWith('@@')) {
      const m = raw.match(/\+(\d+)/);
      newLine = m ? parseInt(m[1], 10) : 0;
      continue;
    }
    if (raw.startsWith('+')) {
      if (scan) rows.push({ file, line: newLine, content: raw.slice(1) });
      newLine++;
    }
    // removed ('-') and metadata lines: ignore (unified=0 has no context)
  }
  return rows;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  let diff;
  try {
    diff = getDiff(args);
  } catch (err) {
    console.error(`check-tokens: git diff failed — ${err.message}`);
    process.exit(2);
  }

  const violations = [];
  for (const { file, line, content } of addedLines(diff)) {
    for (const rule of RULES) {
      const match = content.match(rule.re);
      if (match) {
        violations.push({
          file,
          line,
          rule: rule.id,
          hint: rule.hint,
          text: match[0],
        });
      }
    }
  }

  if (violations.length === 0) {
    console.log(
      'check-tokens: no hardcoded colors / off-scale sizes in changed lines.'
    );
    process.exit(0);
  }

  console.error(
    `\ncheck-tokens: ${violations.length} hardcoded value(s) in changed lines. Use design tokens (src/styles/tokens.css).\n`
  );
  for (const v of violations) {
    console.error(`  ${v.file}:${v.line}  [${v.rule}]  ${v.text}`);
    console.error(`      -> ${v.hint}`);
  }
  console.error(
    '\n  (Scoped to changed lines only. If a match is a genuine exception, adjust ALLOWLIST/RULES in scripts/check-tokens.mjs.)\n'
  );
  process.exit(1);
}

// Run the CLI only when executed directly, not when imported by tests.
if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main();
}
