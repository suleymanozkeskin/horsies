#!/usr/bin/env node
/**
 * Glass contrast gate.
 *
 * A translucent surface has no contrast of its own — what a reader sees is the
 * surface composited over whatever sits behind it. This script reads the real
 * token values out of `src/styles/custom.css`, composites every glass tier over
 * the backdrop it is specified to sit on, and asserts the WCAG ratio of each
 * foreground token against that composite in both themes.
 *
 * It also asserts the two invariants the CSS cannot state itself:
 *
 *   1. Fallback equivalence — each `--glass-fallback-*` token is EXACTLY what
 *      its tier composites to over the canvas. That is what makes the
 *      `prefers-reduced-transparency` / no-`backdrop-filter` / print
 *      rendering the same design rendered flat, not a different one.
 *   2. Tier uniqueness — a selector belongs to exactly one `@glass-tier`.
 *
 * Usage:
 *   bun scripts/check-glass-contrast.mjs           # assert, exit 1 on failure
 *   bun scripts/check-glass-contrast.mjs --report  # print the full matrix
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const CSS_PATH = join(
	dirname(fileURLToPath(import.meta.url)),
	'..',
	'src',
	'styles',
	'custom.css'
);

/**
 * WCAG AA floor for body text. Only text is asserted: a glass surface is
 * delineated by the identity borders it already carries — the 2px neon and 1px
 * gold rims — plus its ambient shadow, so the border tokens keep their pre-glass
 * role. What glass genuinely puts at risk is text legibility.
 *
 * There are no large-text carve-outs. Everything asserted clears 4.5.
 */
const TEXT = 4.5;

/**
 * A foreground may be asserted in only one theme. The light palette keeps its
 * pre-glass gold, and gold on a cream canvas cannot reach 4.5:1 — the ceiling
 * for #F7F3EB is a relative luminance of 0.161, and #DAA520 sits at 0.419.
 * These are the affected tokens, with what they measure on the light canvas:
 *
 *   --horsie-gold          #DAA520  2.02:1   headings, table th, page-search
 *   --sl-color-text-accent #DAA520  2.02:1   TOC current item, sidebar pill bg
 *   --horsie-gold-bright   #C49000  2.59:1   site title, sidebar hover
 *   --sl-color-gray-3      #7A7A68  3.94:1   placeholders, secondary text
 *   --horsie-neon-green    #007A52  4.40:1   ON CHROME ONLY (#EDE8DD band);
 *                                            4.86:1 on the canvas, so it stays
 *                                            asserted on every other tier
 *
 * `--report` still prints their ratios, marked `skip`, so the numbers stay
 * visible instead of vanishing from the matrix. Dark asserts all of them.
 */

/** The four tiers, and the fallback token each must composite to exactly. */
const FALLBACK_EQUIVALENCE = [
	['--glass-chrome-surface', '--glass-fallback-chrome'],
	['--glass-panel-surface', '--glass-fallback-panel'],
	['--glass-strong-surface', '--glass-fallback-strong'],
];

/** Every `@glass-tier` marker that must be present in the stylesheet. */
const REQUIRED_TIERS = ['chrome', 'panel', 'menu', 'window'];

/**
 * The assertion matrix. Each entry: a surface stack (bottom-to-top; the first
 * entry is opaque) plus the foreground tokens that are allowed to sit on it.
 * Adding a glass tier or a foreground token means adding it here — that is the
 * point of the gate.
 */
const CHECKS = [
	{
		tier: 'canvas (flat content)',
		stack: ['--sl-color-bg'],
		foregrounds: [
			'--sl-color-text',
			'--sl-color-gray-2',
			{ token: '--sl-color-gray-3', themes: ['dark'] },
			{ token: '--horsie-gold', themes: ['dark'] },
			{ token: '--horsie-gold-bright', themes: ['dark'] },
			'--horsie-neon-green',
			{ token: '--sl-color-text-accent', themes: ['dark'] },
		],
	},
	{
		tier: 'glass-panel',
		stack: ['--sl-color-bg', '--glass-panel-surface'],
		foregrounds: [
			'--sl-color-text',
			'--sl-color-gray-2',
			{ token: '--sl-color-gray-3', themes: ['dark'] },
			{ token: '--horsie-gold', themes: ['dark'] },
			'--horsie-neon-green',
		],
	},
	{
		tier: 'glass-menu / glass-window',
		stack: ['--sl-color-bg', '--glass-strong-surface'],
		foregrounds: [
			'--sl-color-text',
			'--sl-color-gray-1',
			'--sl-color-gray-2',
			{ token: '--sl-color-gray-3', themes: ['dark'] },
			{ token: '--horsie-gold', themes: ['dark'] },
			{ token: '--horsie-gold-bright', themes: ['dark'] },
			'--horsie-neon-green',
		],
	},
	{
		tier: 'glass-chrome',
		stack: ['--sl-color-bg', '--glass-chrome-surface'],
		foregrounds: [
			'--sl-color-text',
			'--sl-color-gray-2',
			{ token: '--sl-color-gray-3', themes: ['dark'] },
			{ token: '--horsie-gold', themes: ['dark'] },
			{ token: '--horsie-gold-bright', themes: ['dark'] },
			{ token: '--horsie-neon-green', themes: ['dark'] },
		],
	},
	{
		tier: 'sidebar active pill',
		stack: ['--sl-color-text-accent'],
		foregrounds: [{ token: '--sl-color-text-invert', themes: ['dark'] }],
	},
];

/** Strip `/* … *\/` comments so declaration text is never read out of prose. */
function stripComments(css) {
	return css.replace(/\/\*[\s\S]*?\*\//g, '');
}

/**
 * Extract the custom properties declared in the top-level block whose selector
 * list contains `selector`. The selector may be one of several in the list
 * (`[data-theme='dark'], :root[data-theme='dark'] { … }`).
 */
function readBlock(css, selector) {
	const at = css.indexOf(selector);
	if (at === -1) throw new Error(`no ${selector} block in custom.css`);
	const open = css.indexOf('{', at);
	if (open === -1) throw new Error(`${selector} is not followed by a block`);
	const prelude = css.slice(at + selector.length, open);
	if (/[;}]/.test(prelude)) {
		throw new Error(
			`the first occurrence of ${selector} is not a rule prelude (found "${prelude.trim()}" before the brace)`
		);
	}
	let depth = 0;
	let i = open;
	for (; i < css.length; i += 1) {
		if (css[i] === '{') depth += 1;
		else if (css[i] === '}') {
			depth -= 1;
			if (depth === 0) break;
		}
	}
	if (depth !== 0) throw new Error(`unbalanced braces in the ${selector} block`);
	const body = css.slice(open + 1, i);
	/** @type {Record<string,string>} */
	const out = {};
	for (const [, name, value] of body.matchAll(
		/(--[a-z0-9-]+)\s*:\s*([^;]+);/gi
	)) {
		out[name] = value.trim();
	}
	return out;
}

/**
 * Collect the selector list under each `/* @glass-tier <name> *\/` marker.
 * Parsed from the RAW css — the markers are comments.
 */
function readTiers(rawCss) {
	/** @type {Record<string,string[]>} */
	const tiers = {};
	for (const match of rawCss.matchAll(/\/\*\s*@glass-tier\s+([a-z0-9-]+)\s*\*\//gi)) {
		const name = match[1];
		const open = rawCss.indexOf('{', match.index);
		if (open === -1) {
			throw new Error(`@glass-tier ${name} marker is not followed by a rule`);
		}
		tiers[name] = rawCss
			.slice(match.index + match[0].length, open)
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
	}
	return tiers;
}

/** `#rgb`/`#rrggbb`/`rgb()`/`rgba()` -> [r, g, b, a] with 0-255 channels. */
function parseColor(raw) {
	const value = raw.trim();
	if (value.startsWith('#')) {
		const hex = value.slice(1);
		const full =
			hex.length === 3
				? hex
						.split('')
						.map((c) => c + c)
						.join('')
				: hex;
		if (!/^[0-9a-f]{6}$/i.test(full)) {
			throw new Error(`unsupported hex form: ${raw}`);
		}
		return [
			parseInt(full.slice(0, 2), 16),
			parseInt(full.slice(2, 4), 16),
			parseInt(full.slice(4, 6), 16),
			1,
		];
	}
	const rgb = value.match(
		/^rgba?\(\s*([\d.]+)[\s,]+([\d.]+)[\s,]+([\d.]+)(?:\s*[,/]\s*([\d.]+))?\s*\)$/i
	);
	if (rgb) {
		return [
			Number(rgb[1]),
			Number(rgb[2]),
			Number(rgb[3]),
			rgb[4] === undefined ? 1 : Number(rgb[4]),
		];
	}
	throw new Error(`unsupported color form: ${raw}`);
}

/**
 * Resolve a stack entry: a literal color, or a token name to look up. A token
 * whose value is itself a single `var(--other)` reference is dereferenced
 * (`--sl-color-text-accent: var(--horsie-gold)` in the dark block).
 */
function resolve(entry, tokens, seen = []) {
	if (!entry.startsWith('--')) return parseColor(entry);
	if (seen.includes(entry)) {
		throw new Error(`token ${entry} is a var() cycle: ${[...seen, entry].join(' -> ')}`);
	}
	const value = tokens[entry];
	if (value === undefined) throw new Error(`token ${entry} is not declared`);
	const indirect = value.match(/^var\(\s*(--[a-z0-9-]+)\s*\)$/i);
	if (indirect) return resolve(indirect[1], tokens, [...seen, entry]);
	return parseColor(value);
}

/** Source-over compositing of an alpha layer onto an opaque base. */
function composite(base, layer) {
	const a = layer[3];
	return [
		layer[0] * a + base[0] * (1 - a),
		layer[1] * a + base[1] * (1 - a),
		layer[2] * a + base[2] * (1 - a),
		1,
	];
}

function relativeLuminance([r, g, b]) {
	const lin = [r, g, b].map((c) => {
		const s = c / 255;
		return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
	});
	return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2];
}

function contrast(a, b) {
	const la = relativeLuminance(a);
	const lb = relativeLuminance(b);
	return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
}

function hex([r, g, b]) {
	return `#${[r, g, b]
		.map((c) => Math.round(c).toString(16).padStart(2, '0').toUpperCase())
		.join('')}`;
}

const rawCss = readFileSync(CSS_PATH, 'utf8');
const css = stripComments(rawCss);

const root = readBlock(css, ':root');
const themes = [
	['dark', { ...root, ...readBlock(css, "[data-theme='dark']") }],
	['light', { ...root, ...readBlock(css, "[data-theme='light']") }],
];

/*
 * Two tokens the matrix names are not declared where a plain merge would find
 * them, and both are handled explicitly rather than by a guessing mechanism:
 *
 *   --sl-color-bg          declared per theme, never in :root. The merge above
 *                          covers it; assert it so a move to :root is loud.
 *   --sl-color-text-invert declared only in the light block. Dark inherits
 *                          Starlight's derived value, which is --sl-color-black.
 */
for (const [themeName, tokens] of themes) {
	if (tokens['--sl-color-bg'] === undefined) {
		throw new Error(`--sl-color-bg is not declared for the ${themeName} theme`);
	}
	if (tokens['--sl-color-text-invert'] === undefined) {
		tokens['--sl-color-text-invert'] = tokens['--sl-color-black'];
	}
}

const report = process.argv.includes('--report');
const failures = [];

// ── 1. Tier uniqueness ────────────────────────────────────────────────────
const tiers = readTiers(rawCss);
for (const name of REQUIRED_TIERS) {
	if (tiers[name] === undefined) {
		failures.push(`tiers · the /* @glass-tier ${name} */ marker is missing`);
	}
}
/** @type {Map<string,string>} */
const owner = new Map();
for (const [name, selectors] of Object.entries(tiers)) {
	for (const selector of selectors) {
		const previous = owner.get(selector);
		if (previous !== undefined) {
			failures.push(
				`tiers · selector "${selector}" is in both @glass-tier ${previous} and @glass-tier ${name}`
			);
			continue;
		}
		owner.set(selector, name);
	}
}
if (report) {
	console.log('\nTIERS');
	for (const [name, selectors] of Object.entries(tiers)) {
		console.log(`  @glass-tier ${name.padEnd(7)} ${selectors.join(', ')}`);
	}
}

// ── 2. Fallback equivalence, per theme ────────────────────────────────────
for (const [themeName, tokens] of themes) {
	if (report) console.log(`\n${themeName.toUpperCase()} — fallback equivalence`);
	const canvas = resolve('--sl-color-bg', tokens);
	for (const [surfaceToken, fallbackToken] of FALLBACK_EQUIVALENCE) {
		const composed = hex(composite(canvas, resolve(surfaceToken, tokens)));
		const declared = hex(resolve(fallbackToken, tokens));
		const ok = composed === declared;
		if (report) {
			console.log(
				`  ${ok ? 'ok  ' : 'FAIL'} ${fallbackToken.padEnd(24)} ${declared} (composite ${composed})`
			);
		}
		if (!ok) {
			failures.push(
				`${themeName} · ${surfaceToken} over the canvas composites to ${composed}, but ${fallbackToken} is ${declared} — set it to ${composed}`
			);
		}
	}
}

// ── 3. Contrast matrix, per theme ─────────────────────────────────────────
for (const [themeName, tokens] of themes) {
	if (report) console.log(`\n${themeName.toUpperCase()} — contrast`);
	for (const { tier, stack, foregrounds } of CHECKS) {
		let surface = resolve(stack[0], tokens);
		for (const layer of stack.slice(1)) {
			surface = composite(surface, resolve(layer, tokens));
		}
		if (report) console.log(`  ${tier} -> ${hex(surface)}`);
		for (const entry of foregrounds) {
			const token = typeof entry === 'string' ? entry : entry.token;
			const asserted = typeof entry === 'string' || entry.themes.includes(themeName);
			const fg = resolve(token, tokens);
			// A translucent foreground token is composited on the surface too.
			const painted = fg[3] === 1 ? fg : composite(surface, fg);
			const ratio = contrast(painted, surface);
			const ok = ratio >= TEXT;
			if (report) {
				const mark = !asserted ? 'skip' : ok ? 'ok  ' : 'FAIL';
				console.log(
					`    ${mark} ${token.padEnd(24)} ${ratio.toFixed(2)}:1 (needs ${TEXT})${asserted ? '' : ' — not asserted in this theme'}`
				);
			}
			if (asserted && !ok) {
				failures.push(
					`${themeName} · ${tier} · ${token}: ${ratio.toFixed(2)}:1 < ${TEXT}:1`
				);
			}
		}
	}
}

if (failures.length > 0) {
	console.error('\nGlass contrast failures:\n');
	for (const line of failures) console.error(`  ${line}`);
	console.error(
		'\nRaise the tier alpha in src/styles/custom.css, or darken/lighten the' +
			' foreground token for that theme. Do not lower the floor.\n'
	);
	process.exit(1);
}

const pairs = themes.reduce(
	(n, [themeName]) =>
		n +
		CHECKS.reduce(
			(m, c) =>
				m +
				c.foregrounds.filter(
					(e) => typeof e === 'string' || e.themes.includes(themeName)
				).length,
			0
		),
	0
);
console.log(
	`\nGlass contrast: ${pairs} foreground/surface pairs, ` +
		`${FALLBACK_EQUIVALENCE.length * themes.length} fallback equivalences and ` +
		`${owner.size} tier selectors pass.`
);
