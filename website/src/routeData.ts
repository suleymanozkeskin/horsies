import { defineRouteMiddleware } from '@astrojs/starlight/route-data';
import type { StarlightRouteData } from '@astrojs/starlight/route-data';

// Pages under src/content/docs/rust/ document the Rust crates. Every other
// page documents the Python package.
const RUST_SECTION = 'rust';
const RUST_TITLE_SUFFIX = ' (Rust)';
const RUST_HREF_PREFIX = `${import.meta.env.BASE_URL.replace(/\/$/, '')}/${RUST_SECTION}/`;

type Language = 'python' | 'rust';

function pageLanguage(entryId: string): Language {
	return entryId === RUST_SECTION || entryId.startsWith(`${RUST_SECTION}/`) ? 'rust' : 'python';
}

function linkLanguage(href: string): Language {
	return href.startsWith(RUST_HREF_PREFIX) ? 'rust' : 'python';
}

/**
 * Adds "(Rust)" to the page heading, the search result title, `<title>` and
 * `og:title` of a Rust page. Most Rust pages share their title with a Python
 * page, and one search index serves both.
 */
function labelRustTitle(route: StarlightRouteData): void {
	const pageTitle = route.entry.data.title;
	const labelledTitle = `${pageTitle}${RUST_TITLE_SUFFIX}`;
	const titleFromFrontmatter = route.entry.data.head.some((entry) => entry.tag === 'title');
	route.entry.data.title = labelledTitle;
	for (const entry of route.head) {
		if (entry.tag === 'title' && !titleFromFrontmatter) {
			// Starlight writes `<page title> <delimiter> <site title>`.
			if (typeof entry.content !== 'string' || !entry.content.startsWith(`${pageTitle} `)) {
				throw new Error(
					`${route.entry.filePath}: <title> "${String(entry.content)}" does not start with the page title "${pageTitle}"`,
				);
			}
			entry.content = `${labelledTitle}${entry.content.slice(pageTitle.length)}`;
		}
		if (entry.tag === 'meta' && entry.attrs?.property === 'og:title') {
			entry.attrs.content = labelledTitle;
		}
	}
}

/** Keeps the previous and next links of a page within its language. */
function keepPaginationInLanguage(route: StarlightRouteData, language: Language): void {
	const { prev, next } = route.pagination;
	if (prev !== undefined && linkLanguage(prev.href) !== language) route.pagination.prev = undefined;
	if (next !== undefined && linkLanguage(next.href) !== language) route.pagination.next = undefined;
}

export const onRequest = defineRouteMiddleware((context) => {
	const route = context.locals.starlightRoute;
	const language = pageLanguage(route.entry.id);
	switch (language) {
		case 'rust':
			labelRustTitle(route);
			break;
		case 'python':
			break;
		default: {
			const unreachable: never = language;
			throw new Error(`unknown docs language ${String(unreachable)}`);
		}
	}
	keepPaginationInLanguage(route, language);
});
