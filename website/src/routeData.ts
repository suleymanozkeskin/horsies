import { getCollection } from 'astro:content';
import { defineRouteMiddleware } from '@astrojs/starlight/route-data';
import type { StarlightRouteData } from '@astrojs/starlight/route-data';

/**
 * The section of a page. Shared pages state the behaviour and the guarantees
 * of the core, which both clients run on. Python and Rust pages state the API
 * of one client. Site pages are the landing page, the changelog and the
 * migration guides.
 */
type PageSection = 'shared' | 'python' | 'rust' | 'site';

/** Pages under src/content/docs/rust/ are Rust API pages. */
const RUST_DIRECTORY = 'rust';
/**
 * The id of Starlight's own 404 page. Starlight builds it when the docs hold no
 * `404.md`, so it has no frontmatter; it is a site page.
 */
const STARLIGHT_404_ENTRY_ID = '404';
const BASE_PATH = import.meta.env.BASE_URL.replace(/\/$/, '');
const SHARED_BANNER = 'Shared page: this behaviour holds for the Python client and the Rust client.';

/** The title suffix of each section; `undefined` for no suffix. */
function titleSuffix(section: PageSection): string | undefined {
	switch (section) {
		case 'python':
			return ' (Python)';
		case 'rust':
			return ' (Rust)';
		case 'shared':
		case 'site':
			return undefined;
		default: {
			const unreachable: never = section;
			throw new Error(`unknown docs section ${String(unreachable)}`);
		}
	}
}

function isRustEntry(entryId: string): boolean {
	return entryId === RUST_DIRECTORY || entryId.startsWith(`${RUST_DIRECTORY}/`);
}

/**
 * The section of one entry: `rust` by its path, else its `section` field.
 * Throws, naming the file, for a Rust page that declares a section and for
 * any other page that declares none.
 */
function entrySection(entryId: string, filePath: string, declared: 'shared' | 'python' | 'site' | undefined): PageSection {
	if (entryId === STARLIGHT_404_ENTRY_ID && declared === undefined) return 'site';
	if (isRustEntry(entryId)) {
		if (declared !== undefined) {
			throw new Error(`${filePath}: a page under ${RUST_DIRECTORY}/ is a Rust API page; remove "section: ${declared}"`);
		}
		return 'rust';
	}
	if (declared === undefined) {
		throw new Error(`${filePath}: the frontmatter needs "section": shared, python or site`);
	}
	return declared;
}

/** The URL path that Starlight serves an entry at. */
function entryHref(entryId: string): string {
	return entryId === 'index' ? `${BASE_PATH}/` : `${BASE_PATH}/${entryId}/`;
}

let sectionsByHref: Promise<ReadonlyMap<string, PageSection>> | undefined;

/** The section of every page, by URL path. Read once per build. */
function pageSections(): Promise<ReadonlyMap<string, PageSection>> {
	sectionsByHref ??= getCollection('docs').then(
		(entries) =>
			new Map(
				entries.map((entry) => [
					entryHref(entry.id),
					entrySection(entry.id, entry.filePath ?? entry.id, entry.data.section),
				]),
			),
	);
	return sectionsByHref;
}

/**
 * Adds the section's suffix to the page heading, the search result title,
 * `<title>` and `og:title`. Python and Rust API pages often share a title,
 * and one search index serves both.
 */
function labelTitle(route: StarlightRouteData, suffix: string): void {
	const pageTitle = route.entry.data.title;
	const labelledTitle = `${pageTitle}${suffix}`;
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

/** Keeps the previous and next links of a page within its section. */
function keepPaginationInSection(
	route: StarlightRouteData,
	section: PageSection,
	sections: ReadonlyMap<string, PageSection>,
): void {
	const { prev, next } = route.pagination;
	if (prev !== undefined && sections.get(prev.href) !== section) route.pagination.prev = undefined;
	if (next !== undefined && sections.get(next.href) !== section) route.pagination.next = undefined;
}

export const onRequest = defineRouteMiddleware(async (context) => {
	const route = context.locals.starlightRoute;
	const sections = await pageSections();
	const section = entrySection(route.entry.id, route.entry.filePath, route.entry.data.section);
	const suffix = titleSuffix(section);
	if (suffix !== undefined) labelTitle(route, suffix);
	if (section === 'shared') route.entry.data.banner ??= { content: SHARED_BANNER };
	keepPaginationInSection(route, section, sections);
});
