import { defineCollection } from 'astro:content';
import { z } from 'astro/zod';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

/**
 * The section of a page outside `rust/`:
 * - `shared`: behaviour and guarantees of the core, for both clients;
 * - `python`: the API of the Python client;
 * - `site`: the landing page, the changelog and the migration guides.
 *
 * A page under `rust/` is a Rust API page by its path and declares no
 * section. `src/routeData.ts` reads the field and refuses a page that breaks
 * this rule.
 */
const PageSectionSchema = z.enum(['shared', 'python', 'site']);

export const collections = {
	docs: defineCollection({
		loader: docsLoader(),
		schema: docsSchema({ extend: z.object({ section: PageSectionSchema.optional() }) }),
	}),
};
