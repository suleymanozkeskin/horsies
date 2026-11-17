// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import darkThemeS from './src/themes/dark-theme-s.json';

// https://astro.build/config
export default defineConfig({
	site: 'https://suleymanozkeskin.github.io',
	base: '/horsies',
	redirects: {
		// A Rust page that stated behaviour of the core redirects to the shared page.
		// Astro writes a destination as given, so each one starts with the base path.
		'/rust/concepts/architecture': '/horsies/concepts/architecture/',
		'/rust/concepts/queue-modes': '/horsies/concepts/queue-modes/',
		'/rust/concepts/task-lifecycle': '/horsies/concepts/task-lifecycle/',
		'/rust/concepts/workflows/workflow-semantics': '/horsies/concepts/workflows/workflow-semantics/',
		'/rust/internals/database-schema': '/horsies/internals/database-schema/',
		'/rust/internals/performance': '/horsies/internals/performance/',
		'/rust/internals/operational-indexes': '/horsies/internals/operational-indexes/',
		'/rust/monitoring/action-semantics': '/horsies/monitoring/action-semantics/',
		'/rust/monitoring/web-ui-overview': '/horsies/monitoring/web-ui-overview/',
		'/rust/monitoring/syce-overview': '/horsies/monitoring/syce-overview/',
		'/rust/operations/autovacuum-tuning': '/horsies/operations/autovacuum-tuning/',
		'/rust/workers/heartbeats-recovery': '/horsies/workers/heartbeats-recovery/',
		'/rust/workers/worker-architecture': '/horsies/workers/worker-architecture/',
		'/rust/scheduling/scheduler-overview': '/horsies/scheduling/scheduler-overview/',
		'/rust/questions-and-answers': '/horsies/questions-and-answers/',
		// A Rust API page that the 0.8.0 client replaced redirects to its new page;
		// one whose API the client does not have yet redirects to the Rust overview.
		'/rust/tasks/error-handling': '/horsies/rust/tasks/errors/',
		'/rust/tasks/retry-policy': '/horsies/rust/tasks/defining-tasks/',
		'/rust/concepts/result-handling': '/horsies/rust/tasks/retrieving-results/',
		'/rust/configuration/broker-config': '/horsies/rust/configuration/app-config/',
		'/rust/configuration/recovery-config': '/horsies/rust/configuration/app-config/',
		'/rust/configuration/retention-config': '/horsies/rust/configuration/app-config/',
		'/rust/monitoring/broker-methods': '/horsies/rust/monitoring/health-reads/',
		'/rust/workers/concurrency': '/horsies/workers/concurrency/',
		'/rust/internals/serialization': '/horsies/internals/serialization/',
		'/rust/cli': '/horsies/rust/',
		'/rust/concepts/workflows/subworkflows': '/horsies/rust/',
		'/rust/concepts/workflows/typed-node-builder': '/horsies/rust/',
		'/rust/concepts/workflows/workflow-api': '/horsies/rust/',
		'/rust/monitoring/web-ui-deployment': '/horsies/rust/',
		'/rust/operations/cutover-runbook': '/horsies/rust/',
		'/rust/quick-start/getting-started': '/horsies/rust/',
		'/rust/quick-start/01-configuring-horsies': '/horsies/rust/',
		'/rust/quick-start/02-producing-tasks': '/horsies/rust/',
		'/rust/quick-start/03-defining-workflows': '/horsies/rust/',
		'/rust/quick-start/04-scheduling': '/horsies/rust/',
		'/rust/quick-start/05-workflow-patterns': '/horsies/rust/',
		'/rust/scheduling/schedule-config': '/horsies/rust/',
		'/rust/scheduling/schedule-patterns': '/horsies/rust/',
	},
	integrations: [
		starlight({
			title: 'Horsies',
			logo: {
				src: './src/assets/galloping-horsie.jpg',
				alt: 'Horsies',
			},
			head: [
				{
					tag: 'script',
					attrs: { src: '/horsies/scripts/page-search.js', defer: true },
				},
				{
					tag: 'meta',
					attrs: {
						name: 'description',
						content: 'PostgreSQL-backed background task queue and workflow engine for Python and Rust.',
					},
				},
			],
			components: {
				ThemeSelect: './src/components/ThemeSelect.astro',
			},
			customCss: ['./src/styles/custom.css'],
			// Labels the titles of the client API pages, marks the shared pages, and keeps
			// previous/next links within one section.
			routeMiddleware: './src/routeData.ts',
			expressiveCode: {
				themes: [darkThemeS],
				styleOverrides: {
					borderColor: 'rgba(0, 255, 170, 0.4)',
					borderWidth: '2px',
					// Expressive Code derives a frame's outer radius as
					// `borderRadius + borderWidth`, so 6 + 2 lands on the 8px
					// --radius-md in src/styles/custom.css and the code blocks
					// match the cards. EC builds its frames outside that
					// stylesheet, so the value is duplicated here, not referenced.
					borderRadius: '6px',
				},
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/suleymanozkeskin/horsies' },
			],
			// Three sections: shared behaviour pages for both clients, then the API
			// pages of each client. The `section` frontmatter of a page outside rust/
			// names its section; src/routeData.ts reads it.
			sidebar: [
				{
					label: 'Shared: Behaviour',
					items: [
						{
							label: 'Concepts',
							items: [
								{ label: 'Architecture', slug: 'concepts/architecture' },
								{ label: 'Task Lifecycle', slug: 'concepts/task-lifecycle' },
								{ label: 'Queue Modes', slug: 'concepts/queue-modes' },
							],
						},
						{
							label: 'Tasks',
							items: [
								{ label: 'Errors Reference', slug: 'tasks/errors' },
								{ label: 'Retry Policy', slug: 'tasks/retry-policy' },
							],
						},
						{
							label: 'Workflows',
							items: [
								{ label: 'Workflow Semantics', slug: 'concepts/workflows/workflow-semantics' },
							],
						},
						{
							label: 'Workers',
							items: [
								{ label: 'Worker Architecture', slug: 'workers/worker-architecture' },
								{ label: 'Choosing an Execution Mode', slug: 'workers/execution-modes' },
								{ label: 'Shared Async Tasks', slug: 'workers/shared-async-tasks' },
								{ label: 'Shared Thread Tasks', slug: 'workers/shared-thread-tasks' },
								{ label: 'Concurrency', slug: 'workers/concurrency' },
								{ label: 'Heartbeats & Recovery', slug: 'workers/heartbeats-recovery' },
							],
						},
						{
							label: 'Configuration',
							items: [
								{ label: 'Broker Config', slug: 'configuration/broker-config' },
								{ label: 'Remote PostgreSQL', slug: 'configuration/remote-postgres' },
								{ label: 'Recovery Config', slug: 'configuration/recovery-config' },
								{ label: 'Retention Config', slug: 'configuration/retention-config' },
							],
						},
						{
							label: 'Scheduling',
							items: [
								{ label: 'Scheduler Overview', slug: 'scheduling/scheduler-overview' },
							],
						},
						{
							label: 'Monitoring',
							items: [
								{ label: 'Syce Overview', slug: 'monitoring/syce-overview' },
								{ label: 'Worker & Database Health', slug: 'monitoring/worker-health' },
								{ label: 'Web UI Overview', slug: 'monitoring/web-ui-overview' },
								{ label: 'Web UI Deployment & Auth', slug: 'monitoring/web-ui-deployment' },
								{ label: 'Action Semantics', slug: 'monitoring/action-semantics' },
							],
						},
						{
							label: 'Operations',
							items: [
								{ label: 'Atomic Enqueue & Topology', slug: 'operations/atomic-enqueue-and-topology' },
								{ label: 'Deploying Code Changes', slug: 'operations/deploying-code-changes' },
								{ label: 'Autovacuum Tuning', slug: 'operations/autovacuum-tuning' },
								{ label: 'PgBouncer', slug: 'troubleshooting/pgbouncer' },
							],
						},
						{
							label: 'Internals',
							items: [
								{ label: 'Database Schema', slug: 'internals/database-schema' },
								{ label: 'Performance', slug: 'internals/performance' },
								{ label: 'Serialization', slug: 'internals/serialization' },
								{ label: 'Operational Indexes', slug: 'internals/operational-indexes' },
							],
						},
						{ label: 'Questions & Answers', slug: 'questions-and-answers' },
					],
				},
				{
					label: 'Python API',
					items: [
						{
							label: 'Quick Start',
							items: [
								{ label: 'Getting Started', slug: 'quick-start/getting-started' },
								{ label: 'Configuring Horsies', slug: 'quick-start/01-configuring-horsies' },
								{ label: 'Producing Tasks', slug: 'quick-start/02-producing-tasks' },
								{ label: 'Defining Workflows', slug: 'quick-start/03-defining-workflows' },
								{ label: 'Scheduling', slug: 'quick-start/04-scheduling' },
								{ label: 'Workflow Patterns', slug: 'quick-start/05-workflow-patterns' },
								{ label: 'Showcase Application', slug: 'quick-start/06-showcase' },
							],
						},
						{
							label: 'Tasks',
							items: [
								{ label: 'Result Handling', slug: 'concepts/result-handling' },
								{ label: 'Defining Tasks', slug: 'tasks/defining-tasks' },
								{ label: 'Sending Tasks', slug: 'tasks/sending-tasks' },
								{ label: 'Error Handling', slug: 'tasks/error-handling' },
								{ label: 'Error Types', slug: 'tasks/error-types' },
								{ label: 'Task Types', slug: 'tasks/task-types' },
								{ label: 'Retrieving Results', slug: 'tasks/retrieving-results' },
							],
						},
						{
							label: 'Workflows',
							items: [
								{ label: 'Workflow API', slug: 'concepts/workflows/workflow-api' },
								{ label: 'Workflow Examples', slug: 'concepts/workflows/workflow-examples' },
								{ label: 'Subworkflows', slug: 'concepts/workflows/subworkflows' },
								{ label: 'Typed Node Builder', slug: 'concepts/workflows/typed-node-builder' },
								{ label: 'Start a Workflow Once', slug: 'guides/start-workflow-once' },
							],
						},
						{
							label: 'Configuration',
							items: [
								{ label: 'App Config', slug: 'configuration/app-config' },
								{ label: 'Broker Connections', slug: 'configuration/broker-connections' },
							],
						},
						{
							label: 'Workers',
							items: [
								{ label: 'Worker Profiles', slug: 'workers/worker-profiles' },
								{ label: 'Child-Process Hooks', slug: 'workers/child-process-hooks' },
							],
						},
						{
							label: 'Scheduling',
							items: [
								{ label: 'Schedule Patterns', slug: 'scheduling/schedule-patterns' },
								{ label: 'Schedule Config', slug: 'scheduling/schedule-config' },
							],
						},
						{
							label: 'Monitoring',
							items: [
								{ label: 'Health Reads', slug: 'monitoring/health-reads' },
								{ label: 'Broker Methods', slug: 'monitoring/broker-methods' },
								{ label: 'Web UI Mount', slug: 'monitoring/web-ui-mount' },
							],
						},
						{
							label: 'Operations',
							items: [
								{ label: 'Rerun in Your Transaction', slug: 'operations/rerun-in-your-transaction' },
								{ label: 'Code Change Examples', slug: 'operations/code-change-examples' },
								{ label: 'Async Frameworks', slug: 'guides/async-frameworks' },
								{ label: 'Testing', slug: 'operations/testing' },
								{ label: 'CLI Reference', slug: 'cli' },
							],
						},
					],
				},
				{
					// The API of the Rust client. Every slug is under rust/. An item with the
					// badge "not written" has no page yet and links to the issue that tracks it.
					label: 'Rust API',
					items: [
						{ label: 'Overview', slug: 'rust' },
						{
							label: 'Quick Start',
							items: [
								{ label: 'Getting Started', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Tasks',
							items: [
								{ label: 'Defining Tasks', slug: 'rust/tasks/defining-tasks' },
								{ label: 'Task Errors', slug: 'rust/tasks/errors' },
								{ label: 'Sending Tasks', slug: 'rust/tasks/sending-tasks' },
								{ label: 'Retrieving Results', slug: 'rust/tasks/retrieving-results' },
								{ label: 'Blocking API', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Workflows',
							items: [
								{ label: 'Workflow API', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'Typed Node Builder', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'Subworkflows', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Configuration',
							items: [
								{ label: 'App Config', slug: 'rust/configuration/app-config' },
							],
						},
						{
							label: 'Workers',
							items: [
								{ label: 'Worker Setup', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Scheduling',
							items: [
								{ label: 'Schedule Config', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Monitoring',
							items: [
								{ label: 'Health Reads', slug: 'rust/monitoring/health-reads' },
								{ label: 'Monitoring Queries', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'Web UI Mount', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Operations',
							items: [
								{ label: 'Rerun in Your Transaction', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'Async Frameworks', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'Testing', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
								{ label: 'CLI Reference', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
						{
							label: 'Migrations',
							items: [
								{ label: 'Switching to 0.8.0', link: 'https://github.com/suleymanozkeskin/horsies/issues/1412', badge: { text: 'not written', variant: 'caution' } },
							],
						},
					],
				},
				{
					label: 'Migrations',
					items: [
						{ label: 'Migrating to 0.1.2', slug: 'migrations/migration-to-0-1-2' },
						{ label: 'Migrating to 0.5.0', slug: 'migrations/migration-to-0-5-0' },
						{ label: 'Migrating to 0.6.0', slug: 'migrations/migration-to-0-6-0' },
						{ label: 'Migrating to 0.7.0', slug: 'migrations/migration-to-0-7-0' },
					],
				},
				{
					label: 'Changelog',
					items: [
						{ label: 'Changelog', slug: 'changelog' },
					],
				},
				],
		}),
	],
});
