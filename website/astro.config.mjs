// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import darkThemeS from './src/themes/dark-theme-s.json';

// https://astro.build/config
export default defineConfig({
	site: 'https://suleymanozkeskin.github.io',
	base: '/horsies',
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
								{ label: 'Retrieving Results', slug: 'tasks/retrieving-results' },
							],
						},
						{
							label: 'Workflows',
							items: [
								{ label: 'Workflow API', slug: 'concepts/workflows/workflow-api' },
								{ label: 'Subworkflows', slug: 'concepts/workflows/subworkflows' },
								{ label: 'Typed Node Builder', slug: 'concepts/workflows/typed-node-builder' },
								{ label: 'Start a Workflow Once', slug: 'guides/start-workflow-once' },
							],
						},
						{
							label: 'Configuration',
							items: [
								{ label: 'App Config', slug: 'configuration/app-config' },
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
								{ label: 'Broker Methods', slug: 'monitoring/broker-methods' },
							],
						},
						{
							label: 'Operations',
							items: [
								{ label: 'Async Frameworks', slug: 'guides/async-frameworks' },
								{ label: 'Testing', slug: 'operations/testing' },
								{ label: 'CLI Reference', slug: 'cli' },
							],
						},
					],
				},
				{
					// The API of the Rust client. Every slug is under rust/.
					label: 'Rust API',
					collapsed: true,
					items: [
						{ label: 'Overview', slug: 'rust' },
						{
							label: 'Quick Start',
							items: [
								{ label: 'Getting Started', slug: 'rust/quick-start/getting-started' },
								{ label: 'Configuring Horsies', slug: 'rust/quick-start/01-configuring-horsies' },
								{ label: 'Producing Tasks', slug: 'rust/quick-start/02-producing-tasks' },
								{ label: 'Defining Workflows', slug: 'rust/quick-start/03-defining-workflows' },
								{ label: 'Scheduling', slug: 'rust/quick-start/04-scheduling' },
								{ label: 'Workflow Patterns', slug: 'rust/quick-start/05-workflow-patterns' },
							],
						},
						{
							label: 'Q&A',
							items: [
								{ label: 'Questions & Answers', slug: 'rust/questions-and-answers' },
							],
						},
						{
							label: 'Concepts',
							items: [
								{ label: 'Architecture', slug: 'rust/concepts/architecture' },
							],
						},
						{
							label: 'Tasks',
							items: [
								{ label: 'Task Lifecycle', slug: 'rust/concepts/task-lifecycle' },
								{ label: 'Result Handling', slug: 'rust/concepts/result-handling' },
								{ label: 'Defining Tasks', slug: 'rust/tasks/defining-tasks' },
								{ label: 'Sending Tasks', slug: 'rust/tasks/sending-tasks' },
								{ label: 'Error Handling', slug: 'rust/tasks/error-handling' },
								{ label: 'Errors Reference', slug: 'rust/tasks/errors' },
								{ label: 'Retrieving Results', slug: 'rust/tasks/retrieving-results' },
								{ label: 'Retry Policy', slug: 'rust/tasks/retry-policy' },
							],
						},
						{
							label: 'Workflows',
							items: [
								{ label: 'Workflow Semantics', slug: 'rust/concepts/workflows/workflow-semantics' },
								{ label: 'Workflow API', slug: 'rust/concepts/workflows/workflow-api' },
								{ label: 'Subworkflows', slug: 'rust/concepts/workflows/subworkflows' },
							],
						},
						{
							label: 'Configuration',
							items: [
								{ label: 'Queue Modes', slug: 'rust/concepts/queue-modes' },
								{ label: 'App Config', slug: 'rust/configuration/app-config' },
								{ label: 'Broker Config', slug: 'rust/configuration/broker-config' },
								{ label: 'Recovery Config', slug: 'rust/configuration/recovery-config' },
								{ label: 'Retention Config', slug: 'rust/configuration/retention-config' },
							],
						},
						{
							label: 'Workers',
							items: [
								{ label: 'Worker Architecture', slug: 'rust/workers/worker-architecture' },
								{ label: 'Concurrency', slug: 'rust/workers/concurrency' },
								{ label: 'Heartbeats & Recovery', slug: 'rust/workers/heartbeats-recovery' },
							],
						},
						{
							label: 'Monitoring',
							items: [
								{ label: 'Web UI Overview', slug: 'rust/monitoring/web-ui-overview' },
								{ label: 'Deployment & Authentication', slug: 'rust/monitoring/web-ui-deployment' },
								{ label: 'Action Semantics', slug: 'rust/monitoring/action-semantics' },
								{ label: 'Syce Overview', slug: 'rust/monitoring/syce-overview' },
								{ label: 'Broker Methods', slug: 'rust/monitoring/broker-methods' },
							],
						},
						{
							label: 'Scheduling',
							items: [
								{ label: 'Scheduler Overview', slug: 'rust/scheduling/scheduler-overview' },
								{ label: 'Schedule Patterns', slug: 'rust/scheduling/schedule-patterns' },
								{ label: 'Schedule Config', slug: 'rust/scheduling/schedule-config' },
							],
						},
						{
							label: 'CLI',
							items: [
								{ label: 'CLI Reference', slug: 'rust/cli' },
							],
						},
						{
							label: 'Operations',
							items: [
								{ label: 'Autovacuum Tuning', slug: 'rust/operations/autovacuum-tuning' },
								{ label: 'Task-history Cutover', slug: 'rust/operations/cutover-runbook' },
							],
						},
						{
							label: 'Internals',
							items: [
								{ label: 'Database Schema', slug: 'rust/internals/database-schema' },
								{ label: 'Serialization', slug: 'rust/internals/serialization' },
								{ label: 'Performance', slug: 'rust/internals/performance' },
								{ label: 'Operational Indexes', slug: 'rust/internals/operational-indexes' },
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
