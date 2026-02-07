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
						content: 'PostgreSQL-backed background task queue and workflow engine for Python. No Redis, no RabbitMQ — just PostgreSQL.',
					},
				},
			],
			components: {
				ThemeSelect: './src/components/ThemeSelect.astro',
			},
			customCss: ['./src/styles/custom.css'],
			expressiveCode: {
				themes: [darkThemeS],
				styleOverrides: {
					borderColor: 'rgba(0, 255, 170, 0.4)',
					borderWidth: '2px',
				},
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/suleymanozkeskin/horsies' },
			],
			sidebar: [
				{
					label: 'Quick Start',
					items: [
						{ label: 'Getting Started', slug: 'quick-start/getting-started' },
						{ label: 'Configuring Horsies', slug: 'quick-start/01-configuring-horsies' },
						{ label: 'Producing Tasks', slug: 'quick-start/02-producing-tasks' },
						{ label: 'Defining Workflows', slug: 'quick-start/03-defining-workflows' },
						{ label: 'Scheduling', slug: 'quick-start/04-scheduling' },
						{ label: 'Workflow Patterns', slug: 'quick-start/05-workflow-patterns' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Architecture', slug: 'concepts/architecture' },
					],
				},
				{
					label: 'Tasks',
					items: [
						{ label: 'Task Lifecycle', slug: 'concepts/task-lifecycle' },
						{ label: 'Result Handling', slug: 'concepts/result-handling' },
						{ label: 'Defining Tasks', slug: 'tasks/defining-tasks' },
						{ label: 'Sending Tasks', slug: 'tasks/sending-tasks' },
						{ label: 'Error Handling', slug: 'tasks/error-handling' },
						{ label: 'Errors Reference', slug: 'tasks/errors' },
						{ label: 'Retrieving Results', slug: 'tasks/retrieving-results' },
						{ label: 'Retry Policy', slug: 'tasks/retry-policy' },
					],
				},
				{
					label: 'Workflows',
					items: [
						{ label: 'Workflow Semantics', slug: 'concepts/workflows/workflow-semantics' },
						{ label: 'Workflow API', slug: 'concepts/workflows/workflow-api' },
						{ label: 'Subworkflows', slug: 'concepts/workflows/subworkflows' },
					],
				},
				{
					label: 'Configuration',
					items: [
						{ label: 'Queue Modes', slug: 'concepts/queue-modes' },
						{ label: 'App Config', slug: 'configuration/app-config' },
						{ label: 'Broker Config', slug: 'configuration/broker-config' },
						{ label: 'Recovery Config', slug: 'configuration/recovery-config' },
					],
				},
				{
					label: 'Workers',
					items: [
						{ label: 'Worker Architecture', slug: 'workers/worker-architecture' },
						{ label: 'Concurrency', slug: 'workers/concurrency' },
						{ label: 'Heartbeats & Recovery', slug: 'workers/heartbeats-recovery' },
					],
				},
				{
					label: 'Monitoring',
					items: [
						{ label: 'Syce Overview', slug: 'monitoring/syce-overview' },
						{ label: 'Broker Methods', slug: 'monitoring/broker-methods' },
					],
				},
				{
					label: 'Scheduling',
					items: [
						{ label: 'Scheduler Overview', slug: 'scheduling/scheduler-overview' },
						{ label: 'Schedule Patterns', slug: 'scheduling/schedule-patterns' },
						{ label: 'Schedule Config', slug: 'scheduling/schedule-config' },
					],
				},
				{
					label: 'CLI',
					items: [
						{ label: 'CLI Reference', slug: 'cli' },
					],
				},
				{
					label: 'Internals',
					items: [
						{ label: 'Database Schema', slug: 'internals/database-schema' },
						{ label: 'Serialization', slug: 'internals/serialization' },
					],
				},
				{
					label: 'Q&A',
					items: [
						{ label: 'Questions & Answers', slug: 'questions-and-answers' },
					],
				},
			],
		}),
	],
});
