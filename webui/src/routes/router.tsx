// Code-based route tree. The base path comes from the injected deployment
// config, so the same bundle works mounted anywhere.

import { lazy, Suspense } from 'react';

import {
  createRootRoute,
  createRoute,
  createRouter,
  useNavigate,
  useSearch,
} from '@tanstack/react-router';

import { AppShell } from '@/components/app-shell';
import { TaskExplorer } from '@/components/monitoring/task-explorer';
import { uiConfig } from '@/lib/config';
import {
  validateTaskSearch,
  validateWorkflowSearch,
  type TaskSearch,
  type WorkflowSearch,
} from '@/routes/search';

const rootRoute = createRootRoute({ component: AppShell });

// The DAG canvas and the resource charts are the two heaviest dependencies in
// the bundle and neither is needed on the tasks view, which is the landing
// route. Splitting them keeps the first load to the task explorer.
const WorkflowExplorer = lazy(async () => ({
  default: (await import('@/components/workflows/workflow-explorer'))
    .WorkflowExplorer,
}));
const WorkerOverview = lazy(async () => ({
  default: (await import('@/components/monitoring/worker-overview'))
    .WorkerOverview,
}));

function RouteFallback() {
  return (
    <p className="p-4 text-sm text-muted-foreground">Loading…</p>
  );
}

function TasksPage() {
  const search = useSearch({ from: '/' });
  const navigate = useNavigate({ from: '/' });
  return (
    <TaskExplorer
      search={search}
      onSearchChange={patch =>
        // A key whose patch value is `undefined` drops out of the URL, which is
        // how a filter is cleared.
        void navigate({
          search: previous => ({ ...previous, ...patch }) as TaskSearch,
          replace: true,
        })
      }
    />
  );
}

function WorkflowsPage() {
  const search = useSearch({ from: '/workflows' });
  const navigate = useNavigate({ from: '/workflows' });
  return (
    <Suspense fallback={<RouteFallback />}>
      <WorkflowExplorer
        search={search}
        onSearchChange={patch =>
          void navigate({
            search: previous => ({ ...previous, ...patch }) as WorkflowSearch,
            replace: true,
          })
        }
      />
    </Suspense>
  );
}

const tasksRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  validateSearch: validateTaskSearch,
  component: TasksPage,
});

const workflowsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/workflows',
  validateSearch: validateWorkflowSearch,
  component: WorkflowsPage,
});

function WorkersPage() {
  return (
    <Suspense fallback={<RouteFallback />}>
      <WorkerOverview />
    </Suspense>
  );
}

const workersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/workers',
  component: WorkersPage,
});

const routeTree = rootRoute.addChildren([
  tasksRoute,
  workflowsRoute,
  workersRoute,
]);

export const router = createRouter({
  routeTree,
  basepath: uiConfig.basePath,
  defaultPreload: false,
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
