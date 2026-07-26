import { StrictMode } from 'react';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { createRoot } from 'react-dom/client';

import { ToastProvider } from '@/components/ui/toast';
import { LiveProvider } from '@/events/live-provider';
import { initTheme } from '@/lib/theme';
import { router } from '@/routes/router';

import '@/styles/app.css';

// Applied before the first paint so the shell never flashes the wrong theme.
initTheme();

// Defaults are deliberately untouched: cadences are per-query (§ data-freshness
// contract), and a global override would silently change all of them.
const queryClient = new QueryClient();

const container = document.getElementById('root');
if (container === null) {
  throw new Error('Missing #root element in the served index.html.');
}

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
        <LiveProvider>
          <RouterProvider router={router} />
        </LiveProvider>
      </ToastProvider>
    </QueryClientProvider>
  </StrictMode>
);
