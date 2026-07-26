import { apiGet } from '@/lib/http';
import type { MonitoringMeta } from '@/types/meta';

/** Deployment capabilities. A 403 here means the viewer is not authorized at
 * all, which the shell renders as a full-screen state. */
export const getMeta = (): Promise<MonitoringMeta> =>
  apiGet<MonitoringMeta>('/meta');
