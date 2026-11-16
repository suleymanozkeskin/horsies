import { useQuery } from '@tanstack/react-query';

import { ApiError } from '@/lib/http';
import { queryKeys } from '@/lib/query-keys';
import { getMeta } from '@/services/meta-api';
import type { MonitoringMeta } from '@/types/meta';

/** Deployment capabilities, fetched once on boot. A 403 is the authorization
 * verdict, not a transient failure, so it is never retried. */
export function useMeta(): {
  meta: MonitoringMeta | undefined;
  isLoading: boolean;
  error: unknown;
  refetch: () => void;
} {
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: queryKeys.meta(),
    queryFn: getMeta,
    staleTime: Infinity,
    retry: (failureCount, queryError) =>
      !(queryError instanceof ApiError && queryError.status === 403) &&
      failureCount < 2,
  });
  return {
    meta: data,
    isLoading,
    error,
    refetch: () => {
      void refetch();
    },
  };
}
