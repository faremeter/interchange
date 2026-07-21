import { infiniteQueryOptions, type InfiniteData } from "@tanstack/react-query";

import { api } from "@/lib/api";

// One keyset-paginated page of a list, mirroring the wire envelope every
// cursor-backed endpoint returns. `nextCursor` is null once the tail is reached.
export type Page<T> = {
  data: T[];
  nextCursor: string | null;
};

// Adds the page cursor to a request path. The first page carries no cursor; a
// cursor is opaque and may contain URL-reserved characters, and some base paths
// already carry a query string (e.g. `?status=running`), so the cursor is set
// through URLSearchParams rather than interpolated. The dummy origin lets the
// URL parser operate on the relative API paths these callers pass; only the
// path and query are returned.
export function pageRequestPath(
  basePath: string,
  cursor: string | undefined,
): string {
  if (cursor === undefined) return basePath;
  const url = new URL(basePath, "http://internal");
  url.searchParams.set("cursor", cursor);
  return `${url.pathname}${url.search}`;
}

// Builds the infinite-query options for a cursor-backed list endpoint. The five
// explicit type arguments are load-bearing: they force `initialPageParam:
// undefined` and the `string` cursor from `getNextPageParam` to unify to
// `string | undefined`. Inference alone narrows the seed to `undefined` and
// rejects the cursor.
export function infiniteListQuery<T>(queryKey: string[], basePath: string) {
  return infiniteQueryOptions<
    Page<T>,
    Error,
    InfiniteData<Page<T>>,
    string[],
    string | undefined
  >({
    queryKey,
    queryFn: ({ pageParam }) =>
      api<Page<T>>("GET", pageRequestPath(basePath, pageParam)),
    initialPageParam: undefined,
    // TanStack signals "no more pages" with `undefined`; the wire uses `null`.
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
  });
}

export type InfiniteListOptions<T> = ReturnType<typeof infiniteListQuery<T>>;
