import { useInfiniteQuery } from "@tanstack/react-query";

import type { InfiniteListOptions } from "@/lib/queries/pagination";

// Consumes a cursor-backed list query, flattening the fetched pages into a
// single array of rows. Pair the returned pagination controls with
// PaginatedListSentinel to auto-load subsequent pages as the operator scrolls.
export function usePaginatedList<T>(options: InfiniteListOptions<T>) {
  const { data, isLoading, hasNextPage, isFetchingNextPage, fetchNextPage } =
    useInfiniteQuery(options);
  const items = data?.pages.flatMap((page) => page.data) ?? [];
  return { items, isLoading, hasNextPage, isFetchingNextPage, fetchNextPage };
}
