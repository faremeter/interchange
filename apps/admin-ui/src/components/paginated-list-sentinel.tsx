import { useEffect, useRef } from "react";
import type { FetchNextPageOptions } from "@tanstack/react-query";

type PaginatedListSentinelProps = {
  hasNextPage: boolean;
  isFetchingNextPage: boolean;
  fetchNextPage: (options?: FetchNextPageOptions) => Promise<unknown>;
};

export function PaginatedListSentinel({
  hasNextPage,
  isFetchingNextPage,
  fetchNextPage,
}: PaginatedListSentinelProps) {
  // Auto-load the next page when the sentinel scrolls into view. The guard keeps
  // a single fetch in flight; the observer re-attaches whenever
  // `hasNextPage`/`fetchNextPage` change and disconnects on cleanup, so the
  // callback never closes over a stale `isFetchingNextPage`.
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel || !hasNextPage) return;

    const observer = new IntersectionObserver((entries) => {
      if (entries[0]?.isIntersecting && !isFetchingNextPage) {
        void fetchNextPage();
      }
    });
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage]);

  return (
    <>
      <div ref={sentinelRef} />
      {isFetchingNextPage ? (
        <p className="p-3 text-sm text-muted-foreground">Loading more...</p>
      ) : null}
    </>
  );
}
