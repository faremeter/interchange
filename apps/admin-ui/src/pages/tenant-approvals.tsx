import { useEffect, useRef } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useInfiniteQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import {
  tenantApprovalsInfiniteQuery,
  type ApprovalResponse,
} from "@/lib/queries/tenants";
import { parseApprovalSnapshot } from "@/lib/approval-snapshot";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function toolName(approval: ApprovalResponse): string {
  const snapshot = parseApprovalSnapshot(
    approval.toolDefinition,
    approval.toolArguments,
  );
  return snapshot.ok ? snapshot.name : "(unknown tool)";
}

function ApprovalRow({
  approval,
  tenantId,
}: {
  approval: ApprovalResponse;
  tenantId: string;
}) {
  const navigate = useNavigate();
  return (
    <TableRow
      className="cursor-pointer"
      onClick={() =>
        void navigate({
          to: "/tenants/$tenantId/approvals/$approvalId",
          params: { tenantId, approvalId: approval.id },
        })
      }
    >
      <TableCell className="font-mono text-xs">{toolName(approval)}</TableCell>
      <TableCell>
        <Badge variant="outline">{approval.status}</Badge>
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">
        {approval.agentAddress}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {new Date(approval.createdAt).toLocaleString()}
      </TableCell>
    </TableRow>
  );
}

export function TenantApprovalsPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/approvals",
  });
  const { data, isLoading, hasNextPage, isFetchingNextPage, fetchNextPage } =
    useInfiniteQuery(tenantApprovalsInfiniteQuery(tenantId));

  const approvals = data?.pages.flatMap((page) => page.data) ?? [];

  // Auto-load the next page when the sentinel below the table scrolls into
  // view. The guard keeps a single fetch in flight; the observer re-attaches
  // whenever `hasNextPage`/`fetchNextPage` change and disconnects on cleanup,
  // so the callback never closes over a stale `isFetchingNextPage`.
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
    <div>
      <TenantNav tenantId={tenantId} />
      <h2 className="text-lg font-semibold">Approvals</h2>
      <p className="mt-1 text-sm text-muted-foreground">
        Tool calls awaiting an approval decision in this tenant.
      </p>
      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : approvals.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No pending approvals.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Tool</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Agent</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {approvals.map((a) => (
                <ApprovalRow key={a.id} approval={a} tenantId={tenantId} />
              ))}
            </TableBody>
          </Table>
          <div ref={sentinelRef} />
          {isFetchingNextPage ? (
            <p className="p-3 text-sm text-muted-foreground">Loading more...</p>
          ) : null}
        </div>
      )}
    </div>
  );
}
