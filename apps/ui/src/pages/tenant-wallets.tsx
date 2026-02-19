import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantWalletsQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const BACKEND_LABELS: Record<string, string> = {
  crypto: "Crypto",
  fiat: "Fiat",
  credits: "Credits",
};

function BackendBadge({ type }: { type: string }) {
  const variant =
    type === "crypto"
      ? "destructive"
      : type === "fiat"
        ? "secondary"
        : "outline";
  return <Badge variant={variant}>{BACKEND_LABELS[type] ?? type}</Badge>;
}

export function TenantWalletsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: wallets, isLoading } = useQuery(tenantWalletsQuery(tenantId));

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Wallets</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : wallets?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No wallets yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Currency</TableHead>
                <TableHead>Balance</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {wallets?.map((w) => (
                <TableRow key={w.id}>
                  <TableCell className="font-medium">{w.name}</TableCell>
                  <TableCell>
                    <BackendBadge type={w.backendType} />
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {w.currency}
                  </TableCell>
                  <TableCell className="font-mono text-sm">
                    {w.balance}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(w.createdAt).toLocaleDateString()}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
