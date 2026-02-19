import { useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";

import { TenantNav } from "@/components/tenant-nav";
import { tenantCredentialsQuery } from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const TYPE_LABELS: Record<string, string> = {
  api_key: "API Key",
  oauth_token: "OAuth Token",
  certificate: "Certificate",
  other: "Other",
};

function TypeBadge({ type }: { type: string }) {
  const variant =
    type === "certificate"
      ? "secondary"
      : type === "oauth_token"
        ? "outline"
        : "default";
  return <Badge variant={variant}>{TYPE_LABELS[type] ?? type}</Badge>;
}

export function TenantCredentialsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const { data: credentials, isLoading } = useQuery(
    tenantCredentialsQuery(tenantId),
  );

  return (
    <div>
      <TenantNav />

      <h2 className="text-lg font-semibold">Credentials</h2>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : credentials?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No credentials stored.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {credentials?.map((cred) => (
                <TableRow key={cred.id}>
                  <TableCell className="font-medium">{cred.name}</TableCell>
                  <TableCell>
                    <TypeBadge type={cred.type} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {cred.description ?? "-"}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(cred.createdAt).toLocaleDateString()}
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
