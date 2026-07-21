import { useEffect, useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";
import { type CredentialType, credentialTypes } from "@intx/types";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import { PaginatedListSentinel } from "@/components/paginated-list-sentinel";
import { usePaginatedList } from "@/lib/hooks/use-paginated-list";
import {
  createCredentialMutation,
  type CreateCredentialBody,
  tenantProvidersQuery,
  tenantCredentialsInfiniteQuery,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function isCredentialType(v: string): v is CredentialType {
  return (credentialTypes as readonly string[]).includes(v);
}

const TYPE_LABELS: Record<CredentialType, string> = {
  api_key: "API Key",
  oauth_token: "OAuth Token",
  certificate: "Certificate",
  other: "Other",
};

function TypeBadge({ type }: { type: CredentialType }) {
  const variant =
    type === "certificate"
      ? "secondary"
      : type === "oauth_token"
        ? "outline"
        : "default";
  return <Badge variant={variant}>{TYPE_LABELS[type]}</Badge>;
}

export function TenantCredentialsPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/credentials",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const {
    items: credentials,
    isLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = usePaginatedList(tenantCredentialsInfiniteQuery(tenantId));
  const { data: providers, isLoading: providersLoading } = useQuery(
    tenantProvidersQuery(tenantId),
  );

  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createType, setCreateType] = useState<CredentialType>("api_key");
  const [createProviderId, setCreateProviderId] = useState("");
  const [createSecret, setCreateSecret] = useState("");
  const [createDescription, setCreateDescription] = useState("");
  const selectedProviderId = createProviderId || providers?.[0]?.id || "";

  function resetCreateForm() {
    setCreateName("");
    setCreateType("api_key");
    setCreateProviderId("");
    setCreateSecret("");
    setCreateDescription("");
  }

  useEffect(() => {
    const providerId = providers?.[0]?.id;
    if (!createOpen || createProviderId || !providerId) return;
    setCreateProviderId(providerId);
  }, [createOpen, createProviderId, providers]);

  const createMut = useMutation({
    ...createCredentialMutation(tenantId, queryClient),
    onSuccess: () => {
      void createCredentialMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Credentials</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Credential
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : credentials.length === 0 ? (
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
              {credentials.map((cred) => (
                <TableRow
                  key={cred.id}
                  className="cursor-pointer"
                  onClick={() =>
                    void navigate({
                      to: "/tenants/$tenantId/credentials/$credentialId",
                      params: { tenantId, credentialId: cred.id },
                    })
                  }
                >
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
          <PaginatedListSentinel
            hasNextPage={hasNextPage}
            isFetchingNextPage={isFetchingNextPage}
            fetchNextPage={fetchNextPage}
          />
        </div>
      )}

      {/* Create dialog */}
      <Dialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open);
          if (!open) resetCreateForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Credential</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: CreateCredentialBody = {
                providerId: selectedProviderId,
                name: createName.trim(),
                type: createType,
                secret: createSecret,
              };
              if (createDescription.trim())
                body.description = createDescription.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="cred-name">Name</Label>
              <Input
                id="cred-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label>Provider</Label>
              <Select
                value={selectedProviderId}
                onValueChange={setCreateProviderId}
                disabled={providersLoading || !providers?.length}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a provider" />
                </SelectTrigger>
                <SelectContent>
                  {providers?.map((provider) => (
                    <SelectItem key={provider.id} value={provider.id}>
                      {provider.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label>Type</Label>
              <Select
                value={createType}
                onValueChange={(v) => {
                  if (isCredentialType(v)) setCreateType(v);
                }}
              >
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {credentialTypes.map((t) => (
                    <SelectItem key={t} value={t}>
                      {TYPE_LABELS[t]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cred-secret">Secret</Label>
              <Input
                id="cred-secret"
                type="password"
                value={createSecret}
                onChange={(e) => setCreateSecret(e.target.value)}
                required
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cred-description">Description</Label>
              <Input
                id="cred-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending ||
                  !selectedProviderId ||
                  !createName.trim() ||
                  !createSecret
                }
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
