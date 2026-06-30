import { useEffect, useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createModelProviderMutation,
  type CreateModelProviderBody,
  type ModelProviderPluginValue,
  tenantModelProvidersQuery,
  tenantCredentialsQuery,
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

const PLUGINS: ModelProviderPluginValue[] = [
  "anthropic",
  "openai",
  "openai-compatible",
  "google-genai",
];

function isPlugin(v: string): v is ModelProviderPluginValue {
  return (PLUGINS as string[]).includes(v);
}

export function TenantModelProvidersPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/model-providers",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: providers, isLoading } = useQuery(
    tenantModelProvidersQuery(tenantId),
  );
  const { data: credentials, isLoading: credsLoading } = useQuery(
    tenantCredentialsQuery(tenantId),
  );

  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createPlugin, setCreatePlugin] =
    useState<ModelProviderPluginValue>("anthropic");
  const [createBaseURL, setCreateBaseURL] = useState("");
  const [createCredentialId, setCreateCredentialId] = useState("");
  const selectedCredentialId = createCredentialId || credentials?.[0]?.id || "";

  function resetCreateForm() {
    setCreateName("");
    setCreatePlugin("anthropic");
    setCreateBaseURL("");
    setCreateCredentialId("");
  }

  useEffect(() => {
    const credId = credentials?.[0]?.id;
    if (!createOpen || createCredentialId || !credId) return;
    setCreateCredentialId(credId);
  }, [createOpen, createCredentialId, credentials]);

  const createMut = useMutation({
    ...createModelProviderMutation(tenantId, queryClient),
    onSuccess: () => {
      void createModelProviderMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Model Providers</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Provider
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : providers?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No model providers defined on this tenant.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Plugin</TableHead>
                <TableHead>Base URL</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {providers?.map((p) => (
                <TableRow
                  key={p.id}
                  className="cursor-pointer"
                  onClick={() =>
                    void navigate({
                      to: "/tenants/$tenantId/model-providers/$providerId",
                      params: { tenantId, providerId: p.id },
                    })
                  }
                >
                  <TableCell className="font-medium">{p.name}</TableCell>
                  <TableCell>
                    <Badge variant="outline">{p.plugin}</Badge>
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {p.baseURL}
                  </TableCell>
                  <TableCell>
                    {p.disabled ? (
                      <Badge variant="secondary">Disabled</Badge>
                    ) : (
                      <Badge>Enabled</Badge>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      <Dialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open);
          if (!open) resetCreateForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Model Provider</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: CreateModelProviderBody = {
                name: createName.trim(),
                plugin: createPlugin,
                baseURL: createBaseURL.trim(),
                credentialId: selectedCredentialId,
              };
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="provider-name">Name</Label>
              <Input
                id="provider-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label>Plugin</Label>
              <Select
                value={createPlugin}
                onValueChange={(v) => {
                  if (isPlugin(v)) setCreatePlugin(v);
                }}
              >
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PLUGINS.map((p) => (
                    <SelectItem key={p} value={p}>
                      {p}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="provider-baseurl">Base URL</Label>
              <Input
                id="provider-baseurl"
                value={createBaseURL}
                onChange={(e) => setCreateBaseURL(e.target.value)}
                placeholder="https://api.anthropic.com"
                required
              />
            </div>
            <div className="grid gap-2">
              <Label>Credential</Label>
              <Select
                value={selectedCredentialId}
                onValueChange={setCreateCredentialId}
                disabled={credsLoading || !credentials?.length}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a credential" />
                </SelectTrigger>
                <SelectContent>
                  {credentials?.map((cred) => (
                    <SelectItem key={cred.id} value={cred.id}>
                      {cred.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                A provider authenticates with exactly one credential.
              </p>
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending ||
                  !createName.trim() ||
                  !createBaseURL.trim() ||
                  !selectedCredentialId
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
