import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import { PaginatedListSentinel } from "@/components/paginated-list-sentinel";
import { usePaginatedList } from "@/lib/hooks/use-paginated-list";
import {
  createModelOfferingMutation,
  type CreateModelOfferingBody,
  tenantModelOfferingsInfiniteQuery,
  tenantCatalogModelsQuery,
  tenantModelProvidersQuery,
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

export const CAPABILITIES = [
  "vision",
  "audio-input",
  "tool-use",
  "extended-thinking",
  "structured-output",
  "long-context",
  "prompt-caching",
] as const;

export function TenantModelOfferingsPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/model-offerings",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const {
    items: offerings,
    isLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = usePaginatedList(tenantModelOfferingsInfiniteQuery(tenantId));
  const { data: models } = useQuery(tenantCatalogModelsQuery(tenantId));
  const { data: providers } = useQuery(tenantModelProvidersQuery(tenantId));

  const modelName = useMemo(() => {
    const map = new Map<string, string>();
    for (const m of models ?? []) map.set(m.id, m.canonicalName);
    return map;
  }, [models]);
  const providerName = useMemo(() => {
    const map = new Map<string, string>();
    for (const p of providers ?? []) map.set(p.id, p.name);
    return map;
  }, [providers]);

  const [createOpen, setCreateOpen] = useState(false);
  const [createModelId, setCreateModelId] = useState("");
  const [createProviderId, setCreateProviderId] = useState("");
  const [createPriority, setCreatePriority] = useState("0");
  const [createTags, setCreateTags] = useState("");
  const [createCaps, setCreateCaps] = useState<string[]>([]);

  const selectedModelId = createModelId || models?.[0]?.id || "";
  const selectedProviderId = createProviderId || providers?.[0]?.id || "";

  function resetCreateForm() {
    setCreateModelId("");
    setCreateProviderId("");
    setCreatePriority("0");
    setCreateTags("");
    setCreateCaps([]);
  }

  useEffect(() => {
    if (!createOpen) return;
    const m = models?.[0]?.id;
    const p = providers?.[0]?.id;
    if (m && !createModelId) setCreateModelId(m);
    if (p && !createProviderId) setCreateProviderId(p);
  }, [createOpen, models, providers, createModelId, createProviderId]);

  function toggleCap(cap: string) {
    setCreateCaps((prev) =>
      prev.includes(cap) ? prev.filter((c) => c !== cap) : [...prev, cap],
    );
  }

  const createMut = useMutation({
    ...createModelOfferingMutation(tenantId, queryClient),
    onSuccess: () => {
      void createModelOfferingMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Model Offerings</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Offering
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : offerings.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No model offerings defined on this tenant.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Model</TableHead>
                <TableHead>Provider</TableHead>
                <TableHead>Priority</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {offerings.map((o) => (
                <TableRow
                  key={o.id}
                  className="cursor-pointer"
                  onClick={() =>
                    void navigate({
                      to: "/tenants/$tenantId/model-offerings/$offeringId",
                      params: { tenantId, offeringId: o.id },
                    })
                  }
                >
                  <TableCell className="font-medium">
                    {modelName.get(o.modelId) ?? o.modelId}
                  </TableCell>
                  <TableCell>
                    {providerName.get(o.providerId) ?? o.providerId}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {o.priority}
                  </TableCell>
                  <TableCell>
                    {o.disabled ? (
                      <Badge variant="secondary">Disabled</Badge>
                    ) : (
                      <Badge>Enabled</Badge>
                    )}
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

      <Dialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open);
          if (!open) resetCreateForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Model Offering</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const tags = createTags
                .split(",")
                .map((t) => t.trim())
                .filter((t) => t.length > 0);
              const priority = Number.parseInt(createPriority, 10);
              const body: CreateModelOfferingBody = {
                modelId: selectedModelId,
                providerId: selectedProviderId,
                priority: Number.isNaN(priority) ? 0 : priority,
                deploymentTags: tags,
                capabilities: createCaps,
              };
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label>Model</Label>
              <Select
                value={selectedModelId}
                onValueChange={setCreateModelId}
                disabled={!models?.length}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a model" />
                </SelectTrigger>
                <SelectContent>
                  {models?.map((m) => (
                    <SelectItem key={m.id} value={m.id}>
                      {m.canonicalName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label>Provider</Label>
              <Select
                value={selectedProviderId}
                onValueChange={setCreateProviderId}
                disabled={!providers?.length}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a provider" />
                </SelectTrigger>
                <SelectContent>
                  {providers?.map((p) => (
                    <SelectItem key={p.id} value={p.id}>
                      {p.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="offering-priority">Priority</Label>
              <Input
                id="offering-priority"
                type="number"
                value={createPriority}
                onChange={(e) => setCreatePriority(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Lower values are preferred first during source resolution.
              </p>
            </div>
            <div className="grid gap-2">
              <Label>Capabilities</Label>
              <div className="flex flex-wrap gap-2">
                {CAPABILITIES.map((cap) => (
                  <Button
                    key={cap}
                    type="button"
                    size="sm"
                    variant={createCaps.includes(cap) ? "default" : "outline"}
                    onClick={() => toggleCap(cap)}
                  >
                    {cap}
                  </Button>
                ))}
              </div>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="offering-tags">Deployment tags</Label>
              <Input
                id="offering-tags"
                value={createTags}
                onChange={(e) => setCreateTags(e.target.value)}
                placeholder="Comma-separated, optional"
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending || !selectedModelId || !selectedProviderId
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
