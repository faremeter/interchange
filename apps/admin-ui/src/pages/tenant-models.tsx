import { useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import { PaginatedListSentinel } from "@/components/paginated-list-sentinel";
import { usePaginatedList } from "@/lib/hooks/use-paginated-list";
import {
  createCatalogModelMutation,
  type CreateModelBody,
  tenantCatalogModelsInfiniteQuery,
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

export function TenantModelsPage() {
  const { tenantId } = useParams({
    from: "/authed/tenants/$tenantId/models",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const {
    items: models,
    isLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = usePaginatedList(tenantCatalogModelsInfiniteQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createDisplayName, setCreateDisplayName] = useState("");
  const [createDescription, setCreateDescription] = useState("");

  function resetCreateForm() {
    setCreateName("");
    setCreateDisplayName("");
    setCreateDescription("");
  }

  const createMut = useMutation({
    ...createCatalogModelMutation(tenantId, queryClient),
    onSuccess: () => {
      void createCatalogModelMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Models</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Model
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : models.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No models defined on this tenant.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Canonical name</TableHead>
                <TableHead>Display name</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {models.map((m) => (
                <TableRow
                  key={m.id}
                  className="cursor-pointer"
                  onClick={() =>
                    void navigate({
                      to: "/tenants/$tenantId/models/$modelId",
                      params: { tenantId, modelId: m.id },
                    })
                  }
                >
                  <TableCell className="font-medium">
                    {m.canonicalName}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {m.displayName ?? "-"}
                  </TableCell>
                  <TableCell>
                    {m.disabled ? (
                      <Badge variant="secondary">Disabled</Badge>
                    ) : (
                      <Badge>Enabled</Badge>
                    )}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(m.createdAt).toLocaleDateString()}
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
            <DialogTitle>Add Model</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: CreateModelBody = {
                canonicalName: createName.trim(),
              };
              if (createDisplayName.trim())
                body.displayName = createDisplayName.trim();
              if (createDescription.trim())
                body.description = createDescription.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="model-name">Canonical name</Label>
              <Input
                id="model-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                placeholder="e.g. claude-sonnet-4"
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="model-display">Display name</Label>
              <Input
                id="model-display"
                value={createDisplayName}
                onChange={(e) => setCreateDisplayName(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="model-description">Description</Label>
              <Input
                id="model-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={createMut.isPending || !createName.trim()}
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
