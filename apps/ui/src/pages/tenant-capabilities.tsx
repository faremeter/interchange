import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import {
  createCapabilityMutation,
  deleteCapabilityMutation,
  tenantAgentsQuery,
  tenantCapabilitiesQuery,
  updateCapabilityMutation,
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
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

type CapabilityRow = {
  id: string;
  agentId: string;
  agentName: string;
  name: string;
  description: string | null;
  pricing?: {
    base?: { amount: string; currency: string };
    negotiable?: boolean;
  };
};

export function TenantCapabilitiesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: capabilities, isLoading } = useQuery(
    tenantCapabilitiesQuery(tenantId),
  );
  const { data: agents } = useQuery(tenantAgentsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<CapabilityRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<CapabilityRow | null>(null);

  // Create form state
  const [createAgentId, setCreateAgentId] = useState("");
  const [createName, setCreateName] = useState("");
  const [createDescription, setCreateDescription] = useState("");

  // Edit form state
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");

  function resetCreateForm() {
    setCreateAgentId("");
    setCreateName("");
    setCreateDescription("");
  }

  const createMut = useMutation({
    ...createCapabilityMutation(tenantId, queryClient),
    onSuccess: () => {
      createCapabilityMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  const updateMut = useMutation({
    ...updateCapabilityMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateCapabilityMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteCapabilityMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteCapabilityMutation(
        tenantId,
        deleteTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setDeleteTarget(null);
    },
  });

  return (
    <div>
      <TenantNav />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Capabilities</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Capability
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : capabilities?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No capabilities registered.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Agent</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Pricing</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {capabilities?.map((cap) => (
                <TableRow key={cap.id}>
                  <TableCell className="font-medium">{cap.name}</TableCell>
                  <TableCell>
                    <Badge variant="secondary">{cap.agentName}</Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {cap.description ?? "-"}
                  </TableCell>
                  <TableCell>
                    {cap.pricing?.base ? (
                      <span className="font-mono text-xs">
                        {cap.pricing.base.amount} {cap.pricing.base.currency}
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">
                        Free
                      </span>
                    )}
                    {cap.pricing?.negotiable ? (
                      <Badge variant="outline" className="ml-1">
                        negotiable
                      </Badge>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon-xs">
                          <MoreHorizontal />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem
                          onClick={() => {
                            setEditTarget(cap);
                            setEditName(cap.name);
                            setEditDescription(cap.description ?? "");
                          }}
                        >
                          <Pencil />
                          Edit
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(cap)}
                        >
                          <Trash2 />
                          Delete
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
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
            <DialogTitle>Add Capability</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: {
                agentId: string;
                name: string;
                description?: string;
              } = {
                agentId: createAgentId,
                name: createName.trim(),
              };
              if (createDescription.trim())
                body.description = createDescription.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label>Agent</Label>
              <Select value={createAgentId} onValueChange={setCreateAgentId}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select an agent" />
                </SelectTrigger>
                <SelectContent>
                  {agents?.map((a) => (
                    <SelectItem key={a.id} value={a.id}>
                      {a.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cap-name">Name</Label>
              <Input
                id="cap-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cap-description">Description</Label>
              <Input
                id="cap-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending || !createAgentId || !createName.trim()
                }
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Edit dialog */}
      <Dialog
        open={!!editTarget}
        onOpenChange={(open) => {
          if (!open) setEditTarget(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Capability</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: { name?: string; description?: string } = {};
              if (editName.trim() !== editTarget?.name)
                body.name = editName.trim();
              if (editDescription.trim() !== (editTarget?.description ?? ""))
                body.description = editDescription.trim();
              updateMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="edit-cap-name">Name</Label>
              <Input
                id="edit-cap-name"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-cap-description">Description</Label>
              <Input
                id="edit-cap-description"
                value={editDescription}
                onChange={(e) => setEditDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <DialogFooter>
              <Button type="submit" disabled={updateMut.isPending}>
                {updateMut.isPending ? "Saving..." : "Save"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete capability?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the capability &ldquo;
              {deleteTarget?.name}&rdquo; from agent {deleteTarget?.agentName}.
              This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => deleteMut.mutate()}
              disabled={deleteMut.isPending}
            >
              {deleteMut.isPending ? "Deleting..." : "Delete"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
