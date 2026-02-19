import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import { TenantNav } from "@/components/tenant-nav";
import {
  createRoleMutation,
  deleteRoleMutation,
  tenantRolesQuery,
  updateRoleMutation,
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

type RoleFormValues = { name: string; description: string };
type RoleRow = {
  id: string;
  name: string;
  description: string | null;
  isSystem: boolean;
};

function RoleFormDialog({
  open,
  onOpenChange,
  title,
  initial,
  isPending,
  error,
  onSubmit,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  initial: RoleFormValues;
  isPending: boolean;
  error: Error | null;
  onSubmit: (values: RoleFormValues) => void;
}) {
  const [name, setName] = useState(initial.name);
  const [description, setDescription] = useState(initial.description);

  // Reset form when dialog opens with new initial values
  const resetAndOpen = (next: boolean) => {
    if (next) {
      setName(initial.name);
      setDescription(initial.description);
    }
    onOpenChange(next);
  };

  return (
    <Dialog open={open} onOpenChange={resetAndOpen}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            onSubmit({ name: name.trim(), description: description.trim() });
          }}
          className="grid gap-4"
        >
          <div className="grid gap-2">
            <Label htmlFor="role-name">Name</Label>
            <Input
              id="role-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              autoFocus
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="role-description">Description</Label>
            <Input
              id="role-description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional"
            />
          </div>
          <MutationError error={error} />
          <DialogFooter>
            <Button type="submit" disabled={isPending || !name.trim()}>
              {isPending ? "Saving..." : "Save"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export function TenantRolesPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: roles, isLoading } = useQuery(tenantRolesQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<RoleRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<RoleRow | null>(null);

  const createMut = useMutation({
    ...createRoleMutation(tenantId, queryClient),
    onSuccess: () => {
      createRoleMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
    },
  });

  const updateMut = useMutation({
    ...updateRoleMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateRoleMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteRoleMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteRoleMutation(
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
        <h2 className="text-lg font-semibold">Roles</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Create Role
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : roles?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No roles yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {roles?.map((r) => (
                <TableRow key={r.id}>
                  <TableCell className="font-medium">{r.name}</TableCell>
                  <TableCell>
                    <Badge variant={r.isSystem ? "default" : "outline"}>
                      {r.isSystem ? "system" : "custom"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {r.description ?? "-"}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(r.createdAt).toLocaleDateString()}
                  </TableCell>
                  <TableCell>
                    {!r.isSystem && (
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button variant="ghost" size="icon-xs">
                            <MoreHorizontal />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onClick={() => setEditTarget(r)}>
                            <Pencil />
                            Edit
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            variant="destructive"
                            onClick={() => setDeleteTarget(r)}
                          >
                            <Trash2 />
                            Delete
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Create dialog */}
      <RoleFormDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        title="Create Role"
        initial={{ name: "", description: "" }}
        isPending={createMut.isPending}
        error={createMut.error}
        onSubmit={(v) => {
          const body: { name: string; description?: string } = { name: v.name };
          if (v.description) body.description = v.description;
          createMut.mutate(body);
        }}
      />

      {/* Edit dialog */}
      {editTarget && (
        <RoleFormDialog
          open={!!editTarget}
          onOpenChange={(open) => {
            if (!open) setEditTarget(null);
          }}
          title="Edit Role"
          initial={{
            name: editTarget.name,
            description: editTarget.description ?? "",
          }}
          isPending={updateMut.isPending}
          error={updateMut.error}
          onSubmit={(v) => {
            const body: { name?: string; description?: string } = {};
            if (v.name !== editTarget.name) body.name = v.name;
            if (v.description !== (editTarget.description ?? ""))
              body.description = v.description;
            updateMut.mutate(body);
          }}
        />
      )}

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete role?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the role &ldquo;{deleteTarget?.name}
              &rdquo;. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <MutationError error={deleteMut.error} />
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
