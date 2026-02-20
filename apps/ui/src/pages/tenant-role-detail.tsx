import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  roleDetailQuery,
  deleteRoleMutation,
  updateRoleMutation,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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

function Row({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid grid-cols-[160px_1fr] border-b last:border-b-0">
      <dt className="border-r bg-muted/50 px-4 py-3 text-sm font-medium text-muted-foreground">
        {label}
      </dt>
      <dd className="px-4 py-3 text-sm">{children}</dd>
    </div>
  );
}

export function TenantRoleDetailPage() {
  const { tenantId, roleId } = useParams({ strict: false }) as {
    tenantId: string;
    roleId: string;
  };
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: role, isLoading } = useQuery(roleDetailQuery(tenantId, roleId));

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state -- initialized when entering edit mode
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");

  function enterEditMode() {
    if (!role) return;
    setEditName(role.name);
    setEditDescription(role.description ?? "");
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
  }

  const updateMut = useMutation({
    ...updateRoleMutation(tenantId, roleId, queryClient),
    onSuccess: () => {
      updateRoleMutation(tenantId, roleId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "roles", roleId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteRoleMutation(tenantId, roleId, queryClient),
    onSuccess: () => {
      deleteRoleMutation(tenantId, roleId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/roles", params: { tenantId } });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!role) return;
    const body: {
      name?: string;
      description?: string;
    } = {};
    if (editName.trim() !== role.name) body.name = editName.trim();
    if (editDescription.trim() !== (role.description ?? ""))
      body.description = editDescription.trim();
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!role) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/roles"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Roles
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{role.name}</h2>
          {role.description && (
            <p className="mt-1 text-sm text-muted-foreground">
              {role.description}
            </p>
          )}
        </div>
        {!role.isSystem && (
          <div className="flex items-center gap-2">
            {editing ? (
              <Button variant="ghost" size="sm" onClick={cancelEdit}>
                <X className="size-4" />
                Cancel
              </Button>
            ) : (
              <>
                <Button variant="outline" size="sm" onClick={enterEditMode}>
                  <Pencil className="size-4" />
                  Edit
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setDeleteOpen(true)}
                  className="text-destructive hover:text-destructive"
                >
                  <Trash2 className="size-4" />
                  Delete
                </Button>
              </>
            )}
          </div>
        )}
      </div>

      <div className="mt-6">
        {editing ? (
          <form onSubmit={handleSave}>
            <div className="overflow-hidden rounded-lg border">
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-name">Name</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-name"
                    value={editName}
                    onChange={(e) => setEditName(e.target.value)}
                    required
                    autoFocus
                  />
                </div>
              </div>
              <div className="grid grid-cols-[160px_1fr]">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-description">Description</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-description"
                    value={editDescription}
                    onChange={(e) => setEditDescription(e.target.value)}
                    placeholder="Optional"
                  />
                </div>
              </div>
            </div>
            <MutationError error={updateMut.error} />
            <div className="mt-4">
              <Button
                type="submit"
                disabled={updateMut.isPending || !editName.trim()}
              >
                {updateMut.isPending ? "Saving..." : "Save Changes"}
              </Button>
            </div>
          </form>
        ) : (
          <dl className="overflow-hidden rounded-lg border">
            <Row label="Type">
              {role.isSystem ? (
                <Badge variant="default">system</Badge>
              ) : (
                <Badge variant="outline">custom</Badge>
              )}
            </Row>
            <Row label="Description">
              {role.description || (
                <span className="text-muted-foreground">--</span>
              )}
            </Row>
            <Row label="Created">
              {new Date(role.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(role.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete role?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the role &ldquo;{role.name}&rdquo;.
              This action cannot be undone.
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
