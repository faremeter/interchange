import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  catalogModelDetailQuery,
  deleteCatalogModelMutation,
  updateCatalogModelMutation,
  type UpdateModelBody,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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

export function TenantModelDetailPage() {
  const { tenantId, modelId } = useParams({
    from: "/authed/tenants/$tenantId/models/$modelId",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: model, isLoading } = useQuery(
    catalogModelDetailQuery(tenantId, modelId),
  );

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const [editDisplayName, setEditDisplayName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editDisabled, setEditDisabled] = useState(false);

  function enterEditMode() {
    if (!model) return;
    setEditDisplayName(model.displayName ?? "");
    setEditDescription(model.description ?? "");
    setEditDisabled(model.disabled);
    setEditing(true);
  }

  const updateMut = useMutation({
    ...updateCatalogModelMutation(tenantId, modelId, queryClient),
    onSuccess: () => {
      updateCatalogModelMutation(tenantId, modelId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "catalog-models", modelId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteCatalogModelMutation(tenantId, modelId, queryClient),
    onSuccess: () => {
      deleteCatalogModelMutation(tenantId, modelId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/models", params: { tenantId } });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!model) return;
    const body: UpdateModelBody = {};
    if (editDisplayName.trim() !== (model.displayName ?? ""))
      body.displayName = editDisplayName.trim() || null;
    if (editDescription.trim() !== (model.description ?? ""))
      body.description = editDescription.trim() || null;
    if (editDisabled !== model.disabled) body.disabled = editDisabled;
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!model) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/models"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Models
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{model.canonicalName}</h2>
          {model.displayName && (
            <p className="mt-1 text-sm text-muted-foreground">
              {model.displayName}
            </p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {editing ? (
            <Button variant="ghost" size="sm" onClick={() => setEditing(false)}>
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
      </div>

      <div className="mt-6">
        {editing ? (
          <form onSubmit={handleSave}>
            <div className="overflow-hidden rounded-lg border">
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-display">Display name</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-display"
                    value={editDisplayName}
                    onChange={(e) => setEditDisplayName(e.target.value)}
                    placeholder="Optional"
                    autoFocus
                  />
                </div>
              </div>
              <div className="grid grid-cols-[160px_1fr] border-b">
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
              <div className="grid grid-cols-[160px_1fr]">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label>Status</Label>
                </div>
                <div className="px-4 py-2">
                  <Select
                    value={editDisabled ? "disabled" : "enabled"}
                    onValueChange={(v) => setEditDisabled(v === "disabled")}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="enabled">Enabled</SelectItem>
                      <SelectItem value="disabled">Disabled</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
            <MutationError error={updateMut.error} />
            <div className="mt-4">
              <Button type="submit" disabled={updateMut.isPending}>
                {updateMut.isPending ? "Saving..." : "Save Changes"}
              </Button>
            </div>
          </form>
        ) : (
          <dl className="overflow-hidden rounded-lg border">
            <Row label="Canonical name">{model.canonicalName}</Row>
            <Row label="Display name">{model.displayName || "-"}</Row>
            <Row label="Description">{model.description || "-"}</Row>
            <Row label="Status">
              {model.disabled ? (
                <Badge variant="secondary">Disabled</Badge>
              ) : (
                <Badge>Enabled</Badge>
              )}
            </Row>
            <Row label="Created">
              {new Date(model.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(model.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete model?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently deletes the model &ldquo;{model.canonicalName}
              &rdquo; and the offerings that reference it. This action cannot be
              undone.
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
