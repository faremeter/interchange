import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  modelProviderDetailQuery,
  deleteModelProviderMutation,
  updateModelProviderMutation,
  type UpdateModelProviderBody,
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

export function TenantModelProviderDetailPage() {
  const { tenantId, providerId } = useParams({
    from: "/authed/tenants/$tenantId/model-providers/$providerId",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: provider, isLoading } = useQuery(
    modelProviderDetailQuery(tenantId, providerId),
  );

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const [editName, setEditName] = useState("");
  const [editBaseURL, setEditBaseURL] = useState("");
  const [editDisabled, setEditDisabled] = useState(false);

  function enterEditMode() {
    if (!provider) return;
    setEditName(provider.name);
    setEditBaseURL(provider.baseURL);
    setEditDisabled(provider.disabled);
    setEditing(true);
  }

  const updateMut = useMutation({
    ...updateModelProviderMutation(tenantId, providerId, queryClient),
    onSuccess: () => {
      void updateModelProviderMutation(
        tenantId,
        providerId,
        queryClient,
      ).onSuccess();
      void queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "model-providers", providerId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteModelProviderMutation(tenantId, providerId, queryClient),
    onSuccess: () => {
      void deleteModelProviderMutation(
        tenantId,
        providerId,
        queryClient,
      ).onSuccess();
      void navigate({
        to: "/tenants/$tenantId/model-providers",
        params: { tenantId },
      });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!provider) return;
    const body: UpdateModelProviderBody = {};
    if (editName.trim() !== provider.name) body.name = editName.trim();
    if (editBaseURL.trim() !== provider.baseURL)
      body.baseURL = editBaseURL.trim();
    if (editDisabled !== provider.disabled) body.disabled = editDisabled;
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!provider) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/model-providers"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Model Providers
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{provider.name}</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {provider.plugin}
          </p>
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
              <div className="grid grid-cols-[160px_1fr] border-b">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-baseurl">Base URL</Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-baseurl"
                    value={editBaseURL}
                    onChange={(e) => setEditBaseURL(e.target.value)}
                    required
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
            <Row label="Plugin">
              <Badge variant="outline">{provider.plugin}</Badge>
            </Row>
            <Row label="Base URL">{provider.baseURL}</Row>
            <Row label="Credential">{provider.credentialId ?? "-"}</Row>
            <Row label="Wallet">{provider.walletId ?? "-"}</Row>
            <Row label="Status">
              {provider.disabled ? (
                <Badge variant="secondary">Disabled</Badge>
              ) : (
                <Badge>Enabled</Badge>
              )}
            </Row>
            <Row label="Created">
              {new Date(provider.createdAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete provider?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently deletes the provider &ldquo;{provider.name}
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
