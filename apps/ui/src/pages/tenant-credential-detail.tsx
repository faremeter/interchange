import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  credentialDetailQuery,
  deleteCredentialMutation,
  updateCredentialMutation,
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

export function TenantCredentialDetailPage() {
  const { tenantId, credentialId } = useParams({ strict: false }) as {
    tenantId: string;
    credentialId: string;
  };
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: credential, isLoading } = useQuery(
    credentialDetailQuery(tenantId, credentialId),
  );

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state -- initialized when entering edit mode
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSecret, setEditSecret] = useState("");

  function enterEditMode() {
    if (!credential) return;
    setEditName(credential.name);
    setEditDescription(credential.description ?? "");
    setEditSecret("");
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
  }

  const updateMut = useMutation({
    ...updateCredentialMutation(tenantId, credentialId, queryClient),
    onSuccess: () => {
      updateCredentialMutation(tenantId, credentialId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "credentials", credentialId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteCredentialMutation(tenantId, credentialId, queryClient),
    onSuccess: () => {
      deleteCredentialMutation(tenantId, credentialId, queryClient).onSuccess();
      navigate({
        to: "/tenants/$tenantId/credentials",
        params: { tenantId },
      });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!credential) return;
    const body: {
      name?: string;
      description?: string;
      secret?: string;
    } = {};
    if (editName.trim() !== credential.name) body.name = editName.trim();
    if (editDescription.trim() !== (credential.description ?? ""))
      body.description = editDescription.trim();
    if (editSecret) body.secret = editSecret;
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!credential) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/credentials"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Credentials
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{credential.name}</h2>
          {credential.description && (
            <p className="mt-1 text-sm text-muted-foreground">
              {credential.description}
            </p>
          )}
        </div>
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
                  <Label htmlFor="edit-secret">
                    New Secret (leave blank to keep current)
                  </Label>
                </div>
                <div className="px-4 py-2">
                  <Input
                    id="edit-secret"
                    type="password"
                    value={editSecret}
                    onChange={(e) => setEditSecret(e.target.value)}
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
              <TypeBadge type={credential.type} />
            </Row>
            <Row label="Description">{credential.description || "-"}</Row>
            <Row label="Created">
              {new Date(credential.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(credential.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete credential?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the credential &ldquo;
              {credential.name}&rdquo;. This action cannot be undone.
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
