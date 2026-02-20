import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  offeringDetailQuery,
  deleteOfferingMutation,
  updateOfferingMutation,
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

export function TenantOfferingDetailPage() {
  const { tenantId, offeringId } = useParams({ strict: false }) as {
    tenantId: string;
    offeringId: string;
  };
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: offering, isLoading } = useQuery(
    offeringDetailQuery(tenantId, offeringId),
  );

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state -- initialized when entering edit mode
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");

  function enterEditMode() {
    if (!offering) return;
    setEditName(offering.name);
    setEditDescription(offering.description ?? "");
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
  }

  const updateMut = useMutation({
    ...updateOfferingMutation(tenantId, offeringId, queryClient),
    onSuccess: () => {
      updateOfferingMutation(tenantId, offeringId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "offerings", offeringId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteOfferingMutation(tenantId, offeringId, queryClient),
    onSuccess: () => {
      deleteOfferingMutation(tenantId, offeringId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/offerings", params: { tenantId } });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!offering) return;
    const body: {
      name?: string;
      description?: string;
    } = {};
    if (editName.trim() !== offering.name) body.name = editName.trim();
    if (editDescription.trim() !== (offering.description ?? ""))
      body.description = editDescription.trim();
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!offering) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/offerings"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Offerings
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{offering.name}</h2>
          {offering.description && (
            <p className="mt-1 text-sm text-muted-foreground">
              {offering.description}
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
            <Row label="Agent">
              <Badge variant="secondary">{offering.agentName}</Badge>
            </Row>
            <Row label="Pricing">
              {offering.pricing?.base ? (
                <span>
                  {offering.pricing.base.amount}{" "}
                  {offering.pricing.base.currency}
                </span>
              ) : (
                <span>Free</span>
              )}
              {offering.pricing?.negotiable && (
                <Badge variant="outline" className="ml-2">
                  negotiable
                </Badge>
              )}
            </Row>
            {offering.schema && (
              <Row label="Schema">
                <pre className="whitespace-pre-wrap text-xs">
                  <code>{JSON.stringify(offering.schema, null, 2)}</code>
                </pre>
              </Row>
            )}
          </dl>
        )}
      </div>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete offering?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the offering &ldquo;{offering.name}
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
