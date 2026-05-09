import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";
import { type GrantEffect, grantEffects } from "@interchange/types";

import { MutationError } from "@/components/mutation-error";
import {
  grantDetailQuery,
  deleteGrantMutation,
  updateGrantMutation,
  tenantCredentialsQuery,
  tenantRolesQuery,
  tenantPrincipalsQuery,
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
import { Label } from "@/components/ui/label";

function isGrantEffect(v: string): v is GrantEffect {
  return (grantEffects as readonly string[]).includes(v);
}

function EffectBadge({ effect }: { effect: string }) {
  const variant =
    effect === "allow"
      ? "secondary"
      : effect === "deny"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{effect}</Badge>;
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

export function TenantGrantDetailPage() {
  const { tenantId, grantId } = useParams({
    from: "/authed/tenants/$tenantId/grants/$grantId",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: grant, isLoading } = useQuery(
    grantDetailQuery(tenantId, grantId),
  );
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));
  const { data: credentials } = useQuery(tenantCredentialsQuery(tenantId));

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state -- initialized when entering edit mode
  const [editEffect, setEditEffect] = useState<GrantEffect>("allow");

  function enterEditMode() {
    if (!grant) return;
    setEditEffect(grant.effect);
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
  }

  const updateMut = useMutation({
    ...updateGrantMutation(tenantId, grantId, queryClient),
    onSuccess: () => {
      updateGrantMutation(tenantId, grantId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "grants", grantId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteGrantMutation(tenantId, grantId, queryClient),
    onSuccess: () => {
      deleteGrantMutation(tenantId, grantId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/grants", params: { tenantId } });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!grant) return;
    const body: { effect?: GrantEffect } = {};
    if (editEffect !== grant.effect) body.effect = editEffect;
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!grant) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  const resourceName = grant.resource.startsWith("credential:")
    ? (credentials?.find(
        (c) => c.id === grant.resource.slice("credential:".length),
      )?.name ?? grant.resource.slice("credential:".length))
    : grant.resource;

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/grants"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Grants
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{resourceName}</h2>
          <p className="mt-1 text-sm text-muted-foreground">{grant.action}</p>
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
                Revoke
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
                  <Label htmlFor="edit-effect">Effect</Label>
                </div>
                <div className="px-4 py-2">
                  <Select
                    value={editEffect}
                    onValueChange={(v) => {
                      if (isGrantEffect(v)) setEditEffect(v);
                    }}
                  >
                    <SelectTrigger id="edit-effect">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {grantEffects.map((e) => (
                        <SelectItem key={e} value={e}>
                          {e}
                        </SelectItem>
                      ))}
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
            <Row label="Resource">
              <span className="font-mono text-xs">{resourceName}</span>
              {grant.resource.startsWith("credential:") && (
                <Badge variant="outline" className="ml-2 text-[10px]">
                  credential
                </Badge>
              )}
            </Row>
            <Row label="Action">
              <span className="font-mono text-xs">{grant.action}</span>
            </Row>
            <Row label="Effect">
              <EffectBadge effect={grant.effect} />
            </Row>
            <Row label="Origin">{grant.origin}</Row>
            <Row label="Target">
              {grant.roleId ? (
                <Badge variant="secondary">
                  {grant.roleName ??
                    roles?.find((r) => r.id === grant.roleId)?.name ??
                    grant.roleId}
                </Badge>
              ) : grant.principalId ? (
                <Badge variant="outline">
                  {grant.principalName ??
                    principals?.find((p) => p.id === grant.principalId)
                      ?.displayName ??
                    grant.principalId}
                </Badge>
              ) : (
                <span className="text-muted-foreground">--</span>
              )}
            </Row>
            <Row label="Created">
              {new Date(grant.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(grant.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      {/* Revoke confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke grant?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently revoke the grant on &ldquo;
              {resourceName}&rdquo;. This action cannot be undone.
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
              {deleteMut.isPending ? "Revoking..." : "Revoke"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
