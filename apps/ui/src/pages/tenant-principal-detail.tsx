import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Trash2, X } from "lucide-react";
import {
  type UpdatablePrincipalStatus,
  updatablePrincipalStatuses,
} from "@interchange/types";

import { MutationError } from "@/components/mutation-error";
import {
  principalDetailQuery,
  deletePrincipalMutation,
  updatePrincipalMutation,
  assignRoleMutation,
  removeRoleMutation,
  tenantRolesQuery,
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

function isUpdatablePrincipalStatus(v: string): v is UpdatablePrincipalStatus {
  return (updatablePrincipalStatuses as readonly string[]).includes(v);
}

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "active"
      ? "secondary"
      : status === "suspended" || status === "deactivated"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
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

export function TenantPrincipalDetailPage() {
  const { tenantId, principalId } = useParams({
    from: "/authed/tenants/$tenantId/principals/$principalId",
  });
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: principal, isLoading } = useQuery(
    principalDetailQuery(tenantId, principalId),
  );

  const { data: allRoles } = useQuery(tenantRolesQuery(tenantId));

  const [deleteOpen, setDeleteOpen] = useState(false);
  const [selectedStatus, setSelectedStatus] = useState<
    UpdatablePrincipalStatus | ""
  >("");
  const [selectedRoleId, setSelectedRoleId] = useState<string>("");

  const updateMut = useMutation({
    ...updatePrincipalMutation(tenantId, principalId, queryClient),
    onSuccess: () => {
      updatePrincipalMutation(tenantId, principalId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "principals", principalId],
      });
      setSelectedStatus("");
    },
  });

  const assignMut = useMutation({
    ...assignRoleMutation(tenantId, principalId, queryClient),
    onSuccess: () => {
      assignRoleMutation(tenantId, principalId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "principals", principalId],
      });
      setSelectedRoleId("");
    },
  });

  const removeMut = useMutation({
    ...removeRoleMutation(tenantId, principalId, queryClient),
    onSuccess: () => {
      removeRoleMutation(tenantId, principalId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "principals", principalId],
      });
    },
  });

  const deleteMut = useMutation({
    ...deletePrincipalMutation(tenantId, principalId, queryClient),
    onSuccess: () => {
      deletePrincipalMutation(tenantId, principalId, queryClient).onSuccess();
      navigate({
        to: "/tenants/$tenantId/principals",
        params: { tenantId },
      });
    },
  });

  const assignedRoleIds = new Set(principal?.roles.map((r) => r.id) ?? []);
  const unassignedRoles = (allRoles ?? []).filter(
    (r) => !assignedRoleIds.has(r.id),
  );

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!principal) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/principals"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Members
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{principal.displayName}</h2>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setDeleteOpen(true)}
            className="text-destructive hover:text-destructive"
          >
            <Trash2 className="size-4" />
            Remove
          </Button>
        </div>
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Kind">
            <Badge variant="outline">{principal.kind}</Badge>
          </Row>
          <Row label="Status">
            <div className="flex flex-col gap-3">
              <StatusBadge status={principal.status} />
              <div className="flex items-center gap-2">
                <Label htmlFor="status-select" className="sr-only">
                  Change status
                </Label>
                <Select
                  value={selectedStatus || principal.status}
                  onValueChange={(v) => {
                    if (isUpdatablePrincipalStatus(v)) setSelectedStatus(v);
                  }}
                >
                  <SelectTrigger id="status-select" className="h-8 w-44">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {updatablePrincipalStatuses.map((s) => (
                      <SelectItem key={s} value={s}>
                        {s}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  size="sm"
                  disabled={
                    updateMut.isPending ||
                    !selectedStatus ||
                    selectedStatus === principal.status
                  }
                  onClick={() => {
                    if (!selectedStatus) return;
                    updateMut.mutate({ status: selectedStatus });
                  }}
                >
                  {updateMut.isPending ? "Saving..." : "Save"}
                </Button>
              </div>
              <MutationError error={updateMut.error} />
            </div>
          </Row>
          <Row label="Roles">
            <div className="flex flex-col gap-3">
              {principal.roles.length > 0 ? (
                <div className="flex flex-wrap gap-1">
                  {principal.roles.map((role) => (
                    <Badge key={role.id} variant="secondary" className="gap-1">
                      {role.name}
                      <button
                        type="button"
                        className="ml-0.5 rounded-sm hover:bg-muted"
                        disabled={removeMut.isPending}
                        onClick={() => removeMut.mutate(role.id)}
                      >
                        <X className="size-3" />
                      </button>
                    </Badge>
                  ))}
                </div>
              ) : (
                <span className="text-muted-foreground">No roles assigned</span>
              )}
              {unassignedRoles.length > 0 && (
                <div className="flex items-center gap-2">
                  <Label htmlFor="role-select" className="sr-only">
                    Add a role
                  </Label>
                  <Select
                    value={selectedRoleId}
                    onValueChange={setSelectedRoleId}
                  >
                    <SelectTrigger id="role-select" className="h-8 w-52">
                      <SelectValue placeholder="Add a role" />
                    </SelectTrigger>
                    <SelectContent>
                      {unassignedRoles.map((role) => (
                        <SelectItem key={role.id} value={role.id}>
                          {role.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Button
                    size="sm"
                    disabled={assignMut.isPending || !selectedRoleId}
                    onClick={() => assignMut.mutate(selectedRoleId)}
                  >
                    {assignMut.isPending ? "Adding..." : "Add"}
                  </Button>
                </div>
              )}
              <MutationError error={assignMut.error} />
              <MutationError error={removeMut.error} />
            </div>
          </Row>
          <Row label="Created">
            {new Date(principal.createdAt).toLocaleString()}
          </Row>
          <Row label="Updated">
            {new Date(principal.updatedAt).toLocaleString()}
          </Row>
        </dl>
      </div>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove member?</AlertDialogTitle>
            <AlertDialogDescription>
              This will remove &ldquo;{principal.displayName}&rdquo; from this
              tenant. This action cannot be undone.
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
              {deleteMut.isPending ? "Removing..." : "Remove"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
