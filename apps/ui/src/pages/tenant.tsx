import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Pencil } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  tenantAgentsQuery,
  tenantDetailQuery,
  tenantPrincipalsQuery,
  tenantRolesQuery,
  updateTenantMutation,
} from "@/lib/queries/tenants";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function TenantPage() {
  const { tenantId } = useParams({ from: "/authed/tenants/$tenantId" });
  const queryClient = useQueryClient();
  const { data: tenant, isLoading } = useQuery(tenantDetailQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));
  const { data: agents } = useQuery(tenantAgentsQuery(tenantId));
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));

  const [editOpen, setEditOpen] = useState(false);
  const [editName, setEditName] = useState("");

  const updateMut = useMutation({
    ...updateTenantMutation(tenantId, queryClient),
    onSuccess: () => {
      updateTenantMutation(tenantId, queryClient).onSuccess();
      setEditOpen(false);
    },
  });

  if (isLoading) {
    return <p className="text-sm text-muted-foreground">Loading...</p>;
  }

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Members
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">
              {principals?.filter((p) => p.kind === "user").length ?? 0}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Agents
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">{agents?.length ?? 0}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Roles
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-2xl font-semibold">{roles?.length ?? 0}</p>
          </CardContent>
        </Card>
      </div>

      {tenant && (
        <Card className="mt-6">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-sm">Tenant details</CardTitle>
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={() => {
                setEditName(tenant.name);
                setEditOpen(true);
              }}
            >
              <Pencil />
            </Button>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <dt className="text-muted-foreground">Name</dt>
              <dd>{tenant.name}</dd>
              <dt className="text-muted-foreground">Slug</dt>
              <dd className="font-mono">{tenant.slug}</dd>
              <dt className="text-muted-foreground">Domain</dt>
              <dd className="font-mono">{tenant.domain}</dd>
              <dt className="text-muted-foreground">Created</dt>
              <dd>{new Date(tenant.createdAt).toLocaleDateString()}</dd>
            </dl>
          </CardContent>
        </Card>
      )}

      {/* Edit tenant dialog */}
      <Dialog open={editOpen} onOpenChange={setEditOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Tenant</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: { name?: string } = {};
              if (editName.trim() !== tenant?.name) body.name = editName.trim();
              updateMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="tenant-name">Name</Label>
              <Input
                id="tenant-name"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <MutationError error={updateMut.error} />
            <DialogFooter>
              <Button type="submit" disabled={updateMut.isPending}>
                {updateMut.isPending ? "Saving..." : "Save"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
