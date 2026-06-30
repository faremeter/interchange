import { useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";
import {
  type GrantEffect,
  type GrantOrigin,
  grantEffects,
  grantOrigins,
} from "@intx/types";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createGrantMutation,
  tenantCredentialsQuery,
  tenantGrantsQuery,
  tenantPrincipalsQuery,
  tenantRolesQuery,
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

function isGrantEffect(v: string): v is GrantEffect {
  return (grantEffects as readonly string[]).includes(v);
}

function isGrantOrigin(v: string): v is GrantOrigin {
  return (grantOrigins as readonly string[]).includes(v);
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

export function TenantGrantsPage() {
  const { tenantId } = useParams({ from: "/authed/tenants/$tenantId/grants" });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: grants, isLoading } = useQuery(tenantGrantsQuery(tenantId));
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));
  const { data: credentials } = useQuery(tenantCredentialsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [resource, setResource] = useState("");
  const [action, setAction] = useState("");
  const [effect, setEffect] = useState<GrantEffect>("allow");
  const [origin, setOrigin] = useState<GrantOrigin>("role");
  const [targetType, setTargetType] = useState<string>("role");
  const [roleId, setRoleId] = useState("");
  const [principalId, setPrincipalId] = useState("");

  function resetCreateForm() {
    setResource("");
    setAction("");
    setEffect("allow");
    setOrigin("role");
    setTargetType("role");
    setRoleId("");
    setPrincipalId("");
  }

  const createMut = useMutation({
    ...createGrantMutation(tenantId, queryClient),
    onSuccess: () => {
      void createGrantMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  return (
    <div>
      <TenantNav tenantId={tenantId} />

      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Grants</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Create Grant
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : grants?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No grants yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Resource</TableHead>
                <TableHead>Action</TableHead>
                <TableHead>Effect</TableHead>
                <TableHead>Origin</TableHead>
                <TableHead>Target</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {grants?.map((g) => (
                <TableRow
                  key={g.id}
                  className="cursor-pointer"
                  onClick={() =>
                    void navigate({
                      to: "/tenants/$tenantId/grants/$grantId",
                      params: { tenantId, grantId: g.id },
                    })
                  }
                >
                  <TableCell className="font-mono text-xs">
                    {g.resource.startsWith("credential:")
                      ? (credentials?.find(
                          (c) =>
                            c.id === g.resource.slice("credential:".length),
                        )?.name ?? g.resource.slice("credential:".length))
                      : g.resource}
                    {g.resource.startsWith("credential:") && (
                      <Badge variant="outline" className="ml-2 text-[10px]">
                        credential
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {g.action}
                  </TableCell>
                  <TableCell>
                    <EffectBadge effect={g.effect} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {g.origin}
                  </TableCell>
                  <TableCell>
                    {g.roleId && (
                      <Badge variant="secondary">
                        {g.roleName ??
                          roles?.find((r) => r.id === g.roleId)?.name ??
                          g.roleId}
                      </Badge>
                    )}
                    {g.principalId && (
                      <Badge variant="outline">
                        {g.principalName ??
                          principals?.find((p) => p.id === g.principalId)
                            ?.displayName ??
                          g.principalId}
                      </Badge>
                    )}
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
            <DialogTitle>Create Grant</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              createMut.mutate({
                resource: resource.trim(),
                action: action.trim(),
                effect,
                origin,
                ...(targetType === "role" && roleId ? { roleId } : {}),
                ...(targetType === "principal" && principalId
                  ? { principalId }
                  : {}),
              });
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="grant-resource">Resource</Label>
              <Input
                id="grant-resource"
                value={resource}
                onChange={(e) => setResource(e.target.value)}
                placeholder="e.g. tenant:*"
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="grant-action">Action</Label>
              <Input
                id="grant-action"
                value={action}
                onChange={(e) => setAction(e.target.value)}
                placeholder="e.g. read"
                required
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-2">
                <Label>Effect</Label>
                <Select
                  value={effect}
                  onValueChange={(v) => {
                    if (isGrantEffect(v)) setEffect(v);
                  }}
                >
                  <SelectTrigger className="w-full">
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
              <div className="grid gap-2">
                <Label>Origin</Label>
                <Select
                  value={origin}
                  onValueChange={(v) => {
                    if (isGrantOrigin(v)) setOrigin(v);
                  }}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {grantOrigins.map((s) => (
                      <SelectItem key={s} value={s}>
                        {s}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="grid gap-2">
              <Label>Assign to</Label>
              <Select value={targetType} onValueChange={setTargetType}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="role">Role</SelectItem>
                  <SelectItem value="principal">Principal</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {targetType === "role" && (
              <div className="grid gap-2">
                <Label>Role</Label>
                <Select value={roleId} onValueChange={setRoleId}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a role" />
                  </SelectTrigger>
                  <SelectContent>
                    {roles?.map((r) => (
                      <SelectItem key={r.id} value={r.id}>
                        {r.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            {targetType === "principal" && (
              <div className="grid gap-2">
                <Label>Principal</Label>
                <Select value={principalId} onValueChange={setPrincipalId}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a principal" />
                  </SelectTrigger>
                  <SelectContent>
                    {principals?.map((p) => (
                      <SelectItem key={p.id} value={p.id}>
                        {p.displayName}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending ||
                  !resource.trim() ||
                  !action.trim() ||
                  (targetType === "role" && !roleId) ||
                  (targetType === "principal" && !principalId)
                }
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
