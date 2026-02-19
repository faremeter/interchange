import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import {
  createGrantMutation,
  deleteGrantMutation,
  tenantGrantsQuery,
  tenantPrincipalsQuery,
  tenantRolesQuery,
  updateGrantMutation,
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
  DropdownMenuSeparator,
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

type GrantRow = {
  id: string;
  resource: string;
  action: string;
  effect: "allow" | "deny" | "ask";
  source: "system" | "role" | "creator" | "invoker";
  roleId: string | null;
  roleName: string | null;
  principalId: string | null;
  principalName: string | null;
};

const EFFECTS = ["allow", "deny", "ask"] as const;
const SOURCES = ["system", "role", "creator", "invoker"] as const;

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
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: grants, isLoading } = useQuery(tenantGrantsQuery(tenantId));
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));
  const { data: principals } = useQuery(tenantPrincipalsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<GrantRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<GrantRow | null>(null);

  // Create form state
  const [resource, setResource] = useState("");
  const [action, setAction] = useState("");
  const [effect, setEffect] = useState<string>("allow");
  const [source, setSource] = useState<string>("role");
  const [targetType, setTargetType] = useState<string>("role");
  const [roleId, setRoleId] = useState("");
  const [principalId, setPrincipalId] = useState("");

  // Edit form state
  const [editEffect, setEditEffect] = useState<string>("allow");

  function resetCreateForm() {
    setResource("");
    setAction("");
    setEffect("allow");
    setSource("role");
    setTargetType("role");
    setRoleId("");
    setPrincipalId("");
  }

  const createMut = useMutation({
    ...createGrantMutation(tenantId, queryClient),
    onSuccess: () => {
      createGrantMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  const updateMut = useMutation({
    ...updateGrantMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateGrantMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteGrantMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteGrantMutation(
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
                <TableHead>Source</TableHead>
                <TableHead>Target</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {grants?.map((g) => (
                <TableRow key={g.id}>
                  <TableCell className="font-mono text-xs">
                    {g.resource}
                  </TableCell>
                  <TableCell className="font-mono text-xs">
                    {g.action}
                  </TableCell>
                  <TableCell>
                    <EffectBadge effect={g.effect} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {g.source}
                  </TableCell>
                  <TableCell>
                    {g.roleName ? (
                      <Badge variant="secondary">{g.roleName}</Badge>
                    ) : g.roleId ? (
                      <span className="font-mono text-xs text-muted-foreground">
                        {g.roleId}
                      </span>
                    ) : null}
                    {g.principalName ? (
                      <Badge variant="outline">{g.principalName}</Badge>
                    ) : g.principalId ? (
                      <span className="font-mono text-xs text-muted-foreground">
                        {g.principalId}
                      </span>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon-xs">
                          <MoreHorizontal />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem
                          onClick={() => {
                            setEditTarget(g);
                            setEditEffect(g.effect);
                          }}
                        >
                          <Pencil />
                          Edit
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(g)}
                        >
                          <Trash2 />
                          Revoke
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
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
              const body: {
                resource: string;
                action: string;
                effect: "allow" | "deny" | "ask";
                source: "system" | "role" | "creator" | "invoker";
                roleId?: string;
                principalId?: string;
              } = {
                resource: resource.trim(),
                action: action.trim(),
                effect: effect as "allow" | "deny" | "ask",
                source: source as "system" | "role" | "creator" | "invoker",
              };
              if (targetType === "role" && roleId) body.roleId = roleId;
              if (targetType === "principal" && principalId)
                body.principalId = principalId;
              createMut.mutate(body);
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
                <Select value={effect} onValueChange={setEffect}>
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {EFFECTS.map((e) => (
                      <SelectItem key={e} value={e}>
                        {e}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label>Source</Label>
                <Select value={source} onValueChange={setSource}>
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {SOURCES.map((s) => (
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

      {/* Edit dialog (effect only) */}
      <Dialog
        open={!!editTarget}
        onOpenChange={(open) => {
          if (!open) setEditTarget(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Grant</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              updateMut.mutate({
                effect: editEffect as "allow" | "deny" | "ask",
              });
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2 text-sm">
              <div className="flex gap-2">
                <span className="text-muted-foreground">Resource:</span>
                <span className="font-mono">{editTarget?.resource}</span>
              </div>
              <div className="flex gap-2">
                <span className="text-muted-foreground">Action:</span>
                <span className="font-mono">{editTarget?.action}</span>
              </div>
            </div>
            <div className="grid gap-2">
              <Label>Effect</Label>
              <Select value={editEffect} onValueChange={setEditEffect}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {EFFECTS.map((e) => (
                    <SelectItem key={e} value={e}>
                      {e}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  updateMut.isPending || editEffect === editTarget?.effect
                }
              >
                {updateMut.isPending ? "Saving..." : "Save"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Revoke confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke grant?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently revoke the{" "}
              <span className="font-semibold">{deleteTarget?.effect}</span>{" "}
              grant on{" "}
              <span className="font-mono">{deleteTarget?.resource}</span> /{" "}
              <span className="font-mono">{deleteTarget?.action}</span>. This
              action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
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
