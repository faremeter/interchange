import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Plus, Trash2, UserCog } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import {
  deletePrincipalMutation,
  inviteMemberMutation,
  tenantPrincipalsQuery,
  tenantRolesQuery,
  updatePrincipalMutation,
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

type PrincipalRow = {
  id: string;
  displayName: string;
  email?: string;
  kind: "user" | "agent";
  status: "active" | "suspended" | "invited" | "deactivated";
  roles: { id: string; name: string }[];
};

const STATUS_OPTIONS = ["active", "suspended", "deactivated"] as const;

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "active"
      ? "secondary"
      : status === "suspended" || status === "deactivated"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function TenantPrincipalsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: principals, isLoading } = useQuery(
    tenantPrincipalsQuery(tenantId),
  );
  const { data: roles } = useQuery(tenantRolesQuery(tenantId));

  const [inviteOpen, setInviteOpen] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRoleId, setInviteRoleId] = useState("");
  const [statusTarget, setStatusTarget] = useState<PrincipalRow | null>(null);
  const [statusValue, setStatusValue] = useState<string>("");
  const [deleteTarget, setDeleteTarget] = useState<PrincipalRow | null>(null);

  const inviteMut = useMutation({
    ...inviteMemberMutation(tenantId, queryClient),
    onSuccess: () => {
      inviteMemberMutation(tenantId, queryClient).onSuccess();
      setInviteOpen(false);
      setInviteEmail("");
      setInviteRoleId("");
    },
  });

  const updateMut = useMutation({
    ...updatePrincipalMutation(tenantId, statusTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updatePrincipalMutation(
        tenantId,
        statusTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setStatusTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deletePrincipalMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deletePrincipalMutation(
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
        <h2 className="text-lg font-semibold">Members</h2>
        <Button size="sm" onClick={() => setInviteOpen(true)}>
          <Plus className="size-4" />
          Invite Member
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : principals?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No members yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Kind</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Roles</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {principals?.map((p) => (
                <TableRow key={p.id}>
                  <TableCell>
                    <div className="font-medium">{p.displayName}</div>
                    {p.email ? (
                      <div className="text-xs text-muted-foreground">
                        {p.email}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{p.kind}</Badge>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={p.status} />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {p.roles.map((r) => (
                        <Badge key={r.id} variant="secondary">
                          {r.name}
                        </Badge>
                      ))}
                    </div>
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
                            setStatusTarget(p);
                            setStatusValue(p.status);
                          }}
                        >
                          <UserCog />
                          Change Status
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(p)}
                        >
                          <Trash2 />
                          Remove
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

      {/* Invite dialog */}
      <Dialog
        open={inviteOpen}
        onOpenChange={(open) => {
          setInviteOpen(open);
          if (!open) {
            setInviteEmail("");
            setInviteRoleId("");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Invite Member</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: { email: string; roleId?: string } = {
                email: inviteEmail.trim(),
              };
              if (inviteRoleId) body.roleId = inviteRoleId;
              inviteMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="invite-email">Email</Label>
              <Input
                id="invite-email"
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="invite-role">Role (optional)</Label>
              <Select value={inviteRoleId} onValueChange={setInviteRoleId}>
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
            <DialogFooter>
              <Button
                type="submit"
                disabled={inviteMut.isPending || !inviteEmail.trim()}
              >
                {inviteMut.isPending ? "Inviting..." : "Send Invite"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Change status dialog */}
      <Dialog
        open={!!statusTarget}
        onOpenChange={(open) => {
          if (!open) setStatusTarget(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Change Status</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              if (
                statusValue === "active" ||
                statusValue === "suspended" ||
                statusValue === "deactivated"
              ) {
                updateMut.mutate({ status: statusValue });
              }
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label>
                Status for{" "}
                <span className="font-semibold">
                  {statusTarget?.displayName}
                </span>
              </Label>
              <Select value={statusValue} onValueChange={setStatusValue}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUS_OPTIONS.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  updateMut.isPending || statusValue === statusTarget?.status
                }
              >
                {updateMut.isPending ? "Saving..." : "Save"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove member?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently remove &ldquo;{deleteTarget?.displayName}
              &rdquo; from this tenant. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
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
