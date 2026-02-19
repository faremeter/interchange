import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import {
  createCredentialMutation,
  deleteCredentialMutation,
  tenantCredentialsQuery,
  updateCredentialMutation,
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

type CredentialRow = {
  id: string;
  name: string;
  type: "api_key" | "oauth_token" | "certificate" | "other";
  description: string | null;
  createdAt: string;
};

const TYPE_OPTIONS = [
  { value: "api_key", label: "API Key" },
  { value: "oauth_token", label: "OAuth Token" },
  { value: "certificate", label: "Certificate" },
  { value: "other", label: "Other" },
] as const;

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

export function TenantCredentialsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: credentials, isLoading } = useQuery(
    tenantCredentialsQuery(tenantId),
  );

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<CredentialRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<CredentialRow | null>(null);

  // Create form state
  const [createName, setCreateName] = useState("");
  const [createType, setCreateType] = useState<string>("api_key");
  const [createSecret, setCreateSecret] = useState("");
  const [createDescription, setCreateDescription] = useState("");

  // Edit form state
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSecret, setEditSecret] = useState("");

  function resetCreateForm() {
    setCreateName("");
    setCreateType("api_key");
    setCreateSecret("");
    setCreateDescription("");
  }

  const createMut = useMutation({
    ...createCredentialMutation(tenantId, queryClient),
    onSuccess: () => {
      createCredentialMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  const updateMut = useMutation({
    ...updateCredentialMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateCredentialMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteCredentialMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteCredentialMutation(
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
        <h2 className="text-lg font-semibold">Credentials</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Add Credential
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : credentials?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">
          No credentials stored.
        </p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {credentials?.map((cred) => (
                <TableRow key={cred.id}>
                  <TableCell className="font-medium">{cred.name}</TableCell>
                  <TableCell>
                    <TypeBadge type={cred.type} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {cred.description ?? "-"}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(cred.createdAt).toLocaleDateString()}
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
                            setEditTarget(cred);
                            setEditName(cred.name);
                            setEditDescription(cred.description ?? "");
                            setEditSecret("");
                          }}
                        >
                          <Pencil />
                          Edit
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(cred)}
                        >
                          <Trash2 />
                          Delete
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
            <DialogTitle>Add Credential</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: {
                name: string;
                type: "api_key" | "oauth_token" | "certificate" | "other";
                secret: string;
                description?: string;
              } = {
                name: createName.trim(),
                type: createType as
                  | "api_key"
                  | "oauth_token"
                  | "certificate"
                  | "other",
                secret: createSecret,
              };
              if (createDescription.trim())
                body.description = createDescription.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="cred-name">Name</Label>
              <Input
                id="cred-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label>Type</Label>
              <Select value={createType} onValueChange={setCreateType}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {TYPE_OPTIONS.map((t) => (
                    <SelectItem key={t.value} value={t.value}>
                      {t.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cred-secret">Secret</Label>
              <Input
                id="cred-secret"
                type="password"
                value={createSecret}
                onChange={(e) => setCreateSecret(e.target.value)}
                required
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="cred-description">Description</Label>
              <Input
                id="cred-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending || !createName.trim() || !createSecret
                }
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Edit dialog */}
      <Dialog
        open={!!editTarget}
        onOpenChange={(open) => {
          if (!open) setEditTarget(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Credential</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: {
                name?: string;
                description?: string;
                secret?: string;
              } = {};
              if (editName.trim() !== editTarget?.name)
                body.name = editName.trim();
              if (editDescription.trim() !== (editTarget?.description ?? ""))
                body.description = editDescription.trim();
              if (editSecret) body.secret = editSecret;
              updateMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="edit-cred-name">Name</Label>
              <Input
                id="edit-cred-name"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-cred-description">Description</Label>
              <Input
                id="edit-cred-description"
                value={editDescription}
                onChange={(e) => setEditDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-cred-secret">
                New Secret (leave blank to keep current)
              </Label>
              <Input
                id="edit-cred-secret"
                type="password"
                value={editSecret}
                onChange={(e) => setEditSecret(e.target.value)}
              />
            </div>
            <DialogFooter>
              <Button type="submit" disabled={updateMut.isPending}>
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
            <AlertDialogTitle>Delete credential?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the credential &ldquo;
              {deleteTarget?.name}&rdquo;. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
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
