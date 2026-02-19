import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createWalletMutation,
  deleteWalletMutation,
  tenantWalletsQuery,
  updateWalletMutation,
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

type WalletRow = {
  id: string;
  name: string;
  backendType: "crypto" | "fiat" | "credits";
  currency: string;
  balance: string;
  createdAt: string;
};

const BACKEND_OPTIONS = [
  { value: "crypto", label: "Crypto" },
  { value: "fiat", label: "Fiat" },
  { value: "credits", label: "Credits" },
] as const;

const BACKEND_LABELS: Record<string, string> = {
  crypto: "Crypto",
  fiat: "Fiat",
  credits: "Credits",
};

function BackendBadge({ type }: { type: string }) {
  const variant =
    type === "crypto"
      ? "destructive"
      : type === "fiat"
        ? "secondary"
        : "outline";
  return <Badge variant={variant}>{BACKEND_LABELS[type] ?? type}</Badge>;
}

export function TenantWalletsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: wallets, isLoading } = useQuery(tenantWalletsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<WalletRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<WalletRow | null>(null);

  // Create form state
  const [createName, setCreateName] = useState("");
  const [createBackend, setCreateBackend] = useState<string>("fiat");
  const [createCurrency, setCreateCurrency] = useState("");

  // Edit form state
  const [editName, setEditName] = useState("");

  function resetCreateForm() {
    setCreateName("");
    setCreateBackend("fiat");
    setCreateCurrency("");
  }

  const createMut = useMutation({
    ...createWalletMutation(tenantId, queryClient),
    onSuccess: () => {
      createWalletMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
      resetCreateForm();
    },
  });

  const updateMut = useMutation({
    ...updateWalletMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateWalletMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteWalletMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteWalletMutation(
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
        <h2 className="text-lg font-semibold">Wallets</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Create Wallet
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : wallets?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No wallets yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Currency</TableHead>
                <TableHead>Balance</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {wallets?.map((w) => (
                <TableRow key={w.id}>
                  <TableCell className="font-medium">{w.name}</TableCell>
                  <TableCell>
                    <BackendBadge type={w.backendType} />
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {w.currency}
                  </TableCell>
                  <TableCell className="font-mono text-sm">
                    {w.balance}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(w.createdAt).toLocaleDateString()}
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
                            setEditTarget(w);
                            setEditName(w.name);
                          }}
                        >
                          <Pencil />
                          Edit
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(w)}
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
            <DialogTitle>Create Wallet</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              createMut.mutate({
                name: createName.trim(),
                backendType: createBackend as "crypto" | "fiat" | "credits",
                currency: createCurrency.trim().toUpperCase(),
              });
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="wallet-name">Name</Label>
              <Input
                id="wallet-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label>Backend Type</Label>
              <Select value={createBackend} onValueChange={setCreateBackend}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {BACKEND_OPTIONS.map((b) => (
                    <SelectItem key={b.value} value={b.value}>
                      {b.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="wallet-currency">Currency</Label>
              <Input
                id="wallet-currency"
                value={createCurrency}
                onChange={(e) => setCreateCurrency(e.target.value)}
                placeholder="e.g. USD, ETH"
                required
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={
                  createMut.isPending ||
                  !createName.trim() ||
                  !createCurrency.trim()
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
            <DialogTitle>Edit Wallet</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: { name?: string } = {};
              if (editName.trim() !== editTarget?.name)
                body.name = editName.trim();
              updateMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="edit-wallet-name">Name</Label>
              <Input
                id="edit-wallet-name"
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

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete wallet?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the wallet &ldquo;
              {deleteTarget?.name}&rdquo; ({deleteTarget?.currency}). This
              action cannot be undone.
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
