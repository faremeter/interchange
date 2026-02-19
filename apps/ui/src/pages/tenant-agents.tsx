import { useState } from "react";
import { useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MoreHorizontal, Pencil, Plus, Trash2 } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createAgentMutation,
  deleteAgentMutation,
  tenantAgentsQuery,
  updateAgentMutation,
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type AgentFormValues = {
  name: string;
  description: string;
  systemPrompt: string;
};
type AgentRow = {
  id: string;
  name: string;
  description: string | null;
  status: string;
  currentVersion: string;
  createdAt: string;
};

function AgentStatusBadge({ status }: { status: string }) {
  const variant =
    status === "deployed"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function AgentFormDialog({
  open,
  onOpenChange,
  title,
  initial,
  isPending,
  onSubmit,
  error,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  initial: AgentFormValues;
  isPending: boolean;
  onSubmit: (values: AgentFormValues) => void;
  error: Error | null;
}) {
  const [name, setName] = useState(initial.name);
  const [description, setDescription] = useState(initial.description);
  const [systemPrompt, setSystemPrompt] = useState(initial.systemPrompt);

  const resetAndOpen = (next: boolean) => {
    if (next) {
      setName(initial.name);
      setDescription(initial.description);
      setSystemPrompt(initial.systemPrompt);
    }
    onOpenChange(next);
  };

  return (
    <Dialog open={open} onOpenChange={resetAndOpen}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            onSubmit({
              name: name.trim(),
              description: description.trim(),
              systemPrompt: systemPrompt.trim(),
            });
          }}
          className="grid gap-4"
        >
          <div className="grid gap-2">
            <Label htmlFor="agent-name">Name</Label>
            <Input
              id="agent-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              autoFocus
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="agent-description">Description</Label>
            <Input
              id="agent-description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional"
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="agent-prompt">System Prompt</Label>
            <Textarea
              id="agent-prompt"
              value={systemPrompt}
              onChange={(e) => setSystemPrompt(e.target.value)}
              placeholder="Optional"
              rows={4}
            />
          </div>
          <MutationError error={error} />
          <DialogFooter>
            <Button type="submit" disabled={isPending || !name.trim()}>
              {isPending ? "Saving..." : "Save"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export function TenantAgentsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: agents, isLoading } = useQuery(tenantAgentsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [editTarget, setEditTarget] = useState<AgentRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<AgentRow | null>(null);

  const createMut = useMutation({
    ...createAgentMutation(tenantId, queryClient),
    onSuccess: () => {
      createAgentMutation(tenantId, queryClient).onSuccess();
      setCreateOpen(false);
    },
  });

  const updateMut = useMutation({
    ...updateAgentMutation(tenantId, editTarget?.id ?? "", queryClient),
    onSuccess: () => {
      updateAgentMutation(
        tenantId,
        editTarget?.id ?? "",
        queryClient,
      ).onSuccess();
      setEditTarget(null);
    },
  });

  const deleteMut = useMutation({
    ...deleteAgentMutation(tenantId, deleteTarget?.id ?? "", queryClient),
    onSuccess: () => {
      deleteAgentMutation(
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
        <h2 className="text-lg font-semibold">Agents</h2>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          Create Agent
        </Button>
      </div>

      {isLoading ? (
        <p className="mt-4 text-sm text-muted-foreground">Loading...</p>
      ) : agents?.length === 0 ? (
        <p className="mt-4 text-sm text-muted-foreground">No agents yet.</p>
      ) : (
        <div className="mt-4 rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Version</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {agents?.map((a) => (
                <TableRow key={a.id}>
                  <TableCell>
                    <div className="font-medium">{a.name}</div>
                    {a.description && (
                      <div className="text-xs text-muted-foreground">
                        {a.description}
                      </div>
                    )}
                  </TableCell>
                  <TableCell>
                    <AgentStatusBadge status={a.status} />
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    v{a.currentVersion}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {new Date(a.createdAt).toLocaleDateString()}
                  </TableCell>
                  <TableCell>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon-xs">
                          <MoreHorizontal />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem onClick={() => setEditTarget(a)}>
                          <Pencil />
                          Edit
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onClick={() => setDeleteTarget(a)}
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
      <AgentFormDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        title="Create Agent"
        initial={{ name: "", description: "", systemPrompt: "" }}
        isPending={createMut.isPending}
        error={createMut.error}
        onSubmit={(v) => {
          const body: {
            name: string;
            description?: string;
            systemPrompt?: string;
          } = {
            name: v.name,
          };
          if (v.description) body.description = v.description;
          if (v.systemPrompt) body.systemPrompt = v.systemPrompt;
          createMut.mutate(body);
        }}
      />

      {/* Edit dialog */}
      {editTarget && (
        <AgentFormDialog
          open={!!editTarget}
          onOpenChange={(open) => {
            if (!open) setEditTarget(null);
          }}
          title="Edit Agent"
          initial={{
            name: editTarget.name,
            description: editTarget.description ?? "",
            systemPrompt: "",
          }}
          isPending={updateMut.isPending}
          error={updateMut.error}
          onSubmit={(v) => {
            const body: {
              name?: string;
              description?: string;
              systemPrompt?: string;
            } = {};
            if (v.name !== editTarget.name) body.name = v.name;
            if (v.description !== (editTarget.description ?? ""))
              body.description = v.description;
            if (v.systemPrompt) body.systemPrompt = v.systemPrompt;
            updateMut.mutate(body);
          }}
        />
      )}

      {/* Delete confirmation */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete agent?</AlertDialogTitle>
            <AlertDialogDescription>
              This will stop the agent &ldquo;{deleteTarget?.name}&rdquo; and
              deactivate its principal. This action cannot be undone.
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
