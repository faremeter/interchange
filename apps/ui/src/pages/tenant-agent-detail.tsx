import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  agentDetailQuery,
  deleteAgentMutation,
  principalGrantsQuery,
  updateAgentMutation,
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "deployed"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
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

export function TenantAgentDetailPage() {
  const { tenantId, agentId } = useParams({ strict: false }) as {
    tenantId: string;
    agentId: string;
  };
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: agent, isLoading } = useQuery(
    agentDetailQuery(tenantId, agentId),
  );
  const { data: grants } = useQuery({
    ...principalGrantsQuery(tenantId, agent?.principalId ?? ""),
    enabled: !!agent?.principalId,
  });

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state -- initialized when entering edit mode
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSystemPrompt, setEditSystemPrompt] = useState("");

  function enterEditMode() {
    if (!agent) return;
    setEditName(agent.name);
    setEditDescription(agent.description ?? "");
    setEditSystemPrompt(agent.systemPrompt ?? "");
    setEditing(true);
  }

  function cancelEdit() {
    setEditing(false);
  }

  const updateMut = useMutation({
    ...updateAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      updateAgentMutation(tenantId, agentId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      deleteAgentMutation(tenantId, agentId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/agents", params: { tenantId } });
    },
  });

  function handleSave(e: React.FormEvent) {
    e.preventDefault();
    if (!agent) return;
    const body: {
      name?: string;
      description?: string;
      systemPrompt?: string;
    } = {};
    if (editName.trim() !== agent.name) body.name = editName.trim();
    if (editDescription.trim() !== (agent.description ?? ""))
      body.description = editDescription.trim();
    if (editSystemPrompt.trim() !== (agent.systemPrompt ?? ""))
      body.systemPrompt = editSystemPrompt.trim();
    updateMut.mutate(body);
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!agent) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      {/* Back link + header */}
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/agents"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Agents
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{agent.name}</h2>
          {agent.description && (
            <p className="mt-1 text-sm text-muted-foreground">
              {agent.description}
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
              <div className="grid grid-cols-[160px_1fr] border-b">
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
              <div className="grid grid-cols-[160px_1fr]">
                <div className="border-r bg-muted/50 px-4 py-3">
                  <Label htmlFor="edit-prompt">System Prompt</Label>
                </div>
                <div className="px-4 py-2">
                  <Textarea
                    id="edit-prompt"
                    value={editSystemPrompt}
                    onChange={(e) => setEditSystemPrompt(e.target.value)}
                    placeholder="Optional"
                    rows={6}
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
            <Row label="Status">
              <StatusBadge status={agent.status} />
            </Row>
            <Row label="Version">
              <span className="font-mono text-xs">v{agent.currentVersion}</span>
            </Row>
            <Row label="Principal ID">
              <span className="font-mono text-xs">{agent.principalId}</span>
            </Row>
            {agent.systemPrompt && (
              <Row label="System Prompt">
                <pre className="whitespace-pre-wrap text-xs">
                  {agent.systemPrompt}
                </pre>
              </Row>
            )}
            {agent.capabilities &&
              Object.keys(agent.capabilities).length > 0 && (
                <Row label="Capabilities">
                  <div className="flex flex-wrap gap-1">
                    {Object.entries(agent.capabilities).map(([key, val]) => (
                      <Badge key={key} variant="outline">
                        {key}
                        {val !== true ? `: ${String(val)}` : ""}
                      </Badge>
                    ))}
                  </div>
                </Row>
              )}
            {agent.kernelId && (
              <Row label="Kernel ID">
                <span className="font-mono text-xs">{agent.kernelId}</span>
              </Row>
            )}
            <Row label="Created">
              {new Date(agent.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(agent.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      {/* Grants section */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Grants</h3>
        {!grants || grants.length === 0 ? (
          <p className="mt-2 text-sm text-muted-foreground">
            No grants assigned to this agent.
          </p>
        ) : (
          <div className="mt-2 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Resource</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Effect</TableHead>
                  <TableHead>Source</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {grants.map((g) => (
                  <TableRow
                    key={g.id}
                    className="cursor-pointer"
                    onClick={() =>
                      navigate({
                        to: "/tenants/$tenantId/grants/$grantId",
                        params: { tenantId, grantId: g.id },
                      })
                    }
                  >
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
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>

      {/* Delete confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete agent?</AlertDialogTitle>
            <AlertDialogDescription>
              This will stop the agent &ldquo;{agent.name}&rdquo; and deactivate
              its principal. This action cannot be undone.
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
