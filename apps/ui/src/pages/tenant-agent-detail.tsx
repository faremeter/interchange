import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Plus, Trash2, X } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  agentDetailQuery,
  createGrantMutation,
  deleteAgentMutation,
  deleteGrantMutation,
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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

function SourceBadge({ source }: { source: string }) {
  const variant =
    source === "tenant"
      ? "default"
      : source === "creator"
        ? "secondary"
        : "outline";
  return <Badge variant={variant}>{source}</Badge>;
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

const SOURCES = ["tenant", "creator", "invoker"] as const;
const EFFECTS = ["allow", "deny", "ask"] as const;

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

  // Edit form state
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSystemPrompt, setEditSystemPrompt] = useState("");

  // Credential requirement add form
  const [reqProvider, setReqProvider] = useState("");
  const [reqScopes, setReqScopes] = useState("");
  const [reqSource, setReqSource] = useState<string>("tenant");
  const [reqName, setReqName] = useState("");

  // Grant add form
  const [grantResource, setGrantResource] = useState("");
  const [grantAction, setGrantAction] = useState("");
  const [grantEffect, setGrantEffect] = useState<string>("allow");
  const [revokeTarget, setRevokeTarget] = useState<string | null>(null);

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

  const reqMut = useMutation({
    ...updateAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      updateAgentMutation(tenantId, agentId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
      setReqProvider("");
      setReqScopes("");
      setReqSource("tenant");
      setReqName("");
    },
  });

  const deleteMut = useMutation({
    ...deleteAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      deleteAgentMutation(tenantId, agentId, queryClient).onSuccess();
      navigate({ to: "/tenants/$tenantId/agents", params: { tenantId } });
    },
  });

  const grantMut = useMutation({
    ...createGrantMutation(tenantId, queryClient),
    onSuccess: () => {
      createGrantMutation(tenantId, queryClient).onSuccess();
      queryClient.invalidateQueries({
        queryKey: [
          "tenants",
          tenantId,
          "grants",
          { principalId: agent?.principalId },
        ],
      });
      setGrantResource("");
      setGrantAction("");
      setGrantEffect("allow");
    },
  });

  const revokeMut = useMutation({
    ...deleteGrantMutation(tenantId, revokeTarget ?? "", queryClient),
    onSuccess: () => {
      deleteGrantMutation(
        tenantId,
        revokeTarget ?? "",
        queryClient,
      ).onSuccess();
      queryClient.invalidateQueries({
        queryKey: [
          "tenants",
          tenantId,
          "grants",
          { principalId: agent?.principalId },
        ],
      });
      setRevokeTarget(null);
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

  function addRequirement(e: React.FormEvent) {
    e.preventDefault();
    if (!agent) return;
    const existing = agent.credentialRequirements ?? [];
    const scopes = reqScopes
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const req: {
      providerName: string;
      scopes?: string[];
      source: "tenant" | "creator" | "invoker";
      name?: string;
    } = {
      providerName: reqProvider.trim(),
      source: reqSource as "tenant" | "creator" | "invoker",
    };
    if (scopes.length > 0) req.scopes = scopes;
    if (reqName.trim()) req.name = reqName.trim();
    reqMut.mutate({ credentialRequirements: [...existing, req] });
  }

  function removeRequirement(index: number) {
    if (!agent) return;
    const existing = agent.credentialRequirements ?? [];
    reqMut.mutate({
      credentialRequirements: existing.filter((_, i) => i !== index),
    });
  }

  function addGrant(e: React.FormEvent) {
    e.preventDefault();
    if (!agent) return;
    grantMut.mutate({
      resource: grantResource.trim(),
      action: grantAction.trim(),
      effect: grantEffect as "allow" | "deny" | "ask",
      source: "creator",
      principalId: agent.principalId,
    });
  }

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!agent) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  const requirements = agent.credentialRequirements ?? [];

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

      {/* Credential requirements section */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Credential Requirements</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Declares what third-party credentials this agent needs. Resolved at
          launch time via tenant hierarchy walk-up.
        </p>

        {requirements.length > 0 && (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Provider</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Scopes</TableHead>
                  <TableHead>Name</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {requirements.map((req, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">
                      {req.providerName}
                    </TableCell>
                    <TableCell>
                      <SourceBadge source={req.source} />
                    </TableCell>
                    <TableCell>
                      {req.scopes && req.scopes.length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                          {req.scopes.map((s) => (
                            <Badge
                              key={s}
                              variant="outline"
                              className="font-mono text-xs"
                            >
                              {s}
                            </Badge>
                          ))}
                        </div>
                      ) : (
                        <span className="text-muted-foreground">any</span>
                      )}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {req.name ?? "-"}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        onClick={() => removeRequirement(i)}
                        disabled={reqMut.isPending}
                      >
                        <X className="size-3.5" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        <form onSubmit={addRequirement} className="mt-3 flex items-end gap-2">
          <div className="grid gap-1">
            <Label htmlFor="req-provider" className="text-xs">
              Provider
            </Label>
            <Input
              id="req-provider"
              value={reqProvider}
              onChange={(e) => setReqProvider(e.target.value)}
              placeholder="e.g. github"
              className="h-8 w-36 text-xs"
              required
            />
          </div>
          <div className="grid gap-1">
            <Label className="text-xs">Source</Label>
            <Select value={reqSource} onValueChange={setReqSource}>
              <SelectTrigger className="h-8 w-28 text-xs">
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
          <div className="grid gap-1">
            <Label htmlFor="req-scopes" className="text-xs">
              Scopes
            </Label>
            <Input
              id="req-scopes"
              value={reqScopes}
              onChange={(e) => setReqScopes(e.target.value)}
              placeholder="repo, read:org"
              className="h-8 w-40 text-xs"
            />
          </div>
          <div className="grid gap-1">
            <Label htmlFor="req-name" className="text-xs">
              Name
            </Label>
            <Input
              id="req-name"
              value={reqName}
              onChange={(e) => setReqName(e.target.value)}
              placeholder="Optional"
              className="h-8 w-32 text-xs"
            />
          </div>
          <Button
            type="submit"
            size="sm"
            className="h-8"
            disabled={reqMut.isPending || !reqProvider.trim()}
          >
            <Plus className="size-3.5" />
            Add
          </Button>
        </form>
        <MutationError error={reqMut.error} />
      </div>

      {/* Grants section */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Grants</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Authorization grants assigned to this agent&apos;s principal.
        </p>

        {grants && grants.length > 0 && (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Resource</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Effect</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {grants.map((g) => (
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
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        className="text-destructive hover:text-destructive"
                        onClick={() => setRevokeTarget(g.id)}
                      >
                        <X className="size-3.5" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}

        <form onSubmit={addGrant} className="mt-3 flex items-end gap-2">
          <div className="grid gap-1">
            <Label htmlFor="grant-resource" className="text-xs">
              Resource
            </Label>
            <Input
              id="grant-resource"
              value={grantResource}
              onChange={(e) => setGrantResource(e.target.value)}
              placeholder="e.g. credential:*"
              className="h-8 w-44 text-xs"
              required
            />
          </div>
          <div className="grid gap-1">
            <Label htmlFor="grant-action" className="text-xs">
              Action
            </Label>
            <Input
              id="grant-action"
              value={grantAction}
              onChange={(e) => setGrantAction(e.target.value)}
              placeholder="e.g. use"
              className="h-8 w-28 text-xs"
              required
            />
          </div>
          <div className="grid gap-1">
            <Label className="text-xs">Effect</Label>
            <Select value={grantEffect} onValueChange={setGrantEffect}>
              <SelectTrigger className="h-8 w-24 text-xs">
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
          <Button
            type="submit"
            size="sm"
            className="h-8"
            disabled={
              grantMut.isPending || !grantResource.trim() || !grantAction.trim()
            }
          >
            <Plus className="size-3.5" />
            Grant
          </Button>
        </form>
        <MutationError error={grantMut.error} />
      </div>

      {/* Delete agent confirmation */}
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

      {/* Revoke grant confirmation */}
      <AlertDialog
        open={!!revokeTarget}
        onOpenChange={(open) => {
          if (!open) setRevokeTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke grant?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently revoke this grant from the agent. This
              action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <MutationError error={revokeMut.error} />
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => revokeMut.mutate()}
              disabled={revokeMut.isPending}
            >
              {revokeMut.isPending ? "Revoking..." : "Revoke"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
