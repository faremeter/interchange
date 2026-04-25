import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Plus, Trash2, X } from "lucide-react";
import {
  type CredentialRequirementSource,
  type GrantRequirementSource,
  type GrantEffect,
  credentialRequirementSources,
  grantRequirementSources,
  grantEffects,
} from "@interchange/types";

import { MutationError } from "@/components/mutation-error";
import {
  agentAllInstancesQuery,
  agentDetailQuery,
  deleteAgentMutation,
  deployInstanceMutation,
  tenantProvidersQuery,
  tenantRolesQuery,
  updateAgentMutation,
  type AgentInstanceResponse,
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

const DEFINITION_STATUS_LABEL: Record<string, string> = {
  deployed: "active",
  stopped: "retired",
};

function StatusBadge({ status }: { status: string }) {
  const label = DEFINITION_STATUS_LABEL[status] ?? status;
  const variant = status === "deployed" ? "secondary" : "outline";
  return <Badge variant={variant}>{label}</Badge>;
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

function InstanceStatusBadge({
  status,
}: {
  status: AgentInstanceResponse["status"];
}) {
  const variant =
    status === "running"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

export function TenantAgentDetailPage() {
  const { tenantId, agentId } = useParams({ strict: false }) as {
    tenantId: string;
    agentId: string;
  };
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: agent, isLoading: agentLoading } = useQuery(
    agentDetailQuery(tenantId, agentId),
  );
  const { data: instances } = useQuery(
    agentAllInstancesQuery(tenantId, agentId),
  );
  const { data: providers } = useQuery(tenantProvidersQuery(tenantId));
  const { data: tenantRoles } = useQuery(tenantRolesQuery(tenantId));

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Edit form state
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSystemPrompt, setEditSystemPrompt] = useState("");

  // Credential requirement form state
  const [credProvider, setCredProvider] = useState("");
  const [credScopes, setCredScopes] = useState("");
  const [credSource, setCredSource] =
    useState<CredentialRequirementSource>("tenant");

  // Grant requirement form state
  const [grantReqResource, setGrantReqResource] = useState("");
  const [grantReqAction, setGrantReqAction] = useState("");
  const [grantReqSource, setGrantReqSource] =
    useState<GrantRequirementSource>("creator");
  const [grantReqEffect, setGrantReqEffect] = useState<GrantEffect>("allow");

  const credentialRequirements = agent?.credentialRequirements ?? [];
  const grantRequirements = agent?.grantRequirements ?? [];
  const agentRoles = agent?.roles ?? [];

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
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
    },
  });

  const deleteMut = useMutation({
    ...deleteAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      navigate({ to: "/tenants/$tenantId/agents", params: { tenantId } });
    },
  });

  const deployMut = useMutation({
    ...deployInstanceMutation(tenantId, queryClient),
    onSuccess: (data) => {
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "instances"],
      });
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents"],
      });
      navigate({
        to: "/tenants/$tenantId/instances/$instanceId",
        params: { tenantId, instanceId: data.id },
      });
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
    updateMut.mutate(body, { onSuccess: () => setEditing(false) });
  }

  function addCredentialRequirement(e: React.FormEvent) {
    e.preventDefault();
    if (!agent || !credProvider.trim()) return;
    const scopes = credScopes
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const req = {
      providerName: credProvider.trim(),
      source: credSource,
      ...(scopes.length > 0 ? { scopes } : {}),
    };
    updateMut.mutate(
      { credentialRequirements: [...credentialRequirements, req] },
      {
        onSuccess: () => {
          setCredProvider("");
          setCredScopes("");
          setCredSource("tenant");
        },
      },
    );
  }

  function removeCredentialRequirement(index: number) {
    updateMut.mutate({
      credentialRequirements: credentialRequirements.filter(
        (_, i) => i !== index,
      ),
    });
  }

  function addGrantRequirement(e: React.FormEvent) {
    e.preventDefault();
    if (!agent || !grantReqResource.trim() || !grantReqAction.trim()) return;
    const req = {
      resource: grantReqResource.trim(),
      action: grantReqAction.trim(),
      source: grantReqSource,
      effect: grantReqEffect,
    };
    updateMut.mutate(
      { grantRequirements: [...grantRequirements, req] },
      {
        onSuccess: () => {
          setGrantReqResource("");
          setGrantReqAction("");
          setGrantReqSource("creator");
          setGrantReqEffect("allow");
        },
      },
    );
  }

  function removeGrantRequirement(index: number) {
    updateMut.mutate({
      grantRequirements: grantRequirements.filter((_, i) => i !== index),
    });
  }

  function addRole(roleId: string) {
    const currentIds = agentRoles.map((r) => r.id);
    updateMut.mutate({ roleIds: [...currentIds, roleId] });
  }

  function removeRole(roleId: string) {
    const currentIds = agentRoles.map((r) => r.id);
    updateMut.mutate({ roleIds: currentIds.filter((id) => id !== roleId) });
  }

  const availableRoles = (tenantRoles ?? []).filter(
    (r) => !agentRoles.some((ar) => ar.id === r.id),
  );

  if (agentLoading) {
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
            <Row label="Creator Principal">
              <span className="font-mono text-xs">
                {agent.creatorPrincipalId ?? "\u2014"}
              </span>
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
            <Row label="Created">
              {new Date(agent.createdAt).toLocaleString()}
            </Row>
            <Row label="Updated">
              {new Date(agent.updatedAt).toLocaleString()}
            </Row>
          </dl>
        )}
      </div>

      {/* Start Instance */}
      <div className="mt-6 flex items-center gap-2">
        <Button
          size="sm"
          onClick={() => deployMut.mutate({ agentId })}
          disabled={deployMut.isPending || agent.status !== "deployed"}
        >
          {deployMut.isPending ? "Creating..." : "New Instance"}
        </Button>
        <MutationError error={deployMut.error} />
      </div>

      {/* Instances */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Instances</h3>
        {!instances || instances.length === 0 ? (
          <p className="mt-2 text-xs text-muted-foreground">
            No instances launched from this definition.
          </p>
        ) : (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Status</TableHead>
                  <TableHead>Address</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Ended</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {instances.map((inst) => (
                  <TableRow
                    key={inst.id}
                    className="cursor-pointer"
                    onClick={() =>
                      navigate({
                        to: "/tenants/$tenantId/instances/$instanceId",
                        params: { tenantId, instanceId: inst.id },
                      })
                    }
                  >
                    <TableCell>
                      <InstanceStatusBadge status={inst.status} />
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {inst.address}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {new Date(inst.createdAt).toLocaleString()}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {inst.endedAt
                        ? new Date(inst.endedAt).toLocaleString()
                        : "-"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>

      {/* Credential Requirements */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Credential Requirements</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          External service credentials this agent needs at launch. The source
          determines who supplies each credential.
        </p>

        {credentialRequirements.length > 0 && (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Provider</TableHead>
                  <TableHead>Scopes</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {credentialRequirements.map((req, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">
                      {req.providerName}
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
                        <span className="text-xs text-muted-foreground">
                          any
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {req.source}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        className="text-destructive hover:text-destructive"
                        onClick={() => removeCredentialRequirement(i)}
                        disabled={updateMut.isPending}
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

        <div className="mt-4 space-y-3 rounded-lg border p-4">
          <form
            onSubmit={addCredentialRequirement}
            className="flex flex-wrap items-end gap-2"
          >
            <div className="grid gap-1">
              <Label htmlFor="cred-provider" className="text-xs">
                Provider
              </Label>
              <Input
                id="cred-provider"
                list="providers-list"
                value={credProvider}
                onChange={(e) => setCredProvider(e.target.value)}
                placeholder="e.g. github"
                className="h-8 w-40 text-xs"
              />
              <datalist id="providers-list">
                {providers?.map((p) => (
                  <option key={p.id} value={p.name} />
                ))}
              </datalist>
            </div>
            <div className="grid gap-1">
              <Label htmlFor="cred-scopes" className="text-xs">
                Scopes
              </Label>
              <Input
                id="cred-scopes"
                list="scopes-list"
                value={credScopes}
                onChange={(e) => setCredScopes(e.target.value)}
                placeholder="repo, read:org"
                className="h-8 w-36 text-xs"
              />
              <datalist id="scopes-list">
                {providers
                  ?.find(
                    (p) =>
                      p.name.toLowerCase() ===
                      credProvider.trim().toLowerCase(),
                  )
                  ?.scopes?.map((s) => (
                    <option key={s} value={s} />
                  ))}
              </datalist>
            </div>
            <div className="grid gap-1">
              <Label className="text-xs">Source</Label>
              <Select
                value={credSource}
                onValueChange={(v) =>
                  setCredSource(v as CredentialRequirementSource)
                }
              >
                <SelectTrigger className="h-8 w-24 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {credentialRequirementSources.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s === "tenant" ? "org" : s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button
              type="submit"
              size="sm"
              className="h-8"
              disabled={updateMut.isPending || !credProvider.trim()}
            >
              <Plus className="size-3.5" />
              Add
            </Button>
          </form>
          <MutationError error={updateMut.error} />
        </div>
      </div>

      {/* Roles */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Roles</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Roles assigned to this agent. At launch, the instance principal
          inherits these roles and their grants.
        </p>

        {agentRoles.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-2">
            {agentRoles.map((r) => (
              <Badge key={r.id} variant="secondary" className="gap-1 pr-1">
                {r.name}
                <button
                  onClick={() => removeRole(r.id)}
                  disabled={updateMut.isPending}
                  className="ml-1 rounded-sm hover:bg-muted"
                >
                  <X className="size-3" />
                </button>
              </Badge>
            ))}
          </div>
        )}

        {availableRoles.length > 0 && (
          <div className="mt-3 flex items-end gap-2">
            <div className="grid gap-1">
              <Label className="text-xs">Add role</Label>
              <Select onValueChange={addRole} disabled={updateMut.isPending}>
                <SelectTrigger className="h-8 w-48 text-xs">
                  <SelectValue placeholder="Select a role..." />
                </SelectTrigger>
                <SelectContent>
                  {availableRoles.map((r) => (
                    <SelectItem key={r.id} value={r.id}>
                      {r.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {availableRoles.length === 0 && agentRoles.length === 0 && (
          <p className="mt-3 text-xs text-muted-foreground">
            No roles available. Create roles in the tenant settings first.
          </p>
        )}

        <MutationError error={updateMut.error} />
      </div>

      {/* Grant Requirements */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Grant Requirements</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Additional permissions delegated by the definition creator or the
          person launching the instance (invoker).
        </p>

        {grantRequirements.length > 0 && (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Resource</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Effect</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {grantRequirements.map((req, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">
                      {req.resource}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {req.action}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {req.source}
                    </TableCell>
                    <TableCell>
                      <EffectBadge effect={req.effect ?? "allow"} />
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        className="text-destructive hover:text-destructive"
                        onClick={() => removeGrantRequirement(i)}
                        disabled={updateMut.isPending}
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

        <div className="mt-4 space-y-3 rounded-lg border p-4">
          <form
            onSubmit={addGrantRequirement}
            className="flex flex-wrap items-end gap-2"
          >
            <div className="grid gap-1">
              <Label htmlFor="grant-resource" className="text-xs">
                Resource
              </Label>
              <Input
                id="grant-resource"
                value={grantReqResource}
                onChange={(e) => setGrantReqResource(e.target.value)}
                placeholder="e.g. tool:bash"
                className="h-8 w-40 text-xs"
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="grant-action" className="text-xs">
                Action
              </Label>
              <Input
                id="grant-action"
                value={grantReqAction}
                onChange={(e) => setGrantReqAction(e.target.value)}
                placeholder="e.g. invoke"
                className="h-8 w-28 text-xs"
              />
            </div>
            <div className="grid gap-1">
              <Label className="text-xs">Source</Label>
              <Select
                value={grantReqSource}
                onValueChange={(v) =>
                  setGrantReqSource(v as GrantRequirementSource)
                }
              >
                <SelectTrigger className="h-8 w-24 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {grantRequirementSources.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-1">
              <Label className="text-xs">Effect</Label>
              <Select
                value={grantReqEffect}
                onValueChange={(v) => setGrantReqEffect(v as GrantEffect)}
              >
                <SelectTrigger className="h-8 w-24 text-xs">
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
            <Button
              type="submit"
              size="sm"
              className="h-8"
              disabled={
                updateMut.isPending ||
                !grantReqResource.trim() ||
                !grantReqAction.trim()
              }
            >
              <Plus className="size-3.5" />
              Add
            </Button>
          </form>
          <MutationError error={updateMut.error} />
        </div>
      </div>

      {/* Delete agent confirmation */}
      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete agent?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete the agent definition &ldquo;
              {agent.name}&rdquo; and all its instances. This action cannot be
              undone.
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
