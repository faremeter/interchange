import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Pencil, Plus, Trash2, X } from "lucide-react";
import {
  type CredentialRequirementSource,
  type GrantEffect,
  type GrantSource,
  credentialRequirementSources,
  grantEffects,
  grantSources,
} from "@interchange/types";

import { MutationError } from "@/components/mutation-error";
import {
  agentDetailQuery,
  createGrantMutation,
  createSessionMutation,
  deleteAgentMutation,
  deleteGrantMutation,
  endSessionMutation,
  principalGrantsQuery,
  tenantProvidersQuery,
  updateAgentMutation,
} from "@/lib/queries/tenants";
import { api, openStream } from "@/lib/api";
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

// Module-level store keyed by sessionId — survives React Strict Mode remounts.
type ChatMessage = { role: "user" | "assistant"; content: string };
const sessionMessages = new Map<string, ChatMessage[]>();
const sessionStreaming = new Map<string, string>();

function getMessages(sessionId: string): ChatMessage[] {
  const existing = sessionMessages.get(sessionId);
  if (existing) return existing;
  const fresh: ChatMessage[] = [];
  sessionMessages.set(sessionId, fresh);
  return fresh;
}

function getStreaming(sessionId: string): string {
  return sessionStreaming.get(sessionId) ?? "";
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
  const { data: grants } = useQuery({
    ...principalGrantsQuery(tenantId, agent?.principalId ?? ""),
    enabled: !!agent?.principalId,
  });
  const { data: providers } = useQuery(tenantProvidersQuery(tenantId));

  const [editing, setEditing] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  // Chat render trigger — the actual message data lives in module-level Maps
  // (sessionMessages / sessionStreaming) so it survives React Strict Mode remounts.
  const [, forceRender] = useState(0);
  const rerender = () => forceRender((n) => n + 1);

  const [chatInput, setChatInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const closeStreamRef = useRef<(() => void) | null>(null);

  // Edit form state
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editSystemPrompt, setEditSystemPrompt] = useState("");

  // Permission add form state
  const [permProvider, setPermProvider] = useState("");
  const [permScopes, setPermScopes] = useState("");
  const [permSource, setPermSource] =
    useState<CredentialRequirementSource>("tenant");
  const [permEffect, setPermEffect] = useState<GrantEffect>("allow");
  const [permResource, setPermResource] = useState("");
  const [permAction, setPermAction] = useState("");
  const [permGrantSource, setPermGrantSource] =
    useState<GrantSource>("creator");
  const [revokeTarget, setRevokeTarget] = useState<string | null>(null);

  // Merge credential requirements with their corresponding grants
  // A credential permission is identified by matching a requirement to a grant with resource "credential:<providerName>"
  const requirements = agent?.credentialRequirements ?? [];
  const grantsList = grants ?? [];

  interface PermissionRow {
    type: "credential";
    providerName: string;
    scopes: string[];
    source: string;
    name?: string;
    effect: string;
    requirementIndex: number;
    grantId?: string;
  }

  function buildPermissionRows(): PermissionRow[] {
    const rows: PermissionRow[] = [];

    // Build a map of providerName -> effect from grants
    const providerGrantEffects = new Map<string, string>();
    for (const g of grantsList) {
      if (g.resource.startsWith("credential:") && g.action === "use") {
        const providerName = g.resource.replace("credential:", "");
        // If there's already a grant, deny wins, then ask, then allow
        const existing = providerGrantEffects.get(providerName);
        if (!existing) {
          providerGrantEffects.set(providerName, g.effect);
        } else {
          const priority = { deny: 0, ask: 1, allow: 2 };
          if (
            priority[g.effect as keyof typeof priority] <
            priority[existing as keyof typeof priority]
          ) {
            providerGrantEffects.set(providerName, g.effect);
          }
        }
      }
    }

    // Merge requirements with their grants
    for (let i = 0; i < requirements.length; i++) {
      const req = requirements[i];
      if (!req) continue;
      const providerKey = req.providerName.toLowerCase();
      // Find matching grant
      let matchingGrantId: string | undefined;
      for (const g of grantsList) {
        if (
          g.resource.toLowerCase() === `credential:${providerKey}` &&
          g.action === "use"
        ) {
          matchingGrantId = g.id;
          break;
        }
      }

      rows.push({
        type: "credential",
        providerName: req.providerName,
        scopes: req.scopes ?? [],
        source: req.source,
        name: req.name,
        effect: providerGrantEffects.get(providerKey) ?? "allow",
        requirementIndex: i,
        grantId: matchingGrantId,
      });
    }

    return rows;
  }

  // Get resource grants (non-credential)
  function getResourceGrants() {
    return grantsList.filter((g) => !g.resource.startsWith("credential:"));
  }

  const permissionRows = buildPermissionRows();
  const resourceGrants = getResourceGrants();

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
      setEditing(false);
    },
  });

  const deleteMut = useMutation({
    ...deleteAgentMutation(tenantId, agentId, queryClient),
    onSuccess: () => {
      navigate({ to: "/tenants/$tenantId/agents", params: { tenantId } });
    },
  });

  const grantMut = useMutation({
    ...createGrantMutation(tenantId, queryClient),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: [
          "tenants",
          tenantId,
          "grants",
          { principalId: agent?.principalId },
        ],
      });
      resetPermissionForm();
    },
  });

  const revokeMut = useMutation({
    ...deleteGrantMutation(tenantId, revokeTarget ?? "", queryClient),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: [
          "tenants",
          tenantId,
          "grants",
          { principalId: agent?.principalId },
        ],
      });
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
      setRevokeTarget(null);
    },
  });

  const startMut = useMutation({
    ...createSessionMutation(tenantId, queryClient),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
    },
  });

  const stopMut = useMutation({
    ...endSessionMutation(tenantId, agent?.sessionId ?? "", queryClient),
    onSuccess: () => {
      if (agent?.sessionId) {
        sessionMessages.delete(agent.sessionId);
        sessionStreaming.delete(agent.sessionId);
      }
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents"],
      });
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents", agentId],
      });
      rerender();
    },
  });

  // Open/close the EventSource based on agent running state.
  // useEffect cleanup handles Strict Mode correctly and also reconnects
  // when navigating back to an already-running agent.
  const sessionId = agent?.sessionId;
  useEffect(() => {
    if (agent?.status !== "running" || !sessionId) return;

    let cancelled = false;

    type InferenceEvent = {
      type: string;
      seq: number;
      data: Record<string, unknown>;
    };

    const close = openStream(
      `/api/tenants/${tenantId}/sessions/${sessionId}/events`,
      (raw) => {
        if (cancelled) return;
        const event = raw as InferenceEvent;
        if (event.type === "inference.text.delta") {
          const token = (event.data.token as string) ?? "";
          sessionStreaming.set(sessionId, getStreaming(sessionId) + token);
          rerender();
        } else if (event.type === "inference.done") {
          // Extract text from the completed message's content blocks.
          const msg = event.data.message as {
            content: { type: string; text?: string }[];
          };
          const text = msg.content
            .filter((b) => b.type === "text")
            .map((b) => b.text ?? "")
            .join("");
          sessionStreaming.set(sessionId, "");
          if (text) {
            getMessages(sessionId).push({ role: "assistant", content: text });
          }
          rerender();
        } else if (event.type === "reactor.done") {
          sessionStreaming.set(sessionId, "");
          rerender();
        }
      },
      { eventName: "agent.event" },
    );
    closeStreamRef.current = close;

    return () => {
      cancelled = true;
      close();
      closeStreamRef.current = null;
    };
  }, [agent?.status, sessionId, tenantId]);

  function resetPermissionForm() {
    setPermProvider("");
    setPermScopes("");
    setPermSource("tenant");
    setPermEffect("allow");
    setPermResource("");
    setPermAction("");
    setPermGrantSource("creator");
  }

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

  async function handleSendChat(e: React.FormEvent) {
    e.preventDefault();
    if (!chatInput.trim() || isSending || !sessionId) return;
    const userMessage = chatInput.trim();
    setChatInput("");
    getMessages(sessionId).push({ role: "user", content: userMessage });
    rerender();
    setIsSending(true);
    try {
      await api(
        "POST",
        `/api/tenants/${tenantId}/sessions/${sessionId}/messages`,
        { content: userMessage },
      );
    } finally {
      setIsSending(false);
    }
  }

  function addPermission(e: React.FormEvent) {
    e.preventDefault();
    if (!agent) return;

    const targetValue = permProvider.trim() || permResource.trim();
    const isProvider = isKnownProvider(targetValue);

    if (isProvider && permProvider.trim()) {
      // Adding a credential permission
      const scopes = permScopes
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);

      // Always create the grant for credential access
      grantMut.mutate({
        resource: `credential:${permProvider.trim()}`,
        action: "use",
        effect: permEffect,
        source: permGrantSource,
        principalId: agent.principalId,
      });

      // Only add requirement if effect is allow or ask (not deny)
      if (permEffect !== "deny") {
        const existing = agent.credentialRequirements ?? [];
        const req = {
          providerName: permProvider.trim(),
          source: permSource,
          ...(scopes.length > 0 ? { scopes } : {}),
        };
        updateMut.mutate({
          credentialRequirements: [...existing, req],
        });
      }
    } else if (permResource.trim() && permAction.trim()) {
      // Adding a resource grant
      grantMut.mutate({
        resource: permResource.trim(),
        action: permAction.trim(),
        effect: permEffect,
        source: permGrantSource,
        principalId: agent.principalId,
      });
    }
  }

  function removeCredentialPermission(row: PermissionRow) {
    // Remove the requirement
    const existing = agent?.credentialRequirements ?? [];
    const newRequirements = existing.filter(
      (_, i) => i !== row.requirementIndex,
    );
    updateMut.mutate({ credentialRequirements: newRequirements });

    // Remove the grant if it exists
    if (row.grantId) {
      setRevokeTarget(row.grantId);
    }
  }

  // Check if a provider name exists (case-insensitive)
  function isKnownProvider(value: string): boolean {
    if (!providers) return false;
    return providers.some((p) => p.name.toLowerCase() === value.toLowerCase());
  }

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

      {/* Agent Actions */}
      <div className="mt-6 flex items-center gap-2">
        {agent.status === "running" ? (
          <Button
            variant="outline"
            size="sm"
            onClick={() => stopMut.mutate()}
            disabled={stopMut.isPending}
          >
            {stopMut.isPending ? "Stopping..." : "Stop Agent"}
          </Button>
        ) : (
          <Button
            size="sm"
            onClick={() => startMut.mutate({ agentId })}
            disabled={startMut.isPending}
          >
            {startMut.isPending ? "Starting..." : "Start Agent"}
          </Button>
        )}
      </div>

      {/* Chat Section - only show when running */}
      {agent.status === "running" && sessionId && (
        <div className="mt-8">
          <h3 className="text-sm font-semibold">Chat</h3>
          <div className="mt-3 flex flex-col gap-3 rounded-lg border p-4">
            {/* Messages */}
            <div className="flex h-64 flex-col gap-2 overflow-y-auto">
              {getMessages(sessionId).length === 0 &&
              !getStreaming(sessionId) ? (
                <p className="text-sm text-muted-foreground">
                  Start a conversation with the agent...
                </p>
              ) : (
                getMessages(sessionId).map((msg, i) => (
                  <div
                    key={i}
                    className={`rounded p-2 text-sm ${
                      msg.role === "user"
                        ? "bg-muted ml-8"
                        : "mr-8 bg-primary/10"
                    }`}
                  >
                    <span className="font-medium">
                      {msg.role === "user" ? "You" : "Agent"}:
                    </span>{" "}
                    {msg.content}
                  </div>
                ))
              )}
              {getStreaming(sessionId) && (
                <div className="rounded p-2 text-sm mr-8 bg-primary/10">
                  <span className="font-medium">Agent:</span>{" "}
                  {getStreaming(sessionId)}
                </div>
              )}
              {isSending && !getStreaming(sessionId) && (
                <div className="text-sm text-muted-foreground">
                  Agent is thinking...
                </div>
              )}
            </div>

            {/* Input */}
            <form onSubmit={handleSendChat} className="flex gap-2">
              <Input
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
                placeholder="Type a message..."
                disabled={isSending}
              />
              <Button type="submit" disabled={isSending || !chatInput.trim()}>
                Send
              </Button>
            </form>
          </div>
        </div>
      )}

      {/* Unified Permissions section */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Permissions</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          What this agent is allowed to do. Credential permissions control
          access to external services; resource grants control access to
          specific resources.
        </p>

        {/* Permission table */}
        {(permissionRows.length > 0 || resourceGrants.length > 0) && (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Permission</TableHead>
                  <TableHead>Details</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Effect</TableHead>
                  <TableHead className="w-10" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {/* Credential permissions */}
                {permissionRows.map((row) => (
                  <TableRow key={row.requirementIndex}>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-xs">
                          {row.providerName}
                        </span>
                        <Badge variant="outline" className="text-[10px]">
                          credential
                        </Badge>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        {row.scopes.length > 0 ? (
                          <div className="flex flex-wrap gap-1">
                            {row.scopes.map((s) => (
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
                          <span className="text-muted-foreground text-xs">
                            any
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="text-muted-foreground text-xs">
                      {row.source === "tenant" ? "org" : row.source}
                    </TableCell>
                    <TableCell>
                      <EffectBadge effect={row.effect} />
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        className="text-destructive hover:text-destructive"
                        onClick={() => removeCredentialPermission(row)}
                        disabled={updateMut.isPending || revokeMut.isPending}
                      >
                        <X className="size-3.5" />
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {/* Resource grants */}
                {resourceGrants.map((g) => (
                  <TableRow key={g.id}>
                    <TableCell className="font-mono text-xs">
                      {g.resource}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {g.action}
                    </TableCell>
                    <TableCell className="text-muted-foreground text-xs">
                      {g.source}
                    </TableCell>
                    <TableCell>
                      <EffectBadge effect={g.effect} />
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        className="text-destructive hover:text-destructive"
                        onClick={() => setRevokeTarget(g.id)}
                        disabled={revokeMut.isPending}
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

        {/* Add permission form */}
        <div className="mt-4 space-y-3 rounded-lg border p-4">
          <form
            onSubmit={addPermission}
            className="flex items-end gap-2 flex-wrap"
          >
            <div className="grid gap-1">
              <Label htmlFor="perm-target" className="text-xs">
                Provider or Resource
              </Label>
              <Input
                id="perm-target"
                list="providers-list"
                value={permProvider || permResource}
                onChange={(e) => {
                  const val = e.target.value;
                  if (isKnownProvider(val)) {
                    setPermProvider(val);
                    setPermResource("");
                  } else {
                    setPermProvider("");
                    setPermResource(val);
                  }
                }}
                onBlur={(e) => {
                  const val = e.target.value;
                  if (isKnownProvider(val)) {
                    setPermProvider(val);
                    setPermResource("");
                  }
                }}
                placeholder="Select provider or enter resource pattern"
                className="h-8 w-56 text-xs"
              />
              <datalist id="providers-list">
                {providers?.map((p) => (
                  <option key={`provider-${p.id}`} value={p.name} />
                ))}
                {[
                  ...new Set(
                    grantsList
                      .map((g) => g.resource)
                      .filter((r) => !r.startsWith("credential:")),
                  ),
                ].map((r) => (
                  <option key={`grant-${r}`} value={r} />
                ))}
              </datalist>
            </div>

            {/* Provider fields - show if known provider */}
            {isKnownProvider(permProvider) ? (
              <>
                <div className="grid gap-1">
                  <Label htmlFor="perm-scopes" className="text-xs">
                    Scopes
                  </Label>
                  <Input
                    id="perm-scopes"
                    list="scopes-list"
                    value={permScopes}
                    onChange={(e) => setPermScopes(e.target.value)}
                    placeholder="repo, read:org"
                    className="h-8 w-36 text-xs"
                  />
                  <datalist id="scopes-list">
                    {providers
                      ?.find(
                        (p) =>
                          p.name.toLowerCase() ===
                          permProvider.trim().toLowerCase(),
                      )
                      ?.scopes?.map((s) => (
                        <option key={s} value={s} />
                      ))}
                  </datalist>
                </div>
                <div className="grid gap-1">
                  <Label className="text-xs">Source</Label>
                  <Select
                    value={permSource}
                    onValueChange={(v) =>
                      setPermSource(v as CredentialRequirementSource)
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
              </>
            ) : (
              <>
                <div className="grid gap-1">
                  <Label htmlFor="perm-action" className="text-xs">
                    Action
                  </Label>
                  <Input
                    id="perm-action"
                    list="actions-list"
                    value={permAction}
                    onChange={(e) => setPermAction(e.target.value)}
                    placeholder="e.g. read"
                    className="h-8 w-28 text-xs"
                  />
                  <datalist id="actions-list">
                    {[...new Set(grantsList.map((g) => g.action))].map((a) => (
                      <option key={a} value={a} />
                    ))}
                  </datalist>
                </div>
                <div className="grid gap-1">
                  <Label className="text-xs">Source</Label>
                  <Select
                    value={permGrantSource}
                    onValueChange={(v) => setPermGrantSource(v as GrantSource)}
                  >
                    <SelectTrigger className="h-8 w-24 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {grantSources
                        .filter((s) => s !== "system" && s !== "role")
                        .map((s) => (
                          <SelectItem key={s} value={s}>
                            {s}
                          </SelectItem>
                        ))}
                    </SelectContent>
                  </Select>
                </div>
              </>
            )}

            <div className="grid gap-1">
              <Label className="text-xs">Effect</Label>
              <Select
                value={permEffect}
                onValueChange={(v) => setPermEffect(v as GrantEffect)}
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
                grantMut.isPending ||
                (!permProvider.trim() && !permResource.trim())
              }
            >
              <Plus className="size-3.5" />
              Add
            </Button>
          </form>
          <MutationError error={grantMut.error} />
          <MutationError error={updateMut.error} />
        </div>
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
            <AlertDialogTitle>Revoke permission?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently remove this permission from the agent. This
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
