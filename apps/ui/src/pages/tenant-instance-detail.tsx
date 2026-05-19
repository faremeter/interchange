import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowLeft,
  ChevronDown,
  ChevronRight,
  Mail,
  Paperclip,
} from "lucide-react";
import {
  createBrowserTransport,
  createInstanceSession,
  formatAddress,
  parseFromHeader,
  resolveAgentAddress,
  resolveAgentRecipient,
  type AgentActivity,
  type InstanceEvent,
  type InstanceSession,
  type ToolCallEvent,
} from "@intx/hub-client";

import { MutationError } from "@/components/mutation-error";
import {
  instanceDetailQuery,
  stopInstanceMutation,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const transport = createBrowserTransport();

function ToolCallView({ call }: { call: ToolCallEvent }) {
  const [expanded, setExpanded] = useState(false);
  const argsStr = formatToolArgs(call.name, call.arguments);

  return (
    <div className="my-1 rounded border border-border/50 bg-muted/30 text-xs font-mono">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-1 px-2 py-1 text-left hover:bg-muted/50"
      >
        {expanded ? (
          <ChevronDown className="size-3 shrink-0 text-muted-foreground" />
        ) : (
          <ChevronRight className="size-3 shrink-0 text-muted-foreground" />
        )}
        <span className="text-primary">{call.name}</span>
        <span className="truncate text-muted-foreground">({argsStr})</span>
        {call.isError && (
          <span className="ml-auto shrink-0 text-destructive">error</span>
        )}
      </button>
      {expanded && (
        <div className="border-t border-border/50">
          <div className="border-b border-border/30 px-2 py-1 text-muted-foreground">
            {JSON.stringify(call.arguments, null, 2)}
          </div>
          <pre
            className={`whitespace-pre-wrap px-2 py-1 ${call.isError ? "text-destructive" : ""}`}
          >
            {call.result}
          </pre>
        </div>
      )}
    </div>
  );
}

function formatToolArgs(_name: string, args: Record<string, unknown>): string {
  const entries = Object.entries(args);
  if (entries.length === 0) return "";
  return entries
    .map(([k, v]) => {
      const val = typeof v === "string" ? v : JSON.stringify(v);
      const short = val.length > 40 ? val.slice(0, 40) + "..." : val;
      return `${k}: ${short}`;
    })
    .join(", ");
}

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "running"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
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

export function TenantInstanceDetailPage() {
  const { tenantId, instanceId } = useParams({
    from: "/authed/tenants/$tenantId/instances/$instanceId",
  });
  const queryClient = useQueryClient();

  const { data: instance, isLoading } = useQuery(
    instanceDetailQuery(tenantId, instanceId),
  );

  const isRunning = instance?.status === "running";
  const displayStatus = isRunning
    ? (instance?.runtimeStatus ?? "running")
    : (instance?.status ?? "unknown");

  const [, forceRender] = useState(0);

  const [chatInput, setChatInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const chatPinnedRef = useRef(true);
  const sessionRef = useRef<InstanceSession | null>(null);

  useEffect(() => {
    if (!isRunning) {
      sessionRef.current?.destroy();
      sessionRef.current = null;
      return;
    }

    const session = createInstanceSession({
      tenantId,
      instanceId,
      transport,
      onChange: () => forceRender((n) => n + 1),
      onSessionEnded: () => {
        queryClient.invalidateQueries({
          queryKey: ["tenants", tenantId, "instances"],
        });
      },
    });
    sessionRef.current = session;
    const cleanup = session.start();
    return () => {
      cleanup();
      session.destroy();
      sessionRef.current = null;
    };
  }, [isRunning, instanceId, tenantId]);

  const stopMut = useMutation({
    ...stopInstanceMutation(tenantId, queryClient),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "instances"],
      });
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents"],
      });
    },
  });

  async function handleSendChat(e: React.FormEvent) {
    e.preventDefault();
    if (!chatInput.trim() || isSending) return;
    const userMessage = chatInput.trim();
    setChatInput("");
    setIsSending(true);
    try {
      await sessionRef.current?.sendMail(userMessage);
    } finally {
      setIsSending(false);
    }
  }

  useEffect(() => {
    chatPinnedRef.current = true;
  }, [instanceId]);

  useEffect(() => {
    if (!isRunning) return;
    const el = chatScrollRef.current;
    if (el && chatPinnedRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  });

  const session = sessionRef.current;
  const events: InstanceEvent[] = session?.events ?? [];
  const streaming: string = session?.streaming ?? "";
  const activity: AgentActivity | null = session?.activity ?? null;

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!instance) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/instances"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Agents
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">
            <Link
              to="/tenants/$tenantId/agents/$agentId"
              params={{ tenantId, agentId: instance.agentId }}
              className="text-primary hover:underline"
            >
              {instance.agentName}
            </Link>
          </h2>
          <p className="mt-1 font-mono text-xs text-muted-foreground">
            {instance.address}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isRunning && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => stopMut.mutate(instanceId)}
              disabled={stopMut.isPending}
            >
              {stopMut.isPending ? "Stopping..." : "Stop"}
            </Button>
          )}
        </div>
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Status">
            <StatusBadge status={displayStatus} />
          </Row>
          <Row label="Instance ID">
            <span className="font-mono text-xs">{instance.id}</span>
          </Row>
          {instance.kernelId && (
            <Row label="Kernel ID">
              <span className="font-mono text-xs">{instance.kernelId}</span>
            </Row>
          )}
          {instance.sidecarId && (
            <Row label="Sidecar ID">
              <span className="font-mono text-xs">{instance.sidecarId}</span>
            </Row>
          )}
          <Row label="Created">
            {new Date(instance.createdAt).toLocaleString()}
          </Row>
          {instance.endedAt && (
            <Row label="Ended">
              {new Date(instance.endedAt).toLocaleString()}
            </Row>
          )}
        </dl>
      </div>

      <MutationError error={stopMut.error} />

      {isRunning && (
        <div className="mt-8">
          <h3 className="text-sm font-semibold">Chat</h3>
          <div className="mt-3 flex flex-col gap-3 rounded-lg border p-4">
            <div
              ref={chatScrollRef}
              onScroll={(e) => {
                const el = e.currentTarget;
                chatPinnedRef.current =
                  el.scrollTop + el.clientHeight >= el.scrollHeight - 24;
              }}
              className="flex h-64 flex-col gap-2 overflow-y-auto"
            >
              {events.length === 0 && !streaming ? (
                <p className="text-sm text-muted-foreground">
                  Start a conversation with the agent...
                </p>
              ) : (
                events.map((msg) => {
                  const isUser = msg.kind === "mail" && msg.role === "user";
                  const isAssistantTurn = msg.kind === "turn";
                  const isAssistantMail =
                    msg.kind === "mail" && msg.role === "assistant";
                  const isAssistant = isAssistantTurn || isAssistantMail;
                  const agentRecipient =
                    isAssistantMail && msg.kind === "mail"
                      ? resolveAgentRecipient(msg.recipients)
                      : null;
                  const agentSender =
                    isUser && msg.kind === "mail"
                      ? resolveAgentAddress(msg.sender)
                      : null;
                  const isOutboundInterAgent = agentRecipient !== null;
                  const isInboundInterAgent = agentSender !== null;
                  const isInterAgentMail =
                    isOutboundInterAgent || isInboundInterAgent;

                  const label = msg.isError
                    ? "Error"
                    : isInterAgentMail
                      ? null
                      : isAssistant
                        ? "Agent"
                        : msg.kind === "mail"
                          ? parseFromHeader(formatAddress(msg.sender))
                          : null;

                  return (
                    <div
                      key={msg.kind === "mail" ? msg.id : msg.turnId}
                      className={`rounded p-2 text-sm ${
                        msg.isError
                          ? "mr-8 border border-destructive/30 bg-destructive/10 text-destructive"
                          : isInterAgentMail
                            ? "mx-8 border border-border bg-muted/30 text-muted-foreground"
                            : isUser
                              ? "bg-muted ml-8"
                              : "mr-8 bg-primary/10"
                      }`}
                    >
                      {isOutboundInterAgent && agentRecipient !== null ? (
                        <div className="mb-1 inline-flex items-center gap-1 text-xs">
                          <Mail className="size-3" />
                          <span>
                            Sent mail to{" "}
                            <Link
                              to="/tenants/$tenantId/instances/$instanceId"
                              params={{
                                tenantId,
                                instanceId: agentRecipient.instanceId,
                              }}
                              className="font-medium text-primary underline"
                            >
                              {agentRecipient.label}
                            </Link>
                          </span>
                        </div>
                      ) : isInboundInterAgent && agentSender !== null ? (
                        <div className="mb-1 inline-flex items-center gap-1 text-xs">
                          <Mail className="size-3" />
                          <span>
                            Received mail from{" "}
                            <Link
                              to="/tenants/$tenantId/instances/$instanceId"
                              params={{
                                tenantId,
                                instanceId: agentSender.instanceId,
                              }}
                              className="font-medium text-primary underline"
                            >
                              {agentSender.label}
                            </Link>
                          </span>
                        </div>
                      ) : label ? (
                        <span className="font-medium">{label}: </span>
                      ) : null}
                      {isInterAgentMail ? (
                        <div className="text-xs whitespace-pre-wrap">
                          {msg.content}
                        </div>
                      ) : (
                        <span className="whitespace-pre-wrap">
                          {msg.content}
                        </span>
                      )}
                      {msg.kind === "turn" &&
                        msg.toolCalls &&
                        msg.toolCalls.length > 0 && (
                          <div className="mt-1.5">
                            {msg.toolCalls.map((call, i) => (
                              <ToolCallView key={i} call={call} />
                            ))}
                          </div>
                        )}
                      {msg.kind === "turn" &&
                        msg.errors &&
                        msg.errors.length > 0 && (
                          <div className="mt-1.5 space-y-1">
                            {msg.errors.map((err, i) => (
                              <div
                                key={i}
                                className="rounded border border-destructive/30 bg-destructive/10 px-2 py-1 text-xs text-destructive"
                              >
                                <span className="font-medium">
                                  {err.category}:
                                </span>{" "}
                                {err.message}
                              </div>
                            ))}
                          </div>
                        )}
                      {msg.kind === "turn" &&
                        msg.toolErrors &&
                        msg.toolErrors.length > 0 && (
                          <div className="mt-1.5 space-y-1">
                            {msg.toolErrors.map((err, i) => (
                              <div
                                key={i}
                                className="rounded border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-xs text-amber-700 dark:text-amber-400"
                              >
                                <span className="font-medium">
                                  Tool Error ({err.name}):
                                </span>{" "}
                                {err.content}
                              </div>
                            ))}
                          </div>
                        )}
                      {msg.kind === "mail" && msg.attachments.length > 0 && (
                        <div className="mt-1 flex flex-col gap-0.5">
                          {msg.attachments.map((att) => (
                            <a
                              key={att.blobId}
                              href={`/api/tenants/${tenantId}/agents/instances/blobs/${att.blobId}`}
                              target="_blank"
                              rel="noreferrer"
                              className="inline-flex items-center gap-1 text-xs text-primary underline"
                            >
                              <Paperclip className="size-3" />
                              {att.name ?? att.blobId}
                            </a>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })
              )}
              {streaming && (
                <div className="mr-8 rounded bg-primary/10 p-2 text-sm whitespace-pre-wrap">
                  <span className="font-medium">Agent:</span> {streaming}
                </div>
              )}
              {(() => {
                if (activity !== null) {
                  const label =
                    activity.type === "inferring"
                      ? "Thinking..."
                      : activity.type === "tool_call"
                        ? `Calling ${activity.name}...`
                        : activity.type === "tool_running"
                          ? `Running ${activity.name}...`
                          : `Rate limited, retrying in ${Math.ceil(activity.retryAfterMs / 1000)}s...`;
                  return (
                    <div
                      className={`text-sm ${activity.type === "rate_limited" ? "text-amber-600 dark:text-amber-400" : "text-muted-foreground"}`}
                    >
                      {label}
                    </div>
                  );
                }
                if (isSending && !streaming) {
                  return (
                    <div className="text-sm text-muted-foreground">
                      Sending...
                    </div>
                  );
                }
                return null;
              })()}
            </div>

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
    </div>
  );
}
