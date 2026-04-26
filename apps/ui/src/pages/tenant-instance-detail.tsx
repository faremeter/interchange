import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { type } from "arktype";
import {
  InferenceEvent,
  type InferenceEvent as ValidInferenceEvent,
} from "@interchange/types/runtime";

import { MutationError } from "@/components/mutation-error";
import {
  instanceDetailQuery,
  stopInstanceMutation,
} from "@/lib/queries/tenants";
import { api, openStream } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const sessionEndedEvent = type({ type: "'session.ended'" });

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

function parseFromHeader(from: string): string {
  const match = from.match(/^"(.+)"\s+<.+>$/);
  if (match && match[1]) return match[1];
  const local = from.split("@")[0];
  return local ?? from;
}

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
  from: string;
  isError?: boolean;
};
type AgentActivity =
  | { type: "inferring" }
  | { type: "tool_call"; name: string }
  | { type: "tool_running"; name: string };

const instanceMessages = new Map<string, ChatMessage[]>();
const instanceStreaming = new Map<string, string>();
const instanceActivity = new Map<string, AgentActivity | null>();

function getMessages(instanceId: string): ChatMessage[] {
  const existing = instanceMessages.get(instanceId);
  if (existing) return existing;
  const fresh: ChatMessage[] = [];
  instanceMessages.set(instanceId, fresh);
  return fresh;
}

function getStreaming(instanceId: string): string {
  return instanceStreaming.get(instanceId) ?? "";
}

function clearInstanceState(instanceId: string) {
  instanceMessages.delete(instanceId);
  instanceStreaming.delete(instanceId);
  instanceActivity.delete(instanceId);
}

export function TenantInstanceDetailPage() {
  const { tenantId, instanceId } = useParams({ strict: false }) as {
    tenantId: string;
    instanceId: string;
  };
  const queryClient = useQueryClient();

  const { data: instance, isLoading } = useQuery(
    instanceDetailQuery(tenantId, instanceId),
  );

  const isRunning = instance?.status === "running";
  const displayStatus = isRunning
    ? (instance?.runtimeStatus ?? "running")
    : (instance?.status ?? "unknown");

  const [, forceRender] = useState(0);
  const rerender = () => forceRender((n) => n + 1);

  const [chatInput, setChatInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const chatPinnedRef = useRef(true);

  // Hydrate chat history from the hub's DB on mount/reload.
  useEffect(() => {
    if (!isRunning) return;
    if (getMessages(instanceId).length > 0) return;

    let cancelled = false;

    type MessagePart = { type: string; content?: string | null };
    type MessageRow = {
      role: "user" | "assistant";
      from: string;
      status: string;
      parts: MessagePart[];
    };

    void (async () => {
      const messages = await api<MessageRow[]>(
        "GET",
        `/api/tenants/${tenantId}/agents/instances/${instanceId}/messages?limit=100`,
      );
      if (cancelled) return;
      if (getMessages(instanceId).length > 0) return;

      const history: ChatMessage[] = [];
      for (const msg of messages) {
        if (msg.status !== "delivered") continue;
        const hasError = msg.parts.some((p) => p.type === "error");
        const text = msg.parts
          .filter(
            (p): p is MessagePart & { content: string } =>
              p.type === "text" && typeof p.content === "string",
          )
          .map((p) => p.content)
          .join("");
        if (text) {
          history.push({
            role: msg.role,
            content: text,
            from: msg.from,
            ...(hasError ? { isError: true } : {}),
          });
        }
      }

      if (history.length > 0) {
        instanceMessages.set(instanceId, history);
        rerender();
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [isRunning, instanceId, tenantId]);

  // Open/close the EventSource based on instance running state.
  useEffect(() => {
    if (!isRunning) return;

    let cancelled = false;
    let hadInferenceError = false;

    const close = openStream(
      `/api/tenants/${tenantId}/agents/instances/${instanceId}/events`,
      (raw) => {
        if (cancelled) return;

        if (!(sessionEndedEvent(raw) instanceof type.errors)) {
          queryClient.invalidateQueries({
            queryKey: ["tenants", tenantId, "instances"],
          });
          return;
        }

        const validated = InferenceEvent(raw);
        if (validated instanceof type.errors) return;
        const event: ValidInferenceEvent = validated as ValidInferenceEvent;
        switch (event.type) {
          case "inference.start":
            instanceActivity.set(instanceId, { type: "inferring" });
            rerender();
            break;
          case "inference.text.delta":
            instanceStreaming.set(
              instanceId,
              getStreaming(instanceId) + event.data.token,
            );
            instanceActivity.set(instanceId, null);
            rerender();
            break;
          case "inference.tool_call.start":
            instanceActivity.set(instanceId, {
              type: "tool_call",
              name: event.data.name,
            });
            rerender();
            break;
          case "tool.start":
            instanceActivity.set(instanceId, {
              type: "tool_running",
              name: event.data.call.name,
            });
            rerender();
            break;
          case "tool.done":
            instanceActivity.set(instanceId, null);
            rerender();
            break;
          case "inference.done": {
            const text = event.data.message.content
              .filter(
                (b): b is typeof b & { type: "text" } => b.type === "text",
              )
              .map((b) => b.text)
              .join("");
            instanceStreaming.set(instanceId, "");
            if (text) {
              getMessages(instanceId).push({
                role: "assistant",
                content: text,
                from: `${instanceId}@agent`,
              });
            }
            rerender();
            break;
          }
          case "message.received": {
            const inbound = event.data.message;
            if (inbound.content) {
              getMessages(instanceId).push({
                role: "user",
                content: inbound.content,
                from: inbound.headers.from,
              });
            }
            rerender();
            break;
          }
          case "inference.error":
            hadInferenceError = true;
            instanceStreaming.set(instanceId, "");
            instanceActivity.set(instanceId, null);
            rerender();
            break;
          case "connector.reply":
            instanceStreaming.set(instanceId, "");
            instanceActivity.set(instanceId, null);
            // Only push reply content on error turns. On normal turns
            // inference.done already pushed the assistant message.
            if (hadInferenceError) {
              hadInferenceError = false;
              if (event.data.content) {
                getMessages(instanceId).push({
                  role: "assistant",
                  content: event.data.content,
                  from: `${instanceId}@agent`,
                  isError: true,
                });
              }
            }
            rerender();
            break;
          case "reactor.done":
            instanceStreaming.set(instanceId, "");
            instanceActivity.set(instanceId, null);
            rerender();
            break;
        }
      },
      { eventName: "agent.event" },
    );
    return () => {
      cancelled = true;
      close();
      instanceActivity.set(instanceId, null);
    };
  }, [isRunning, instanceId, tenantId]);

  const stopMut = useMutation({
    ...stopInstanceMutation(tenantId, queryClient),
    onSuccess: () => {
      clearInstanceState(instanceId);
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "instances"],
      });
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents"],
      });
      rerender();
    },
  });

  async function handleSendChat(e: React.FormEvent) {
    e.preventDefault();
    if (!chatInput.trim() || isSending) return;
    const userMessage = chatInput.trim();
    setChatInput("");
    setIsSending(true);
    try {
      await api(
        "POST",
        `/api/tenants/${tenantId}/agents/instances/${instanceId}/messages`,
        { content: userMessage },
      );
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
          Instances
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
              {getMessages(instanceId).length === 0 &&
              !getStreaming(instanceId) ? (
                <p className="text-sm text-muted-foreground">
                  Start a conversation with the agent...
                </p>
              ) : (
                getMessages(instanceId).map((msg, i) => (
                  <div
                    key={i}
                    className={`rounded p-2 text-sm ${
                      msg.isError
                        ? "mr-8 border border-destructive/30 bg-destructive/10 text-destructive"
                        : msg.role === "user"
                          ? "bg-muted ml-8"
                          : "mr-8 bg-primary/10"
                    }`}
                  >
                    {(() => {
                      const label = msg.isError
                        ? "Error"
                        : msg.role === "assistant"
                          ? "Agent"
                          : parseFromHeader(msg.from);
                      if (!label) return null;
                      return <span className="font-medium">{label}: </span>;
                    })()}
                    {msg.content}
                  </div>
                ))
              )}
              {getStreaming(instanceId) && (
                <div className="mr-8 rounded bg-primary/10 p-2 text-sm">
                  <span className="font-medium">Agent:</span>{" "}
                  {getStreaming(instanceId)}
                </div>
              )}
              {(() => {
                const activity = instanceActivity.get(instanceId) ?? null;
                if (activity !== null) {
                  const label =
                    activity.type === "inferring"
                      ? "Thinking..."
                      : activity.type === "tool_call"
                        ? `Calling ${activity.name}...`
                        : `Running ${activity.name}...`;
                  return (
                    <div className="text-sm text-muted-foreground">{label}</div>
                  );
                }
                if (isSending && !getStreaming(instanceId)) {
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
