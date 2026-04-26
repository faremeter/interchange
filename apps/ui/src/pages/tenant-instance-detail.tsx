import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, Paperclip } from "lucide-react";
import { type } from "arktype";
import {
  InferenceEvent,
  type InferenceEvent as ValidInferenceEvent,
} from "@interchange/types/runtime";
import { MailResponse, InferenceTurnResponse } from "@interchange/types";

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

const MailDeliveredEvent = type({
  type: "'mail.delivered'",
  data: {
    "from?": type({
      name: "string | null",
      email: "string",
    }).array(),
    "to?": type({
      name: "string | null",
      email: "string",
    }).array(),
    "subject?": "string | null",
    "sentAt?": "string | null",
    bodyValues: "Record<string, unknown>",
    textBody: type({
      partId: "string",
      type: "string",
    }).array(),
    "htmlBody?": type({
      partId: "string",
      type: "string",
    }).array(),
    "attachments?": type({
      blobId: "string",
      "name?": "string | null",
      type: "string",
      size: "number",
    }).array(),
    headers: "Record<string, string>",
    receivedAt: "string",
  },
});

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

function extractMailText(mail: MailResponse): string {
  const firstTextPart = mail.textBody[0];
  if (!firstTextPart) return "";
  const bodyValue = mail.bodyValues[firstTextPart.partId];
  if (
    typeof bodyValue === "object" &&
    bodyValue !== null &&
    "value" in bodyValue
  ) {
    const v = (bodyValue as { value?: unknown }).value;
    if (typeof v === "string") return v;
  }
  return "";
}

function formatAddress(addr: { name: string | null; email: string }): string {
  return addr.name ?? addr.email;
}

type ChatMessage =
  | {
      kind: "mail";
      role: "user" | "assistant";
      content: string;
      senderLabel: string;
      timestamp: string;
      attachments: MailResponse["attachments"];
      isError?: boolean;
    }
  | {
      kind: "turn";
      content: string;
      timestamp: string;
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

function mailToMessage(
  mail: MailResponse,
  instanceAddress: string,
): ChatMessage {
  const firstSender = mail.from[0];
  const isFromAgent =
    firstSender !== undefined &&
    (firstSender.email.includes(instanceAddress) ||
      mail.direction === "outbound");
  const role: "user" | "assistant" = isFromAgent ? "assistant" : "user";
  const senderLabel = firstSender ? formatAddress(firstSender) : "Unknown";
  const content = extractMailText(mail);

  return {
    kind: "mail",
    role,
    content,
    senderLabel,
    timestamp: mail.receivedAt,
    attachments: mail.attachments,
  };
}

function turnToMessage(turn: InferenceTurnResponse): ChatMessage | null {
  const textParts = turn.parts.filter(
    (p): p is typeof p & { content: string } =>
      p.type === "text" &&
      typeof p.content === "string" &&
      p.content.length > 0,
  );
  if (textParts.length === 0) return null;
  const content = textParts.map((p) => p.content).join("");
  const isError =
    turn.status === "failed" || turn.parts.some((p) => p.type === "error");

  return {
    kind: "turn",
    content,
    timestamp: turn.startedAt,
    ...(isError ? { isError: true } : {}),
  };
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

    void (async () => {
      const [mailRes, turnsRes] = await Promise.all([
        api<{ data: MailResponse[] }>(
          "GET",
          `/api/tenants/${tenantId}/agents/instances/${instanceId}/mail?limit=100`,
        ),
        api<{ data: InferenceTurnResponse[] }>(
          "GET",
          `/api/tenants/${tenantId}/agents/instances/${instanceId}/turns?limit=100`,
        ),
      ]);

      if (cancelled) return;
      if (getMessages(instanceId).length > 0) return;

      const instanceAddress = instance?.address ?? "";

      const mailMessages: ChatMessage[] = mailRes.data.map((m) =>
        mailToMessage(m, instanceAddress),
      );

      const turnMessages: ChatMessage[] = turnsRes.data
        .map(turnToMessage)
        .filter((m): m is ChatMessage => m !== null);

      const all = [...mailMessages, ...turnMessages].sort((a, b) =>
        a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0,
      );

      if (all.length > 0) {
        instanceMessages.set(instanceId, all);
        rerender();
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [isRunning, instanceId, tenantId, instance?.address]);

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

        // Handle mail.delivered before checking InferenceEvent to avoid
        // confusion between the two event namespaces.
        const mailEvent = MailDeliveredEvent(raw);
        if (!(mailEvent instanceof type.errors)) {
          const messages = getMessages(instanceId);
          const already = messages.some(
            (m) => m.timestamp === mailEvent.data.receivedAt,
          );
          if (!already) {
            const instanceAddress = instance?.address ?? "";
            const firstSender = mailEvent.data.from?.[0];
            const isFromAgent =
              firstSender !== undefined &&
              firstSender.email.includes(instanceAddress);
            const role: "user" | "assistant" = isFromAgent
              ? "assistant"
              : "user";
            const senderLabel = firstSender
              ? formatAddress(firstSender)
              : "Unknown";

            const firstTextPart = mailEvent.data.textBody[0];
            let content = "";
            if (firstTextPart) {
              const bodyValue = mailEvent.data.bodyValues[firstTextPart.partId];
              if (
                typeof bodyValue === "object" &&
                bodyValue !== null &&
                "value" in bodyValue
              ) {
                const v = (bodyValue as { value?: unknown }).value;
                if (typeof v === "string") content = v;
              }
            }

            messages.push({
              kind: "mail",
              role,
              content,
              senderLabel,
              timestamp: mailEvent.data.receivedAt,
              attachments: (mailEvent.data.attachments ?? []).map((att) => ({
                blobId: att.blobId,
                name: att.name ?? null,
                type: att.type,
                size: att.size,
              })),
            });
            rerender();
          }
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
            instanceStreaming.set(instanceId, "");
            instanceActivity.set(instanceId, null);
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
            // inference.done already cleared the streaming buffer.
            if (hadInferenceError) {
              hadInferenceError = false;
              if (event.data.content) {
                getMessages(instanceId).push({
                  kind: "turn",
                  content: event.data.content,
                  timestamp: new Date().toISOString(),
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
  }, [isRunning, instanceId, tenantId, instance?.address]);

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
        `/api/tenants/${tenantId}/agents/instances/${instanceId}/mail`,
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
                getMessages(instanceId).map((msg, i) => {
                  const isUser = msg.kind === "mail" && msg.role === "user";
                  const isAssistantTurn = msg.kind === "turn";
                  const isAssistantMail =
                    msg.kind === "mail" && msg.role === "assistant";
                  const isAssistant = isAssistantTurn || isAssistantMail;

                  const label = msg.isError
                    ? "Error"
                    : isAssistant
                      ? "Agent"
                      : msg.kind === "mail"
                        ? parseFromHeader(msg.senderLabel)
                        : null;

                  return (
                    <div
                      key={i}
                      className={`rounded p-2 text-sm ${
                        msg.isError
                          ? "mr-8 border border-destructive/30 bg-destructive/10 text-destructive"
                          : isUser
                            ? "bg-muted ml-8"
                            : "mr-8 bg-primary/10"
                      }`}
                    >
                      {label ? (
                        <span className="font-medium">{label}: </span>
                      ) : null}
                      {msg.content}
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
