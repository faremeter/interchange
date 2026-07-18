import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";

import { parseApprovalSnapshot } from "@/lib/approval-snapshot";

/**
 * Renders an approval's tool snapshot in the same collapsible idiom the
 * live-run tool view uses, adapted for a pending call: it shows the tool name
 * and description with no execution result (the call has not run), and expands
 * to the arguments and the tool's input schema.
 */
export function ApprovalToolSnapshotView({
  toolDefinition,
  toolArguments,
}: {
  toolDefinition: Record<string, unknown>;
  toolArguments: Record<string, unknown>;
}) {
  const [expanded, setExpanded] = useState(true);
  const snapshot = parseApprovalSnapshot(toolDefinition, toolArguments);

  if (!snapshot.ok) {
    return (
      <div className="rounded border border-destructive/50 bg-destructive/10 px-2 py-1 text-xs text-destructive">
        Malformed tool snapshot: missing the tool name or description.
      </div>
    );
  }

  return (
    <div className="rounded border border-border/50 bg-muted/30 text-xs font-mono">
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
        <span className="text-primary">{snapshot.name}</span>
        <span className="truncate text-muted-foreground">
          {snapshot.description}
        </span>
      </button>
      {expanded && (
        <div className="border-t border-border/50">
          <div className="border-b border-border/30 px-2 py-1">
            <div className="mb-1 text-muted-foreground">arguments</div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(snapshot.arguments, null, 2)}
            </pre>
          </div>
          <div className="px-2 py-1">
            <div className="mb-1 text-muted-foreground">input schema</div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(snapshot.inputSchema, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
