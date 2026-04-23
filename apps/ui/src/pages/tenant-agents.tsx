import { useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus } from "lucide-react";

import { TenantNav } from "@/components/tenant-nav";
import { MutationError } from "@/components/mutation-error";
import {
  createAgentMutation,
  tenantAgentsQuery,
  type AgentResponse,
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

function StatusBadge({ status }: { status: AgentResponse["status"] }) {
  const variant = status === "deployed" ? "secondary" : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function AgentRow({
  agent,
  tenantId,
}: {
  agent: {
    id: string;
    name: string;
    description: string | null;
    status: AgentResponse["status"];
    currentVersion: string;
    createdAt: string;
  };
  tenantId: string;
}) {
  const navigate = useNavigate();

  return (
    <TableRow
      className="cursor-pointer"
      onClick={() =>
        navigate({
          to: "/tenants/$tenantId/agents/$agentId",
          params: { tenantId, agentId: agent.id },
        })
      }
    >
      <TableCell>
        <div className="font-medium">{agent.name}</div>
        {agent.description && (
          <div className="text-xs text-muted-foreground">
            {agent.description}
          </div>
        )}
      </TableCell>
      <TableCell>
        <StatusBadge status={agent.status} />
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">
        v{agent.currentVersion}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {new Date(agent.createdAt).toLocaleDateString()}
      </TableCell>
    </TableRow>
  );
}

export function TenantAgentsPage() {
  const { tenantId } = useParams({ strict: false }) as { tenantId: string };
  const queryClient = useQueryClient();
  const { data: agents, isLoading } = useQuery(tenantAgentsQuery(tenantId));

  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createDescription, setCreateDescription] = useState("");
  const [createSystemPrompt, setCreateSystemPrompt] = useState("");

  function resetCreateForm() {
    setCreateName("");
    setCreateDescription("");
    setCreateSystemPrompt("");
  }

  const createMut = useMutation({
    ...createAgentMutation(tenantId, queryClient),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: ["tenants", tenantId, "agents"],
      });
      setCreateOpen(false);
      resetCreateForm();
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
              </TableRow>
            </TableHeader>
            <TableBody>
              {agents?.map((a) => (
                <AgentRow key={a.id} agent={a} tenantId={tenantId} />
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
            <DialogTitle>Create Agent</DialogTitle>
          </DialogHeader>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const body: {
                name: string;
                description?: string;
                systemPrompt?: string;
              } = { name: createName.trim() };
              if (createDescription.trim())
                body.description = createDescription.trim();
              if (createSystemPrompt.trim())
                body.systemPrompt = createSystemPrompt.trim();
              createMut.mutate(body);
            }}
            className="grid gap-4"
          >
            <div className="grid gap-2">
              <Label htmlFor="agent-name">Name</Label>
              <Input
                id="agent-name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                required
                autoFocus
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="agent-description">Description</Label>
              <Input
                id="agent-description"
                value={createDescription}
                onChange={(e) => setCreateDescription(e.target.value)}
                placeholder="Optional"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="agent-prompt">System Prompt</Label>
              <Textarea
                id="agent-prompt"
                value={createSystemPrompt}
                onChange={(e) => setCreateSystemPrompt(e.target.value)}
                placeholder="Optional"
                rows={4}
              />
            </div>
            <MutationError error={createMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={createMut.isPending || !createName.trim()}
              >
                {createMut.isPending ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
