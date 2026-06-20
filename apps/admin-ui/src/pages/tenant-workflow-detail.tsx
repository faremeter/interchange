import { useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  deliverWorkflowSignalMutation,
  deployWorkflowMutation,
  workflowDeploymentsQuery,
  workflowDetailQuery,
  type WorkflowDeployment,
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const APPROVE_SIGNAL_NAME = "approve";

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

function DeploymentStatusBadge({ status }: { status: string }) {
  const variant =
    status === "running" || status === "deployed"
      ? "secondary"
      : status === "error"
        ? "destructive"
        : "outline";
  return <Badge variant={variant}>{status}</Badge>;
}

function isAwaitingSignal(status: string): boolean {
  return status === "running" || status === "deployed";
}

export function TenantWorkflowDetailPage() {
  const { tenantId, workflowId } = useParams({
    from: "/authed/tenants/$tenantId/workflows/$workflowId",
  });
  const queryClient = useQueryClient();

  const { data: workflow, isLoading } = useQuery(
    workflowDetailQuery(tenantId, workflowId),
  );
  const { data: deployments } = useQuery(workflowDeploymentsQuery(tenantId));

  const [launchSource, setLaunchSource] = useState({
    id: "",
    provider: "",
    baseURL: "",
    apiKey: "",
    model: "",
  });

  const [approveTarget, setApproveTarget] = useState<{
    deploymentId: string;
    signalId: string;
  } | null>(null);
  const [approveRunId, setApproveRunId] = useState("");

  const deployMut = useMutation({
    ...deployWorkflowMutation(tenantId, queryClient),
    onSuccess: () => {
      setLaunchSource({
        id: "",
        provider: "",
        baseURL: "",
        apiKey: "",
        model: "",
      });
    },
  });

  const signalMut = useMutation(
    deliverWorkflowSignalMutation(
      tenantId,
      approveTarget?.deploymentId ?? "",
      queryClient,
    ),
  );

  const workflowDeployments = (deployments ?? []).filter(
    (d) => d.definitionAssetId === workflowId,
  );

  function openApprove(deployment: WorkflowDeployment) {
    setApproveTarget({
      deploymentId: deployment.id,
      signalId: crypto.randomUUID(),
    });
    setApproveRunId("");
    signalMut.reset();
  }

  function closeApprove() {
    setApproveTarget(null);
    setApproveRunId("");
  }

  function submitLaunch(e: React.FormEvent) {
    e.preventDefault();
    deployMut.mutate({
      assetId: workflowId,
      sources: [
        {
          id: launchSource.id.trim(),
          provider: launchSource.provider.trim(),
          baseURL: launchSource.baseURL.trim(),
          apiKey: launchSource.apiKey,
          model: launchSource.model.trim(),
        },
      ],
      defaultSource: launchSource.id.trim(),
    });
  }

  function submitApprove(e: React.FormEvent) {
    e.preventDefault();
    if (!approveTarget) return;
    signalMut.mutate(
      {
        runId: approveRunId.trim(),
        signalName: APPROVE_SIGNAL_NAME,
        signalId: approveTarget.signalId,
      },
      { onSuccess: closeApprove },
    );
  }

  const launchReady =
    launchSource.id.trim() !== "" &&
    launchSource.provider.trim() !== "" &&
    launchSource.baseURL.trim() !== "" &&
    launchSource.apiKey !== "" &&
    launchSource.model.trim() !== "";

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!workflow) {
    return <div className="p-4 text-sm text-muted-foreground">Not found.</div>;
  }

  return (
    <div>
      <div className="mb-6">
        <Link
          to="/tenants/$tenantId/workflows"
          params={{ tenantId }}
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-3.5" />
          Workflow Definitions
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">
            {workflow.displayName ?? workflow.name}
          </h2>
          <p className="mt-1 font-mono text-xs text-muted-foreground">
            {workflow.name}
          </p>
        </div>
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Asset ID">
            <span className="font-mono text-xs">{workflow.id}</span>
          </Row>
          <Row label="Created">
            {new Date(workflow.createdAt).toLocaleString()}
          </Row>
          <Row label="Updated">
            {new Date(workflow.updatedAt).toLocaleString()}
          </Row>
        </dl>
      </div>

      {/* Launch Workflow */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Launch Workflow</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Deploys this workflow definition. The step agents launch against the
          inference source you supply below.
        </p>
        <form
          onSubmit={submitLaunch}
          className="mt-4 space-y-3 rounded-lg border p-4"
        >
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="grid gap-1">
              <Label htmlFor="source-id" className="text-xs">
                Source ID
              </Label>
              <Input
                id="source-id"
                value={launchSource.id}
                onChange={(e) =>
                  setLaunchSource((s) => ({ ...s, id: e.target.value }))
                }
                placeholder="e.g. anthropic:claude-sonnet-4-6"
                className="h-8 text-xs"
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="source-provider" className="text-xs">
                Provider
              </Label>
              <Input
                id="source-provider"
                value={launchSource.provider}
                onChange={(e) =>
                  setLaunchSource((s) => ({ ...s, provider: e.target.value }))
                }
                placeholder="e.g. anthropic"
                className="h-8 text-xs"
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="source-model" className="text-xs">
                Model
              </Label>
              <Input
                id="source-model"
                value={launchSource.model}
                onChange={(e) =>
                  setLaunchSource((s) => ({ ...s, model: e.target.value }))
                }
                placeholder="e.g. claude-sonnet-4-6"
                className="h-8 text-xs"
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="source-base-url" className="text-xs">
                Base URL
              </Label>
              <Input
                id="source-base-url"
                value={launchSource.baseURL}
                onChange={(e) =>
                  setLaunchSource((s) => ({ ...s, baseURL: e.target.value }))
                }
                placeholder="https://api.anthropic.com"
                className="h-8 text-xs"
              />
            </div>
            <div className="grid gap-1 sm:col-span-2">
              <Label htmlFor="source-api-key" className="text-xs">
                API Key
              </Label>
              <Input
                id="source-api-key"
                type="password"
                value={launchSource.apiKey}
                onChange={(e) =>
                  setLaunchSource((s) => ({ ...s, apiKey: e.target.value }))
                }
                className="h-8 text-xs"
              />
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="submit"
              size="sm"
              disabled={deployMut.isPending || !launchReady}
            >
              {deployMut.isPending ? "Launching..." : "Launch Workflow"}
            </Button>
            <MutationError error={deployMut.error} />
          </div>
        </form>
      </div>

      {/* Deployments */}
      <div className="mt-8">
        <h3 className="text-sm font-semibold">Deployments</h3>
        {workflowDeployments.length === 0 ? (
          <p className="mt-2 text-xs text-muted-foreground">
            No deployments launched from this definition.
          </p>
        ) : (
          <div className="mt-3 rounded-lg border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Deployment</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead className="w-24" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {workflowDeployments.map((d) => (
                  <TableRow key={d.id}>
                    <TableCell className="font-mono text-xs">{d.id}</TableCell>
                    <TableCell>
                      <DeploymentStatusBadge status={d.status} />
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {new Date(d.createdAt).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      {isAwaitingSignal(d.status) && (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => openApprove(d)}
                        >
                          Approve
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>

      {/* Approve signal dialog */}
      <Dialog
        open={approveTarget !== null}
        onOpenChange={(open) => {
          if (!open) closeApprove();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Approve workflow run</DialogTitle>
          </DialogHeader>
          <form onSubmit={submitApprove} className="grid gap-4">
            <p className="text-xs text-muted-foreground">
              Delivers the &ldquo;{APPROVE_SIGNAL_NAME}&rdquo; signal to the run
              awaiting approval. The run identifier is the run of the deployment
              that paused on the approval step.
            </p>
            <div className="grid gap-2">
              <Label htmlFor="approve-run-id">Run ID</Label>
              <Input
                id="approve-run-id"
                value={approveRunId}
                onChange={(e) => setApproveRunId(e.target.value)}
                required
                autoFocus
              />
            </div>
            <MutationError error={signalMut.error} />
            <DialogFooter>
              <Button
                type="submit"
                disabled={signalMut.isPending || approveRunId.trim() === ""}
              >
                {signalMut.isPending ? "Approving..." : "Approve"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
