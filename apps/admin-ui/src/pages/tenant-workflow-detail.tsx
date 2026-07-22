import { useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";

import { MutationError } from "@/components/mutation-error";
import {
  deliverWorkflowSignalMutation,
  deployWorkflowMutation,
  findAwaitingSignal,
  isTerminalRunEvents,
  triggerWorkflowRunMutation,
  workflowDeploymentsQuery,
  workflowDetailQuery,
  workflowRunEventsQuery,
  workflowRunsQuery,
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
import { Textarea } from "@/components/ui/textarea";

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

  const [openDeploymentId, setOpenDeploymentId] = useState<string | null>(null);

  const [approveTarget, setApproveTarget] = useState<{
    deploymentId: string;
    signalId: string;
    signalName: string;
    runIdLocked: boolean;
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

  function openManualApprove(deployment: WorkflowDeployment) {
    setApproveTarget({
      deploymentId: deployment.id,
      signalId: crypto.randomUUID(),
      signalName: APPROVE_SIGNAL_NAME,
      runIdLocked: false,
    });
    setApproveRunId("");
    signalMut.reset();
  }

  function openDiscoveredApprove(
    deploymentId: string,
    runId: string,
    signalName: string,
  ) {
    setApproveTarget({
      deploymentId,
      signalId: crypto.randomUUID(),
      signalName,
      runIdLocked: true,
    });
    setApproveRunId(runId);
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
        signalName: approveTarget.signalName,
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
                placeholder="e.g. anthropic:claude-sonnet-5"
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
                placeholder="e.g. claude-sonnet-5"
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
                  <TableHead className="w-44" />
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
                      <div className="flex items-center justify-end gap-2">
                        <Button
                          size="sm"
                          variant={
                            openDeploymentId === d.id ? "secondary" : "outline"
                          }
                          onClick={() =>
                            setOpenDeploymentId((cur) =>
                              cur === d.id ? null : d.id,
                            )
                          }
                        >
                          {openDeploymentId === d.id ? "Hide runs" : "Runs"}
                        </Button>
                        {isAwaitingSignal(d.status) && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => openManualApprove(d)}
                          >
                            Approve
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>

      {/* Run console for the selected deployment */}
      {openDeploymentId !== null && (
        <DeploymentRunConsole
          key={openDeploymentId}
          tenantId={tenantId}
          deploymentId={openDeploymentId}
          onApprove={openDiscoveredApprove}
        />
      )}

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
              Delivers the &ldquo;
              {approveTarget?.signalName ?? APPROVE_SIGNAL_NAME}
              &rdquo; signal to the run awaiting approval.{" "}
              {approveTarget?.runIdLocked
                ? "The run identifier was discovered from the run's event stream."
                : "Enter the run identifier of the run that paused on the approval step."}
            </p>
            <div className="grid gap-2">
              <Label htmlFor="approve-run-id">Run ID</Label>
              <Input
                id="approve-run-id"
                value={approveRunId}
                onChange={(e) => setApproveRunId(e.target.value)}
                readOnly={approveTarget?.runIdLocked ?? false}
                className="font-mono text-xs"
                required
                autoFocus={!(approveTarget?.runIdLocked ?? false)}
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

function DeploymentRunConsole({
  tenantId,
  deploymentId,
  onApprove,
}: {
  tenantId: string;
  deploymentId: string;
  onApprove: (deploymentId: string, runId: string, signalName: string) => void;
}) {
  const queryClient = useQueryClient();
  const [triggerContent, setTriggerContent] = useState("");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const { data: runIds, error: runsError } = useQuery(
    workflowRunsQuery(tenantId, deploymentId),
  );

  const triggerMut = useMutation({
    ...triggerWorkflowRunMutation(tenantId, deploymentId, queryClient),
    onSuccess: () => setTriggerContent(""),
  });

  function submitTrigger(e: React.FormEvent) {
    e.preventDefault();
    triggerMut.mutate({ content: triggerContent });
  }

  const runs = runIds ?? [];

  return (
    <div className="mt-6 rounded-lg border bg-muted/20 p-4">
      <h4 className="font-mono text-xs font-semibold">{deploymentId}</h4>

      <form onSubmit={submitTrigger} className="mt-3 grid gap-2">
        <Label htmlFor="trigger-content" className="text-xs">
          Trigger message
        </Label>
        <Textarea
          id="trigger-content"
          value={triggerContent}
          onChange={(e) => setTriggerContent(e.target.value)}
          placeholder="The message that starts a run for this deployment"
          className="min-h-20 text-xs"
        />
        <div className="flex items-center gap-2">
          <Button
            type="submit"
            size="sm"
            disabled={triggerMut.isPending || triggerContent.trim() === ""}
          >
            {triggerMut.isPending ? "Starting..." : "Start run"}
          </Button>
          <MutationError error={triggerMut.error} />
        </div>
        {triggerMut.data && (
          <p className="text-xs text-muted-foreground">
            Triggered message{" "}
            <span className="font-mono">{triggerMut.data.messageId}</span> to{" "}
            <span className="font-mono">{triggerMut.data.address}</span>
          </p>
        )}
      </form>

      <div className="mt-4">
        <p className="text-xs font-semibold text-muted-foreground">Runs</p>
        <MutationError error={runsError} />
        {runs.length === 0 ? (
          <p className="mt-1 text-xs text-muted-foreground">
            No runs yet for this deployment.
          </p>
        ) : (
          <div className="mt-2 flex flex-wrap gap-2">
            {runs.map((runId) => (
              <Button
                key={runId}
                size="sm"
                variant={selectedRunId === runId ? "secondary" : "outline"}
                className="font-mono text-xs"
                onClick={() =>
                  setSelectedRunId((cur) => (cur === runId ? null : runId))
                }
              >
                {runId}
              </Button>
            ))}
          </div>
        )}
      </div>

      {selectedRunId !== null && (
        <RunEventTimeline
          key={selectedRunId}
          tenantId={tenantId}
          deploymentId={deploymentId}
          runId={selectedRunId}
          onApprove={onApprove}
        />
      )}
    </div>
  );
}

function RunEventTimeline({
  tenantId,
  deploymentId,
  runId,
  onApprove,
}: {
  tenantId: string;
  deploymentId: string;
  runId: string;
  onApprove: (deploymentId: string, runId: string, signalName: string) => void;
}) {
  const { data, error, isLoading } = useQuery(
    workflowRunEventsQuery(tenantId, deploymentId, runId),
  );

  if (error) {
    return (
      <div className="mt-4">
        <MutationError error={error} />
      </div>
    );
  }

  if (isLoading || !data) {
    return (
      <p className="mt-4 text-xs text-muted-foreground">Loading events...</p>
    );
  }

  const events = data.events;
  const terminal = isTerminalRunEvents(events);
  const awaiting = findAwaitingSignal(events);

  return (
    <div className="mt-4 rounded-lg border bg-background p-3">
      <div className="flex items-center justify-between">
        <p className="text-xs font-semibold">
          Run <span className="font-mono">{data.runId}</span>
        </p>
        <Badge variant={terminal ? "outline" : "secondary"}>
          {terminal ? "terminal" : "live"}
        </Badge>
      </div>

      {awaiting && (
        <div className="mt-3 flex items-center justify-between gap-2 rounded-md border border-dashed p-2">
          <p className="text-xs text-muted-foreground">
            Awaiting signal{" "}
            <span className="font-mono">{awaiting.signalName}</span>
          </p>
          <Button
            size="sm"
            onClick={() =>
              onApprove(deploymentId, data.runId, awaiting.signalName)
            }
          >
            Approve
          </Button>
        </div>
      )}

      {events.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">
          No events recorded yet.
        </p>
      ) : (
        <ol className="mt-2 space-y-1">
          {events.map((event) => (
            <li
              key={event.seq}
              className="grid grid-cols-[2.5rem_1fr] gap-2 text-xs"
            >
              <span className="font-mono text-muted-foreground">
                {event.seq}
              </span>
              <span>
                <span className="font-medium">{event.type}</span>
                {Object.keys(event.body).length > 0 && (
                  <span className="ml-2 font-mono text-muted-foreground">
                    {JSON.stringify(event.body)}
                  </span>
                )}
              </span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
