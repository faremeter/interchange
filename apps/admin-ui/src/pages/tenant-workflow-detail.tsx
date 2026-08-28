import { useMemo, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";

import { findAwaitingSignal, isTerminalRunEvents } from "@intx/hub-client";

import { MutationError } from "@/components/mutation-error";
import {
  deliverWorkflowSignalMutation,
  deployWorkflowMutation,
  triggerWorkflowRunMutation,
  tenantResolvedModelsQuery,
  workflowDeploymentsQuery,
  workflowDetailQuery,
  workflowRunEventsQuery,
  workflowRunsQuery,
  type WorkflowDeployment,
} from "@/lib/queries/tenants";
import { Badge } from "@/components/ui/badge";
import { RunEventList } from "@/components/run-event-list";
import {
  StatusBadge,
  DEPLOYMENT_STATUS_VARIANTS,
} from "@/components/status-badge";
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
import {
  buildDeployInput,
  isSourceKind,
  launchReady as isLaunchReady,
  SOURCE_KIND_LABELS,
  SOURCE_KINDS,
  type SourceKind,
} from "./tenant-workflow-launch";

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

function isAwaitingSignal(status: string): boolean {
  return status === "running" || status === "deployed";
}

export function WorkflowModelOfferingField({
  discoveryUnavailable,
  offerings,
  value,
  onValueChange,
}: {
  discoveryUnavailable: boolean;
  offerings: readonly { id: string; label: string }[];
  value: string;
  onValueChange: (value: string) => void;
}) {
  if (discoveryUnavailable) {
    return (
      <div className="grid gap-1 sm:col-span-2">
        <Label htmlFor="source-offering-id" className="text-xs">
          Model offering ID
        </Label>
        <Input
          id="source-offering-id"
          value={value}
          onChange={(e) => onValueChange(e.target.value)}
          placeholder="ofr_..."
          className="h-8 font-mono text-xs"
        />
        <p className="text-xs text-muted-foreground">
          Model discovery is unavailable. Enter a known catalog offering ID.
        </p>
      </div>
    );
  }

  return (
    <div className="grid gap-1 sm:col-span-2">
      <Label htmlFor="source-offering" className="text-xs">
        Model offering
      </Label>
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger
          id="source-offering"
          size="sm"
          className="w-full text-xs"
        >
          <SelectValue placeholder="Select a catalog offering" />
        </SelectTrigger>
        <SelectContent>
          {offerings.map((offering) => (
            <SelectItem
              key={offering.id}
              value={offering.id}
              className="text-xs"
            >
              {offering.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
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

  const { data: resolvedModels, isError: resolvedModelsUnavailable } = useQuery(
    tenantResolvedModelsQuery(tenantId),
  );
  const launchOfferings = useMemo(
    () =>
      (resolvedModels ?? []).flatMap((model) =>
        model.offerings.map((offering) => ({
          id: offering.offeringId,
          label: `${model.displayName ?? model.canonicalName} — ${offering.providerName}`,
        })),
      ),
    [resolvedModels],
  );
  const [launchOfferingId, setLaunchOfferingId] = useState("");
  const selectedLaunchOfferingId =
    launchOfferingId || launchOfferings[0]?.id || "";

  // Where this workflow's code is sourced from. The picker builds any
  // `WorkflowDefinitionSource` variant; `assetId` defaults to this workflow's
  // own asset because deploying its own source tree is the common path, but the
  // operator can point at any asset or an external registry. Fields not used by
  // the selected `kind` are ignored when the deploy input is built.
  const initialLaunchDefinition = {
    kind: "asset-source" as SourceKind,
    entry: "",
    registry: "",
    assetId: workflowId,
    commitSha: "",
    packageName: "",
    pin: "",
  };
  const [launchDefinition, setLaunchDefinition] = useState(
    initialLaunchDefinition,
  );

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
      setLaunchOfferingId("");
      setLaunchDefinition(initialLaunchDefinition);
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
    deployMut.mutate(
      buildDeployInput(launchDefinition, selectedLaunchOfferingId),
    );
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

  const launchReady = isLaunchReady(launchDefinition, selectedLaunchOfferingId);

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
          catalog offering selected below.
        </p>
        <form
          onSubmit={submitLaunch}
          className="mt-4 space-y-3 rounded-lg border p-4"
        >
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="grid gap-1">
              <Label htmlFor="definition-kind" className="text-xs">
                Source kind
              </Label>
              <Select
                value={launchDefinition.kind}
                onValueChange={(v) => {
                  if (isSourceKind(v))
                    setLaunchDefinition((d) => ({ ...d, kind: v }));
                }}
              >
                <SelectTrigger
                  id="definition-kind"
                  size="sm"
                  className="w-full text-xs"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {SOURCE_KINDS.map((kind) => (
                    <SelectItem key={kind} value={kind} className="text-xs">
                      {SOURCE_KIND_LABELS[kind]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-1">
              <Label htmlFor="definition-entry" className="text-xs">
                Entry module
              </Label>
              <Input
                id="definition-entry"
                value={launchDefinition.entry}
                onChange={(e) =>
                  setLaunchDefinition((d) => ({ ...d, entry: e.target.value }))
                }
                placeholder="e.g. ./workflow.mjs"
                className="h-8 text-xs"
              />
            </div>
          </div>
          {launchDefinition.kind === "registry" && (
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="grid gap-1">
                <Label htmlFor="definition-registry" className="text-xs">
                  Registry
                </Label>
                <Input
                  id="definition-registry"
                  value={launchDefinition.registry}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({
                      ...d,
                      registry: e.target.value,
                    }))
                  }
                  placeholder="the sidecar registry name"
                  className="h-8 text-xs"
                />
              </div>
              <div className="grid gap-1">
                <Label htmlFor="definition-pin" className="text-xs">
                  Pin
                </Label>
                <Input
                  id="definition-pin"
                  value={launchDefinition.pin}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({ ...d, pin: e.target.value }))
                  }
                  placeholder="e.g. my-workflow@^1.0.0"
                  className="h-8 text-xs"
                />
              </div>
            </div>
          )}
          {launchDefinition.kind === "asset-tarball" && (
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="grid gap-1">
                <Label htmlFor="definition-asset-id" className="text-xs">
                  Asset ID
                </Label>
                <Input
                  id="definition-asset-id"
                  value={launchDefinition.assetId}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({
                      ...d,
                      assetId: e.target.value,
                    }))
                  }
                  placeholder="the source asset's id"
                  className="h-8 text-xs"
                />
              </div>
              <div className="grid gap-1">
                <Label htmlFor="definition-pin" className="text-xs">
                  Pin
                </Label>
                <Input
                  id="definition-pin"
                  value={launchDefinition.pin}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({ ...d, pin: e.target.value }))
                  }
                  placeholder="e.g. my-workflow@^1.0.0"
                  className="h-8 text-xs"
                />
              </div>
            </div>
          )}
          {launchDefinition.kind === "asset-source" && (
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="grid gap-1">
                <Label htmlFor="definition-asset-id" className="text-xs">
                  Asset ID
                </Label>
                <Input
                  id="definition-asset-id"
                  value={launchDefinition.assetId}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({
                      ...d,
                      assetId: e.target.value,
                    }))
                  }
                  placeholder="the source asset's id"
                  className="h-8 text-xs"
                />
              </div>
              <div className="grid gap-1">
                <Label htmlFor="definition-commit" className="text-xs">
                  Commit SHA
                </Label>
                <Input
                  id="definition-commit"
                  value={launchDefinition.commitSha}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({
                      ...d,
                      commitSha: e.target.value,
                    }))
                  }
                  placeholder="the source asset's current commit"
                  className="h-8 text-xs"
                />
              </div>
              <div className="grid gap-1 sm:col-span-2">
                <Label htmlFor="definition-package-name" className="text-xs">
                  Package name (optional)
                </Label>
                <Input
                  id="definition-package-name"
                  value={launchDefinition.packageName}
                  onChange={(e) =>
                    setLaunchDefinition((d) => ({
                      ...d,
                      packageName: e.target.value,
                    }))
                  }
                  placeholder="monorepo member; blank for a single-package tree"
                  className="h-8 text-xs"
                />
              </div>
            </div>
          )}
          <div className="grid gap-3 sm:grid-cols-2">
            <WorkflowModelOfferingField
              discoveryUnavailable={resolvedModelsUnavailable}
              offerings={launchOfferings}
              value={selectedLaunchOfferingId}
              onValueChange={setLaunchOfferingId}
            />
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
                      <StatusBadge
                        status={d.status}
                        variants={DEPLOYMENT_STATUS_VARIANTS}
                      />
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

      <RunEventList events={events} />
    </div>
  );
}
