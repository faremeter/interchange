import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import {
  createBrowserTransport,
  createRunSession,
  findAwaitingSignal,
  type RunSession,
  type WorkflowRunEvent,
} from "@intx/hub-client";

import { runDetailQuery } from "@/lib/queries/tenants";
import { StatusBadge, RUN_STATUS_VARIANTS } from "@/components/status-badge";
import { RunActivityView } from "@/components/run-activity-view";

const transport = createBrowserTransport();

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

// Polls the run's committed event log and renders it as a live timeline. The
// session stops polling once the run reaches a terminal event, so a settled run
// shows its final history without further reads.
function RunActivity({ tenantId, runId }: { tenantId: string; runId: string }) {
  const [, forceRender] = useState(0);
  const [error, setError] = useState<Error | null>(null);
  const sessionRef = useRef<RunSession | null>(null);

  useEffect(() => {
    const session = createRunSession({
      tenantId,
      runId,
      transport,
      onChange: () => {
        setError(null);
        forceRender((n) => n + 1);
      },
      onError: (err) => setError(err),
    });
    sessionRef.current = session;
    const stop = session.start();
    return () => {
      stop();
      session.destroy();
      sessionRef.current = null;
    };
  }, [tenantId, runId]);

  const session = sessionRef.current;
  const events: WorkflowRunEvent[] = session?.events ?? [];
  const hydrated = session?.hydrated ?? false;
  const terminal = session?.terminal ?? false;
  const awaiting = findAwaitingSignal(events);

  return (
    <RunActivityView
      events={events}
      hydrated={hydrated}
      terminal={terminal}
      awaiting={awaiting}
      error={error}
    />
  );
}

export function TenantRunDetailPage() {
  const { tenantId, runId } = useParams({
    from: "/authed/tenants/$tenantId/workflows/runs/$runId",
  });

  const { data: run, isLoading } = useQuery(runDetailQuery(tenantId, runId));

  if (isLoading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading...</div>;
  }

  if (!run) {
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
          Workflows
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-lg font-semibold">{run.definitionName}</h2>
          <p className="mt-1 font-mono text-xs text-muted-foreground">
            {run.address}
          </p>
        </div>
      </div>

      <div className="mt-6">
        <dl className="overflow-hidden rounded-lg border">
          <Row label="Status">
            <StatusBadge status={run.status} variants={RUN_STATUS_VARIANTS} />
          </Row>
          <Row label="Run ID">
            <span className="font-mono text-xs">{run.id}</span>
          </Row>
          {run.kernelId && (
            <Row label="Kernel ID">
              <span className="font-mono text-xs">{run.kernelId}</span>
            </Row>
          )}
          {run.sidecarId && (
            <Row label="Sidecar ID">
              <span className="font-mono text-xs">{run.sidecarId}</span>
            </Row>
          )}
          <Row label="Created">{new Date(run.createdAt).toLocaleString()}</Row>
          {run.endedAt && (
            <Row label="Ended">{new Date(run.endedAt).toLocaleString()}</Row>
          )}
        </dl>
      </div>

      {/* Key on runId so navigating between runs remounts the activity view
          with a fresh session, rather than briefly rendering the prior run's
          timeline from the retained session ref before the first poll. */}
      <RunActivity key={runId} tenantId={tenantId} runId={runId} />
    </div>
  );
}
