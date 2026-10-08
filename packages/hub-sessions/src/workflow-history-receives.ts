/**
 * Pending-projection rows whose workflow-run pack receive is still running in
 * this process. A receive may advance Git after recovery reads it, so recovery
 * must not claim its row; one that has ended (or never started here) has final
 * Git effects, since the Hub is the single writer of these repositories.
 */
export function createWorkflowHistoryReceiveTracker() {
  const inFlight = new Set<string>();
  return {
    begin(id: string): void {
      inFlight.add(id);
    },
    end(id: string): void {
      inFlight.delete(id);
    },
    isInFlight(id: string): boolean {
      return inFlight.has(id);
    },
  };
}

export type WorkflowHistoryReceiveTracker = ReturnType<
  typeof createWorkflowHistoryReceiveTracker
>;
