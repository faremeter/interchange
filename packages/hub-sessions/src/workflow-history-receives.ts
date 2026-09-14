/**
 * Pending-projection rows whose workflow-run pack receive is still running in
 * this process. Such a receive may advance Git after recovery reads it, so
 * recovery must not claim its row. A receive registers before its row is
 * inserted and leaves only after its Git transaction ends, so a listed row that
 * is absent here belongs to a receive whose effect on Git is final. The Hub is
 * the single writer of its workflow-run repositories, so rows left by an
 * earlier process are always final.
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
