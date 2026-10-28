// The supervisor's side of a new child's startup handshake.
//
// A child reports the definition it verified as its first upstream frame,
// then announces `ready` once it has applied its authority and resumed its
// runs. The spawn path and the recycle path both read the same two frames
// off the child's control-receive iterator.

import { type } from "arktype";

import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";

import type { ControlPayload } from "../ipc/control-payloads";

/** What a child's `definition` frame establishes once it checks out. */
export interface ChildDefinitionReport {
  readonly childPid: number;
  readonly projection: WorkflowProjectionDefinition;
}

/**
 * Read the child's `definition` frame and check its projection against the
 * approved hash. Rejects with the child's own reason when it could not load
 * or verify its closure, and rejects when the reported projection does not
 * hash to `approvedHash`: the child runs author code, so its own check is not
 * enough for the supervisor to trust what it reports.
 */
export async function waitForDefinition(
  iter: AsyncGenerator<ControlPayload, void, void>,
  approvedHash: string,
): Promise<ChildDefinitionReport> {
  // Explicit `next()` rather than `for await`, so the generator is not
  // finalized: `waitForReady` and then the supervisor's upstream-control
  // pump keep reading the same iterator.
  const next = await iter.next();
  if (next.done === true) {
    throw new Error(
      "workflow-host supervisor: control channel ended before the child reported its definition",
    );
  }
  // The receiver's bootstrap admits only a `definition` frame first.
  const payload = next.value;
  if (payload.type !== "definition") {
    throw new Error(
      `workflow-host supervisor: expected the child's definition frame first, got ${payload.type}`,
    );
  }
  const { result } = payload.data;
  if (!result.ok) {
    throw new Error(
      `workflow-host supervisor: the workflow child could not load its definition: ${result.error}`,
    );
  }
  const projection = WorkflowProjectionDefinition(result.projection);
  if (projection instanceof type.errors) {
    throw new Error(
      `workflow-host supervisor: the workflow child reported a malformed definition: ${projection.summary}`,
    );
  }
  // Hash the validated value, the one the host goes on to use.
  const reportedHash = await computeWireDefinitionHash(projection);
  if (reportedHash !== approvedHash) {
    throw new Error(
      `workflow-host supervisor: the workflow child reported a definition hashing to ${reportedHash}, not the approved ${approvedHash}`,
    );
  }
  return { childPid: payload.data.childPid, projection };
}

/**
 * Iterate the control-receive iterator until the child's `ready` frame
 * arrives. Upstream payloads other than `ready` (e.g. `recycle.request`)
 * appear after `ready`; the supervisor's `pumpUpstreamControl` consumes them
 * off the same iterator once the handshake is done.
 */
export async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
): Promise<void> {
  // Explicit `next()` rather than `for await ... return`, so the generator
  // is NOT finalized via `iter.return()` when ready lands: a finalized
  // generator would yield `{done: true}` to the pump and silently drop the
  // child's later upstream frames.
  while (true) {
    const next = await iter.next();
    if (next.done === true) {
      throw new Error(
        "workflow-host supervisor: control channel ended before child emitted ready",
      );
    }
    if (next.value.type === "ready") return;
    // The child sends nothing else before `ready`, but the receiver already
    // verified the frame's signature, so a stray one is dropped rather than
    // crashing the iterator.
  }
}
