// Per-deployment supervisor.
//
// The supervisor is the host-side object that owns one workflow-process
// child for the lifetime of an active deployment: an in-host object,
// not a separate OS process. The host process holds one supervisor
// instance per deployment.
//
// Spawn lifecycle:
//   1. Mint a fresh `channelId` (16 bytes hex).
//   2. Mint a fresh 32-byte HMAC key for the event channel.
//   3. Mint a fresh Ed25519 keypair for the control channel (the
//      "IPC signing key" -- orthogonal to the supervisor's principal-
//      signing key the host's `signAsPrincipal` callback wraps).
//   4. Build a spawn-time env carrying only:
//        - `IPC_CHANNEL_ID`
//        - `IPC_HMAC_KEY` (hex)
//        - `HOST_PUBKEY` (the IPC keypair's 32-byte public key, hex;
//          NEVER the private key, NEVER the principal-signing key)
//      plus the substrate-config keys the host injected.
//   5. Invoke `bindings.subprocessSpawner` with the binary path and
//      env. The spawner returns a handle exposing the control
//      channel writer/reader and the event channel reader.
//   6. Wire the control-channel sender (Ed25519-signed by the IPC
//      private key) and the event-channel receiver (HMAC-verified).
//   7. Wait for the child's `ready` frame on the control channel;
//      hold any inbound mail in the supervisor's buffer until then.
//   8. Register the deployment's mail address via the mail bus.
//   9. Forward inbound mail to the child via `trigger.fire` frames.
//
// The supervisor's `Bun.spawn` is invoked via the injected
// `bindings.subprocessSpawner` callback so tests stub it. The
// supervisor spawns the binary and does not depend on the
// `runWorkflowChild` body.
//
// CancelRequested signing:
//   Every CancelRequested origin -- `self`, `supervisor-drain`,
//   `supervisor-operator`, `hub-admin` -- flows through the same
//   supervisor-signed path via `commitCancelRequested`. The `self`-
//   origin case is the workflow-process forwarding its stated reason
//   over the control IPC; the supervisor wraps it into a signed
//   event without consulting the child for the signature.

import { getLogger } from "@intx/log";

import { generateKeyPair } from "@intx/crypto-node";
import type { CancelOrigin } from "@intx/workflow";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  receiveEventChannel,
  type ControlChannelSender,
  type ControlPayload,
  type EventPayload,
} from "../ipc/index";

import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import { commitRunEvent } from "./run-event-signing";
import type {
  RecordRunEvent,
  SubprocessHandle,
  SupervisorDeployFrame,
  WorkflowSupervisorBindings,
} from "./types";

const logger = getLogger(["workflow-host", "supervisor"]);

/**
 * Public surface returned by `createWorkflowSupervisor`. Each method
 * advances the supervisor through one lifecycle transition; the
 * supervisor's internal state is encapsulated.
 */
export interface WorkflowSupervisor {
  /**
   * Single ingress for `agent.deploy` frames. The supervisor owns
   * the routing decision between trivial (1-step) and multi-step
   * workflows -- the host hands the frame off and never re-decides.
   *
   * Trivial branch (Option Z, locked):
   *   - Calls `bindings.trivialLaunch(frame)` directly.
   *   - Does NOT open an IPC channel.
   *   - Does NOT spawn a workflow-process child.
   *   - Does NOT emit any workflow-run event. `signAsPrincipal`
   *     is not invoked; for a trivial deploy there is no
   *     workflow-process child to cancel, so cancellation stays
   *     session-destroy at the host's layer.
   *   - Does NOT assemble a `credentialsSnapshot`; the trivial
   *     branch leaves `getCredentialsSnapshot()` returning `null`.
   *
   * Multi-step branch (`steps.length >= 2`):
   *   - Provisions per-step `agent-state` repos, mints keys,
   *     spawns the workflow-process child via `subprocessSpawner`,
   *     registers the deployment's mail address, waits for the
   *     child's `ready` frame, and assembles the
   *     `credentialsSnapshot`. This is the body of `spawn(opts)`,
   *     which is the multi-step branch's worker today.
   *
   * The `agent.deploy` wire frame today carries only a
   * `HarnessConfig` (no workflow definition); every frame is
   * therefore trivial. The supervisor codifies the seam so a frame-
   * format extension that carries a `WorkflowDefinition` is a pure
   * data-shape change.
   */
  deploy(frame: SupervisorDeployFrame): Promise<void>;
  /**
   * Spawn the workflow-process child, complete the IPC handshake,
   * assemble the credentialsSnapshot, register the deployment's mail
   * address, and begin forwarding inbound mail. Resolves once the
   * child's `ready` frame has been received and credentials have
   * been pushed.
   */
  spawn(opts: SpawnOpts): Promise<SpawnResult>;
  /**
   * Sign and commit a CancelRequested event under the named origin.
   * Used by the host directly for `supervisor-operator` and `hub-
   * admin` origins; the `self` origin is invoked indirectly by the
   * supervisor when the child requests cancellation over the
   * control IPC.
   */
  requestCancel(opts: CancelRequestOpts): Promise<CancelCommitInfo>;
  /**
   * Tear the deployment down: unregister the mail address, kill the
   * child, dispose subscriptions, await child exit. Idempotent.
   */
  shutdown(): Promise<void>;
  /**
   * Drain primitives and wait for in-flight work to settle. Stub in
   * this commit; the full implementation lands with the drain
   * controller in a later commit.
   */
  drain(opts: DrainOpts): Promise<void>;
  /**
   * Recycle the child: drain -> kill -> respawn with a fresh
   * channelId. Stub in this commit; the full implementation lands
   * with recycle in a later commit.
   */
  recycle(opts: RecycleOpts): Promise<void>;
  /**
   * Current snapshot of the credentials pushed to the child. Surfaced
   * so the host can audit the per-step contentHash without
   * round-tripping the substrate. Returns `null` before spawn.
   */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
}

export type SpawnOpts = {
  /** Step ids in this deployment's `stepOrder` for credentials assembly. */
  stepOrder: readonly string[];
  /** Content hash of the deployment's workflow definition. */
  definitionHash: string;
  /**
   * Callback the supervisor invokes for each verified InferenceEvent
   * the child publishes. Mirrors the existing `agent.event` event
   * sink the host exposes; the supervisor is the in-host translator.
   */
  onInferenceEvent: (event: EventPayload) => void;
};

export type SpawnResult = {
  /** Child process pid. */
  pid: number;
  /** IPC channelId minted for this spawn. */
  channelId: string;
  /** Initial credentials snapshot pushed to the child. */
  credentialsSnapshot: CredentialsSnapshot;
};

export type CancelRequestOpts = {
  runId: string;
  origin: CancelOrigin;
  reason: string;
  /** ISO-8601 commit timestamp. */
  at: string;
};

export type CancelCommitInfo = {
  commitSha: string;
  seq: number;
};

export type DrainOpts = {
  deadlineMs: number;
};

export type RecycleOpts = {
  reason: string;
};

/**
 * Construct a per-deployment supervisor. All host-specific
 * dependencies are pulled in via `bindings`; nothing in the
 * supervisor reaches into `process.env` or a singleton.
 */
export function createWorkflowSupervisor(
  bindings: WorkflowSupervisorBindings,
): WorkflowSupervisor {
  let state: SupervisorState = { phase: "idle" };
  /**
   * Buffer for inbound mail that arrives after `subscribeMailForAddress`
   * fires but before the child signals `ready`. Once `ready` lands,
   * the supervisor drains this buffer into the control channel as
   * `trigger.fire` frames; subsequent inbound mail bypasses the
   * buffer.
   */
  const mailBuffer: Uint8Array[] = [];

  async function deploy(frame: SupervisorDeployFrame): Promise<void> {
    // The `agent.deploy` wire frame currently carries only a
    // `HarnessConfig`, which is the trivial-workflow shape (a single
    // step derived from the agent's harness). The branching seam
    // exists for the multi-step extension; for every frame today
    // the supervisor calls trivialLaunch unchanged.
    //
    // Trivial-branch invariants:
    //   - No IPC opens, no child spawn, no mail-bus registration.
    //   - credentialsSnapshot is not assembled; the multi-step
    //     branch owns it. `getCredentialsSnapshot()` continues to
    //     return null on the trivial path.
    //   - Run-lifecycle events are committed inline from the
    //     supervisor process via `signAsPrincipal`. The supervisor
    //     hands `recordRunEvent` to the host's `trivialLaunch`; the
    //     host calls it from its per-message reactor / harness
    //     lifecycle moments (`message.run.started` /
    //     `message.run.ended`) so the on-disk event chain matches
    //     the multi-step branch byte-for-byte.
    const recordRunEvent: RecordRunEvent = (event) =>
      commitRunEvent({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        ref: bindings.workflowRunRef,
        deploymentId: bindings.deploymentId,
        event,
        signAsPrincipal: bindings.signAsPrincipal,
      });
    await bindings.trivialLaunch({
      agentAddress: frame.agentAddress,
      agentId: frame.agentId,
      config: frame.config,
      hubPublicKey: frame.hubPublicKey,
      recordRunEvent,
    });
  }

  async function spawn(opts: SpawnOpts): Promise<SpawnResult> {
    if (state.phase !== "idle") {
      throw new Error(
        `supervisor: spawn called in phase ${state.phase}; expected idle`,
      );
    }
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    const ipcKeypair = await (bindings.ipcKeyPairFactory ?? generateKeyPair)();
    const env: Record<string, string> = {
      ...bindings.substrateEnv,
      IPC_CHANNEL_ID: channelId,
      IPC_HMAC_KEY: bytesToHex(hmacKey),
      HOST_PUBKEY: bytesToHex(ipcKeypair.publicKey),
      DEPLOYMENT_ID: bindings.deploymentId,
      DEFINITION_HASH: opts.definitionHash,
      MAILBOX_ADDRESS: bindings.deploymentMailAddress,
    };

    const handle = bindings.subprocessSpawner({
      binaryPath: bindings.binaryPath,
      env,
    });

    const controlSender = createControlChannelSender({
      privateKeySeed: ipcKeypair.privateKey,
      channelId,
      writer: handle.controlWriter,
    });

    // Defer the control-receive iterator construction so the child
    // has a chance to write `ready` before the supervisor starts
    // pumping. The receiver's onCrash callback is wired into the
    // supervisor's process-kill path so a frame violation tears the
    // child down.
    const onCrash = (reason: string): void => {
      logger.error`workflow-process control channel crash: {reason}`;
      void shutdownInternal({ reason });
    };

    const controlIncoming = receiveControlChannel({
      publicKey: ipcKeypair.publicKey,
      channelId,
      reader: handle.controlReader,
      onCrash,
    });

    const readyPromise = waitForReady(controlIncoming, opts.onInferenceEvent);

    // Wire the event channel receiver. The supervisor is the verifier;
    // an HMAC/seq/channelId violation calls `onCrash` and the child
    // is killed.
    const eventIter = receiveEventChannel({
      hmacKey,
      channelId,
      reader: handle.eventReader,
      onCrash: (reason) => {
        logger.error`workflow-process event channel crash: {reason}`;
        void shutdownInternal({ reason });
      },
    });
    const eventPump = pumpEvents(eventIter, opts.onInferenceEvent);

    state = {
      phase: "starting",
      handle,
      controlSender,
      channelId,
      eventPump,
      onInferenceEvent: opts.onInferenceEvent,
      mailUnsubscribe: null,
      credentialsSnapshot: null,
    };

    const credentialsSnapshot = await assembleCredentialsSnapshot({
      repoStore: bindings.repoStore,
      principal: bindings.readPrincipal,
      stepOrder: opts.stepOrder,
      deploymentId: bindings.deploymentId,
      deriveStepAddress: bindings.deriveStepAddress,
      ...(bindings.deriveStepRepoId !== undefined
        ? { deriveStepRepoId: bindings.deriveStepRepoId }
        : {}),
    });
    state.credentialsSnapshot = credentialsSnapshot;

    bindings.mailBus.registerAddress(bindings.deploymentMailAddress);
    const mailUnsubscribe = bindings.mailBus.subscribeMailForAddress(
      bindings.deploymentMailAddress,
      (rawMessage) => {
        // While starting, buffer; once ready, forward immediately.
        if (state.phase === "starting") {
          mailBuffer.push(rawMessage);
          return;
        }
        if (state.phase === "running") {
          const sender = state.controlSender;
          void forwardMail(sender, rawMessage).catch((cause) => {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.error`forwardMail failed: ${message}`;
          });
          return;
        }
        // In any other phase (shutting down, stopped), drop -- the
        // host's higher-level lifecycle is already tearing the
        // deployment down.
      },
    );
    state.mailUnsubscribe = mailUnsubscribe;

    const readyInfo = await readyPromise;

    // Drain the buffered mail before transitioning to running. The
    // ordering matters: any inbound mail that landed during
    // `starting` must hit the child in arrival order before the
    // first post-ready message.
    state = {
      phase: "running",
      handle,
      controlSender,
      channelId,
      eventPump,
      onInferenceEvent: opts.onInferenceEvent,
      mailUnsubscribe,
      credentialsSnapshot,
    };
    while (mailBuffer.length > 0) {
      const message = mailBuffer.shift();
      if (message === undefined) break;
      await forwardMail(controlSender, message);
    }

    return {
      pid: readyInfo.childPid,
      channelId,
      credentialsSnapshot,
    };
  }

  async function requestCancel(
    opts: CancelRequestOpts,
  ): Promise<CancelCommitInfo> {
    const result = await commitCancelRequested({
      substrate: bindings.repoStore,
      repoId: bindings.workflowRunRepoId,
      ref: bindings.workflowRunRef,
      deploymentId: bindings.deploymentId,
      runId: opts.runId,
      origin: opts.origin,
      reason: opts.reason,
      at: opts.at,
      signAsPrincipal: bindings.signAsPrincipal,
    });
    return { commitSha: result.commitSha, seq: result.seq };
  }

  async function shutdown(): Promise<void> {
    await shutdownInternal({ reason: "shutdown requested" });
  }

  async function shutdownInternal(opts: { reason: string }): Promise<void> {
    if (state.phase === "idle" || state.phase === "stopped") return;
    const prior = state;
    state = { phase: "stopping" };
    if (prior.phase === "starting" || prior.phase === "running") {
      if (prior.mailUnsubscribe !== null) prior.mailUnsubscribe();
      try {
        bindings.mailBus.unregisterAddress(bindings.deploymentMailAddress);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`mail bus unregisterAddress threw: ${message}`;
      }
      prior.handle.kill();
      await prior.handle.exited.catch(() => {
        /* swallowed: the host has already been told the deployment is
           coming down; an error surfaced from the spawner is the
           process exiting with a non-zero code, which is what the
           shutdown path expects. */
      });
      await prior.eventPump.catch(() => {
        /* swallowed for the same reason as above. */
      });
    }
    state = { phase: "stopped" };
    logger.info`supervisor shutdown complete (${opts.reason})`;
  }

  function drain(_opts: DrainOpts): Promise<void> {
    // Stub for this commit; the drain controller lands separately.
    // The supervisor exposes the seam now so callers binding the
    // public surface compile against the eventual shape.
    return Promise.reject(
      new Error(
        "workflow-host supervisor: drain() is a stub; full implementation lands with the drain commit",
      ),
    );
  }

  function recycle(_opts: RecycleOpts): Promise<void> {
    // Stub for this commit; the recycle flow lands separately.
    return Promise.reject(
      new Error(
        "workflow-host supervisor: recycle() is a stub; full implementation lands with the recycle commit",
      ),
    );
  }

  function getCredentialsSnapshot(): CredentialsSnapshot | null {
    if (state.phase === "starting" || state.phase === "running") {
      return state.credentialsSnapshot;
    }
    return null;
  }

  return {
    deploy,
    spawn,
    requestCancel,
    shutdown,
    drain,
    recycle,
    getCredentialsSnapshot,
  };
}

type SupervisorState =
  | { phase: "idle" }
  | { phase: "stopping" }
  | { phase: "stopped" }
  | ({ phase: "starting" } & ActiveState)
  | ({ phase: "running" } & ActiveState);

type ActiveState = {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
  onInferenceEvent: (event: EventPayload) => void;
  mailUnsubscribe: (() => void) | null;
  credentialsSnapshot: CredentialsSnapshot | null;
};

/**
 * Iterate the control-channel receive iterator until the child's
 * `ready` frame arrives. Non-ready frames are processed (currently
 * the only such frame is the future `self`-cancel request the child
 * uses for the Q3 path, which lands with the recycle/drain work).
 */
async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
  _onInferenceEvent: (event: EventPayload) => void,
): Promise<{ childPid: number }> {
  for await (const payload of iter) {
    if (payload.type === "ready") {
      return { childPid: payload.data.childPid };
    }
    // Other payload types on the supervisor's *receive* side are
    // child-initiated control upstream (e.g. a self-cancel request
    // forwarded from the child); the protocol's full upstream
    // vocabulary lands with the recycle/drain commits.
  }
  throw new Error(
    "workflow-host supervisor: control channel ended before child emitted ready",
  );
}

/**
 * Drain the event-channel receive iterator into the host-supplied
 * sink. The function resolves when the iterator ends (child exit or
 * crash callback fired). Any thrown error is logged and surfaced
 * to the supervisor's shutdown path.
 */
async function pumpEvents(
  iter: AsyncGenerator<EventPayload, void, void>,
  onInferenceEvent: (event: EventPayload) => void,
): Promise<void> {
  for await (const event of iter) {
    onInferenceEvent(event);
  }
}

/**
 * Forward an inbound RFC 2822 message to the child as a
 * `trigger.fire` control frame. The supervisor mints a
 * message-receivedAt timestamp at forward time so the child sees
 * monotonically-increasing timestamps across reordered IPC frames.
 */
async function forwardMail(
  sender: ControlChannelSender,
  rawMessage: Uint8Array,
): Promise<void> {
  // The IPC `trigger.fire` payload carries the runId, messageId, and
  // receivedAt. The supervisor parses the RFC 2822 envelope's
  // Message-ID and mints a runId at fire time (one run per trigger
  // fire per discovery Q3.1). For this commit the runId/messageId
  // derivation is a thin pass: the message bytes are surfaced as the
  // `messageId` discriminator; later commits substitute the real
  // RFC 2822 envelope parse when the deploy-mail bridging lands.
  const messageId = await deriveMessageId(rawMessage);
  await sender.send({
    type: "trigger.fire",
    data: {
      runId: messageId,
      messageId,
      receivedAt: Date.now(),
    },
  });
}

/**
 * Derive a stable message identifier from the raw bytes the bus
 * delivered. Hashing the raw message keeps the identifier
 * deterministic across retries without requiring an RFC 2822 parse
 * at this layer.
 */
async function deriveMessageId(rawMessage: Uint8Array): Promise<string> {
  const crypto = await import("node:crypto");
  return crypto.createHash("sha256").update(rawMessage).digest("hex");
}

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}
