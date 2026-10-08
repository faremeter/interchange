// Canonical reactor construction: size-cap transform, authz before-tool
// extension, audit collector flushing at checkpoint and shutdown, and a
// BlobReader over the context store. Consumers should use this instead of
// calling `createReactor` directly so the wiring stays consistent.

import { getLogger } from "@intx/log";
import type { CredentialMaterialResolver } from "@intx/types";
import {
  createBlobReader,
  type BlobReader,
  type AuditStore,
  type BeforeToolExtension,
  type Compactor,
  type ContextStore,
  type ContextTransform,
  type InferenceSource,
  type ReactorDirector,
  type ToolDefinition,
  type ToolResultTransform,
  type ToolRunner,
} from "@intx/types/runtime";

import { createAuditCollector, type AuditCollector } from "./audit-collector";
import {
  createAuthzExtension,
  type AuthzExtensionOptions,
} from "./authz-extension";
import type { CorrelationValidator } from "./correlation";
import type { Dependencies } from "./harness";
import {
  createReactor,
  type Reactor,
  type ReactorConfig,
  type ReactorEmittedEvent,
} from "./reactor";
import { createSizeCapTransform } from "./transforms";

const logger = getLogger(["interchange", "assembly"]);

const DEFAULT_SIZE_CAP_MAX_CHARS = 10_000;

/**
 * Configuration for `createReactorAssembly`. Optional fields toggle the
 * composed extensions: `authorize` adds authz, `auditStore` adds an audit
 * collector, and a default size-cap transform is always prepended. The
 * `contextStore` is passed through unwrapped.
 */
export type ReactorAssemblyConfig = {
  sessionId: string;
  director: ReactorDirector;
  source: InferenceSource;
  /**
   * Fail over `source` to the next entry in the priority-ordered source
   * list, in place, returning false at the end of the list. Omit for a
   * single-source reactor with no failover target.
   */
  failOverToNextSource?: () => boolean;
  /** Reset `source` to the most-preferred source, in place. */
  resetToPreferredSource?: () => void;
  /** Resolves the active source's credential secret at send time. Optional;
   *  the harness defaults fail-closed when omitted. */
  readMaterial?: CredentialMaterialResolver;
  toolRunner: ToolRunner;
  contextStore: ContextStore;
  onEvent: (event: ReactorEmittedEvent) => void;

  authorize?: AuthzExtensionOptions["authorize"];
  /** Tool definitions for the authz approval snapshot; only consumed with
   *  `authorize`. The production edge always supplies the resolved set. */
  toolDefinitions?: readonly ToolDefinition[];
  auditStore?: AuditStore;
  beforeToolExtensions?: BeforeToolExtension[];
  toolResultTransforms?: ToolResultTransform[];
  contextTransforms?: ContextTransform[];
  compactors?: Record<string, Compactor>;
  sizeCapMaxChars?: number;

  afterCheckpoint?: () => Promise<void>;
  onShutdown?: () => Promise<void>;

  deps: Dependencies;
  correlationValidator?: CorrelationValidator;
  inferenceRunner?: ReactorConfig["inferenceRunner"];
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
  doomLoopThreshold?: number | false;
};

/**
 * Output of `createReactorAssembly`. `blobReader` resolves tool-output URIs
 * against the same context store the reactor commits to; `auditCollector` is
 * undefined when no `auditStore` was supplied.
 */
export type ReactorAssembly = {
  reactor: Reactor;
  blobReader: BlobReader;
  auditCollector: AuditCollector | undefined;
};

/**
 * Build the standard reactor wiring (size-cap, authz, audit, blob reader).
 * Direct `createReactor` use is reserved for reactor-internal tests and
 * consumers needing a different composition.
 */
export function createReactorAssembly(
  config: ReactorAssemblyConfig,
): ReactorAssembly {
  const {
    sessionId,
    director,
    source,
    failOverToNextSource,
    resetToPreferredSource,
    readMaterial,
    toolRunner,
    contextStore,
    onEvent,
    authorize,
    toolDefinitions,
    auditStore,
    beforeToolExtensions: callerBeforeToolExtensions,
    toolResultTransforms: callerToolResultTransforms,
    contextTransforms,
    compactors,
    sizeCapMaxChars,
    afterCheckpoint: callerAfterCheckpoint,
    onShutdown: callerOnShutdown,
    deps,
    correlationValidator,
    inferenceRunner,
    gateTimeout,
    shutdownTimeoutMs,
    doomLoopThreshold,
  } = config;

  // Created up-front so the authz extension can route decisions through
  // `onDecision`; without an auditStore, authz runs without recording.
  const auditCollector: AuditCollector | undefined =
    auditStore !== undefined ? createAuditCollector(sessionId) : undefined;

  // Feed the collector tool.start/tool.done events (correlated with authz
  // decisions by callId); message.received goes only to the caller.
  const composedOnEvent =
    auditCollector !== undefined
      ? (event: ReactorEmittedEvent) => {
          if (event.type !== "message.received") {
            auditCollector.onEvent(event);
          }
          onEvent(event);
        }
      : onEvent;

  // Authz runs before caller-supplied extensions; without authz the caller's
  // list passes through unchanged.
  const authzExtension =
    authorize !== undefined
      ? createAuthzExtension({
          authorize,
          ...(auditCollector !== undefined
            ? { onDecision: (d) => auditCollector.onDecision(d) }
            : {}),
          ...(toolDefinitions !== undefined ? { toolDefinitions } : {}),
        })
      : undefined;

  const composedBeforeToolExtensions: BeforeToolExtension[] | undefined =
    authzExtension !== undefined
      ? [authzExtension, ...(callerBeforeToolExtensions ?? [])]
      : callerBeforeToolExtensions;

  // Size-cap is always first so caller transforms see bounded inline content.
  const sizeCapTransform = createSizeCapTransform({
    maxChars: sizeCapMaxChars ?? DEFAULT_SIZE_CAP_MAX_CHARS,
    contextStore,
  });
  const composedToolResultTransforms: ToolResultTransform[] = [
    sizeCapTransform,
    ...(callerToolResultTransforms ?? []),
  ];

  // Flush audit records before the caller's hooks observe the boundary.
  async function flushAudit(): Promise<void> {
    if (auditCollector === undefined || auditStore === undefined) return;
    const records = auditCollector.flush();
    if (records.length > 0) {
      await auditStore.commitAudit(records);
    }
  }

  const composedAfterCheckpoint: (() => Promise<void>) | undefined =
    auditCollector !== undefined
      ? async () => {
          await flushAudit();
          if (callerAfterCheckpoint !== undefined) {
            await callerAfterCheckpoint();
          }
        }
      : callerAfterCheckpoint;

  const composedOnShutdown: (() => Promise<void>) | undefined =
    auditCollector !== undefined
      ? async () => {
          const inflight = auditCollector.pending();
          if (inflight > 0) {
            logger.warn`${inflight} audit records in flight at shutdown, these tool calls will not be recorded`;
          }
          await flushAudit();
          if (callerOnShutdown !== undefined) {
            await callerOnShutdown();
          }
        }
      : callerOnShutdown;

  // exactOptionalPropertyTypes is on: only set optional keys when defined.
  const reactorConfig: ReactorConfig = {
    sessionId,
    director,
    source,
    ...(failOverToNextSource !== undefined ? { failOverToNextSource } : {}),
    ...(resetToPreferredSource !== undefined ? { resetToPreferredSource } : {}),
    ...(readMaterial !== undefined ? { readMaterial } : {}),
    toolRunner,
    contextStore,
    onEvent: composedOnEvent,
    deps,
    toolResultTransforms: composedToolResultTransforms,
    ...(composedBeforeToolExtensions !== undefined
      ? { beforeToolExtensions: composedBeforeToolExtensions }
      : {}),
    ...(contextTransforms !== undefined ? { contextTransforms } : {}),
    ...(compactors !== undefined ? { compactors } : {}),
    ...(composedAfterCheckpoint !== undefined
      ? { afterCheckpoint: composedAfterCheckpoint }
      : {}),
    ...(composedOnShutdown !== undefined
      ? { onShutdown: composedOnShutdown }
      : {}),
    ...(correlationValidator !== undefined ? { correlationValidator } : {}),
    ...(inferenceRunner !== undefined ? { inferenceRunner } : {}),
    ...(gateTimeout !== undefined ? { gateTimeout } : {}),
    ...(shutdownTimeoutMs !== undefined ? { shutdownTimeoutMs } : {}),
    ...(doomLoopThreshold !== undefined ? { doomLoopThreshold } : {}),
  };

  const reactor = createReactor(reactorConfig);
  const blobReader = createBlobReader(contextStore);

  return { reactor, blobReader, auditCollector };
}
