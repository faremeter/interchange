// Reactor assembly helper.
//
// `createReactorAssembly` is the canonical way to construct a reactor when the
// caller wants the standard wiring: a default size-cap tool-result transform,
// authz as a before-tool extension, an audit collector that flushes at
// checkpoint and shutdown boundaries, and a `BlobReader` over the supplied
// context store. Future reactor consumers (the harness today; in-process agent
// runtimes tomorrow) should use this helper rather than calling `createReactor`
// directly so the wiring stays consistent across composition points.

import { getLogger } from "@interchange/log";
import {
  createBlobReader,
  type BlobReader,
  type AuditStore,
  type BeforeToolExtension,
  type Compactor,
  type ContextStore,
  type ContextTransform,
  type ProviderConfig,
  type ReactorDirector,
  type ToolResultTransform,
  type ToolRunner,
} from "@interchange/types/runtime";

import { createAuditCollector, type AuditCollector } from "./audit-collector";
import {
  createAuthzExtension,
  type AuthzExtensionOptions,
} from "./authz-extension";
import type { CorrelationValidator } from "./correlation";
import { createDefaultDependencies, type Dependencies } from "./harness";
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
 * Configuration for `createReactorAssembly`. Required fields mirror
 * `ReactorConfig`. Optional fields toggle the composed extensions: the helper
 * builds an authz before-tool extension when `authorize` is supplied, builds
 * and wires an audit collector when `auditStore` is supplied, and always
 * prepends a default size-cap tool-result transform.
 *
 * The helper does NOT wrap the supplied `contextStore`; callers that need
 * additional behavior (the harness wraps for connector-thread state) layer
 * that on themselves before passing the store in.
 */
export type ReactorAssemblyConfig = {
  sessionId: string;
  director: ReactorDirector;
  providerConfig: ProviderConfig;
  toolRunner: ToolRunner;
  contextStore: ContextStore;
  onEvent: (event: ReactorEmittedEvent) => void;

  authorize?: AuthzExtensionOptions["authorize"];
  auditStore?: AuditStore;
  beforeToolExtensions?: BeforeToolExtension[];
  toolResultTransforms?: ToolResultTransform[];
  contextTransforms?: ContextTransform[];
  compactors?: Record<string, Compactor>;
  sizeCapMaxChars?: number;

  afterCheckpoint?: () => Promise<void>;
  onShutdown?: () => Promise<void>;

  deps?: Dependencies;
  correlationValidator?: CorrelationValidator;
  inferenceRunner?: ReactorConfig["inferenceRunner"];
  gateTimeout?: number;
  shutdownTimeoutMs?: number;
};

/**
 * Output of `createReactorAssembly`. The `reactor` is started by the caller as
 * usual. `blobReader` is exposed so the caller can pass it to tool factories
 * that resolve `tool-output:///{callId}` URIs against the same context store
 * the reactor commits to. `auditCollector` is `undefined` when no `auditStore`
 * was supplied.
 */
export type ReactorAssembly = {
  reactor: Reactor;
  blobReader: BlobReader;
  auditCollector: AuditCollector | undefined;
};

/**
 * Build the standard reactor wiring. This is the canonical reactor-assembly
 * path: any consumer that needs the default size-cap transform, authz, audit
 * collection, and blob reader should call this helper instead of constructing
 * a `ReactorConfig` by hand. Direct `createReactor` use is reserved for
 * reactor-internal tests and any future consumer that genuinely needs a
 * different composition.
 */
export function createReactorAssembly(
  config: ReactorAssemblyConfig,
): ReactorAssembly {
  const {
    sessionId,
    director,
    providerConfig,
    toolRunner,
    contextStore,
    onEvent,
    authorize,
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
  } = config;

  // Audit collector is created up-front so the authz extension can route its
  // decisions through `onDecision`. When no auditStore is supplied, no
  // collector is created and authz runs without decision recording.
  const auditCollector: AuditCollector | undefined =
    auditStore !== undefined ? createAuditCollector(sessionId) : undefined;

  // When an audit collector is present, intercept the reactor's event stream
  // to feed it tool.start / tool.done events (the collector correlates these
  // with authz decisions by callId). message.received is reactor-internal and
  // is forwarded to the caller but not to the collector. Without a collector,
  // the caller's onEvent is used directly.
  const composedOnEvent =
    auditCollector !== undefined
      ? (event: ReactorEmittedEvent) => {
          if (event.type !== "message.received") {
            auditCollector.onEvent(event);
          }
          onEvent(event);
        }
      : onEvent;

  // Authz is composed in front of any caller-supplied before-tool extensions
  // so policy enforcement runs first. Without authz, the caller's list (if
  // any) is passed through unchanged.
  const authzExtension =
    authorize !== undefined
      ? createAuthzExtension({
          authorize,
          ...(auditCollector !== undefined
            ? { onDecision: (d) => auditCollector.onDecision(d) }
            : {}),
        })
      : undefined;

  const composedBeforeToolExtensions: BeforeToolExtension[] | undefined =
    authzExtension !== undefined
      ? [authzExtension, ...(callerBeforeToolExtensions ?? [])]
      : callerBeforeToolExtensions;

  // The size-cap transform is always prepended so oversized payloads spill
  // before any caller transform sees them. Caller transforms run after and
  // can rely on the inline content already being bounded.
  const sizeCapTransform = createSizeCapTransform({
    maxChars: sizeCapMaxChars ?? DEFAULT_SIZE_CAP_MAX_CHARS,
    contextStore,
  });
  const composedToolResultTransforms: ToolResultTransform[] = [
    sizeCapTransform,
    ...(callerToolResultTransforms ?? []),
  ];

  // Audit flush wraps the caller's lifecycle hooks: the helper's flush runs
  // first so the records produced by the just-completed cycle are persisted
  // before the caller's hook observes the checkpoint or shutdown boundary.
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

  // ReactorConfig.deps is required; default to createDefaultDependencies()
  // when the caller did not supply one so we never pass `undefined`.
  const resolvedDeps: Dependencies = deps ?? createDefaultDependencies();

  // exactOptionalPropertyTypes is on: only set optional keys when defined.
  const reactorConfig: ReactorConfig = {
    sessionId,
    director,
    providerConfig,
    toolRunner,
    contextStore,
    onEvent: composedOnEvent,
    deps: resolvedDeps,
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
  };

  const reactor = createReactor(reactorConfig);
  const blobReader = createBlobReader(contextStore);

  return { reactor, blobReader, auditCollector };
}
